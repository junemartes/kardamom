//! Multi-version state cache: for each address, and each
//! (address, slot) pair, a small version list `(tx_index, value)`. A read
//! at index i sees the highest write below i, or else the block-input
//! view. This is the same layering `ExecScope`'s commit cache uses
//! sequentially, made concurrent.
//!
//! Under pessimistic scheduling, a DAG edge orders any two transactions
//! that touch the same cell, so version lists are effectively written in
//! index order and readers never race their own predecessors. The lists
//! still use a sorted insert, and reads still record `(cell,
//! version-seen)`. The validation pass replays those records against the
//! final lists. This catches a prediction miss (false independence) and
//! triggers the sequential-fallback invariant.
//!
//! The fee sink is not published here (the `Accumulator` boundary): every
//! worker reads its block-start value, and the commit pass computes the
//! exact prefix sums instead.

use std::sync::RwLock;

use crate::FastMap;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_exec_core::delta::WriteSet;

/// One published account version: the (nonce, balance, `code_hash`) tuple a
/// `WriteSet` carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountVersion {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
}

/// What a read observed: the publishing transaction's index, or `None` for
/// the block-input view. Recorded per read, replayed at validation.
pub type SeenVersion = Option<u32>;

/// A recorded read for validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRecord {
    Account(Address, SeenVersion),
    Slot(Address, B256, SeenVersion),
    /// Bytecode lookup. Content-addressed, so there is no version. A miss
    /// that a later CREATE fills is real staleness: the reader executed
    /// against absent code. `true` means the cache served the read.
    Code(B256, bool),
}

/// Shard count. Do not widen this without a new measurement.
const SHARDS: usize = 64;

#[allow(
    clippy::cast_possible_truncation,
    reason = "h % SHARDS is < SHARDS, a small usize constant, so it always fits back in usize"
)]
fn shard_of(bytes: &[u8]) -> usize {
    // Addresses and slot keys are high-entropy in their low bytes (they
    // are hashes or counters). Fold the last 8 bytes; they carry the
    // entropy.
    let h = bytes
        .iter()
        .rev()
        .take(8)
        .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3)
        });
    (h % SHARDS as u64) as usize
}

/// A cell's version list: `(tx_index, value)`, sorted by index.
type Versions<V> = Vec<(u32, V)>;
type Shard<K, V> = RwLock<FastMap<K, Versions<V>>>;

/// Sharded cell storage: one `RwLock<FastMap<K, Versions<V>>>` per
/// shard, published to and read from by `shard_of`'s index. Shared by
/// `MvCache`'s account and storage tables, which only differ in `K`
/// and `V`.
struct Shards<K, V>(Vec<Shard<K, V>>);

impl<K: Eq + std::hash::Hash + Copy, V: Copy> Shards<K, V> {
    fn new(n: usize) -> Self {
        Self(crate::shard_vec(n))
    }

    /// Sorted-insert publish: keeps results correct even if a
    /// prediction miss let writers race out of index order —
    /// validation still catches the miss.
    ///
    /// # Panics
    /// Panics if `shard`'s lock is poisoned (a worker thread panicked
    /// while holding it).
    fn publish(&self, shard: usize, key: K, idx: u32, v: V) {
        let mut g = self.0[shard].write().expect("mv poisoned");
        let list = g.entry(key).or_default();
        match list.binary_search_by_key(&idx, |(i, _)| *i) {
            Ok(p) => list[p] = (idx, v),
            Err(p) => list.insert(p, (idx, v)),
        }
    }

    /// Highest version strictly below `idx`.
    ///
    /// # Panics
    /// Panics if `shard`'s lock is poisoned (a worker thread panicked
    /// while holding it).
    fn read(&self, shard: usize, key: &K, idx: u32) -> Option<(u32, V)> {
        let g = self.0[shard].read().expect("mv poisoned");
        let list = g.get(key)?;
        let p = list.partition_point(|(i, _)| *i < idx);
        (p > 0).then(|| list[p - 1])
    }

    /// Visit every cell's latest version across all shards: `sink` gets
    /// the key and the top (highest tx index) version, or nothing for
    /// a cell with an empty version list.
    fn fold_last_version(&self, mut sink: impl FnMut(K, &V)) {
        for sh in &self.0 {
            Self::fold_shard_last_version(sh, &mut sink);
        }
    }

    /// Visit one shard's cells. The `for` loop in
    /// [`Self::fold_last_version`] stays free of a branch.
    fn fold_shard_last_version(sh: &Shard<K, V>, sink: &mut impl FnMut(K, &V)) {
        let g = sh.read().expect("mv poisoned");
        for (k, list) in g.iter() {
            Self::sink_last_version(*k, list, sink);
        }
    }

    /// Visit one cell's latest version, or nothing for an empty version
    /// list. The `for` loop in [`Self::fold_shard_last_version`] stays
    /// free of a branch.
    fn sink_last_version(k: K, list: &Versions<V>, sink: &mut impl FnMut(K, &V)) {
        let Some((_, v)) = list.last() else {
            return;
        };
        sink(k, v);
    }

    /// Scrub every shard in place: past `keep_keys_cap` entries, drop
    /// the whole map; otherwise keep the keys and clear each entry's
    /// version vec, so the next block's publishes re-fill a warm
    /// buffer.
    fn scrub(&self, keep_keys_cap: usize) {
        for sh in &self.0 {
            Self::scrub_shard(sh, keep_keys_cap);
        }
    }

    /// Scrub one shard in place: past `keep_keys_cap` entries, drop the
    /// whole map; otherwise keep the keys and clear each entry's version
    /// vec. The `for` loop in [`Self::scrub`] stays free of a branch.
    fn scrub_shard(sh: &Shard<K, V>, keep_keys_cap: usize) {
        let mut g = sh.write().expect("mv poisoned");
        if g.len() > keep_keys_cap {
            g.clear();
            return;
        }
        for v in g.values_mut() {
            v.clear();
        }
    }
}

/// Sharded multi-version store. Version lists are kept sorted by tx index
/// via binary-search insert (append in the common pessimistic case).
pub struct MvCache {
    accounts: Shards<Address, AccountVersion>,
    storage: Shards<(Address, B256), U256>,
    /// Content-addressed CREATE bytecode. No versioning is needed, since
    /// a hash is its own content. Append-only.
    code: RwLock<FastMap<B256, Bytes>>,
}

impl Default for MvCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MvCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            accounts: Shards::new(SHARDS),
            storage: Shards::new(SHARDS),
            code: RwLock::new(FastMap::with_hasher(crate::FnvBuild)),
        }
    }

    /// Publish one transaction's writes in the only safe order: code and
    /// storage first, accounts last.
    ///
    /// A reader reaches a contract's code and storage only through its
    /// account (revm loads `basic`, then `code_by_hash`, then `SLOAD`).
    /// Publishing the account version last gives a happens-before order:
    /// whoever sees the new account also finds its code and storage
    /// already there. The reverse order let a concurrent reader load a
    /// freshly created account with `code_hash = H`, miss `H` in the
    /// cache, fall back to the snapshot, and execute against empty code.
    /// This was a silent divergence that read validation could not see,
    /// because the account read was legitimately current and code reads
    /// carry no version. `skip_account` is the fee sink, which this
    /// method never publishes (the `Accumulator` boundary).
    pub fn publish_write_set(&self, idx: u32, ws: &WriteSet, skip_account: Address) {
        for (hash, code) in &ws.code {
            self.publish_code(*hash, Bytes::clone(code));
        }
        for ((addr, key), value) in &ws.storage {
            self.publish_slot(idx, *addr, *key, *value);
        }
        for (addr, fields) in &ws.accounts {
            self.publish_account_unless_skipped(
                idx,
                *addr,
                AccountVersion {
                    nonce: fields.nonce,
                    balance: fields.balance,
                    code_hash: fields.code_hash,
                },
                skip_account,
            );
        }
    }

    /// Publish one account version, unless it is the skipped fee sink.
    /// The `for` loop in [`Self::publish_write_set`] stays free of a
    /// branch.
    fn publish_account_unless_skipped(
        &self,
        idx: u32,
        addr: Address,
        v: AccountVersion,
        skip_account: Address,
    ) {
        if addr == skip_account {
            return;
        }
        self.publish_account(idx, addr, v);
    }

    /// Publish one transaction's account write. The sorted insert keeps
    /// results correct even if a prediction miss let writers race out of
    /// index order — validation still catches the miss.
    ///
    /// # Panics
    /// Panics if this shard's lock is poisoned (a worker thread
    /// panicked while holding it).
    pub fn publish_account(&self, idx: u32, addr: Address, v: AccountVersion) {
        self.accounts
            .publish(shard_of(addr.as_slice()), addr, idx, v);
    }

    /// Publish one transaction's storage write. See [`Self::publish_account`].
    ///
    /// # Panics
    /// Panics if this shard's lock is poisoned (a worker thread
    /// panicked while holding it).
    pub(crate) fn publish_slot(&self, idx: u32, addr: Address, key: B256, value: U256) {
        self.storage
            .publish(shard_of(addr.as_slice()), (addr, key), idx, value);
    }

    /// Publish `CREATE`d bytecode, content-addressed. First write wins.
    ///
    /// # Panics
    /// Panics if the code table's lock is poisoned (a worker thread
    /// panicked while holding it).
    pub(crate) fn publish_code(&self, hash: B256, code: Bytes) {
        self.code
            .write()
            .expect("mv poisoned")
            .entry(hash)
            .or_insert(code);
    }

    /// Highest account version strictly below `idx`.
    ///
    /// # Panics
    /// Panics if this shard's lock is poisoned (a worker thread
    /// panicked while holding it).
    pub fn read_account(&self, idx: u32, addr: &Address) -> Option<(u32, AccountVersion)> {
        self.accounts.read(shard_of(addr.as_slice()), addr, idx)
    }

    /// Highest slot version strictly below `idx`.
    ///
    /// # Panics
    /// Panics if this shard's lock is poisoned (a worker thread
    /// panicked while holding it).
    pub fn read_slot(&self, idx: u32, addr: &Address, key: &B256) -> Option<(u32, U256)> {
        self.storage
            .read(shard_of(addr.as_slice()), &(*addr, *key), idx)
    }

    /// Between-block scrub for pooled reuse: drop every entry but keep
    /// every allocation. Shard tables and version-vec buffers lose their
    /// entries, but the maps keep their capacity, so the next block's
    /// publishes re-fill warm pages instead of mapping fresh ones. The
    /// caller must guarantee quiescence (the reaper scrubs only unwrapped
    /// caches).
    pub(crate) fn scrub(&self) {
        // Keep the keys and their version-vec buffers. Hot cells recur
        // block after block, so an entry with a cleared vec lets the
        // next block's publish push into a warm buffer instead of
        // allocating a new one (a cell with no versions reads and
        // validates exactly like an absent cell). A drifting key set
        // would grow the maps without bound, so past the size cap this
        // falls back to a full clear.
        const KEEP_KEYS_CAP: usize = 1024; // per shard; about 64 shards
        self.accounts.scrub(KEEP_KEYS_CAP);
        self.storage.scrub(KEEP_KEYS_CAP);
        self.code.write().expect("mv poisoned").clear();
    }

    /// Compute the block's final write view: for each cell, the highest
    /// version (the last writer's value, exactly what the commit fold
    /// computes), plus all `CREATEd` code. The repair path uses this to
    /// turn a predecessor's mv layer into a mergeable delta. It costs a
    /// full fold and only runs on the rare paths that need a
    /// `PendingDelta` shape instead of probing the cache directly.
    pub(crate) fn final_delta(&self) -> kardamom_exec_core::delta::PendingDelta {
        let mut d = kardamom_exec_core::delta::PendingDelta::new();
        self.accounts.fold_last_version(|addr, v: &AccountVersion| {
            d.accounts.insert(
                addr,
                kardamom_exec_core::delta::AccountFields {
                    nonce: v.nonce,
                    balance: v.balance,
                    code_hash: v.code_hash,
                },
            );
        });
        self.storage.fold_last_version(|key, v: &U256| {
            d.storage.insert(key, *v);
        });
        for (h, b) in self.code.read().expect("mv poisoned").iter() {
            d.code.insert(*h, b.clone());
        }
        d
    }

    pub(crate) fn read_code(&self, hash: &B256) -> Option<Bytes> {
        self.code.read().expect("mv poisoned").get(hash).cloned()
    }

    fn has_code(&self, hash: &B256) -> bool {
        self.code.read().expect("mv poisoned").contains_key(hash)
    }

    /// Replay one read record against the final lists: does the version the
    /// transaction observed still equal the highest version below it? A
    /// mismatch means a lower-index transaction published after the read
    /// — false independence.
    pub fn validate(&self, idx: u32, r: &ReadRecord) -> bool {
        match r {
            ReadRecord::Account(addr, seen) => {
                self.read_account(idx, addr).map(|(i, _)| i) == *seen
            }
            ReadRecord::Slot(addr, key, seen) => {
                self.read_slot(idx, addr, key).map(|(i, _)| i) == *seen
            }
            // A miss the cache can now serve means the reader ran
            // against code that a concurrent CREATE had not published yet.
            ReadRecord::Code(hash, hit) => *hit || !self.has_code(hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn av(balance: u64) -> AccountVersion {
        AccountVersion {
            nonce: 0,
            balance: U256::from(balance),
            code_hash: B256::ZERO,
        }
    }

    #[test]
    fn read_sees_highest_below_index() {
        let mv = MvCache::new();
        let a = Address::with_last_byte(1);
        mv.publish_account(2, a, av(20));
        mv.publish_account(5, a, av(50));
        assert_eq!(mv.read_account(1, &a), None);
        assert_eq!(mv.read_account(3, &a).unwrap(), (2, av(20)));
        assert_eq!(
            mv.read_account(5, &a).unwrap(),
            (2, av(20)),
            "strictly below"
        );
        assert_eq!(mv.read_account(9, &a).unwrap(), (5, av(50)));
    }

    #[test]
    fn out_of_order_publish_stays_sorted() {
        let mv = MvCache::new();
        let a = Address::with_last_byte(2);
        let k = B256::with_last_byte(7);
        mv.publish_slot(9, a, k, U256::from(90u64));
        mv.publish_slot(3, a, k, U256::from(30u64));
        assert_eq!(mv.read_slot(5, &a, &k).unwrap(), (3, U256::from(30u64)));
        assert_eq!(mv.read_slot(10, &a, &k).unwrap(), (9, U256::from(90u64)));
    }

    /// Regression test for a silent divergence: a reader that can see a
    /// freshly created account must also be able to see its code.
    /// Publishing accounts before code let a concurrent transaction load
    /// the account, miss the hash, fall back to the snapshot, and execute
    /// against empty code. Validation could not catch this, because the
    /// account read was current and code reads carry no version. The
    /// ordered publish is the fix; this test hammers the window a wrong
    /// order would open.
    #[test]
    fn account_version_never_precedes_its_code() {
        use kardamom_exec_core::delta::WriteSet;
        let mv = std::sync::Arc::new(MvCache::new());
        let created = Address::with_last_byte(0xC1);
        let code = Bytes::from_static(&[0x60, 0x00, 0x54, 0x00]);
        let hash = alloy_primitives::keccak256(&code);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader = {
            let mv = mv.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut observed = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    observed += poll_created_account_visible(&mv, created);
                }
                observed
            })
        };

        // Publish the same CREATE write set repeatedly into fresh caches
        // so the reader keeps racing the window.
        for _ in 0..2_000 {
            let mut ws = WriteSet::default();
            ws.accounts.push((
                created,
                kardamom_exec_core::delta::AccountFields {
                    nonce: 1,
                    balance: U256::ZERO,
                    code_hash: hash,
                },
            ));
            ws.code.push((hash, code.clone()));
            ws.finish();
            mv.publish_write_set(0, &ws, Address::repeat_byte(0xEE));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().expect("reader must not panic");
    }

    /// Poll once for the created account. Returns 1 if observed, after
    /// asserting its code is visible too; else 0. The `while` loop in
    /// [`account_version_never_precedes_its_code`] stays free of a
    /// branch.
    fn poll_created_account_visible(mv: &MvCache, created: Address) -> u64 {
        let Some((_, a)) = mv.read_account(1, &created) else {
            return 0;
        };
        assert!(
            mv.read_code(&a.code_hash).is_some(),
            "account visible with code_hash {:?} but its code is not",
            a.code_hash
        );
        1
    }

    #[test]
    fn code_miss_is_convicted_when_a_create_fills_it() {
        let mv = MvCache::new();
        let hash = B256::with_last_byte(9);
        // Served from the base layer (cache miss) — valid while the cache
        // stays empty.
        let rec = ReadRecord::Code(hash, false);
        assert!(mv.validate(3, &rec));
        // A concurrent CREATE published it: the reader ran against absent
        // code and must be wounded.
        mv.publish_code(hash, Bytes::from_static(&[0x00]));
        assert!(!mv.validate(3, &rec));
        // A read that hit the cache is never stale.
        assert!(mv.validate(3, &ReadRecord::Code(hash, true)));
    }

    #[test]
    fn validation_convicts_a_late_lower_write() {
        let mv = MvCache::new();
        let a = Address::with_last_byte(3);
        // Tx 6 read the block-input view (None) — then tx 4 published.
        let r = ReadRecord::Account(a, None);
        assert!(mv.validate(6, &r));
        mv.publish_account(4, a, av(40));
        assert!(
            !mv.validate(6, &r),
            "a lower-index write invalidates the read"
        );
        // A read that did see tx 4 validates.
        assert!(mv.validate(6, &ReadRecord::Account(a, Some(4))));
    }
}
