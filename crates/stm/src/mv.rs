//! Multi-version state cache (spec §P2): per (address) and (address, slot),
//! a small version list `(tx_index, value)`. A read at index i sees the
//! highest write BELOW i, else the block-input view — the same layering
//! `ExecScope`'s commit cache encodes sequentially, made concurrent.
//!
//! Under pessimistic scheduling, two txs touching the same cell are ordered
//! by a DAG edge, so version lists are effectively written in index order
//! and readers never race their own predecessors. The lists still
//! sorted-insert and reads still record `(cell, version-seen)` — the
//! validation pass replays those records against the final lists, which is
//! what catches a prediction miss (false independence) and triggers the
//! sequential-fallback invariant.
//!
//! The fee sink is NOT published here (the `Accumulator` boundary): every
//! worker reads its block-start value, and the commit pass materializes the
//! exact prefix sums instead.

use std::sync::RwLock;

use crate::FastMap;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_exec_core::delta::WriteSet;

/// One published account version: the (nonce, balance, code_hash) tuple a
/// `WriteSet` carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountVersion {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
}

/// What a read observed: the publishing tx's index, or `None` for the
/// block-input view. Recorded per read, replayed at validation.
pub type SeenVersion = Option<u32>;

/// A recorded read for validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRecord {
    Account(Address, SeenVersion),
    Slot(Address, B256, SeenVersion),
    /// Bytecode lookup. Content-addressed, so there is no version — but a
    /// MISS that a later CREATE fills is a real staleness: the reader
    /// executed against absent code. `true` = served from the cache.
    Code(B256, bool),
}

/// Shard count. Widening this to 1024 was tried and REVERTED: the theory
/// was that ~180 live cells over 64 shards made workers bounce each
/// other's lock lines, but it measured neutral (78ms vs 76ms), so the
/// contention is not shard collisions and the extra 64KB/block bought
/// nothing.
const SHARDS: usize = 64;

fn shard_of(bytes: &[u8]) -> usize {
    // Addresses and slot keys are high-entropy in their LOW bytes (they
    // are hashes or counters), and folding only the first 8 was clustering
    // structured addresses. Fold the tail instead.
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in bytes.iter().rev().take(8) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    (h % SHARDS as u64) as usize
}

/// A cell's version list: `(tx_index, value)`, sorted by index.
type Versions<V> = Vec<(u32, V)>;
type Shard<K, V> = RwLock<FastMap<K, Versions<V>>>;

/// Sharded multi-version store. Version lists are kept sorted by tx index
/// via binary-search insert (append in the common pessimistic case).
pub struct MvCache {
    accounts: Vec<Shard<Address, AccountVersion>>,
    storage: Vec<Shard<(Address, B256), U256>>,
    /// Content-addressed CREATE bytecode — no versioning (a hash IS its
    /// content), append-only.
    code: RwLock<FastMap<B256, Bytes>>,
}

impl Default for MvCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MvCache {
    pub fn new() -> Self {
        Self {
            accounts: (0..SHARDS)
                .map(|_| RwLock::new(FastMap::with_hasher(crate::FnvBuild)))
                .collect(),
            storage: (0..SHARDS)
                .map(|_| RwLock::new(FastMap::with_hasher(crate::FnvBuild)))
                .collect(),
            code: RwLock::new(FastMap::with_hasher(crate::FnvBuild)),
        }
    }

    /// Publish one tx's writes in the ONLY safe order: code and storage
    /// first, ACCOUNTS LAST.
    ///
    /// A reader reaches a contract's code and storage only THROUGH its
    /// account (revm loads `basic` → `code_by_hash` → `SLOAD`), so making
    /// the account version the last thing published gives a
    /// happens-before: whoever sees the new account finds its code and
    /// storage already there. The reverse order (accounts first) let a
    /// concurrent reader load a freshly-CREATEd account carrying
    /// `code_hash = H`, miss `H` in the cache, fall back to the snapshot,
    /// and execute against EMPTY code — a silent divergence that read
    /// validation cannot see (its account read was legitimately current,
    /// and code reads carry no version). `skip_account` is the fee sink
    /// (Accumulator: never published).
    pub fn publish_write_set(&self, idx: u32, ws: &WriteSet, skip_account: Address) {
        for (hash, code) in ws.code.iter() {
            self.publish_code(*hash, Bytes::clone(code));
        }
        for ((addr, key), value) in ws.storage.iter() {
            self.publish_slot(idx, *addr, *key, *value);
        }
        for (addr, (nonce, balance, code_hash)) in ws.accounts.iter() {
            if *addr == skip_account {
                continue;
            }
            self.publish_account(
                idx,
                *addr,
                AccountVersion {
                    nonce: *nonce,
                    balance: *balance,
                    code_hash: *code_hash,
                },
            );
        }
    }

    /// Publish one tx's account write. Sorted-insert keeps correctness
    /// even if a prediction miss let writers race out of index order
    /// (validation still convicts the miss).
    pub fn publish_account(&self, idx: u32, addr: Address, v: AccountVersion) {
        let mut g = self.accounts[shard_of(addr.as_slice())]
            .write()
            .expect("mv poisoned");
        let list = g.entry(addr).or_default();
        match list.binary_search_by_key(&idx, |(i, _)| *i) {
            Ok(p) => list[p] = (idx, v),
            Err(p) => list.insert(p, (idx, v)),
        }
    }

    pub fn publish_slot(&self, idx: u32, addr: Address, key: B256, value: U256) {
        let mut g = self.storage[shard_of(addr.as_slice())]
            .write()
            .expect("mv poisoned");
        let list = g.entry((addr, key)).or_default();
        match list.binary_search_by_key(&idx, |(i, _)| *i) {
            Ok(p) => list[p] = (idx, value),
            Err(p) => list.insert(p, (idx, value)),
        }
    }

    pub fn publish_code(&self, hash: B256, code: Bytes) {
        self.code
            .write()
            .expect("mv poisoned")
            .entry(hash)
            .or_insert(code);
    }

    /// Highest account version strictly below `idx`.
    pub fn read_account(&self, idx: u32, addr: &Address) -> Option<(u32, AccountVersion)> {
        let g = self.accounts[shard_of(addr.as_slice())]
            .read()
            .expect("mv poisoned");
        let list = g.get(addr)?;
        let p = list.partition_point(|(i, _)| *i < idx);
        (p > 0).then(|| list[p - 1])
    }

    /// Highest slot version strictly below `idx`.
    pub fn read_slot(&self, idx: u32, addr: &Address, key: &B256) -> Option<(u32, U256)> {
        let g = self.storage[shard_of(addr.as_slice())]
            .read()
            .expect("mv poisoned");
        let list = g.get(&(*addr, *key))?;
        let p = list.partition_point(|(i, _)| *i < idx);
        (p > 0).then(|| list[p - 1])
    }

    pub fn read_code(&self, hash: &B256) -> Option<Bytes> {
        self.code.read().expect("mv poisoned").get(hash).cloned()
    }

    fn has_code(&self, hash: &B256) -> bool {
        self.code.read().expect("mv poisoned").contains_key(hash)
    }

    /// Replay one read record against the final lists: does the version the
    /// tx observed still equal the highest version below it? A mismatch
    /// means a lower-index tx published AFTER the read — false independence.
    pub fn validate(&self, idx: u32, r: &ReadRecord) -> bool {
        match r {
            ReadRecord::Account(addr, seen) => {
                self.read_account(idx, addr).map(|(i, _)| i) == *seen
            }
            ReadRecord::Slot(addr, key, seen) => {
                self.read_slot(idx, addr, key).map(|(i, _)| i) == *seen
            }
            // A miss that the cache can now serve means the reader ran
            // against code a concurrent CREATE had not published yet.
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

    /// REGRESSION (silent divergence): a reader that can see a
    /// freshly-CREATEd account MUST be able to see its code. Publishing
    /// accounts before code let a concurrent tx load the account, miss the
    /// hash, fall back to the snapshot, and execute against EMPTY code —
    /// and validation could not catch it (the account read was current;
    /// code reads carry no version). The ordered publish is the fix; this
    /// hammers the window a wrong order would open.
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
                    if let Some((_, a)) = mv.read_account(1, &created) {
                        assert!(
                            mv.read_code(&a.code_hash).is_some(),
                            "account visible with code_hash {:?} but its code is not",
                            a.code_hash
                        );
                        observed += 1;
                    }
                }
                observed
            })
        };

        // Publish the same CREATE write set repeatedly into fresh caches
        // so the reader keeps racing the window.
        for _ in 0..2_000 {
            let mut ws = WriteSet::default();
            ws.accounts.push((created, (1, U256::ZERO, hash)));
            ws.code.push((hash, code.clone()));
            ws.finish();
            mv.publish_write_set(0, &ws, Address::repeat_byte(0xEE));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().expect("reader must not panic");
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
        // A read that HIT the cache is never stale.
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
        // A read that DID see tx 4 validates.
        assert!(mv.validate(6, &ReadRecord::Account(a, Some(4))));
    }
}
