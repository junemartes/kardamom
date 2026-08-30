//! Per-tx write-set accumulation and deterministic hashing.
//!
//! `WriteSet` is the per-tx unit. `kardamom_types::BlockDelta` is the
//! per-block accumulator that the state writer consumes. The key invariant
//! is that `WriteSet::hash()` gives the same value on every executor
//! replica, for any given tx.
//!
//! This module computes no state root. The executor never emits a
//! state-root commitment. Per-tx `write_set_hash` is the only determinism
//! witness here; block-level attestation is a future validator concern.
//!
//! Selfdestruct semantics are out of scope for version 0. The EVM
//! hardforks this executor targets do not produce selfdestruct effects on
//! existing chains (EIP-6780 reduced its visible effect to a balance
//! transfer within the same tx). Add a `destroyed` flag when the runtime
//! needs it.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::delta::CodeEntry;
use kardamom_types::{AccountChange, BlockDelta, StorageChange};

/// One transaction's write effects, as sorted small vectors.
///
/// This used to store three `BTreeMap`s. A B-tree allocates a 1KB-class
/// leaf node per map, even for a few entries. DHAT measured this at about
/// 2.7KB of the roughly 5.4KB/tx execution-path total. A typical tx writes
/// 2 to 4 accounts, 0 to 10 slots, and 0 or 1 code entries, so inline
/// `SmallVec`s make the common case allocation-free. Canonical iteration
/// (the hash contract) comes from sorting on build, not from tree order.
/// Builders push entries, then call [`WriteSet::finish`]. [`WriteSet::hash`]
/// debug-asserts that the set is sorted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[allow(clippy::type_complexity)]
pub struct WriteSet {
    /// (address, (nonce, balance, code_hash)), sorted by address.
    pub accounts: smallvec::SmallVec<[(Address, (u64, U256, B256)); 3]>,
    /// ((address, slot_key), value), sorted by key.
    pub storage: smallvec::SmallVec<[((Address, B256), U256); 8]>,
    /// (code_hash, bytecode), sorted by hash.
    pub code: smallvec::SmallVec<[(B256, Bytes); 1]>,
}

impl WriteSet {
    /// Sort into canonical order. Builders push entries in revm's
    /// nondeterministic HashMap iteration order. Call this before the set
    /// is hashed, applied, or compared. Keys are unique by construction
    /// (one entry per account or slot per tx), so an unstable sort is safe.
    pub fn finish(&mut self) {
        self.accounts.sort_unstable_by_key(|(a, _)| *a);
        self.storage.sort_unstable_by_key(|(k, _)| *k);
        self.code.sort_unstable_by_key(|(h, _)| *h);
        debug_assert!(self.accounts.windows(2).all(|w| w[0].0 < w[1].0));
        debug_assert!(self.storage.windows(2).all(|w| w[0].0 < w[1].0));
    }

    /// Look up an account by key. This is a linear search, which is safe
    /// because the set is tiny, and works before `finish` runs.
    pub fn account(&self, addr: &Address) -> Option<&(u64, U256, B256)> {
        self.accounts
            .iter()
            .find(|(a, _)| a == addr)
            .map(|(_, v)| v)
    }

    /// Deterministic keccak256 hash of the write set.
    ///
    /// See [`WriteSet::encode`] for the exact byte layout. Encoding uses an
    /// explicit width and endianness for every field, so two replicas on
    /// different architectures produce identical bytes.
    pub fn hash(&self) -> B256 {
        // The byte sequence is the consensus contract. How it reaches the
        // hash sponge is not. One encoder ([`WriteSet::encode`]) feeds two
        // sinks: a stack buffer for the common case, and the sponge itself
        // for a write set too large for the buffer (for example, a CREATE
        // that carries bytecode). So the two paths cannot drift apart.
        debug_assert!(
            self.accounts.windows(2).all(|w| w[0].0 < w[1].0)
                && self.storage.windows(2).all(|w| w[0].0 < w[1].0),
            "WriteSet::finish() not called before hash()"
        );
        let mut h = alloy_primitives::Keccak256::new();
        const INLINE: usize = 1024;
        // Worst case per entry, with every field at full width.
        let need = 1
            + 10
            + self.accounts.len() * (20 + 1 + 10 + 32 + 32)
            + 10
            + self.storage.len() * (1 + 20 + 32 + 32)
            + 10;
        if self.code.is_empty() && need <= INLINE {
            let mut sink = BufSink {
                buf: [0u8; INLINE],
                n: 0,
            };
            self.encode(&mut sink);
            h.update(&sink.buf[..sink.n]);
        } else {
            self.encode(&mut h);
        }
        h.finalize()
    }

    /// The consensus encoding (v2, compact). This encoding is canonical
    /// and injective. Every integer is minimal-width, with its width
    /// carried. The entry order is the sorted order that `finish()`
    /// establishes. So one write set has exactly one encoding, and one
    /// encoding describes exactly one write set.
    ///
    /// ```text
    /// u8   version (0x02)
    /// var  n_accounts
    ///   per account, sorted by address:
    ///     [20] address
    ///     u8   flags: bits0-5 = balance byte length (0..=32)
    ///                 bits6-7 = 0 KECCAK_EMPTY, 1 ZERO, 2 explicit
    ///     var  nonce
    ///     [..] balance, big-endian, leading zeros stripped
    ///     [32] code_hash            (only when flags bits6-7 == 2)
    /// var  n_storage
    ///   per slot, sorted by (address, key):
    ///     u8   flags: bits0-5 = value byte length (0..=32)
    ///                 bit6    = address equals the previous entry's
    ///     [20] address              (only when bit6 == 0)
    ///     [32] key
    ///     [..] value, big-endian, leading zeros stripped
    /// var  n_code
    ///   per entry, sorted by hash: [32] hash, var len, [len] bytes
    /// ```
    ///
    /// Why the compaction: both engines pay for this hash, since it is
    /// the per-tx determinism witness. Measurements show 1.1µs/tx, about
    /// 22% of a sequential transfer block, and about 70% of the STM
    /// commit tail's work. The cost comes from Keccak-f permutations
    /// (about 290ns each), so the only real lever is byte count. The
    /// previous fixed-width encoding put a 3-account transfer at 297
    /// bytes (3 permutations). Minimal-width balances, varint nonces,
    /// and a code-hash tag put it near 100 bytes (one permutation). The
    /// witness's meaning does not change: same inputs, same coverage,
    /// fewer bytes.
    fn encode<S: WsSink>(&self, s: &mut S) {
        s.put(&[WS_ENCODING_V2]);
        put_varint(s, self.accounts.len() as u64);
        for (addr, (nonce, balance, code_hash)) in &self.accounts {
            let (bal, blen) = minimal_be(balance);
            let code_tag: u8 = if *code_hash == KECCAK_EMPTY_HASH {
                0
            } else if code_hash.is_zero() {
                1
            } else {
                2
            };
            s.put(addr.as_slice());
            s.put(&[blen as u8 | (code_tag << 6)]);
            put_varint(s, *nonce);
            s.put(&bal[32 - blen..]);
            if code_tag == 2 {
                s.put(code_hash.as_slice());
            }
        }
        put_varint(s, self.storage.len() as u64);
        let mut prev: Option<&Address> = None;
        for ((addr, key), value) in &self.storage {
            let (val, vlen) = minimal_be(value);
            let same = prev == Some(addr);
            s.put(&[vlen as u8 | (u8::from(same) << 6)]);
            if !same {
                s.put(addr.as_slice());
            }
            s.put(key.as_slice());
            s.put(&val[32 - vlen..]);
            prev = Some(addr);
        }
        put_varint(s, self.code.len() as u64);
        for (code_hash, bytes) in &self.code {
            s.put(code_hash.as_slice());
            put_varint(s, bytes.len() as u64);
            s.put(bytes.as_ref());
        }
    }
}

impl WriteSet {
    /// Encoded length. This is test-only, so a test can check
    /// structurally that a transfer fits in one keccak block.
    pub fn encoded_len_for_test(&self) -> usize {
        struct Counter(usize);
        impl WsSink for Counter {
            fn put(&mut self, b: &[u8]) {
                self.0 += b.len();
            }
        }
        let mut c = Counter(0);
        self.encode(&mut c);
        c.0
    }
}

/// Version byte for the write-set encoding. See [`WriteSet::encode`].
const WS_ENCODING_V2: u8 = 0x02;

/// `keccak256([])`. Revm uses this as the code hash for every account
/// without code, so it is worth one tag value instead of 32 bytes on
/// every externally owned account (EOA).
const KECCAK_EMPTY_HASH: B256 = B256::new([
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
]);

/// A destination for the canonical encoding: a stack buffer or the hash
/// sponge.
trait WsSink {
    fn put(&mut self, bytes: &[u8]);
}

struct BufSink<const N: usize> {
    buf: [u8; N],
    n: usize,
}

impl<const N: usize> WsSink for BufSink<N> {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.buf[self.n..self.n + bytes.len()].copy_from_slice(bytes);
        self.n += bytes.len();
    }
}

impl WsSink for alloy_primitives::Keccak256 {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.update(bytes);
    }
}

/// LEB128 encoding, canonical (minimal length).
#[inline]
fn put_varint<S: WsSink>(s: &mut S, mut v: u64) {
    let mut buf = [0u8; 10];
    let mut n = 0;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf[n] = byte;
            n += 1;
            break;
        }
        buf[n] = byte | 0x80;
        n += 1;
    }
    s.put(&buf[..n]);
}

/// Big-endian bytes with leading zeros removed, plus the length. Zero
/// has length 0.
#[inline]
fn minimal_be(v: &U256) -> ([u8; 32], usize) {
    let bytes = v.to_be_bytes::<32>();
    let lead = bytes.iter().take_while(|b| **b == 0).count();
    (bytes, 32 - lead)
}

/// Mutable per-block accumulator that the exec thread carries between txs.
/// It holds the same canonical data as `WriteSet`. This lets the exec
/// thread layer running writes over the snapshot for the next tx, and
/// serialize to `kardamom_types::BlockDelta` at block close.
#[derive(Debug, Default, Clone)]
pub struct PendingDelta {
    // These are hash maps on purpose (they used to be `BTreeMap`s). The
    // accumulator only does upserts, gets, and unordered iteration, so it
    // does not need order. B-tree construction was the single largest
    // fixed cost in the commit tail, about 170ns per node, and stayed
    // that way after several optimization attempts. The one consumer
    // that needs order, the state writer's cursor walk, gets it from
    // `finalize`, which sorts once, after dedup, off the
    // execution-critical path. Iteration order here is nondeterministic.
    // Any serialization must go through `finalize`, and it does.
    pub accounts: DeltaMap<Address, (u64, U256, B256)>,
    pub storage: DeltaMap<(Address, B256), U256>,
    pub code: DeltaMap<B256, Bytes>,
}

/// The accumulator's map type: a hash map that works in `no_std`.
pub type DeltaMap<K, V> = hashbrown::HashMap<K, V, hashbrown::DefaultHashBuilder>;

impl PendingDelta {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge a per-tx `WriteSet` into the running delta. Later writes
    /// overwrite earlier ones. This matches the sequential execution
    /// model: the last tx to touch a slot wins for the block.
    pub fn apply(&mut self, ws: WriteSet) {
        for (addr, v) in ws.accounts {
            self.accounts.insert(addr, v);
        }
        for (k, v) in ws.storage {
            self.storage.insert(k, v);
        }
        for (h, b) in ws.code {
            self.code.insert(h, b);
        }
    }

    /// Merge another block's delta over this one. The other delta's writes
    /// win. This keeps the pipelined commit's single merged read layer
    /// over multiple unsettled blocks, so per-tx cache seeding stays at
    /// one layer of cost, no matter the pipeline depth. `code` entries
    /// are `Bytes`, which are reference-counted, so clones are cheap.
    pub fn merge_from(&mut self, other: &PendingDelta) {
        for (addr, v) in &other.accounts {
            self.accounts.insert(*addr, *v);
        }
        for (k, v) in &other.storage {
            self.storage.insert(*k, *v);
        }
        for (h, b) in &other.code {
            self.code.insert(*h, b.clone());
        }
    }

    /// Finalize: produce a wire-shape `BlockDelta`, ready for the state
    /// writer. `receipts` are the block's per-tx receipts, in arrival
    /// order. The caller supplies them (the actor or replayer holds
    /// them). The writer persists them into the `receipts` and
    /// `tx_hash_index` tables, so `eth_getTransactionReceipt` can answer
    /// from durable state after a restart.
    pub fn finalize(self, block_number: u64, receipts: Vec<kardamom_types::Receipt>) -> BlockDelta {
        // This is the one place that establishes canonical order. The
        // accumulator maps are unordered. Every serialized or persisted
        // view of a block delta passes through here and leaves sorted.
        // Cost: one sort of the deduped entry set, about 0.5ms for 8,000
        // entries, paid once and off the execution-critical path, instead
        // of paying for B-tree construction on that path.
        let mut accounts: Vec<AccountChange> = self
            .accounts
            .into_iter()
            .map(|(address, (nonce, balance, code_hash))| AccountChange {
                address,
                nonce,
                balance,
                code_hash,
            })
            .collect();
        accounts.sort_unstable_by_key(|a| a.address);
        let mut storage: Vec<StorageChange> = self
            .storage
            .into_iter()
            .map(|((address, key), value)| StorageChange {
                address,
                key,
                value,
            })
            .collect();
        storage.sort_unstable_by_key(|s| (s.address, s.key));
        let mut code: Vec<CodeEntry> = self
            .code
            .into_iter()
            .map(|(code_hash, code)| CodeEntry { code_hash, code })
            .collect();
        code.sort_unstable_by_key(|c| c.code_hash);
        BlockDelta {
            block_number,
            accounts,
            storage,
            code,
            receipts,
        }
    }
}

// NOTE: there is no `block_delta_root` or state-root function here. The
// executor does not compute or publish a state-root commitment. The sealed
// BlockBoundary on tx_receipts is slim. The state writer flushes the delta
// to libmdbx, and that ends the executor's role in block closure.

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};

    fn sample_account(b: u64, n: u64) -> (u64, U256, B256) {
        (n, U256::from(b), B256::repeat_byte(0xCC))
    }

    #[test]
    fn empty_write_set_has_stable_hash() {
        let h1 = WriteSet::default().hash();
        let h2 = WriteSet::default().hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_is_independent_of_insertion_order() {
        // finish() sorts the entries, so even if a revm wrapper inserts
        // them in a different order, the hash is the same.
        let a1 = Address::from([0x11u8; 20]);
        let a2 = Address::from([0x22u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.accounts.push((a1, sample_account(10, 1)));
        ws_a.accounts.push((a2, sample_account(20, 2)));
        ws_a.storage
            .push(((a1, B256::from(U256::from(1u64))), U256::from(100u64)));
        ws_a.storage
            .push(((a2, B256::from(U256::from(2u64))), U256::from(200u64)));

        let mut ws_b = WriteSet::default();
        ws_b.accounts.push((a2, sample_account(20, 2)));
        ws_b.accounts.push((a1, sample_account(10, 1)));
        ws_b.storage
            .push(((a2, B256::from(U256::from(2u64))), U256::from(200u64)));
        ws_b.storage
            .push(((a1, B256::from(U256::from(1u64))), U256::from(100u64)));

        ws_a.finish();
        ws_b.finish();
        assert_eq!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_value_change() {
        let addr = Address::from([0x11u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.storage
            .push(((addr, B256::from(U256::from(1u64))), U256::from(100u64)));

        let mut ws_b = WriteSet::default();
        ws_b.storage
            .push(((addr, B256::from(U256::from(1u64))), U256::from(101u64)));

        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_nonce_change() {
        let addr = Address::from([0x11u8; 20]);
        let mut ws_a = WriteSet::default();
        ws_a.accounts.push((addr, sample_account(0, 0)));
        let mut ws_b = WriteSet::default();
        ws_b.accounts.push((addr, sample_account(0, 1)));
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_covers_code_bytes() {
        let h = B256::repeat_byte(0xAA);
        let mut ws_a = WriteSet::default();
        ws_a.code.push((h, Bytes::from_static(&[0x60, 0x00])));
        let mut ws_b = WriteSet::default();
        ws_b.code.push((h, Bytes::from_static(&[0x60, 0x01])));
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn apply_write_set_merges_and_overwrites() {
        let addr = Address::from([0x11u8; 20]);
        let mut delta = PendingDelta::default();

        let mut ws1 = WriteSet::default();
        ws1.accounts.push((addr, sample_account(10, 1)));
        ws1.storage
            .push(((addr, B256::from(U256::from(1u64))), U256::from(100u64)));
        delta.apply(ws1);

        let mut ws2 = WriteSet::default();
        ws2.accounts.push((addr, sample_account(15, 2)));
        ws2.storage
            .push(((addr, B256::from(U256::from(1u64))), U256::from(200u64)));
        delta.apply(ws2);

        let (nonce, balance, _ch) = delta.accounts[&addr];
        assert_eq!(balance, U256::from(15u64));
        assert_eq!(nonce, 2);
        assert_eq!(
            delta.storage[&(addr, B256::from(U256::from(1u64)))],
            U256::from(200u64)
        );
    }

    #[test]
    fn finalize_produces_canonical_block_delta() {
        let a1 = Address::from([0x11u8; 20]);
        let a2 = Address::from([0x22u8; 20]);
        let mut delta = PendingDelta::default();
        delta.accounts.insert(a2, sample_account(20, 0));
        delta.accounts.insert(a1, sample_account(10, 1));

        let receipts = vec![kardamom_types::Receipt {
            block_number: 7,
            ..Default::default()
        }];
        let bd = delta.finalize(7, receipts);
        assert_eq!(bd.block_number, 7);
        assert_eq!(bd.accounts.len(), 2);
        // finalize() sorts by address; check that the order is deterministic.
        assert_eq!(bd.accounts[0].address, a1);
        assert_eq!(bd.accounts[1].address, a2);
        // The block's receipts ride inside the delta, so the writer
        // persists them into the receipts and tx_hash_index tables.
        assert_eq!(bd.receipts.len(), 1);
    }
}
