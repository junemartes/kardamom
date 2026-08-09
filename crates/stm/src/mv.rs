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

use std::collections::HashMap;
use std::sync::RwLock;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;

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
}

const SHARDS: usize = 64;

fn shard_of(bytes: &[u8]) -> usize {
    // Cheap stable shard: fold a few bytes. Distribution quality is
    // irrelevant beyond avoiding one hot lock.
    let mut h = 0usize;
    for b in bytes.iter().take(8) {
        h = h.wrapping_mul(31).wrapping_add(*b as usize);
    }
    h % SHARDS
}

/// A cell's version list: `(tx_index, value)`, sorted by index.
type Versions<V> = Vec<(u32, V)>;
type Shard<K, V> = RwLock<HashMap<K, Versions<V>>>;

/// Sharded multi-version store. Version lists are kept sorted by tx index
/// via binary-search insert (append in the common pessimistic case).
pub struct MvCache {
    accounts: Vec<Shard<Address, AccountVersion>>,
    storage: Vec<Shard<(Address, B256), U256>>,
    /// Content-addressed CREATE bytecode — no versioning (a hash IS its
    /// content), append-only.
    code: RwLock<HashMap<B256, Bytes>>,
}

impl Default for MvCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MvCache {
    pub fn new() -> Self {
        Self {
            accounts: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            storage: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            code: RwLock::new(HashMap::new()),
        }
    }

    /// Publish one tx's account write. Sorted-insert keeps correctness even
    /// if a prediction miss let writers race out of index order (validation
    /// still convicts the miss).
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
