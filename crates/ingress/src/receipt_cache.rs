//! In-memory `tx_receipts` index that the ingress proxy uses.
//!
//! This gives two views over the same receipts. The proxy's `tx_receipts`
//! watcher, from executor to ingress, fills both through
//! [`ReceiptCache::insert`]:
//!
//! - `(Address, u64) -> Receipt` for retry-dedup in `submit_raw`.
//! - `B256 -> Receipt` to answer `eth_getTransactionReceipt(tx_hash)`
//!   without a join against the state DB.
//!
//! The enriched `types::Receipt` carries `from`, `nonce`, and `tx_hash`
//! directly, so one `tx_receipts` subscription feeds both indexes.

use std::num::NonZeroUsize;
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use dashmap::DashMap;

use kardamom_types::Receipt;

/// Bounded. Eviction order is arbitrary, because `DashMap` does not
/// expose insertion order. An evicted sender resubmits, and the sequencer dedupes
/// the resubmit through the past-nonce path. A lookup on
/// `eth_getTransactionReceipt` for an evicted entry returns `null`. A
/// future v1 fallback can query the state DB instead.
pub(crate) struct ReceiptCache {
    by_sender_nonce: DashMap<(Address, u64), Arc<Receipt>>,
    by_tx_hash: DashMap<B256, Arc<Receipt>>,
    capacity: NonZeroUsize,
}

impl ReceiptCache {
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            by_sender_nonce: DashMap::new(),
            by_tx_hash: DashMap::new(),
            capacity,
        }
    }

    /// Inserts one receipt into both indexes. Eviction order is
    /// arbitrary, because `DashMap` does not expose insertion order. A
    /// duplicate (sender, nonce) entry overwrites the older row. This is
    /// one allocation shared by both indexes.
    pub(crate) fn insert(&self, receipt: Receipt) {
        self.evict_if_full(&self.by_sender_nonce);
        self.evict_if_full(&self.by_tx_hash);
        let receipt = Arc::new(receipt);
        self.by_sender_nonce
            .insert((receipt.from, receipt.nonce), receipt.clone());
        self.by_tx_hash.insert(receipt.tx_hash, receipt);
    }

    /// Evicts one entry from `map` if it has reached capacity.
    ///
    /// Picks an arbitrary key to evict. `DashMap`'s `Iter` holds a read
    /// guard on the shard it is positioned on, and `remove()` needs that
    /// shard's write guard. `victim` is computed in its own `let`
    /// statement, not inlined into the `if let` condition below: a
    /// temporary created in an `if let`'s condition is not dropped until
    /// the end of the `if let` body, so an inlined
    /// `if let Some(key) = map.iter().next()... { map.remove(&key) }`
    /// would still hold the shard's read guard live while `remove()`
    /// asks that same shard for its write guard — deadlock, on one
    /// thread, against itself. Ending the `let` statement first drops
    /// the iterator (and its guard) before `map.remove` ever runs.
    fn evict_if_full<K: Eq + std::hash::Hash + Copy>(&self, map: &DashMap<K, Arc<Receipt>>) {
        if map.len() < self.capacity.get() {
            return;
        }
        let victim = map.iter().next().map(|e| *e.key());
        if let Some(key) = victim {
            map.remove(&key);
        }
    }

    pub(crate) fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.by_sender_nonce
            .get(&(sender, nonce))
            .map(|r| (**r).clone())
    }

    pub(crate) fn lookup_by_tx_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.by_tx_hash.get(&tx_hash).map(|r| (**r).clone())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_sender_nonce.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::receipt;

    #[tokio::test]
    async fn lookup_returns_inserted() {
        let c = ReceiptCache::new(NonZeroUsize::new(8).unwrap());
        let s = Address::repeat_byte(0x33);
        let h = B256::repeat_byte(0x11);
        c.insert(receipt(s, 1, h, 1));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx.term_offset, 1);
        assert!(c.lookup(s, 2).is_none());
        // The same entry is also indexed by tx_hash.
        assert_eq!(c.lookup_by_tx_hash(h).unwrap().tx_hash, h);
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x22)).is_none());
    }

    // Inserting past capacity must evict an entry, not deadlock: see
    // `evict_if_full`'s doc comment for the guard-ordering rule this
    // guards.
    #[tokio::test(flavor = "current_thread")]
    async fn insert_past_capacity_evicts_without_deadlock() {
        let cap = 64usize;
        let c = ReceiptCache::new(NonZeroUsize::new(cap).unwrap());
        // Insert well past capacity, with a distinct (sender, nonce,
        // tx_hash) each time.
        for i in 0..(cap as u64 * 4) {
            let b = (i % 251) as u8;
            c.insert(receipt(
                Address::repeat_byte(b ^ 0x5a),
                i,
                B256::repeat_byte(b ^ 0xa5),
                i32::try_from(i).unwrap(),
            ));
        }
        // Bounded: the cache never exceeds capacity, since eviction keeps
        // it in check.
        assert!(
            c.len() <= cap,
            "cache must stay bounded: {} > {cap}",
            c.len()
        );
        // Still usable: the cache can retrieve the most recent insert.
        let last = (cap as u64 * 4) - 1;
        assert!(
            c.lookup(Address::repeat_byte(((last % 251) as u8) ^ 0x5a), last)
                .is_some()
        );
    }
}
