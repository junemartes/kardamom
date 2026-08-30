//! In-memory tx_receipts index that the ingress proxy uses.
//!
//! This gives two views over the same receipts. The proxy's tx_receipts
//! watcher, from executor to ingress, fills both through
//! [`ReceiptCache::insert`]:
//!
//! - `(Address, u64) -> Receipt` for retry-dedup in `submit_raw`.
//! - `B256 -> Receipt` to answer `eth_getTransactionReceipt(tx_hash)`
//!   without a join against the state DB.
//!
//! The enriched `types::Receipt` now carries `from`, `nonce`, and
//! `tx_hash` directly. So one tx_receipts subscription feeds both
//! indexes, and the old `CachedReceipt` side channel is no longer needed.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
use dashmap::DashMap;

use kardamom_types::Receipt;

/// Bounded. Eviction order is arbitrary, because DashMap does not expose
/// insertion order. An evicted sender resubmits, and the sequencer dedupes
/// the resubmit through the past-nonce path. A lookup on
/// `eth_getTransactionReceipt` for an evicted entry returns `null`. A
/// future v1 fallback can query the state DB instead.
pub struct ReceiptCache {
    by_sender_nonce: Arc<DashMap<(Address, u64), std::sync::Arc<Receipt>>>,
    by_tx_hash: Arc<DashMap<B256, std::sync::Arc<Receipt>>>,
    capacity: usize,
}

impl ReceiptCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            by_sender_nonce: Arc::new(DashMap::new()),
            by_tx_hash: Arc::new(DashMap::new()),
            capacity,
        }
    }

    /// Inserts one receipt into both indexes. Eviction order is
    /// arbitrary, because DashMap does not expose insertion order. A
    /// duplicate (sender, nonce) entry overwrites the older row.
    pub fn insert(&self, receipt: Receipt) {
        self.evict_if_full(&self.by_sender_nonce);
        self.evict_if_full(&self.by_tx_hash);
        // This is one allocation shared by both indexes. Before this
        // change, each index stored its own full Receipt clone, logs
        // included.
        let receipt = std::sync::Arc::new(receipt);
        self.by_sender_nonce
            .insert((receipt.from, receipt.nonce), receipt.clone());
        self.by_tx_hash.insert(receipt.tx_hash, receipt);
    }

    fn evict_if_full<K: Eq + std::hash::Hash + Copy>(
        &self,
        map: &DashMap<K, std::sync::Arc<Receipt>>,
    ) {
        if map.len() >= self.capacity {
            // Copy a victim key out, and drop the iterator before calling
            // `remove()`. DashMap's `Iter` holds a read guard on the
            // shard it is positioned on. `remove()` needs that shard's
            // write guard. So removing an entry while the iterator is
            // still alive deadlocks that shard against itself: a read
            // guard held, and a write guard requested, on the same
            // thread. The previous form,
            // `if .. && let Some(entry) = map.iter().next() { .. map.remove(..) }`,
            // kept the `map.iter()` temporary alive to the end of the
            // block. That deadlocked the tx_receipts watcher the first
            // time the cache reached capacity, and froze receipt delivery
            // on that replica until restart. Binding the key in its own
            // statement drops the iterator at the `;`.
            let victim = map.iter().next().map(|e| *e.key());
            if let Some(key) = victim {
                map.remove(&key);
            }
        }
    }

    pub fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.by_sender_nonce
            .get(&(sender, nonce))
            .map(|r| (**r).clone())
    }

    pub fn lookup_by_tx_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.by_tx_hash.get(&tx_hash).map(|r| (**r).clone())
    }

    pub fn len(&self) -> usize {
        self.by_sender_nonce.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_sender_nonce.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    fn make_receipt(sender: Address, nonce: u64, tx_hash: B256, idx: i32) -> Receipt {
        Receipt {
            tx_idx: BPosition {
                term_id: 0,
                term_offset: idx,
            },
            tx_hash,
            status: true,
            gas_used: 21_000,
            from: sender,
            nonce,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn lookup_returns_inserted() {
        let c = ReceiptCache::new(8);
        let s = Address::repeat_byte(0x33);
        let h = B256::repeat_byte(0x11);
        c.insert(make_receipt(s, 1, h, 1));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx.term_offset, 1);
        assert!(c.lookup(s, 2).is_none());
        // The same entry is also indexed by tx_hash.
        assert_eq!(c.lookup_by_tx_hash(h).unwrap().tx_hash, h);
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x22)).is_none());
    }

    // Regression test: inserting past capacity must evict an entry, not
    // deadlock. The old `evict_if_full` held a DashMap iterator, a shard
    // read guard, across `remove()`, which needs the shard's write guard.
    // This deadlocked the first time the cache filled up, and froze the
    // ingress tx_receipts watcher under sustained load. This test hangs
    // on the old code; the harness timeout turns that hang into a visible
    // failure.
    #[tokio::test(flavor = "current_thread")]
    async fn insert_past_capacity_evicts_without_deadlock() {
        let cap = 64usize;
        let c = ReceiptCache::new(cap);
        // Insert well past capacity, with a distinct (sender, nonce,
        // tx_hash) each time.
        for i in 0..(cap as u64 * 4) {
            let b = (i % 251) as u8;
            c.insert(make_receipt(
                Address::repeat_byte(b ^ 0x5a),
                i,
                B256::repeat_byte(b ^ 0xa5),
                i as i32,
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
