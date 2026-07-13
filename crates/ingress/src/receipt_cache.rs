//! In-memory tx_receipts index used by the ingress proxy.
//!
//! Two views over the same receipts, populated by the proxy's tx_receipts
//! watcher (executor → ingress) via [`ReceiptCache::insert`]:
//!
//! - `(Address, u64) → Receipt` for retry-dedup in `submit_raw`.
//! - `B256 → Receipt` for answering `eth_getTransactionReceipt(tx_hash)`
//!   without joining against the state DB.
//!
//! Because the enriched `types::Receipt` now carries `from`, `nonce`, and
//! `tx_hash` directly, one tx_receipts subscription feeds both indexes —
//! the old `CachedReceipt` side-channel is no longer needed.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
use dashmap::DashMap;

use kardamom_types::Receipt;

/// Bounded FIFO eviction. Duplicates outside the window will re-submit and
/// the sequencer dedupes via the past-nonce path; misses on
/// `eth_getTransactionReceipt` for evicted entries return `null` (a future
/// v1 fallback can hit the state DB).
pub struct ReceiptCache {
    by_sender_nonce: Arc<DashMap<(Address, u64), Receipt>>,
    by_tx_hash: Arc<DashMap<B256, Receipt>>,
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

    /// Insert one receipt into both indexes. Eviction is arbitrary (DashMap
    /// doesn't expose FIFO); duplicate (sender, nonce) entries overwrite the
    /// older row.
    pub fn insert(&self, receipt: Receipt) {
        self.evict_if_full(&self.by_sender_nonce);
        self.evict_if_full(&self.by_tx_hash);
        self.by_sender_nonce
            .insert((receipt.from, receipt.nonce), receipt.clone());
        self.by_tx_hash.insert(receipt.tx_hash, receipt);
    }

    fn evict_if_full<K: Eq + std::hash::Hash + Copy>(&self, map: &DashMap<K, Receipt>) {
        if map.len() >= self.capacity {
            // Copy a victim key out and DROP the iterator BEFORE `remove()`.
            // DashMap's `Iter` holds a read-guard on the shard it is positioned
            // on, and `remove()` needs that shard's write-guard — so removing
            // while the iterator is still alive self-deadlocks on that shard
            // (read held + write requested, same thread). The previous
            // `if .. && let Some(entry) = map.iter().next() { .. map.remove(..) }`
            // form kept the `map.iter()` temporary alive to the end of the
            // block (let-chain temporary lifetime), so it deadlocked the
            // tx_receipts watcher the moment the cache first reached capacity —
            // freezing receipt delivery on that replica until restart. Binding
            // the key in its own statement drops the iterator at the `;`.
            let victim = map.iter().next().map(|e| *e.key());
            if let Some(key) = victim {
                map.remove(&key);
            }
        }
    }

    pub fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.by_sender_nonce
            .get(&(sender, nonce))
            .map(|r| r.clone())
    }

    pub fn lookup_by_tx_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.by_tx_hash.get(&tx_hash).map(|r| r.clone())
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
        // Same entry also indexed by tx_hash.
        assert_eq!(c.lookup_by_tx_hash(h).unwrap().tx_hash, h);
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x22)).is_none());
    }

    // Regression: inserting past capacity must EVICT, not deadlock. The old
    // `evict_if_full` held a DashMap iterator (shard read-guard) across
    // `remove()` (shard write-guard) and self-deadlocked the moment the cache
    // first filled — which froze the ingress tx_receipts watcher under sustained
    // load (cluster-e2e ingress-churn freeze). This test would hang on the old
    // code; the harness timeout turns that hang into a visible failure.
    #[tokio::test(flavor = "current_thread")]
    async fn insert_past_capacity_evicts_without_deadlock() {
        let cap = 64usize;
        let c = ReceiptCache::new(cap);
        // Insert well past capacity. Distinct (sender, nonce, tx_hash) each time.
        for i in 0..(cap as u64 * 4) {
            let b = (i % 251) as u8;
            c.insert(make_receipt(
                Address::repeat_byte(b ^ 0x5a),
                i,
                B256::repeat_byte(b ^ 0xa5),
                i as i32,
            ));
        }
        // Bounded: never exceeds capacity (FIFO/arbitrary eviction kept it in check).
        assert!(
            c.len() <= cap,
            "cache must stay bounded: {} > {cap}",
            c.len()
        );
        // Still usable: the most recent insert is retrievable.
        let last = (cap as u64 * 4) - 1;
        assert!(
            c.lookup(Address::repeat_byte(((last % 251) as u8) ^ 0x5a), last)
                .is_some()
        );
    }
}
