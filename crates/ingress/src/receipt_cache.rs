//! In-memory tx_receipts index used by the ingress proxy.
//!
//! Two views over the same receipts, populated by a single background task
//! that consumes the tx_receipts broadcast (executor → ingress):
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
use tokio::sync::broadcast;

use kardamom_types::Receipt;

use crate::channels::IngressSubscription;

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

    /// Spawn a background task that consumes the tx_receipts broadcast and
    /// populates both indexes. Returns once spawned.
    pub fn spawn_consumer<S: IngressSubscription>(self: &Arc<Self>, sub: &S) {
        let mut rx: broadcast::Receiver<Receipt> = sub.subscribe_receipts();
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(r) => me.insert(r),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Best-effort cache: we dropped some entries; keep
                        // consuming.
                        continue;
                    }
                }
            }
        });
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
        if map.len() >= self.capacity
            && let Some(entry) = map.iter().next()
        {
            let key = *entry.key();
            drop(entry);
            map.remove(&key);
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
    use crate::channels::MockChannels;
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

    #[tokio::test]
    async fn consumer_populates_from_broadcast() {
        let (mock, _rx) = MockChannels::new(1);
        let cache = Arc::new(ReceiptCache::new(64));
        cache.spawn_consumer(&mock);
        let s = Address::repeat_byte(0x44);
        let h = B256::repeat_byte(0x77);
        let _ = mock.receipt_bus.send(make_receipt(s, 9, h, 9));
        // Allow the spawned task time to consume.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(cache.lookup(s, 9).unwrap().tx_idx.term_offset, 9);
        assert_eq!(cache.lookup_by_tx_hash(h).unwrap().tx_idx.term_offset, 9);
    }
}
