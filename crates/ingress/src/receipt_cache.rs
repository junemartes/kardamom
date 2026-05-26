//! Receipt-cache channel subscriber.
//!
//! Maintains an in-memory `(sender, nonce) -> Receipt` map populated from
//! the receipt-cache broadcast. On a client retry, any proxy in the cluster
//! can answer the prior receipt without re-submitting to the sequencer.

use std::sync::Arc;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::broadcast;

use kardamom_types::{CachedReceipt, Receipt};

use crate::channels::IngressSubscription;

/// Bounded by config; FIFO eviction is acceptable because duplicates outside
/// the window will just re-submit and the sequencer dedupes via the past-nonce
/// path.
pub struct ReceiptCache {
    map: Arc<DashMap<(Address, u64), Receipt>>,
    capacity: usize,
}

impl ReceiptCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            map: Arc::new(DashMap::new()),
            capacity,
        }
    }

    /// Spawn a background task that consumes the receipt-cache broadcast
    /// and populates the in-memory map. Returns once spawned.
    pub fn spawn_consumer<S: IngressSubscription>(self: &Arc<Self>, sub: &S) {
        let mut rx: broadcast::Receiver<CachedReceipt> = sub.subscribe_receipt_cache();
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(c) => me.insert(c.sender, c.nonce, c.receipt),
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

    pub fn insert(&self, sender: Address, nonce: u64, receipt: Receipt) {
        if self.map.len() >= self.capacity {
            // DashMap doesn't expose FIFO; evict an arbitrary entry. v0
            // acceptable; revisit if the cache hit rate suffers.
            if let Some(entry) = self.map.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.map.remove(&key);
            }
        }
        self.map.insert((sender, nonce), receipt);
    }

    pub fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.map.get(&(sender, nonce)).map(|r| r.clone())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    use crate::channels::MockChannels;
    use kardamom_types::BPosition;

    fn dummy(idx: i32) -> Receipt {
        Receipt {
            tx_idx: BPosition {
                term_id: 0,
                term_offset: idx,
            },
            tx_hash: B256::ZERO,
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
        }
    }

    #[tokio::test]
    async fn lookup_returns_inserted() {
        let c = ReceiptCache::new(8);
        let s = Address::repeat_byte(0x33);
        c.insert(s, 1, dummy(1));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx.term_offset, 1);
        assert!(c.lookup(s, 2).is_none());
    }

    #[tokio::test]
    async fn consumer_populates_from_broadcast() {
        let (mock, _rx) = MockChannels::new(1);
        let cache = Arc::new(ReceiptCache::new(64));
        cache.spawn_consumer(&mock);
        let s = Address::repeat_byte(0x44);
        let _ = mock.receipt_cache_bus.send(CachedReceipt {
            sender: s,
            nonce: 9,
            tx_hash: B256::ZERO,
            receipt: dummy(9),
        });
        // Allow the spawned task time to consume.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(cache.lookup(s, 9).unwrap().tx_idx.term_offset, 9);
    }
}
