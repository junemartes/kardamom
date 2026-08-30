//! First-wins tx-hash dedup set for the tx_receipts MDS fan-in.
//!
//! With the multi-destination subscription, all N executor replicas replay
//! the same canonical order and emit identical receipts. So each receipt
//! arrives up to N times on ingress's combined stream. The proxy's receipt
//! watcher calls [`SeenReceipts::insert`] for every receipt, and only
//! processes the ones it has not seen before. This makes a tx's
//! must-deliver ack, and its cache and pending updates, fire exactly once.
//!
//! The set has a bound: it evicts the oldest entry first, by insertion
//! order. This keeps it from growing without limit on a long-running
//! ingress. The capacity only needs to exceed the in-flight window between
//! a receipt's first and last replica copy. The N executors replay the
//! same order at close to the same rate, so those copies arrive close
//! together. A capacity in the tens of thousands is far more than that
//! window. If the capacity is too small, the only failure is that a very
//! late duplicate, evicted before its twin arrived, gets reprocessed. This
//! is harmless, because `on_receipt` and the cache insert are themselves
//! idempotent. So this set is a fast-path optimization over sinks that are
//! already idempotent, not a set that correctness depends on.

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

use alloy_primitives::B256;

/// Default ring capacity. 1<<16 hashes is about 2 MiB, a small cost. It is
/// far larger than the N-replica duplicate window for any realistic N.
pub const DEFAULT_CAPACITY: usize = 1 << 16;

/// Thread-safe, bounded, first-wins set of receipt tx hashes.
pub struct SeenReceipts {
    inner: Mutex<Inner>,
    capacity: usize,
}

struct Inner {
    set: HashSet<B256>,
    /// Insertion order. Used to evict the oldest entry once `set` reaches
    /// `capacity`.
    order: VecDeque<B256>,
}

impl SeenReceipts {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "SeenReceipts capacity must be > 0");
        Self {
            inner: Mutex::new(Inner {
                set: HashSet::with_capacity(capacity),
                order: VecDeque::with_capacity(capacity),
            }),
            capacity,
        }
    }

    /// Records `tx_hash`. Returns `true` if the hash was newly inserted; the
    /// caller should process this receipt. Returns `false` if the hash was
    /// already present, as a duplicate replica copy; the caller should
    /// drop it.
    pub fn insert(&self, tx_hash: B256) -> bool {
        let mut g = self.inner.lock().expect("SeenReceipts poisoned");
        if !g.set.insert(tx_hash) {
            return false;
        }
        g.order.push_back(tx_hash);
        if g.order.len() > self.capacity
            && let Some(evicted) = g.order.pop_front()
        {
            g.set.remove(&evicted);
        }
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().set.len()
    }
}

impl Default for SeenReceipts {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_wins_then_duplicates_dropped() {
        let s = SeenReceipts::new(16);
        let h = B256::repeat_byte(0xAB);
        // The first copy is newly inserted, so the caller must process it.
        assert!(s.insert(h), "first receipt must be accepted");
        // Later identical copies, from replica 2 through N, are dropped.
        assert!(!s.insert(h), "duplicate must be dropped");
        assert!(!s.insert(h), "duplicate must be dropped");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn distinct_hashes_all_accepted() {
        let s = SeenReceipts::new(16);
        for i in 0..8u8 {
            assert!(s.insert(B256::repeat_byte(i)));
        }
        assert_eq!(s.len(), 8);
        // Inserting any of them again is a duplicate.
        assert!(!s.insert(B256::repeat_byte(3)));
    }

    #[test]
    fn fifo_eviction_bounds_size() {
        let s = SeenReceipts::new(4);
        // Fill the set past its capacity.
        for i in 0..6u8 {
            assert!(s.insert(B256::repeat_byte(i)));
        }
        assert_eq!(s.len(), 4, "set is bounded to capacity");
        // The two oldest entries (0, 1) were evicted, so they read as new
        // again. This is harmless, because the downstream sinks are
        // idempotent. The newest entries are still present and dedup.
        assert!(!s.insert(B256::repeat_byte(5)), "recent hash still deduped");
        assert!(s.insert(B256::repeat_byte(0)), "evicted hash reads as new");
    }
}
