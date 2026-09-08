//! First-wins tx-hash dedup set for the `tx_receipts` MDS fan-in.
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
//!
//! Only the `tx_receipts` watcher task touches a `SeenReceipts` value, so it
//! owns one directly, by value, with no `Arc` or lock.

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;

use alloy_primitives::B256;

/// Default ring capacity. 1<<16 hashes is about 2 MiB, a small cost. It is
/// far larger than the N-replica duplicate window for any realistic N.
pub(crate) const DEFAULT_CAPACITY: usize = 1 << 16;

/// Bounded, first-wins set of receipt tx hashes. Owned by one task; see
/// the module docs.
pub(crate) struct SeenReceipts {
    set: HashSet<B256>,
    /// Insertion order. Used to evict the oldest entry once `set` reaches
    /// `capacity`.
    order: VecDeque<B256>,
    capacity: NonZeroUsize,
}

impl SeenReceipts {
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            set: HashSet::with_capacity(capacity.get()),
            order: VecDeque::with_capacity(capacity.get()),
            capacity,
        }
    }

    /// Records `tx_hash`. Returns `true` if the hash was newly inserted; the
    /// caller should process this receipt. Returns `false` if the hash was
    /// already present, as a duplicate replica copy; the caller should
    /// drop it.
    #[must_use]
    pub(crate) fn insert(&mut self, tx_hash: B256) -> bool {
        if !self.set.insert(tx_hash) {
            return false;
        }
        self.order.push_back(tx_hash);
        if self.order.len() > self.capacity.get()
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.set.len()
    }
}

impl Default for SeenReceipts {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(DEFAULT_CAPACITY).expect("DEFAULT_CAPACITY is nonzero"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn first_wins_then_duplicates_dropped() {
        let mut s = SeenReceipts::new(capacity(16));
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
        let mut s = SeenReceipts::new(capacity(16));
        for i in 0..8u8 {
            assert!(s.insert(B256::repeat_byte(i)));
        }
        assert_eq!(s.len(), 8);
        // Inserting any of them again is a duplicate.
        assert!(!s.insert(B256::repeat_byte(3)));
    }

    #[test]
    fn fifo_eviction_bounds_size() {
        let mut s = SeenReceipts::new(capacity(4));
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
