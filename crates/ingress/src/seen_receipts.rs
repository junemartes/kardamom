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

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;

use alloy_primitives::B256;

/// Default ring capacity. 1<<16 hashes is about 2 MiB, a small cost. It is
/// far larger than the N-replica duplicate window for any realistic N.
pub(crate) const DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(1 << 16).unwrap();

/// Bounded, first-wins set of receipt tx hashes, each with the block of
/// its receipt. Owned by one task; see the module docs.
pub(crate) struct SeenReceipts {
    set: HashMap<B256, u64>,
    /// Insertion order. Used to evict the oldest entry once `set` reaches
    /// `capacity`.
    order: VecDeque<B256>,
    capacity: NonZeroUsize,
}

impl SeenReceipts {
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            set: HashMap::with_capacity(capacity.get()),
            order: VecDeque::with_capacity(capacity.get()),
            capacity,
        }
    }

    /// Records `tx_hash`, of a receipt in `block`. Returns `true` if the
    /// hash was newly inserted; the caller should process this receipt.
    /// Returns `false` if the hash was already present, as a duplicate
    /// replica copy; the caller should drop it.
    #[must_use]
    pub(crate) fn insert(&mut self, tx_hash: B256, block: u64) -> bool {
        if self.set.contains_key(&tx_hash) {
            return false;
        }
        self.set.insert(tx_hash, block);
        self.order.push_back(tx_hash);
        if self.order.len() > self.capacity.get()
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        true
    }

    /// Forgets the hashes of the receipts above `to_block`. After a
    /// rollback, such a transaction can come again with its old hash in a
    /// new block. A copy of a voided receipt can then pass once more,
    /// which the idempotent sinks allow. The hashes at or below `to_block`
    /// stay: their receipts are still valid.
    pub(crate) fn forget_above(&mut self, to_block: u64) {
        let Self { set, order, .. } = self;
        set.retain(|_, block| *block <= to_block);
        order.retain(|hash| set.contains_key(hash));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.set.len()
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

    fn capacity(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn first_wins_then_duplicates_dropped() {
        let mut s = SeenReceipts::new(capacity(16));
        let h = B256::repeat_byte(0xAB);
        // The first copy is newly inserted, so the caller must process it.
        assert!(s.insert(h, 1), "first receipt must be accepted");
        // Later identical copies, from replica 2 through N, are dropped.
        assert!(!s.insert(h, 1), "duplicate must be dropped");
        assert!(!s.insert(h, 1), "duplicate must be dropped");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn distinct_hashes_all_accepted() {
        let mut s = SeenReceipts::new(capacity(16));
        for i in 0..8u8 {
            assert!(s.insert(B256::repeat_byte(i), 1));
        }
        assert_eq!(s.len(), 8);
        // Inserting any of them again is a duplicate.
        assert!(!s.insert(B256::repeat_byte(3), 1));
    }

    #[test]
    fn a_rollback_forgets_only_the_hashes_above_its_block() {
        let mut s = SeenReceipts::new(capacity(8));
        let (kept, voided) = (B256::repeat_byte(0x01), B256::repeat_byte(0x02));
        assert!(s.insert(kept, 4));
        assert!(s.insert(voided, 5));
        s.forget_above(4);
        assert!(!s.insert(kept, 4), "a hash at the block stays");
        assert!(s.insert(voided, 6), "a hash above the block reads as new");
        assert_eq!(s.len(), 2);
        assert_eq!(s.order.len(), 2, "the order follows the set");
    }

    #[test]
    fn fifo_eviction_bounds_size() {
        let mut s = SeenReceipts::new(capacity(4));
        // Fill the set past its capacity.
        for i in 0..6u8 {
            assert!(s.insert(B256::repeat_byte(i), 1));
        }
        assert_eq!(s.len(), 4, "set is bounded to capacity");
        // The two oldest entries (0, 1) were evicted, so they read as new
        // again. This is harmless, because the downstream sinks are
        // idempotent. The newest entries are still present and dedup.
        assert!(
            !s.insert(B256::repeat_byte(5), 1),
            "recent hash still deduped"
        );
        assert!(
            s.insert(B256::repeat_byte(0), 1),
            "evicted hash reads as new"
        );
    }
}
