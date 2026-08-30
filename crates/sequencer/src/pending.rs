//! Per-sender future-nonce buffer.
//!
//! A bounded `BTreeMap<u64, T>`, keyed by nonce. On overflow, it keeps the
//! lowest `capacity` nonces and drops the furthest-future one.
//!
//! Why lowest-wins matters: the buffer holds nonces above the sender's
//! expected nonce, waiting to drain as a contiguous run once the gap fills.
//! The lowest buffered nonces are closest to `expected`, and most likely to
//! become drainable soon. The highest are furthest away. Evicting the
//! smallest nonce (the old behavior) punches a gap directly in front of the
//! run. This wedges the sender permanently: every later nonce stays
//! "future" forever, with no recovery path. Dropping the furthest-future
//! nonce instead never breaks the low run. The dropped transaction is a
//! far-future nonce that the client resubmits long before it is needed. So
//! overflow degrades to transient shedding, not a permanent wedge.
//!
//! `drain_consecutive_from(start)` walks ascending keys, and yields the
//! contiguous run that starts at `start`. The first gap stops the drain.

use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    /// The buffer was full. The furthest-future buffered nonce
    /// (`evicted_nonce`) was dropped to make room for this lower nonce,
    /// to keep the drainable run.
    EvictedFuture {
        evicted_nonce: u64,
    },
    /// The buffer was full, and this nonce was itself the furthest in the
    /// future. It was rejected, not buffered, so the lower drainable run is
    /// kept. The client resubmits when this nonce comes back within the
    /// window.
    RejectedTooFar {
        nonce: u64,
    },
    DroppedBufferDisabled,
}

#[derive(Debug)]
pub struct PendingBuffer<T> {
    capacity: usize,
    inner: BTreeMap<u64, T>,
}

impl<T> PendingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn contains(&self, nonce: u64) -> bool {
        self.inner.contains_key(&nonce)
    }

    /// Lowest buffered nonce, if any.
    pub fn lowest_nonce(&self) -> Option<u64> {
        self.inner.keys().next().copied()
    }

    pub fn insert(&mut self, nonce: u64, value: T) -> InsertOutcome {
        if self.capacity == 0 {
            return InsertOutcome::DroppedBufferDisabled;
        }
        // Pre-compute these values. Then the match arms below do not need
        // to re-borrow `self.inner` after taking an entry handle.
        let already_present = self.inner.contains_key(&nonce);
        let at_capacity = !already_present && self.inner.len() >= self.capacity;
        if already_present {
            self.inner.insert(nonce, value);
            return InsertOutcome::Replaced;
        }
        if at_capacity {
            // The buffer is full. Keep the lowest `capacity` nonces (the
            // drainable run). The furthest-future nonce loses: either an
            // already-buffered max, or this incoming nonce if it is the
            // new max.
            let max = *self
                .inner
                .keys()
                .next_back()
                .expect("non-empty since len >= capacity >= 1");
            if nonce > max {
                // The incoming nonce is the furthest future. Reject it, and keep the run.
                return InsertOutcome::RejectedTooFar { nonce };
            }
            self.inner.remove(&max);
            self.inner.insert(nonce, value);
            return InsertOutcome::EvictedFuture { evicted_nonce: max };
        }
        self.inner.insert(nonce, value);
        InsertOutcome::Inserted
    }

    /// Insert without capacity enforcement, even for a disabled, capacity-0
    /// buffer. Only the backpressure rebuffer path
    /// ([`crate::state::PartitionState::reinsert_for_retry`]) uses this.
    /// The items being re-inserted were just drained out of this buffer
    /// (plus at most one in-flight ingress item). Enforcing capacity here
    /// could evict the lowest rebuffered nonce, and permanently lose a ref
    /// whose nonce the state machine already advanced past. That is exactly
    /// the data loss the rebuffer exists to prevent. Any overshoot is
    /// transient and bounded to one drained batch; the next successful
    /// flush drains it back out.
    pub fn reinsert(&mut self, nonce: u64, value: T) {
        self.inner.insert(nonce, value);
    }

    /// Drop every buffered entry with a nonce below `floor`. Returns how
    /// many were dropped. The receipt-floor advance
    /// ([`crate::state::PartitionState::advance_floor`]) uses this. Entries
    /// below an executed-truth floor are proven duplicates of already
    /// executed transactions, so dropping them can never create a
    /// canonical gap.
    pub fn drop_below(&mut self, floor: u64) -> usize {
        let keep = self.inner.split_off(&floor);
        let dropped = self.inner.len();
        self.inner = keep;
        dropped
    }

    /// Drain the contiguous run of nonces starting at `start`. Stops at the
    /// first gap. Returned items are removed from the buffer.
    pub fn drain_consecutive_from(&mut self, start: u64) -> DrainConsecutive<'_, T> {
        DrainConsecutive {
            buf: self,
            next: start,
        }
    }

    /// Remove and return the value at `nonce` if present.
    pub fn remove(&mut self, nonce: u64) -> Option<T> {
        self.inner.remove(&nonce)
    }
}

pub struct DrainConsecutive<'a, T> {
    buf: &'a mut PendingBuffer<T>,
    next: u64,
}

impl<T> Iterator for DrainConsecutive<'_, T> {
    type Item = (u64, T);
    fn next(&mut self) -> Option<Self::Item> {
        let v = self.buf.inner.remove(&self.next)?;
        let n = self.next;
        self.next = self.next.checked_add(1)?;
        Some((n, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_under_capacity() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(4);
        assert!(matches!(b.insert(10, 1), InsertOutcome::Inserted));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn insert_existing_replaces() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(4);
        assert!(matches!(b.insert(10, 1), InsertOutcome::Inserted));
        assert!(matches!(b.insert(10, 2), InsertOutcome::Replaced));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn insert_full_incoming_furthest_is_rejected() {
        // Buffer holds {10,11}. Incoming 12 is the furthest future value,
        // so it is rejected. The low run {10,11} stays intact.
        let mut b: PendingBuffer<u32> = PendingBuffer::new(2);
        assert!(matches!(b.insert(10, 1), InsertOutcome::Inserted));
        assert!(matches!(b.insert(11, 2), InsertOutcome::Inserted));
        let r = b.insert(12, 3);
        assert!(
            matches!(r, InsertOutcome::RejectedTooFar { nonce: 12 }),
            "got {r:?}"
        );
        assert!(b.contains(10));
        assert!(b.contains(11));
        assert!(!b.contains(12));
    }

    #[test]
    fn insert_full_incoming_lower_evicts_furthest() {
        // Buffer holds {10,12}. Incoming 11 is lower than the max (12), so
        // drop 12 and keep {10,11}. Lowest-wins tightens the drainable run
        // toward `expected`.
        let mut b: PendingBuffer<u32> = PendingBuffer::new(2);
        assert!(matches!(b.insert(10, 1), InsertOutcome::Inserted));
        assert!(matches!(b.insert(12, 2), InsertOutcome::Inserted));
        let r = b.insert(11, 3);
        assert!(
            matches!(r, InsertOutcome::EvictedFuture { evicted_nonce: 12 }),
            "got {r:?}"
        );
        assert!(b.contains(10));
        assert!(b.contains(11));
        assert!(!b.contains(12));
    }

    #[test]
    fn overflow_never_wedges_the_front_of_the_run() {
        // This is the regression this whole change exists for. `expected` is
        // 10. The sender floods far-future nonces past capacity. With
        // lowest-wins, the contiguous run 10..=13 is always kept, so when 10
        // arrives the full run drains, with no permanent gap. The old
        // evict-oldest behavior would have dropped 10 first and stranded
        // the sender forever.
        let cap = 4;
        let mut b: PendingBuffer<u32> = PendingBuffer::new(cap);
        // Buffer the run just above expected first...
        for n in 10..10 + cap as u64 {
            assert!(matches!(b.insert(n, n as u32), InsertOutcome::Inserted));
        }
        // Then flood higher nonces. Every one is rejected, and the run is untouched.
        for n in 100..120u64 {
            assert!(matches!(
                b.insert(n, n as u32),
                InsertOutcome::RejectedTooFar { .. }
            ));
        }
        // The contiguous run from 10 is fully intact.
        let drained: Vec<u64> = b.drain_consecutive_from(10).map(|(n, _)| n).collect();
        assert_eq!(drained, vec![10, 11, 12, 13]);
    }

    #[test]
    fn drain_yields_only_consecutive_run() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(8);
        b.insert(5, 50);
        b.insert(6, 60);
        b.insert(8, 80); // gap at 7
        let drained: Vec<_> = b.drain_consecutive_from(5).collect();
        assert_eq!(drained, vec![(5, 50), (6, 60)]);
        assert!(b.contains(8));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn drain_empty_when_first_missing() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(4);
        b.insert(5, 50);
        let drained: Vec<_> = b.drain_consecutive_from(3).collect();
        assert!(drained.is_empty());
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn zero_capacity_disabled() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(0);
        assert!(matches!(
            b.insert(10, 1),
            InsertOutcome::DroppedBufferDisabled
        ));
        assert_eq!(b.len(), 0);
    }
}
