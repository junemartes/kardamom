//! Per-sender future-nonce buffer.
//!
//! Bounded `BTreeMap<u64, T>` keyed by nonce. On overflow we keep the **lowest**
//! `capacity` nonces and drop the furthest-future one.
//!
//! Why lowest-wins matters: the buffer holds nonces *above* the sender's
//! expected nonce, waiting to drain as a contiguous run once the gap fills. The
//! lowest buffered nonces are the ones closest to `expected` and most likely to
//! become drainable soon; the highest are furthest away. Evicting the *smallest*
//! (the historical behaviour) punches a gap directly in front of the run, which
//! wedges the sender **permanently** — every later nonce stays "future" forever,
//! with no recovery path (2026-07-18 load-saturation campaign). Dropping the
//! *furthest-future* nonce instead never breaks the low run: the dropped tx is a
//! far-future nonce the client re-submits long before it is needed, so overflow
//! degrades to transient shedding rather than a permanent wedge.
//!
//! `drain_consecutive_from(start)` walks ascending keys and yields the
//! contiguous run starting at `start`; the first gap stops the drain.

use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    /// Buffer was full; the furthest-future buffered nonce (`evicted_nonce`) was
    /// dropped to make room for this lower nonce, preserving the drainable run.
    EvictedFuture {
        evicted_nonce: u64,
    },
    /// Buffer was full and this nonce was itself the furthest-future — rejected
    /// rather than buffered, so the lower drainable run is preserved. The
    /// client re-submits when this nonce comes back within the window.
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

    pub fn insert(&mut self, nonce: u64, value: T) -> InsertOutcome {
        if self.capacity == 0 {
            return InsertOutcome::DroppedBufferDisabled;
        }
        // We pre-compute these so the subsequent match arms don't need to
        // re-borrow `self.inner` after taking an entry handle.
        let already_present = self.inner.contains_key(&nonce);
        let at_capacity = !already_present && self.inner.len() >= self.capacity;
        if already_present {
            self.inner.insert(nonce, value);
            return InsertOutcome::Replaced;
        }
        if at_capacity {
            // Full: keep the lowest `capacity` nonces (the drainable run). The
            // furthest-future nonce is the loser — either an already-buffered
            // max, or this incoming nonce if it is itself the new max.
            let max = *self
                .inner
                .keys()
                .next_back()
                .expect("non-empty since len >= capacity >= 1");
            if nonce > max {
                // Incoming is the furthest-future: reject it, keep the run.
                return InsertOutcome::RejectedTooFar { nonce };
            }
            self.inner.remove(&max);
            self.inner.insert(nonce, value);
            return InsertOutcome::EvictedFuture { evicted_nonce: max };
        }
        self.inner.insert(nonce, value);
        InsertOutcome::Inserted
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
        // Buffer {10,11}; incoming 12 is the furthest-future → rejected, and the
        // low run {10,11} is preserved intact.
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
        // Buffer {10,12}; incoming 11 is lower than the max (12) → drop 12, keep
        // {10,11}. Lowest-wins tightens the drainable run toward `expected`.
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
        // The regression this whole change exists for. `expected` is 10; the
        // sender floods far-future nonces past capacity. With lowest-wins the
        // contiguous run 10..=13 is always retained, so when 10 arrives the full
        // run drains — no permanent gap. (Old evict-oldest would have dropped 10
        // first and stranded the sender forever.)
        let cap = 4;
        let mut b: PendingBuffer<u32> = PendingBuffer::new(cap);
        // Buffer the run just above expected first...
        for n in 10..10 + cap as u64 {
            assert!(matches!(b.insert(n, n as u32), InsertOutcome::Inserted));
        }
        // ...then flood higher nonces; every one is rejected, run untouched.
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
