//! Per-sender future-nonce buffer.
//!
//! Bounded `BTreeMap<u64, T>` keyed by nonce. On overflow the smallest nonce
//! is evicted (LRU-by-nonce — says "evict the oldest to make room").
//! `drain_consecutive_from(start)` walks ascending keys and yields the
//! contiguous run starting at `start`; the first gap stops the drain.

use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    EvictedOldest { evicted_nonce: u64 },
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
            let oldest = *self
                .inner
                .keys()
                .next()
                .expect("non-empty since len >= capacity >= 1");
            self.inner.remove(&oldest);
            self.inner.insert(nonce, value);
            return InsertOutcome::EvictedOldest {
                evicted_nonce: oldest,
            };
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
    fn insert_full_evicts_oldest() {
        let mut b: PendingBuffer<u32> = PendingBuffer::new(2);
        assert!(matches!(b.insert(10, 1), InsertOutcome::Inserted));
        assert!(matches!(b.insert(11, 2), InsertOutcome::Inserted));
        let r = b.insert(12, 3);
        let InsertOutcome::EvictedOldest { evicted_nonce } = r else {
            panic!("expected eviction, got {r:?}");
        };
        assert_eq!(evicted_nonce, 10);
        assert!(b.contains(11));
        assert!(b.contains(12));
        assert!(!b.contains(10));
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
