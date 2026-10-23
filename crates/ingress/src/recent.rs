//! A bounded map of recent entries, oldest first.
//!
//! Many tasks read and write the map at once: the RPC tasks run on every
//! worker thread. The map is a `DashMap`, and a lock-free ring keeps the
//! insert order. The insert that fills the ring retires the oldest key. A
//! retired key that came again since then loses its newer value too. So a
//! value can leave early, but the map never holds more than its capacity.

use std::hash::Hash;
use std::num::NonZeroUsize;

use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;

/// The newest values, at most `capacity` of them, by key.
pub(crate) struct RecentMap<K, V> {
    map: DashMap<K, V>,
    order: ArrayQueue<K>,
}

impl<K, V> RecentMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            map: DashMap::new(),
            order: ArrayQueue::new(capacity.get()),
        }
    }

    /// A copy of the value of `key`.
    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Keep `value` for `key`. A full ring retires the oldest key.
    pub(crate) fn insert(&self, key: K, value: V) {
        self.map.insert(key.clone(), value);
        if let Some(oldest) = self.order.force_push(key) {
            self.map.remove(&oldest);
        }
    }

    /// Forget every entry.
    pub(crate) fn clear(&self) {
        self.map.clear();
        std::iter::from_fn(|| self.order.pop()).for_each(drop);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    pub(crate) fn ring_len(&self) -> usize {
        self.order.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(capacity: usize) -> RecentMap<u32, u32> {
        RecentMap::new(NonZeroUsize::new(capacity).unwrap())
    }

    #[test]
    fn a_value_is_kept_and_the_oldest_retires() {
        let m = map(2);
        m.insert(1, 10);
        m.insert(2, 20);
        assert_eq!(m.get(&1), Some(10));
        m.insert(3, 30);
        assert_eq!(m.get(&1), None, "the oldest key retires");
        assert_eq!((m.get(&2), m.get(&3)), (Some(20), Some(30)));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn a_clear_forgets_every_entry() {
        let m = map(4);
        m.insert(1, 10);
        m.insert(2, 20);
        m.clear();
        assert_eq!(m.get(&1), None);
        assert_eq!((m.len(), m.ring_len()), (0, 0));
    }
}
