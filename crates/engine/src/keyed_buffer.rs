//! A bounded, cursor-pruned buffer keyed by an increasing index. A
//! producer thread inserts values. One consumer thread takes them in
//! increasing key order, and waits briefly for a key that has not arrived.
//!
//! The validator keeps its replica results here (the BAL by block number,
//! the receipts by canonical index). A consumer of the executor stream
//! keeps the records of each canonical index here.
//!
//! # Why a `Condvar`, not a channel
//!
//! - The consumer waits on a KEY with a DEADLINE, not on the next item.
//!   A channel is FIFO. The keyed map would still have to exist beside
//!   it, and `tokio::sync::mpsc` has no `recv_timeout` for the sync side.
//! - The producer only takes the mutex for a short critical section with
//!   no await in it, and calls `notify_all`, which never blocks. So no
//!   tokio task parks on a std primitive.
//! - `Condvar::wait_timeout` gives the deadline rule of
//!   [`KeyedBuffer::take`] with one primitive.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroUsize;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use kardamom_types::BPosition;

/// Key of a [`KeyedBuffer`]. It maps to the increasing index (block
/// number or canonical record index) that the catch-up and pruning logic
/// uses.
pub trait BufKey: Ord + Copy {
    fn index(self) -> u64;
    fn from_index(index: u64) -> Self;
}

impl BufKey for u64 {
    fn index(self) -> u64 {
        self
    }
    fn from_index(index: u64) -> Self {
        index
    }
}

impl BufKey for BPosition {
    fn index(self) -> u64 {
        self.as_index()
    }
    fn from_index(index: u64) -> Self {
        BPosition::from_index(index)
    }
}

/// Why a buffer refused an item, or did not hand it over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    /// It arrived below the window of the consumer's cursor, or its key
    /// has no checked result left.
    Late,
    /// The key already holds the same item from the same source.
    Repeat,
    /// The key already holds the bound of distinct items, or its item
    /// already names the bound of sources.
    Bound,
    /// The buffer was full, and its key was the highest one.
    Evicted,
    /// Its key is more than the reach above the consumer's cursor.
    Ahead,
}

impl Skip {
    /// The stable id: the `reason` label of a metric.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Late => "late",
            Self::Repeat => "repeat",
            Self::Bound => "bound",
            Self::Evicted => "evicted",
            Self::Ahead => "ahead",
        }
    }
}

/// What one key of a [`KeyedBuffer`] holds, and how a later insert for
/// the same key joins it.
pub trait Slot: Sized {
    type Item;
    fn first(item: Self::Item) -> Self;
    /// Join a later insert for the key, or say why the slot refuses it.
    ///
    /// # Errors
    ///
    /// Returns the reason the slot refuses `item`.
    fn join(&mut self, item: Self::Item) -> Result<(), Skip>;
    /// The items the slot holds.
    fn results(&self) -> usize;
}

/// The limits of one buffer. The distances are in key index units.
#[derive(Clone, Copy)]
pub struct Bounds {
    /// Max retained keys. On overflow, the highest key is evicted, so a
    /// key far ahead never pushes out a key near the consumer.
    pub cap: NonZeroUsize,
    /// Catch-up skip horizon. See [`KeyedBuffer::take`].
    pub lookbehind: u64,
    /// How far below the consumer's cursor an insert is still taken. The
    /// next take hands it over as a late entry. Zero keeps only the
    /// cursor key itself.
    pub late_window: u64,
    /// How far above the consumer's cursor an insert is still taken. A
    /// key beyond it is a wrong or wrapped key, not a live one.
    pub reach: u64,
}

/// How many buffered keys must lie beyond the catch-up horizon before
/// the consumer skips a key. A real live head is a run of keys. One
/// wrong key far ahead does not turn the skip on.
const SKIP_EVIDENCE: usize = 4;

/// The buffer. Each take hands over every entry below its key, and an
/// insert outside the window around the cursor is refused, so late or
/// stale data cannot stay in it.
pub struct KeyedBuffer<K, S> {
    inner: Mutex<KeyedInner<K, S>>,
    cv: Condvar,
    bounds: Bounds,
}

struct KeyedInner<K, S> {
    map: BTreeMap<K, S>,
    /// Index of the latest key requested by `take`. Requests only
    /// increase.
    cursor: Option<u64>,
}

/// What an insert did: the reason the buffer refused the item, and how
/// many items the cap evicted.
pub struct Inserted {
    pub refused: Option<Skip>,
    pub evicted: usize,
}

/// What a take hands over: the slot of the key, the slots below it, and
/// how many items it dropped beyond the reach.
pub struct Took<K, S> {
    pub current: Option<S>,
    pub below: BTreeMap<K, S>,
    pub beyond: usize,
}

/// One cycle of [`KeyedBuffer::take`]'s wait loop.
enum TakeStep<'a, K, S> {
    /// The final result: found, or given up on.
    Done(Option<S>),
    /// Not ready yet; wait another cycle with this guard.
    Retry(MutexGuard<'a, KeyedInner<K, S>>),
}

impl<K: BufKey, S: Slot> KeyedBuffer<K, S> {
    #[must_use]
    pub fn new(bounds: Bounds) -> Self {
        Self {
            inner: Mutex::new(KeyedInner {
                map: BTreeMap::new(),
                cursor: None,
            }),
            cv: Condvar::new(),
            bounds,
        }
    }

    /// Insert `item` under `key`, and wake the waiters when the buffer
    /// took it.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned: a thread panicked inside it.
    pub fn insert(&self, key: K, item: S::Item) -> Inserted {
        let inserted = self.insert_locked(key, item);
        if inserted.refused.is_none() {
            self.cv.notify_all();
        }
        inserted
    }

    /// Insert `item` under the lock, and release the lock when this
    /// returns. One insert adds at most one key, so at most one key is
    /// evicted.
    fn insert_locked(&self, key: K, item: S::Item) -> Inserted {
        let mut g = self.inner.lock().expect("keyed buffer lock poisoned");
        let refused =
            g.cursor
                .and_then(|c| self.outside(key, c))
                .or_else(|| match g.map.entry(key) {
                    Entry::Vacant(e) => {
                        e.insert(S::first(item));
                        None
                    }
                    Entry::Occupied(mut e) => e.get_mut().join(item).err(),
                });
        let evicted = (g.map.len() > self.bounds.cap.get())
            .then(|| g.map.pop_last())
            .flatten()
            .map_or(0, |(_, slot)| slot.results());
        Inserted { refused, evicted }
    }

    /// Why `key` is outside the window around `cursor`, if it is.
    fn outside(&self, key: K, cursor: u64) -> Option<Skip> {
        let index = key.index();
        if index < cursor.saturating_sub(self.bounds.late_window) {
            return Some(Skip::Late);
        }
        (index > cursor.saturating_add(self.bounds.reach)).then_some(Skip::Ahead)
    }

    /// Take the slot for `key`, and wait up to `timeout` for it to
    /// arrive. Also hands over every slot below `key`: the entries that
    /// arrived after their own take, or that no take requested. The
    /// current slot is `None` if it never arrives.
    ///
    /// The take ends early with `None` when a run of keys lies more than
    /// the lookbehind above `key`: the live head is far ahead, so the
    /// item aged out of the live stream and does not arrive.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned: a thread panicked inside it.
    pub fn take(&self, key: K, timeout: Duration) -> Took<K, S> {
        // Use a deadline, not a fresh timeout per wakeup. Inserts for
        // other keys call notify_all all the time. A fresh timeout per
        // wakeup would mean a wait for a key that never arrives never
        // times out.
        let deadline = Instant::now() + timeout;
        let mut g = self.inner.lock().expect("keyed buffer lock poisoned");
        // Requests only increase: everything below `key` goes to the
        // caller now. Entries that arrived before the first take can lie
        // beyond the reach; they go now.
        let cursor = g.cursor.map_or(key.index(), |c| c.max(key.index()));
        g.cursor = Some(cursor);
        let beyond = g.map.split_off(&K::from_index(
            cursor.saturating_add(self.bounds.reach).saturating_add(1),
        ));
        let upper = g.map.split_off(&key);
        let below = std::mem::replace(&mut g.map, upper);
        let beyond = beyond.values().map(Slot::results).sum();
        loop {
            match self.take_step(g, &key, deadline) {
                TakeStep::Done(current) => {
                    return Took {
                        current,
                        below,
                        beyond,
                    };
                }
                TakeStep::Retry(next_g) => g = next_g,
            }
        }
    }

    /// One wait cycle of [`Self::take`]: try the value, then the
    /// catch-up check, then wait out the remaining deadline (or take the
    /// final value at the deadline).
    fn take_step<'a>(
        &'a self,
        mut g: MutexGuard<'a, KeyedInner<K, S>>,
        key: &K,
        deadline: Instant,
    ) -> TakeStep<'a, K, S> {
        if let Some(v) = g.map.remove(key) {
            return TakeStep::Done(Some(v));
        }
        let horizon = K::from_index(
            key.index()
                .saturating_add(self.bounds.lookbehind)
                .saturating_add(1),
        );
        if g.map.range(horizon..).nth(SKIP_EVIDENCE - 1).is_some() {
            return TakeStep::Done(None);
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return TakeStep::Done(g.map.remove(key));
        };
        let (mut g2, wait) = self
            .cv
            .wait_timeout(g, remaining)
            .expect("keyed buffer lock poisoned");
        if wait.timed_out() {
            return TakeStep::Done(g2.map.remove(key));
        }
        TakeStep::Retry(g2)
    }

    /// The count of buffered keys.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned: a thread panicked inside it.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("keyed buffer lock poisoned")
            .map
            .len()
    }

    /// Whether no key is buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
