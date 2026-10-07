//! Verification buffers: a shared, bounded, cursor-pruned core, plus the
//! replica buffers for the BAL (by block number), receipts and account rows
//! (by canonical `tx_idx`), and the per-block claim index buffer.
//!
//! A replica buffer keeps every distinct result of each key, with the
//! sessions that published it. A second result for a key never replaces
//! the first, so the consumer compares the result of every replica. A
//! session that repeats a result it already published adds nothing, so a
//! replica that restarts in a loop takes no extra space.
//!
//! The binary's Aeron subscriber tasks fill the buffers. The sync exec and
//! commit threads drain them, and wait briefly for the matching data to
//! arrive.
//!
//! # Why a `Condvar`, not a tokio channel
//!
//! The async/sync seam elsewhere in the validator uses tokio primitives
//! (`tokio::sync::mpsc`, `CancellationToken`). This buffer keeps a
//! `Mutex` + `Condvar` on purpose:
//!
//! - The consumer waits on a KEY with a DEADLINE, not on the next item.
//!   A channel is FIFO; the keyed map would still have to exist beside it,
//!   and `tokio::sync::mpsc` has no `recv_timeout` for the sync side.
//! - The wait lives entirely on the std thread. The async producer only
//!   takes the mutex for a short, await-free critical section and calls
//!   `notify_all`, which never blocks. So no tokio task ever parks on a
//!   std primitive, and the exec thread needs no runtime handle.
//! - `Condvar::wait_timeout` gives the deadline semantics `take` depends on
//!   (see the comment in [`KeyedBuffer::take`]) with one primitive.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use kardamom_types::{AccountRow, BPosition, BlockDelta, Receipt};

use crate::metrics;
use crate::replica::{Check, Distinct, ReplicaId, Skip, Taken};

/// Key of a verification buffer. It maps to the increasing index (block
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

/// What one key of a [`KeyedBuffer`] holds, and how a later insert for
/// the same key joins it.
trait Slot: Sized {
    type Item;
    fn first(item: Self::Item) -> Self;
    /// Join a later insert for the key, or say why the slot refuses it.
    fn join(&mut self, item: Self::Item) -> Result<(), Skip>;
    /// The replica results the slot holds.
    fn results(&self) -> usize;
}

/// The latest insert for a key replaces the earlier one. The claim
/// index is a scheduling hint, not a checked result, so one copy is
/// enough.
struct Latest<V>(V);

impl<V> Slot for Latest<V> {
    type Item = V;
    fn first(item: V) -> Self {
        Self(item)
    }
    fn join(&mut self, item: V) -> Result<(), Skip> {
        self.0 = item;
        Ok(())
    }
    fn results(&self) -> usize {
        1
    }
}

/// Upper bound on the distinct results of one key. Correct replicas
/// publish one value, so a second value already shows a divergence.
const MAX_DISTINCT: usize = 8;
/// Upper bound on the sessions named for one distinct result. It covers
/// the replicas, plus the new sessions of replicas that restart inside
/// the window.
const MAX_SESSIONS: usize = 32;

/// The distinct results of one key, in arrival order.
struct Results<V>(Vec<Distinct<V>>);

impl<V: PartialEq> Slot for Results<V> {
    type Item = (ReplicaId, V);
    fn first((replica, value): (ReplicaId, V)) -> Self {
        Self(vec![Distinct {
            value,
            replicas: vec![replica],
        }])
    }
    fn join(&mut self, (replica, value): (ReplicaId, V)) -> Result<(), Skip> {
        let full = self.0.len() >= MAX_DISTINCT;
        match self.0.iter_mut().find(|d| d.value == value) {
            Some(d) if d.replicas.contains(&replica) => Err(Skip::Repeat),
            Some(d) if d.replicas.len() >= MAX_SESSIONS => Err(Skip::Bound),
            Some(d) => {
                d.replicas.push(replica);
                Ok(())
            }
            None if full => Err(Skip::Bound),
            None => {
                self.0.push(Distinct {
                    value,
                    replicas: vec![replica],
                });
                Ok(())
            }
        }
    }
    fn results(&self) -> usize {
        Distinct::results(&self.0)
    }
}

/// The limits of one buffer. The distances are in key index units.
#[derive(Clone, Copy)]
struct Bounds {
    /// Max retained keys. On overflow, the highest key is evicted, so a
    /// key far ahead never pushes out a key near the consumer. The
    /// consumer treats missing data as "could not verify", never as a
    /// divergence, so eviction can only leave a key unverified.
    cap: NonZeroUsize,
    /// Catch-up skip horizon. See [`KeyedBuffer::take_step`].
    lookbehind: u64,
    /// How far below the consumer's cursor an insert is still taken. The
    /// next take hands it over as a late entry. Zero keeps only the
    /// cursor key itself.
    late_window: u64,
    /// How far above the consumer's cursor an insert is still taken. A
    /// key beyond it is a wrong or wrapped key, not a live one.
    reach: u64,
}

/// How many buffered keys must lie beyond the catch-up horizon before
/// the consumer skips a key. A real live head is a run of keys. One
/// wrong key far ahead does not turn the skip on.
const SKIP_EVIDENCE: usize = 4;

/// Shared core of the replica buffers and [`ClaimBuffer`]. The producer
/// task inserts values. The sync consumer thread calls `take` in
/// increasing key order, and waits briefly for matching data. The buffer
/// is bounded and cursor-pruned, so late or stale data can never leak:
/// each take hands over every entry below its key, and an insert outside
/// the window around the cursor is refused.
struct KeyedBuffer<K, S> {
    inner: Mutex<KeyedInner<K, S>>,
    cv: Condvar,
    bounds: Bounds,
}

struct KeyedInner<K, S> {
    map: BTreeMap<K, S>,
    /// Index of the latest key requested by `take`. Requests only
    /// increase. An insert more than the late window below this index is
    /// for a key that the consumer can no longer use, so the buffer
    /// refuses it. This stops the buffer from holding dead entries.
    cursor: Option<u64>,
}

/// What an insert did: the reason the buffer refused the item, and how
/// many replica results the cap evicted.
struct Inserted {
    refused: Option<Skip>,
    evicted: usize,
}

/// What a take hands over: the slot of the key, the slots below it, and
/// how many replica results it dropped beyond the reach.
struct Took<K, S> {
    current: Option<S>,
    below: BTreeMap<K, S>,
    beyond: usize,
}

/// One cycle of [`KeyedBuffer::take`]'s wait loop.
enum TakeStep<'a, K, S> {
    /// The final result: found, or given up on.
    Done(Option<S>),
    /// Not ready yet; wait another cycle with this guard.
    Retry(std::sync::MutexGuard<'a, KeyedInner<K, S>>),
}

impl<K: BufKey, S: Slot> KeyedBuffer<K, S> {
    fn new(bounds: Bounds) -> Self {
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
    fn insert(&self, key: K, item: S::Item) -> Inserted {
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
        let mut g = self
            .inner
            .lock()
            .expect("verification buffer lock poisoned");
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
    /// current slot is `None` if it never arrives. The caller treats that
    /// as "could not verify", never as a divergence.
    fn take(&self, key: K, timeout: Duration) -> Took<K, S> {
        // Use a deadline, not a fresh timeout per wakeup. Inserts for other
        // keys call notify_all on every block (about 250ms to 2s on a live
        // chain). A fresh timeout per wakeup would mean a wait for a key
        // that never arrives never times out, and the consumer hangs
        // forever on one lost item while the buffer keeps filling.
        let deadline = std::time::Instant::now() + timeout;
        let mut g = self
            .inner
            .lock()
            .expect("verification buffer lock poisoned");
        // Requests only increase: everything below `key` goes to the
        // caller now. Remember the cursor, so inserts outside the window
        // are refused. Entries that arrived before the first take can
        // lie beyond the reach; they go now.
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
    /// final value at the deadline). `Done` carries the result to
    /// return; `Retry` carries the guard for another cycle.
    fn take_step<'a>(
        &'a self,
        mut g: std::sync::MutexGuard<'a, KeyedInner<K, S>>,
        key: &K,
        deadline: std::time::Instant,
    ) -> TakeStep<'a, K, S> {
        if let Some(v) = g.map.remove(key) {
            return TakeStep::Done(Some(v));
        }
        // Catch-up check: if the live head (a run of buffered keys) is
        // far ahead of `key`, this item has aged out of the live stream's
        // buffer and will never arrive. Return None now instead of
        // waiting out the timeout, so the validator catches up fast after
        // a cold start or a lapse longer than the live buffer. A
        // caught-up validator asks for keys near the head, so this check
        // never triggers and verification runs as normal.
        let horizon = K::from_index(
            key.index()
                .saturating_add(self.bounds.lookbehind)
                .saturating_add(1),
        );
        if g.map.range(horizon..).nth(SKIP_EVIDENCE - 1).is_some() {
            return TakeStep::Done(None);
        }
        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            return TakeStep::Done(g.map.remove(key));
        };
        let (mut g2, wait) = self
            .cv
            .wait_timeout(g, remaining)
            .expect("verification buffer lock poisoned");
        if wait.timed_out() {
            return TakeStep::Done(g2.map.remove(key));
        }
        TakeStep::Retry(g2)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("verification buffer lock poisoned")
            .map
            .len()
    }
}

/// Buffer of one kind of executor result: every distinct result of each
/// key, with the sessions that published it. The binary's subscriber
/// task calls [`insert`](Self::insert); the sync consumer thread calls
/// [`take`](Self::take). Every result the buffer does not hand over
/// counts in `validator_replica_results_unchecked_total`.
pub struct ReplicaBuffer<K, V> {
    core: KeyedBuffer<K, Results<V>>,
    check: Check,
}

impl<K: BufKey, V: PartialEq> ReplicaBuffer<K, V> {
    fn with(check: Check, bounds: Bounds) -> Self {
        Self {
            core: KeyedBuffer::new(bounds),
            check,
        }
    }

    /// Insert the result `value` that `replica` published for `key`.
    pub fn insert(&self, key: K, replica: ReplicaId, value: V) {
        let inserted = self.core.insert(key, (replica, value));
        if let Some(skip) = inserted.refused {
            metrics::counter_replica_unchecked(self.check, skip, 1);
        }
        metrics::counter_replica_unchecked(self.check, Skip::Evicted, inserted.evicted);
    }

    /// Take the results for `key`, and wait up to `timeout` for the first
    /// to arrive. Also hands over the results of lower keys that arrived
    /// after their take. An empty `current` means "could not verify", not
    /// a divergence. See [`KeyedBuffer::take`] for the deadline rule and
    /// [`KeyedBuffer::take_step`] for the catch-up skip.
    pub fn take(&self, key: K, timeout: Duration) -> Taken<K, V> {
        let took = self.core.take(key, timeout);
        metrics::counter_replica_unchecked(self.check, Skip::Ahead, took.beyond);
        Taken {
            current: took.current.map(|r| r.0).unwrap_or_default(),
            late: took.below.into_iter().map(|(k, r)| (k, r.0)).collect(),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.core.len()
    }
}

/// Buffer of executor-published BALs, keyed by block number. The Aeron
/// `tx_bal` subscriber task inserts; the sync exec thread takes, and
/// waits briefly for the matching block.
pub type BalBuffer = ReplicaBuffer<u64, BlockDelta>;

impl BalBuffer {
    /// How far below the live head a requested block must be to count
    /// as unrecoverable backlog. Its BAL has aged out of the live
    /// `tx_bal` multicast buffer and will never arrive, so the validator
    /// commits it unverified at once instead of waiting. A caught-up
    /// validator asks for blocks near the head (a smaller lag than this
    /// value), so it always waits and verifies. Only a validator catching
    /// up from a cold start, or after a lapse longer than the multicast
    /// buffer, skips the wait.
    pub(crate) const BACKLOG_LOOKBEHIND: u64 = 16;
    /// Bound on buffered blocks. Each block holds at most `MAX_DISTINCT`
    /// whole `BlockDelta` values, the heavyweight case. This is about 17
    /// to 35 minutes of chain at a 250ms-to-2s block rate, well beyond the
    /// verify window, so eviction fires only if the consumer stalls
    /// outright.
    pub(crate) const MAX_BUFFERED: NonZeroUsize =
        NonZeroUsize::new(1024).expect("compile-time constant");
    /// How many blocks below the checked block a replica's BAL is still
    /// compared: a replica that lags the validator by up to this many
    /// blocks is checked. The validator keeps one checked write-set for
    /// each block of the window.
    pub(crate) const CHECK_WINDOW: u64 = 64;
    /// How many blocks above the checked block a BAL is still taken:
    /// about 3 days of chain at 250ms blocks. A validator further behind
    /// than this rebuilds from a peer checkpoint, not by catch-up.
    pub(crate) const REACH: u64 = 1 << 20;
    const BOUNDS: Bounds = Bounds {
        cap: Self::MAX_BUFFERED,
        lookbehind: Self::BACKLOG_LOOKBEHIND,
        late_window: Self::CHECK_WINDOW,
        reach: Self::REACH,
    };

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::with(Check::Bal, Self::BOUNDS))
    }

    #[cfg(test)]
    fn with_cap(cap: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self::with(
            Check::Bal,
            Bounds {
                cap,
                ..Self::BOUNDS
            },
        ))
    }
}

/// Buffer of executor-published receipts and account rows, both keyed by
/// canonical `tx_idx`. The `tx_receipts` subscriber task fills it; the
/// commit thread drains it.
pub struct ReceiptBuffer {
    pub receipts: ReplicaBuffer<BPosition, Receipt>,
    /// Published account rows, keyed by their batch's end position. Only a
    /// batch end carries rows, so most positions have no entry. The rows
    /// travel in the frame that carried the end receipt, so they are
    /// present whenever that receipt is, and the commit thread takes them
    /// with no wait. Rows have no late window: rows that arrive after the
    /// commit thread passed their position meet a later local state.
    pub rows: ReplicaBuffer<BPosition, Vec<AccountRow>>,
}

impl ReceiptBuffer {
    /// This mirrors [`BalBuffer::BACKLOG_LOOKBEHIND`], but in canonical
    /// records rather than blocks. When the buffered head is this far
    /// ahead of the requested `tx_idx`, the executor's receipt for the
    /// requested tx has aged out of the live `tx_receipts` stream and will
    /// never arrive. The buffer skips it at once, marked unverified,
    /// instead of blocking the commit thread for the full wait per
    /// historical tx. 4096 records is about the same reach as the BAL
    /// heuristic's 16 blocks, at a few hundred tx per block. A caught-up
    /// validator's requests trail the head by less than this, so it always
    /// waits and verifies.
    const BACKLOG_LOOKBEHIND: u64 = 4096;
    /// Bound on buffered positions. Each position holds at most
    /// `MAX_DISTINCT` receipts. Receipts are small structs; this cap is
    /// only a leak guard.
    const MAX_BUFFERED: NonZeroUsize = NonZeroUsize::new(1 << 16).expect("compile-time constant");
    /// How many canonical records below the checked one a replica's
    /// receipt is still compared. The validator keeps one checked receipt
    /// for each record of the window.
    pub(crate) const CHECK_WINDOW: u64 = 4096;
    /// How many canonical records above the checked one a receipt is still
    /// taken: [`BalBuffer::REACH`] blocks of 4096 records.
    const REACH: u64 = 1 << 32;
    const BOUNDS: Bounds = Bounds {
        cap: Self::MAX_BUFFERED,
        lookbehind: Self::BACKLOG_LOOKBEHIND,
        late_window: Self::CHECK_WINDOW,
        reach: Self::REACH,
    };

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            receipts: ReplicaBuffer::with(Check::Receipt, Self::BOUNDS),
            rows: ReplicaBuffer::with(
                Check::Rows,
                Bounds {
                    late_window: 0,
                    ..Self::BOUNDS
                },
            ),
        })
    }
}

/// Per-block EIP-7928 claim index for parallel validation. This is separate
/// from [`BalBuffer`], so the merged-delta check path stays untouched. A
/// missing claim index falls back to sequential re-execution, never to a
/// gap in verification.
pub struct ClaimBuffer {
    core: KeyedBuffer<u64, Latest<(NonZeroU16, Arc<crate::parallel::ClaimIndex>)>>,
}

impl Default for ClaimBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(Bounds {
                late_window: 0,
                ..BalBuffer::BOUNDS
            }),
        }
    }
}

impl ClaimBuffer {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Insert a block's claims with the granularity the frame declared. The
    /// validator's view of the ladder must come from the wire, from what
    /// the executor actually produced, never from local config. Zero is
    /// never a legal granularity; the caller parses it once, at the wire
    /// boundary (`bin/kardamom-validator/pumps.rs`, `BalPump::index_claims`), so it
    /// is already a `NonZeroU16` by the time it reaches here.
    pub fn insert(&self, block: u64, granularity: NonZeroU16, claims: crate::parallel::ClaimIndex) {
        self.core.insert(block, (granularity, Arc::new(claims)));
    }

    /// [`insert`](Self::insert) for an already-shared index — the BAL pump
    /// decodes each frame once and feeds both the parallel engine's buffer
    /// and the outbox extractor's without cloning the index.
    pub fn insert_arc(
        &self,
        block: u64,
        granularity: NonZeroU16,
        claims: Arc<crate::parallel::ClaimIndex>,
    ) {
        self.core.insert(block, (granularity, claims));
    }

    /// Take `block`'s claims, and wait up to `timeout`. On `None`, the
    /// caller re-executes this block sequentially.
    pub fn take(
        &self,
        block: u64,
        timeout: Duration,
    ) -> Option<(NonZeroU16, Arc<crate::parallel::ClaimIndex>)> {
        self.core
            .take(block, timeout)
            .current
            .map(|latest| latest.0)
    }
}

#[cfg(test)]
#[path = "buffers_tests.rs"]
mod tests;
