//! Verification buffers: a shared, bounded, cursor-pruned core, plus typed
//! wrappers for the BAL (by block number), receipts (by canonical `tx_idx`),
//! and per-block claim indexes.
//!
//! The BAL and receipt buffers keep one result for each executor replica.
//! A second result for a key never replaces the first, so the consumer
//! compares the result of every replica.
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
use crate::replica::{Arrival, Check, ReplicaId, Taken};

/// Key of a verification buffer. It maps to the increasing index (block
/// number or canonical record index) that the catch-up and pruning logic
/// uses.
pub(crate) trait BufKey: Ord + Copy {
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
    /// Join a later insert for the key. Returns whether the slot took it.
    fn join(&mut self, item: Self::Item) -> bool;
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
    fn join(&mut self, item: V) -> bool {
        self.0 = item;
        true
    }
}

/// Upper bound on the replica results of one key. It covers the
/// executor replicas, plus the new sessions of replicas that restart
/// inside the window.
const MAX_REPLICAS: usize = 8;

/// The results of one key, at most one for each replica, in arrival
/// order. A second result of a replica for the key is dropped, so each
/// replica's result is checked once.
struct Arrivals<V>(Vec<Arrival<V>>);

impl<V> Slot for Arrivals<V> {
    type Item = Arrival<V>;
    fn first(item: Arrival<V>) -> Self {
        Self(vec![item])
    }
    fn join(&mut self, item: Arrival<V>) -> bool {
        let new = self.0.len() < MAX_REPLICAS && self.0.iter().all(|a| a.replica != item.replica);
        if new {
            self.0.push(item);
        }
        new
    }
}

impl<K: Copy, V> Taken<K, V> {
    /// The take result of a replica buffer: the slot of the key, and the
    /// slots of the lower keys.
    fn of(current: Option<Arrivals<V>>, below: BTreeMap<K, Arrivals<V>>) -> Self {
        Self {
            current: current.map(|a| a.0).unwrap_or_default(),
            late: below
                .into_iter()
                .flat_map(|(key, slot)| slot.0.into_iter().map(move |a| (key, a)))
                .collect(),
        }
    }
}

/// Shared core of [`BalBuffer`], [`ReceiptBuffer`] and [`ClaimBuffer`].
/// The producer task inserts values. The sync consumer thread calls
/// `take` in increasing key order, and waits briefly for matching data.
/// The buffer is bounded and cursor-pruned, so late or stale data can
/// never leak: each take hands over every entry below its key, and an
/// insert below the late window is dropped.
struct KeyedBuffer<K: BufKey, S> {
    inner: Mutex<KeyedInner<K, S>>,
    cv: Condvar,
    /// Max retained keys. On overflow, the oldest key is evicted. The
    /// consumer treats missing data as "could not verify", never as a
    /// divergence, so eviction can only leave a block or tx unverified.
    cap: NonZeroUsize,
    /// Catch-up skip horizon, in index units. See [`take`](Self::take).
    lookbehind: u64,
    /// How far below the consumer's cursor an insert is still taken, in
    /// index units. The next take hands it over as a late entry. Zero
    /// keeps only the cursor key itself.
    late_window: u64,
}

struct KeyedInner<K: BufKey, S> {
    map: BTreeMap<K, S>,
    /// Index of the latest key requested by `take`. Requests only
    /// increase. An insert more than the late window below this index is
    /// for a key that the consumer can no longer use, so the buffer drops
    /// it. This stops the buffer from holding dead entries.
    cursor: Option<u64>,
}

/// One cycle of [`KeyedBuffer::take`]'s wait loop.
enum TakeStep<'a, K: BufKey, S> {
    /// The final result: found, or given up on.
    Done(Option<S>),
    /// Not ready yet; wait another cycle with this guard.
    Retry(std::sync::MutexGuard<'a, KeyedInner<K, S>>),
}

impl<K: BufKey, S: Slot> KeyedBuffer<K, S> {
    fn new(cap: NonZeroUsize, lookbehind: u64, late_window: u64) -> Self {
        Self {
            inner: Mutex::new(KeyedInner {
                map: BTreeMap::new(),
                cursor: None,
            }),
            cv: Condvar::new(),
            cap,
            lookbehind,
            late_window,
        }
    }

    /// Insert `item` under `key`. Returns whether the buffer took it: an
    /// insert below the late window is dropped, and the slot can refuse
    /// it.
    fn insert(&self, key: K, item: S::Item) -> bool {
        let taken = self.insert_locked(key, item);
        if taken {
            self.cv.notify_all();
        }
        taken
    }

    /// Insert `item` under the lock, and release the lock when this
    /// returns. Returns whether the map changed, so the caller knows to
    /// notify waiters.
    fn insert_locked(&self, key: K, item: S::Item) -> bool {
        let mut g = self
            .inner
            .lock()
            .expect("verification buffer lock poisoned");
        // No future take hands over a key below the late window.
        if g.cursor
            .is_some_and(|c| key.index() < c.saturating_sub(self.late_window))
        {
            return false;
        }
        let taken = match g.map.entry(key) {
            Entry::Vacant(e) => {
                e.insert(S::first(item));
                true
            }
            Entry::Occupied(mut e) => e.get_mut().join(item),
        };
        while g.map.len() > self.cap.get() {
            g.map.pop_first();
        }
        taken
    }

    /// Take the slot for `key`, and wait up to `timeout` for it to
    /// arrive. Also returns every slot below `key`: the entries that
    /// arrived after their own take, or that no take requested. The
    /// current slot is `None` if it never arrives. The caller treats that
    /// as "could not verify", never as a divergence.
    fn take(&self, key: K, timeout: Duration) -> (Option<S>, BTreeMap<K, S>) {
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
        // caller now. Remember the cursor, so inserts below the late
        // window are dropped.
        g.cursor = Some(g.cursor.map_or(key.index(), |c| c.max(key.index())));
        let upper = g.map.split_off(&key);
        let below = std::mem::replace(&mut g.map, upper);
        loop {
            match self.take_step(g, &key, deadline) {
                TakeStep::Done(result) => return (result, below),
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
        // Catch-up check: if the live head (the highest buffered key) is
        // far ahead of `key`, this item has aged out of the live
        // stream's buffer and will never arrive. Return None now
        // instead of waiting out the timeout, so the validator catches
        // up fast after a cold start or a lapse longer than the live
        // buffer. A caught-up validator asks for keys near the head, so
        // this check never triggers and verification runs as normal.
        if let Some((&head, _)) = g.map.last_key_value()
            && head.index() > key.index().saturating_add(self.lookbehind)
        {
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

/// Buffer of executor-published BALs, keyed by block number, with one
/// BAL for each replica. The Aeron `tx_bal` subscriber task calls
/// [`insert`](Self::insert). The sync exec thread calls
/// [`take`](Self::take), and waits briefly for the matching block.
pub struct BalBuffer {
    core: KeyedBuffer<u64, Arrivals<BlockDelta>>,
}

impl Default for BalBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(
                Self::MAX_BUFFERED,
                Self::BACKLOG_LOOKBEHIND,
                Self::CHECK_WINDOW,
            ),
        }
    }
}

impl BalBuffer {
    /// How far below the live head (the highest buffered block) a
    /// requested block must be to count as unrecoverable backlog. Its BAL
    /// has aged out of the live `tx_bal` multicast buffer and will never
    /// arrive, so the validator commits it unverified at once instead of
    /// waiting. A caught-up validator asks for blocks near the head (a
    /// smaller lag than this value), so it always waits and verifies.
    /// Only a validator catching up from a cold start, or after a lapse
    /// longer than the multicast buffer, skips the wait.
    pub(crate) const BACKLOG_LOOKBEHIND: u64 = 16;
    /// Bound on buffered blocks. Each block holds at most
    /// `MAX_REPLICAS` whole `BlockDelta` values, the heavyweight case.
    /// This is about 17 to 35 minutes of chain at a 250ms-to-2s block
    /// rate, well beyond the verify window, so eviction fires only if the
    /// consumer stalls outright.
    pub(crate) const MAX_BUFFERED: NonZeroUsize =
        NonZeroUsize::new(1024).expect("compile-time constant");
    /// How many blocks below the checked block a replica's BAL is still
    /// compared: a replica that lags the validator by up to this many
    /// blocks is checked. The validator keeps one checked write-set for
    /// each block of the window.
    pub(crate) const CHECK_WINDOW: u64 = 64;

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[cfg(test)]
    fn with_cap(cap: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            core: KeyedBuffer::new(cap, Self::BACKLOG_LOOKBEHIND, Self::CHECK_WINDOW),
        })
    }

    /// Insert the BAL that `replica` published. A BAL below the check
    /// window, or a second BAL of the replica for the block, is not
    /// compared, and counts as unchecked.
    pub fn insert(&self, replica: ReplicaId, delta: BlockDelta) {
        let block = delta.block_number;
        if !self.core.insert(
            block,
            Arrival {
                replica,
                value: delta,
            },
        ) {
            metrics::counter_replica_unchecked(Check::Bal);
        }
    }

    /// Take the replica BALs for `block`, and wait up to `timeout` for
    /// the first to arrive. Also hands over the BALs of lower blocks that
    /// arrived after their take. An empty `current` means "could not
    /// verify", not a divergence. See [`KeyedBuffer::take`] for the
    /// deadline and catch-up rules.
    pub fn take(&self, block: u64, timeout: Duration) -> Taken<u64, BlockDelta> {
        let (current, below) = self.core.take(block, timeout);
        Taken::of(current, below)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.core.len()
    }
}

/// Buffer of executor-published receipts, keyed by canonical `tx_idx`,
/// with one receipt for each replica. The `tx_receipts` subscriber task
/// fills it; the commit thread drains it.
pub struct ReceiptBuffer {
    core: KeyedBuffer<BPosition, Arrivals<Receipt>>,
    /// Published account rows, keyed by their batch's end position, with
    /// one set of rows for each replica. Only a batch end carries rows,
    /// so most positions have no entry. The rows travel in the frame that
    /// carried the end receipt, so they are present whenever that receipt
    /// is, and the commit thread takes them with no wait.
    rows: KeyedBuffer<BPosition, Arrivals<Vec<AccountRow>>>,
}

impl Default for ReceiptBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(
                Self::MAX_BUFFERED,
                Self::BACKLOG_LOOKBEHIND,
                Self::CHECK_WINDOW,
            ),
            rows: KeyedBuffer::new(Self::MAX_BUFFERED, Self::BACKLOG_LOOKBEHIND, 0),
        }
    }
}

impl ReceiptBuffer {
    /// This mirrors [`BalBuffer::BACKLOG_LOOKBEHIND`], but in canonical
    /// records rather than blocks. When the highest buffered `tx_idx` is
    /// this far ahead of the requested one, the executor's receipt for the
    /// requested tx has aged out of the live `tx_receipts` stream and will
    /// never arrive. The buffer skips it at once, marked unverified,
    /// instead of blocking the commit thread for the full wait per
    /// historical tx. 4096 records is about the same reach as the BAL
    /// heuristic's 16 blocks, at a few hundred tx per block. A caught-up
    /// validator's requests trail the head by less than this, so it always
    /// waits and verifies.
    const BACKLOG_LOOKBEHIND: u64 = 4096;
    /// Bound on buffered positions. Each position holds at most
    /// `MAX_REPLICAS` receipts. Receipts are small structs; this cap is
    /// only a leak guard.
    const MAX_BUFFERED: NonZeroUsize = NonZeroUsize::new(1 << 16).expect("compile-time constant");
    /// How many canonical records below the checked one a replica's
    /// receipt is still compared. The validator keeps one checked receipt
    /// for each record of the window.
    pub(crate) const CHECK_WINDOW: u64 = 4096;

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Insert the receipt that `replica` published. A receipt below the
    /// check window, or a second receipt of the replica for the position,
    /// is not compared, and counts as unchecked.
    pub fn insert(&self, replica: ReplicaId, receipt: Receipt) {
        let idx = receipt.tx_idx;
        if !self.core.insert(
            idx,
            Arrival {
                replica,
                value: receipt,
            },
        ) {
            metrics::counter_replica_unchecked(Check::Receipt);
        }
    }

    /// Take the replica receipts for `idx`, and wait up to `timeout` for
    /// the first to arrive. Also hands over the receipts of lower
    /// positions that arrived after their take. See [`KeyedBuffer::take`]
    /// for the deadline rules and the aged-out catch-up skip.
    pub fn take(&self, idx: BPosition, timeout: Duration) -> Taken<BPosition, Receipt> {
        let (current, below) = self.core.take(idx, timeout);
        Taken::of(current, below)
    }

    /// Record the account rows of a batch that `replica` published, under
    /// the batch's end position. A frame that arrives after the commit
    /// thread passed that position is dropped, and its rows count as
    /// unverified.
    pub fn insert_rows(&self, replica: ReplicaId, end: BPosition, rows: Vec<AccountRow>) {
        if !self.rows.insert(
            end,
            Arrival {
                replica,
                value: rows,
            },
        ) {
            metrics::counter_rows_unverified(1);
        }
    }

    /// The published rows of the batches that end at `idx`, one set for
    /// each replica whose frame has arrived. No wait. `late` holds the
    /// rows that arrived after the commit thread passed their position.
    pub fn take_rows(&self, idx: BPosition) -> Taken<BPosition, Vec<AccountRow>> {
        let (current, below) = self.rows.take(idx, Duration::ZERO);
        Taken::of(current, below)
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
            core: KeyedBuffer::new(BalBuffer::MAX_BUFFERED, BalBuffer::BACKLOG_LOOKBEHIND, 0),
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
        self.core.take(block, timeout).0.map(|latest| latest.0)
    }
}

#[cfg(test)]
#[path = "buffers_tests.rs"]
mod tests;
