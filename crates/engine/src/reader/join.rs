//! The join buffer, its config, and the dedup window and join wait that use
//! it.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::warn;

use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

use super::ports::JoinRecovery;

/// A `tx_data` join key: `(sequencer_id, session_id, tx_data_position)`.
/// Here `sequencer_id` is the `tx_data` lane index (`TxRef::shard_id`),
/// not a publisher identity.
///
/// The key includes `session_id` (the Aeron publisher session), because
/// Aeron positions are per-session. Under active/active ingress, two
/// publishers on one shard can emit fragments with the same `(term_id,
/// term_offset)`. So `(sequencer_id, tx_data_position)` alone is ambiguous.
/// The sequencer stamps the session into `TxRef.tx_data_session_id`, and the
/// lookup uses it.
///
/// `shard` and `session` are plain `u8`/`i32`, matching the wire fields on
/// `kardamom_types::TxRef` that key this lookup.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct TxDataKey {
    pub(super) shard: u8,
    pub(super) session: i32,
    pub(super) position: BPosition,
}

impl TxDataKey {
    pub(super) fn new(shard: u8, session: i32, position: BPosition) -> Self {
        Self {
            shard,
            session,
            position,
        }
    }

    fn from_ref(tx_ref: &kardamom_types::TxRef) -> Self {
        Self {
            shard: tx_ref.shard_id,
            session: tx_ref.tx_data_session_id,
            position: tx_ref.tx_data_position,
        }
    }
}

/// Lookup-and-remove join buffer, keyed by [`TxDataKey`].
///
/// `TxData` reader threads insert with [`JoinBuffer::insert`]. The
/// `tx_ordering` reader reads with [`JoinBuffer::take`], which removes on a
/// hit. The in-flight window bounds its size, typically a few thousand
/// entries (~100 MB at envelope-sized values).
///
/// This is shared across M+1 threads with an `Arc`. It uses `DashMap`, with
/// per-shard locks, because the access pattern is M concurrent inserts and
/// one concurrent reader. It is never iterated.
#[derive(Clone, Default)]
pub struct JoinBuffer {
    inner: Arc<DashMap<TxDataKey, TxEnvelope>>,
}

impl JoinBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn insert(&self, key: TxDataKey, env: TxEnvelope) {
        self.inner.insert(key, env);
    }

    /// Remove and return the envelope at `key`. Return `None` if it is not
    /// present yet.
    #[must_use]
    pub(super) fn take(&self, key: TxDataKey) -> Option<TxEnvelope> {
        self.inner.remove(&key).map(|kv| kv.1)
    }

    /// Current entry count. Used by tests and by the periodic growth-monitor
    /// warning the `tx_ordering` reader emits.
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Tunables for the reader / join layer.
#[derive(Clone, Debug)]
pub struct ReaderConfig {
    /// Upper bound on how long the `tx_ordering` reader waits for a `TxRef`'s
    /// envelope to land on its `tx_data`. 100 ms matches the rule: a few µs of
    /// A-publisher lag is fine, anything more is an upstream failure.
    pub join_timeout: Duration,
    /// How long a join waits in-band before the first archive-refetch attempt,
    /// when a [`JoinRecovery`] is wired. Long enough that ordinary publisher lag
    /// never triggers a refetch. Short enough that a real loss recovers well
    /// inside the join budget. Further attempts repeat on the same cadence
    /// until `join_timeout` expires.
    pub join_refetch_after: Duration,
    /// Polling interval used during the join wait. A smaller value recovers
    /// from lag faster, but uses more CPU. A larger value does the opposite.
    /// 50 µs is well below the 100 ms ceiling.
    pub join_poll_interval: Duration,
    /// Soft warn threshold on the join buffer's size. Emits a `warn!` log when
    /// crossed. This applies no back-pressure; that is the publisher's job.
    pub buffer_warn_threshold: usize,
    /// Capacity of the canonical-id dedup window on the `tx_ordering` reader.
    /// Duplicates of one canonical id come from the P racing sequencers'
    /// republications, which land close together, within the sequencers'
    /// publish spread. So the window only has to outlast that spread, not the
    /// whole stream. 2^20 ids (~32 MiB of hashes) gives ~10 s of headroom, even
    /// at 100k tx/s.
    ///
    /// Zero would make [`DedupWindow::first_seen`] evict the id it just
    /// inserted, turning dedup silently off: a duplicate `TxRef` would then
    /// miss the join, execute twice, or misalign the next boundary. The type
    /// makes that state unrepresentable.
    pub dedup_window: NonZeroUsize,
}

/// Default [`ReaderConfig::dedup_window`] capacity: 2^20 ids.
const DEFAULT_DEDUP_WINDOW: NonZeroUsize = NonZeroUsize::new(1 << 20).expect("1 << 20 is nonzero");

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            join_timeout: Duration::from_millis(100),
            join_refetch_after: Duration::from_secs(10),
            join_poll_interval: Duration::from_micros(50),
            buffer_warn_threshold: 10_000,
            dedup_window: DEFAULT_DEDUP_WINDOW,
        }
    }
}

/// Bounded first-seen window for canonical-id dedup, FIFO-evicted.
///
/// `first_seen` returns `false` for an id already in the window. Once more
/// than `capacity` ids are held, the oldest is evicted. This is safe,
/// because duplicates of one canonical id (the racing sequencers'
/// republications) arrive close together, well inside the window.
pub(super) struct DedupWindow {
    pub(super) seen: std::collections::HashSet<alloy_primitives::B256>,
    pub(super) fifo: std::collections::VecDeque<alloy_primitives::B256>,
    capacity: NonZeroUsize,
}

impl DedupWindow {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            fifo: std::collections::VecDeque::new(),
            capacity,
        }
    }

    /// Record `id`. Return `false` if it is already in the window.
    pub(super) fn first_seen(&mut self, id: alloy_primitives::B256) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.fifo.push_back(id);
        if self.fifo.len() > self.capacity.get()
            && let Some(evicted) = self.fifo.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }
}

/// Joins one `TxRef` against the buffer with the full join budget, mixing
/// in bounded archive-refetch attempts when a [`JoinRecovery`] is wired.
///
/// Timeline: wait `join_refetch_after` in-band, to cover ordinary publisher
/// lag. On a miss, refetch the missing range from the durability archives
/// and keep waiting, repeating until `join_timeout` runs out. Without a
/// recovery, this becomes a single bounded wait. A refetch error is
/// non-fatal here; endpoints rotate inside the implementation, and the
/// join timeout stays the final arbiter.
pub(super) struct JoinWait<'a> {
    buffer: &'a JoinBuffer,
    cfg: &'a ReaderConfig,
    key: TxDataKey,
    deadline: Instant,
    recovery: &'a mut Option<JoinRecovery>,
}

impl<'a> JoinWait<'a> {
    /// # Errors
    ///
    /// Returns `Err` if `Instant::now() + cfg.join_timeout` overflows. A
    /// panic here (the `+` operator's behavior) would take down the
    /// `tx_ordering` reader thread instead of failing this one join.
    pub(super) fn new(
        buffer: &'a JoinBuffer,
        recovery: &'a mut Option<JoinRecovery>,
        tx_ref: &kardamom_types::TxRef,
        cfg: &'a ReaderConfig,
    ) -> Result<Self, crate::error::ExecutorError> {
        let deadline = Instant::now()
            .checked_add(cfg.join_timeout)
            .ok_or_else(|| {
                crate::error::ExecutorError::State(format!(
                    "join_timeout {:?} overflowed Instant",
                    cfg.join_timeout
                ))
            })?;
        Ok(Self {
            buffer,
            cfg,
            key: TxDataKey::from_ref(tx_ref),
            deadline,
            recovery,
        })
    }

    pub(super) fn run(mut self) -> Option<TxEnvelope> {
        let first_slice = match self.recovery {
            Some(_) => self.cfg.join_refetch_after.min(self.cfg.join_timeout),
            None => self.cfg.join_timeout,
        };
        if let Some(env) = self.wait_for(first_slice) {
            return Some(env);
        }
        loop {
            match self.poll_once() {
                JoinStep::GiveUp => return None,
                JoinStep::Take(env) => return Some(env),
                JoinStep::Retry => {}
            }
        }
    }

    /// One refetch-then-wait attempt, after the initial wait in
    /// [`Self::run`] misses. [`Self::run`]'s loop stays a plain dispatch on
    /// the result.
    fn poll_once(&mut self) -> JoinStep {
        if self.recovery.is_none() {
            // No recovery wired: the wait in `run` was the whole budget.
            return JoinStep::GiveUp;
        }
        if Instant::now() >= self.deadline {
            return JoinStep::GiveUp;
        }
        self.refetch_once();
        let slice = self.next_slice();
        if slice.is_zero() {
            return match self.buffer.take(self.key) {
                Some(env) => JoinStep::Take(env),
                None => JoinStep::GiveUp,
            };
        }
        match self.wait_for(slice) {
            Some(env) => JoinStep::Take(env),
            None => JoinStep::Retry,
        }
    }

    /// One archive refetch attempt for a join miss. Every recovered
    /// envelope is inserted into the buffer under its own key, so a later
    /// [`Self::wait_for`] call can pick it up.
    fn refetch_once(&mut self) {
        let buffer = self.buffer;
        let key = self.key;
        let Some(r) = self.recovery.as_mut() else {
            return;
        };
        warn!(
            target: "kardamom_executor::reader",
            sequencer_id = key.shard,
            session_id = key.session,
            tx_data_position = ?key.position,
            "join miss on tx_data — refetching from durability archive"
        );
        let mut recovered = 0u64;
        match r.recover_tx_data(
            key.shard,
            key.session,
            key.position,
            &mut |loc: TxDataLoc, env: TxEnvelope| {
                buffer.insert(TxDataKey::new(key.shard, loc.session_id, loc.position), env);
                // A cold diagnostic counter: saturate rather than let a
                // pathological refetch wrap it back toward zero.
                recovered = recovered.saturating_add(1);
            },
        ) {
            Ok(_) => tracing::info!(
                target: "kardamom_executor::reader",
                sequencer_id = key.shard,
                recovered,
                "archive refetch complete"
            ),
            Err(e) => warn!(
                target: "kardamom_executor::reader",
                sequencer_id = key.shard,
                error = %e,
                "archive refetch failed; will retry within the join budget"
            ),
        }
    }

    /// The wait slice for the next [`Self::wait_for`] call after a
    /// refetch: the configured refetch cadence, capped by whatever remains
    /// of the join deadline.
    fn next_slice(&self) -> Duration {
        self.cfg
            .join_refetch_after
            .min(self.deadline.saturating_duration_since(Instant::now()))
    }

    /// Spin until the `tx_data` envelope keyed by `self.key` lands on the
    /// buffer, or `timeout` elapses.
    fn wait_for(&self, timeout: Duration) -> Option<TxEnvelope> {
        if let Some(env) = self.buffer.take(self.key) {
            return Some(env);
        }
        // An overflowed sum only means "past the deadline already", which
        // is the correct outcome for a `timeout` this large; clamp to the
        // join deadline rather than treat it as a fixup.
        let deadline = Instant::now()
            .checked_add(timeout)
            .map_or(self.deadline, |t| t.min(self.deadline));
        loop {
            match self.wait_step(deadline) {
                JoinStep::Take(env) => return Some(env),
                JoinStep::GiveUp => return None,
                JoinStep::Retry => {}
            }
        }
    }

    /// One poll of the join buffer, after sleeping one poll interval. The
    /// loop in [`Self::wait_for`] stays a plain dispatch on the result.
    fn wait_step(&self, deadline: Instant) -> JoinStep {
        thread::sleep(self.cfg.join_poll_interval);
        match self.buffer.take(self.key) {
            Some(env) => JoinStep::Take(env),
            None if Instant::now() >= deadline => JoinStep::GiveUp,
            None => JoinStep::Retry,
        }
    }
}

/// One join-wait step: give up, take the recovered envelope, or retry.
/// Shared by [`JoinWait::run`]'s refetch loop and [`JoinWait::wait_for`]'s
/// poll loop.
enum JoinStep {
    GiveUp,
    Take(TxEnvelope),
    Retry,
}
