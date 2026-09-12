//! Archive-backed refetch of lossy side-stream ranges (`tx_data` / `tx_deposits`).
//!
//! The multicast side streams are lossy for any subscriber that missed
//! frames (an image lapse under load, a publisher racing a subscriber
//! restart, a node-kill blackout, or a process restart mid-chain). The
//! durability recordings live on remote nodes' archives: the ingress nodes
//! record every `tx_data` shard stream (multicast means each ingress archive
//! is a full mirror), and the da-watcher's node records `tx_deposits`. No
//! consumer-local archive has them, so recovery must fetch the missed
//! range from a remote archive, not a local one. This module is the
//! consumer-side client that makes the remote copies reachable in-band. It
//! connects to a configured archive control endpoint over UDP, resolves
//! the recording for `(stream, publisher session)`, and runs a bounded
//! replay of `[from, recorded-position)` onto a private unicast endpoint,
//! delivering decoded records to the caller as they arrive.
//!
//! Design constraints:
//!   * Bounded replay only: `length = recorded_position - from`. A
//!     `length = i64::MAX` replay means follow-live and never completes. The
//!     archive rejects outright a range that extends past the recording.
//!   * Off the hot path: this client owns a dedicated [`AeronRuntime`] (its
//!     own conductor thread) plus its own archive control session. Nothing
//!     here shares the live subscriptions' runtime, so a slow refetch can
//!     never starve live polling.
//!   * Session-keyed: the replay channel pins the original publisher session
//!     id, so replayed frame headers carry the recorded positions and
//!     session. The caller's join keys match exactly, and a restarted
//!     publisher (its own session, its own recording) can never be confused
//!     with its predecessor. This is what makes refetch robust where a
//!     replay-from-origin merge fails on multi-recording streams.
//!
//! Failure model: any error goes back to the caller, which retries inside
//! its join budget. Each failed call rotates to the next control endpoint,
//! so a dead archive node (for example the chaos suite killing an ingress
//! driver) costs one short attempt before the mirror serves the range.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kardamom_types::{BPosition, Deposit, TxDataLoc, TxEnvelope};
use rusteron_archive::AeronArchiveReplayParams;
use tracing::{info, warn};

use crate::aeron_live::{AeronRuntime, PollRecv, TxDataSubscription, TypedSubscription};
use crate::archive_catalog::ArchiveCatalog;
use crate::config::{AeronConfig, ChannelUri};
use crate::error::LogError;
use crate::recorder::{ArchiveSession, connect_archive_with_timeout};
use crate::term_layout::TermLayout;

/// How long a single archive control connect may take before the endpoint is
/// declared down and rotated. This is short, because it runs inside a
/// join-timeout budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Replay drain: stop when this much silence follows the last fragment. At
/// that point the bounded replay has delivered everything it will. A hard
/// cap still applies.
const DRAIN_IDLE: Duration = Duration::from_millis(500);
const DRAIN_CAP: Duration = Duration::from_secs(8);

/// Node-local transport config for the refetch client.
pub struct RefetchConfig {
    /// Remote archive control endpoints (`host:port`) recording `tx_data`.
    pub tx_data_endpoints: Vec<String>,
    /// Remote archive control endpoints (`host:port`) recording `tx_deposits`.
    pub tx_deposits_endpoints: Vec<String>,
    /// This node's UDP endpoint (`host:port`) for archive control responses.
    pub response_endpoint: String,
    /// This node's UDP endpoint (`host:port`) that replayed fragments land
    /// on.
    pub replay_endpoint: String,
    /// Media-driver dir shared with the service (same node).
    pub aeron_dir: Option<PathBuf>,
    /// Base Aeron config the control session clones its knobs from.
    pub aeron: AeronConfig,
}

/// The refetch client. There is one per consumer process. The `tx_ordering`
/// reader thread calls it on join misses, so everything is lazy: no Aeron
/// resources exist until the first miss.
pub struct ArchiveRefetcher {
    cfg: RefetchConfig,
    /// Lazily spawned dedicated runtime for the assembled replay
    /// subscriptions. This is never the service's own runtime.
    rt: Option<AeronRuntime>,
    /// The live control session and the endpoint it is connected to.
    /// Always both or neither: a session with no known endpoint (or an
    /// endpoint with no session) is not a state this client can act on,
    /// so one `Option` carries both instead of two `Option`s that could
    /// disagree.
    live: Option<Live>,
    next_endpoint: usize,
    /// Replay subscriptions per `(stream_id, publisher session)`. The
    /// runtime has no close-subscription call, so entries are reused across
    /// refetches: a new bounded replay onto the same channel forms a fresh
    /// image on the same subscription. This map stays small, bounded by
    /// publisher restarts.
    tx_data_subs: HashMap<(i32, i32), TxDataSubscription>,
    deposit_subs: HashMap<(i32, i32), TypedSubscription<Deposit>>,
}

/// A live archive control session and the endpoint it is connected to.
struct Live {
    endpoint: String,
    session: ArchiveSession,
}

/// A resolved, ready-to-start bounded replay: the raw start position, the
/// length to replay, and the destination endpoint the archive should
/// stream it onto.
struct ReplayPlan {
    from_raw: i64,
    len: i64,
    endpoint: String,
}

/// A recording resolved on the remote archive.
struct FoundRecording {
    recording_id: i64,
    session_id: i32,
    start_position: i64,
    /// Power-of-two term length and initial term id, checked once when
    /// this recording was read from the catalog (see [`TermLayout::new`]).
    /// `Err` when the archive reported a malformed term length.
    /// `raw_position` re-raises this on every call rather than
    /// re-checking, matching the previous per-call check.
    term_layout: Result<TermLayout, String>,
}

/// Spawn the dedicated replay runtime, pointed at `aeron_dir` when given.
fn spawn_runtime(aeron_dir: Option<&std::path::Path>) -> Result<AeronRuntime, LogError> {
    match aeron_dir {
        Some(dir) => AeronRuntime::spawn_with_dir(dir),
        None => AeronRuntime::spawn_default(),
    }
}

impl ArchiveRefetcher {
    #[must_use]
    pub fn new(cfg: RefetchConfig) -> Self {
        Self {
            cfg,
            rt: None,
            live: None,
            next_endpoint: 0,
            tx_data_subs: HashMap::new(),
            deposit_subs: HashMap::new(),
        }
    }

    /// Refetch `tx_data` envelopes for `stream_id` on publisher session
    /// `session_id`, from `from` to the recording's current recorded
    /// position. Delivers each `(loc, envelope)` to `sink`. Returns the
    /// delivered count for the whole missed range, not just one envelope.
    /// One refetch heals the entire blackout window.
    ///
    /// # Errors
    ///
    /// Returns an error if no archive endpoint is reachable, if no
    /// recording matches `stream_id`/`session_id`, if the recording's
    /// term layout cannot decode `from`, or if starting the bounded
    /// replay fails.
    pub fn fetch_tx_data(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        mut sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.tx_data_endpoints.clone();
        let recs = self.list_or_rotate(&endpoints, stream_id)?;
        let Some(rec) = FoundRecording::resolve_session(recs, session_id) else {
            self.rotate();
            return Err(LogError::Aeron(format!(
                "refetch: no recording for stream {stream_id} session {session_id} on this archive"
            )));
        };
        let Some(plan) = self.prepare_replay(&endpoints, &rec, from, stream_id, session_id)? else {
            return Ok(0);
        };

        // The subscription must exist before the replay starts: Aeron does
        // not replay pre-subscription history, so a subscription opened
        // after the remote archive begins streaming can miss its earliest
        // frames. Take it out of the map (or open a fresh one) rather
        // than look it up in place, since `ensure_session` below also
        // needs `&mut self`, and a live borrow from `HashMap::entry`/
        // `get_mut` cannot overlap that. Put it back once done, on
        // every path, so a subscription is never silently dropped.
        let key = (stream_id, rec.session_id);
        let mut rx = if let Some(rx) = self.tx_data_subs.remove(&key) {
            rx
        } else {
            let sub_uri = format!(
                "aeron:udp?endpoint={}|session-id={}",
                plan.endpoint, rec.session_id
            );
            self.ensure_runtime()?
                .open_tx_data_subscription(&sub_uri, stream_id)?
        };

        let session = self.ensure_session(&endpoints)?;
        let replay_result = Self::start_bounded_replay(session, &rec, stream_id, &plan);
        let delivered = if replay_result.is_ok() {
            Self::drain(&mut rx, |(loc, env)| sink(loc, env))
        } else {
            0
        };
        self.tx_data_subs.insert(key, rx);
        if let Err(e) = replay_result {
            warn!(error = %e, "refetch: replay start failed; rotating endpoint");
            self.rotate();
            return Err(e);
        }
        if delivered == 0 {
            // A non-empty range that yields nothing signals a corrupt
            // recording at this layer. With replay-side CRC validation on
            // (`aeron.archive.replay.checksum`), the archive fails a corrupt
            // range on its side, so no fragments arrive. Rotate so the join
            // loop's next refetch attempt reads the mirror archive instead of
            // staying pinned to the bad copy.
            warn!(
                stream_id,
                session_id,
                from_raw = plan.from_raw,
                replay_len = plan.len,
                "refetch: replay produced no fragments (corrupt recording?); rotating endpoint"
            );
            self.rotate();
        }
        info!(
            stream_id,
            session_id,
            from_raw = plan.from_raw,
            replay_len = plan.len,
            delivered,
            "tx_data refetch drained"
        );
        Ok(delivered)
    }

    /// Refetch `tx_deposits` from `from`, best effort across all recordings of
    /// the stream. A `DepositRef` carries no publisher session, and deposit
    /// volume is tiny, so a recording whose position space does not contain
    /// `from` replays from its own start.
    ///
    /// # Errors
    ///
    /// Returns an error if no archive endpoint is reachable, if no
    /// `tx_deposits` recording exists for `stream_id`, or if starting a
    /// bounded replay fails.
    pub fn fetch_deposits(
        &mut self,
        stream_id: i32,
        from: BPosition,
        mut sink: impl FnMut(BPosition, Deposit),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.tx_deposits_endpoints.clone();
        let recs = self.list_or_rotate(&endpoints, stream_id)?;
        if recs.is_empty() {
            self.rotate();
            return Err(LogError::Aeron(format!(
                "refetch: no tx_deposits recordings (stream {stream_id}) on this archive"
            )));
        }
        // Compute each recording's replay plan up front (a real archive
        // error here still propagates via `?`), then keep only the ones
        // with something to replay. This is what lets the loop below
        // skip an empty range without an `if` of its own.
        let mut plans = Vec::with_capacity(recs.len());
        for rec in recs {
            let from_raw = rec
                .raw_position(from)
                .unwrap_or(rec.start_position)
                .max(rec.start_position);
            let (len, endpoint) = self.replay_bounds(&endpoints, &rec, from_raw)?;
            plans.push((
                rec,
                ReplayPlan {
                    from_raw,
                    len,
                    endpoint,
                },
            ));
        }

        let mut delivered = 0u64;
        for (rec, plan) in plans.into_iter().filter(|(_, plan)| plan.len > 0) {
            delivered +=
                self.replay_one_deposit_recording(stream_id, &endpoints, &rec, &plan, &mut sink)?;
        }
        info!(stream_id, delivered, "tx_deposits refetch drained");
        Ok(delivered)
    }

    /// One deposit recording's replay, for [`Self::fetch_deposits`]'s loop:
    /// swap its subscription in (or open a fresh one), replay the bounded
    /// range, drain what arrives into `sink`, then swap the subscription
    /// back. On a failed replay start, rotates the endpoint and returns
    /// the error.
    fn replay_one_deposit_recording(
        &mut self,
        stream_id: i32,
        endpoints: &[String],
        rec: &FoundRecording,
        plan: &ReplayPlan,
        sink: &mut impl FnMut(BPosition, Deposit),
    ) -> Result<u64, LogError> {
        // Subscription before replay start; see the matching comment in
        // `fetch_tx_data`. Take it out of the map (or open a fresh one),
        // and put it back once done, on every path.
        let key = (stream_id, rec.session_id);
        let mut rx = if let Some(rx) = self.deposit_subs.remove(&key) {
            rx
        } else {
            let sub_uri = format!(
                "aeron:udp?endpoint={}|session-id={}",
                plan.endpoint, rec.session_id
            );
            self.ensure_runtime()?
                .open_subscription::<Deposit>(&sub_uri, stream_id)?
        };

        let session = self.ensure_session(endpoints)?;
        let replay_result = Self::start_bounded_replay(session, rec, stream_id, plan);
        let delivered = if replay_result.is_ok() {
            Self::drain(&mut rx, |(pos, dep)| sink(pos, dep))
        } else {
            0
        };
        self.deposit_subs.insert(key, rx);
        if let Err(e) = replay_result {
            warn!(error = %e, "refetch: deposit replay start failed; rotating endpoint");
            self.rotate();
            return Err(e);
        }
        Ok(delivered)
    }

    // ---- internals --------------------------------------------------------

    /// Make sure the dedicated replay runtime is spawned, and return it.
    fn ensure_runtime(&mut self) -> Result<&AeronRuntime, LogError> {
        match &mut self.rt {
            Some(rt) => Ok(rt),
            slot @ None => {
                let rt = spawn_runtime(self.cfg.aeron_dir.as_deref())?;
                Ok(slot.insert(rt))
            }
        }
    }

    /// Make sure a live control session exists against one of `endpoints`,
    /// starting at the rotation cursor, and return it. Tries each
    /// endpoint at most once per call.
    fn ensure_session(&mut self, endpoints: &[String]) -> Result<&ArchiveSession, LogError> {
        if endpoints.is_empty() {
            return Err(LogError::Aeron(
                "refetch: no archive endpoints configured".into(),
            ));
        }
        // `take` moves the old session out and ends its borrow at once.
        // The `&mut self` call below then needs no borrow of `self.live`.
        let live = match self.live.take() {
            Some(live) if endpoints.iter().any(|e| e == &live.endpoint) => live,
            _ => self.reconnect(endpoints)?,
        };
        Ok(&self.live.insert(live).session)
    }

    /// Try each endpoint once, starting at the rotation cursor, until one
    /// connects. Behind [`Self::ensure_session`].
    fn reconnect(&mut self, endpoints: &[String]) -> Result<Live, LogError> {
        let mut last_err: Option<LogError> = None;
        for attempt in 0..endpoints.len() {
            match self.try_endpoint(endpoints, attempt) {
                Ok(live) => return Ok(live),
                Err(e) => last_err = Some(e),
            }
        }
        // `self.live` is already `None`: the only caller,
        // `ensure_session`, `take`s it before calling `reconnect`.
        self.next_endpoint = self.next_endpoint.wrapping_add(1);
        Err(last_err.unwrap_or_else(|| LogError::Aeron("refetch: no endpoint reachable".into())))
    }

    /// Connect the control session at rotation offset `attempt` from the
    /// cursor. Advances the cursor to this endpoint on success.
    fn try_endpoint(&mut self, endpoints: &[String], attempt: usize) -> Result<Live, LogError> {
        // The cursor advances with `wrapping_add` here and in `rotate`, so
        // it can reach `usize::MAX` after enough rotations. Add the same
        // way, so this never panics on overflow.
        let idx = self.next_endpoint.wrapping_add(attempt) % endpoints.len();
        let ep = &endpoints[idx];
        let mut acfg = self.cfg.aeron.clone();
        acfg.archive_control_request_channel =
            ChannelUri::new_trusted(format!("aeron:udp?endpoint={ep}"));
        acfg.archive_control_response_channel =
            ChannelUri::new_trusted(format!("aeron:udp?endpoint={}", self.cfg.response_endpoint));
        match connect_archive_with_timeout(self.cfg.aeron_dir.as_deref(), &acfg, CONNECT_TIMEOUT) {
            Ok(session) => {
                info!(endpoint = %ep, "refetch: archive control connected");
                self.next_endpoint = idx;
                Ok(Live {
                    endpoint: ep.clone(),
                    session,
                })
            }
            Err(e) => {
                warn!(endpoint = %ep, error = %e, "refetch: archive control connect failed");
                Err(e)
            }
        }
    }

    fn rotate(&mut self) {
        self.live = None;
        self.next_endpoint = self.next_endpoint.wrapping_add(1);
    }

    fn list_or_rotate(
        &mut self,
        endpoints: &[String],
        stream_id: i32,
    ) -> Result<Vec<FoundRecording>, LogError> {
        // `ensure_session` returns the cached session when it is fresh.
        // This call is cheap; it does not reconnect.
        let session = self.ensure_session(endpoints)?;
        match Self::list_recordings(session, stream_id) {
            Ok(recs) => Ok(recs),
            Err(e) => {
                warn!(error = %e, "refetch: catalog listing failed; rotating endpoint");
                self.rotate();
                Err(e)
            }
        }
    }

    /// Resolve `from` to a raw stream position on `rec`, then bound the
    /// replay against the recorded position. Returns `None` when nothing
    /// is recorded at or after `from` (not an error: the caller has
    /// nothing to fetch; this logs `from_raw` itself, since the caller
    /// then has no bound to log). On a bounds error, rotates the endpoint
    /// before returning, matching [`Self::list_or_rotate`].
    fn prepare_replay(
        &mut self,
        endpoints: &[String],
        rec: &FoundRecording,
        from: BPosition,
        stream_id: i32,
        session_id: i32,
    ) -> Result<Option<ReplayPlan>, LogError> {
        let from_raw = rec.raw_position(from)?;
        let (len, endpoint) = match self.replay_bounds(endpoints, rec, from_raw) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "refetch: replay bounds failed; rotating endpoint");
                self.rotate();
                return Err(e);
            }
        };
        if len <= 0 {
            info!(
                stream_id,
                session_id, from_raw, "refetch: nothing recorded at/after the requested position"
            );
            return Ok(None);
        }
        Ok(Some(ReplayPlan {
            from_raw,
            len,
            endpoint,
        }))
    }

    /// Bound the replay: `[from_raw, recorded-position)` on the connected
    /// archive. Also returns the replay destination endpoint.
    fn replay_bounds(
        &mut self,
        endpoints: &[String],
        rec: &FoundRecording,
        from_raw: i64,
    ) -> Result<(i64, String), LogError> {
        // `ensure_session` returns the cached session when it is fresh.
        // This call is cheap; it does not reconnect.
        let session = self.ensure_session(endpoints)?;
        let archive = &session.archive;
        // An active recording uses the current recorded position; a stopped
        // one uses its stop position.
        let bound = match archive.get_recording_position(rec.recording_id) {
            Ok(p) if p >= 0 => p,
            _ => archive
                .get_stop_position(rec.recording_id)
                .map_err(|e| LogError::Aeron(format!("get_stop_position: {e}")))?,
        };
        if from_raw < rec.start_position {
            return Err(LogError::Aeron(format!(
                "refetch: position {from_raw} precedes recording {} start {} — range not recoverable",
                rec.recording_id, rec.start_position
            )));
        }
        Ok((bound - from_raw, self.cfg.replay_endpoint.clone()))
    }

    /// List every recording for `stream_id` in the archive's catalog
    /// (paged). Each recording's [`TermLayout`] is checked once here, not
    /// on every later [`FoundRecording::raw_position`] call.
    fn list_recordings(
        session: &ArchiveSession,
        stream_id: i32,
    ) -> Result<Vec<FoundRecording>, LogError> {
        let mut recs = Vec::new();
        session
            .archive
            .for_each_recording_of_stream(stream_id, |desc| {
                recs.push(FoundRecording {
                    recording_id: desc.recording_id(),
                    session_id: desc.session_id(),
                    start_position: desc.start_position(),
                    term_layout: TermLayout::new(desc.term_buffer_length(), desc.initial_term_id()),
                });
            })?;
        Ok(recs)
    }

    /// Start a bounded replay of `[plan.from_raw, plan.from_raw +
    /// plan.len)` onto `plan.endpoint`, pinning the original session id
    /// so replayed frames keep their recorded headers. Returns the
    /// replay session id.
    fn start_bounded_replay(
        session: &ArchiveSession,
        rec: &FoundRecording,
        stream_id: i32,
        plan: &ReplayPlan,
    ) -> Result<i64, LogError> {
        let params = AeronArchiveReplayParams::new(0, i32::MAX, plan.from_raw, plan.len, 0, 0)
            .map_err(|e| LogError::Aeron(format!("replay params: {e}")))?;
        let channel = crate::ffi::c_uri(
            &format!(
                "aeron:udp?endpoint={}|session-id={}",
                plan.endpoint, rec.session_id
            ),
            "replay channel",
        )?;
        session
            .archive
            .start_replay(rec.recording_id, &channel, stream_id, &params)
            .map_err(|e| LogError::Aeron(format!("start_replay: {e}")))
    }

    /// Drain a replay subscription: deliver until [`DRAIN_IDLE`] of
    /// silence after the last fragment (bounded replay exhausted) or the
    /// [`DRAIN_CAP`].
    ///
    /// The refetcher runs on the `tx_ordering` reader thread — a plain
    /// std thread with no tokio runtime entered — so each wait is a
    /// blocking receive with an idle timeout via [`recv_timeout`]: the
    /// thread parks on the channel and wakes on the next fragment. No
    /// `try_recv` + sleep busy loop, and no runtime of its own.
    fn drain<S: PollRecv>(rx: &mut S, mut deliver: impl FnMut(S::Item)) -> u64 {
        let deadline = Instant::now() + DRAIN_CAP;
        let mut delivered = 0u64;
        loop {
            let ControlFlow::Continue(item) = drain_step(rx, deadline) else {
                return delivered;
            };
            deliver(item);
            delivered += 1;
        }
    }
}

/// One [`ArchiveRefetcher::drain`] step: `Break` means the idle budget ran out
/// or the source ended (replay exhausted, or the runtime is gone); either
/// way the caller stops. `Continue` carries one item the caller delivers
/// and counts.
fn drain_step<S: PollRecv>(rx: &mut S, deadline: Instant) -> ControlFlow<(), S::Item> {
    let budget = DRAIN_IDLE.min(deadline.saturating_duration_since(Instant::now()));
    if budget.is_zero() {
        return ControlFlow::Break(());
    }
    match recv_timeout(rx, budget) {
        Ok(Some(item)) => ControlFlow::Continue(item),
        // Idle timeout (replay exhausted) or channel closed (runtime gone).
        Err(RecvTimeout) | Ok(None) => ControlFlow::Break(()),
    }
}

impl FoundRecording {
    /// Pick the most recent recording of `session_id` on this stream. A
    /// publisher restart starts a new recording under the same session
    /// id, and the higher `recording_id` is the fresher one.
    fn resolve_session(recs: Vec<FoundRecording>, session_id: i32) -> Option<FoundRecording> {
        recs.into_iter()
            .filter(|r| r.session_id == session_id)
            .max_by_key(|r| r.recording_id)
    }

    /// Compute the raw stream position of a fragment-start [`BPosition`]
    /// within this recording's position space, using its [`TermLayout`].
    /// The archive replay API addresses recordings by these raw
    /// positions.
    fn raw_position(&self, pos: BPosition) -> Result<i64, LogError> {
        let layout = self.term_layout.as_ref().map_err(|e| {
            LogError::Aeron(format!("refetch: recording {} has {e}", self.recording_id))
        })?;
        layout
            .position_of(pos, self.recording_id)
            .map_err(|e| LogError::Aeron(format!("refetch: {e}")))
    }
}

/// The wait in [`recv_timeout`] elapsed with nothing received.
struct RecvTimeout;

/// Blocking receive with a timeout on a [`PollRecv`] subscription from a
/// thread that is NOT inside a tokio runtime. Both `PollRecv`
/// implementations wrap a tokio `UnboundedReceiver`, which only offers
/// `blocking_recv` (no timeout) and async `recv` (needs a timer for
/// `timeout`), so this drives `poll_recv` by hand with a waker that unparks
/// the calling thread: park until woken or the deadline, re-poll, repeat.
/// Spurious unparks just cause an extra poll.
///
/// `Ok(None)` means the channel closed; `Err(RecvTimeout)` means the deadline
/// passed.
fn recv_timeout<S: PollRecv>(
    rx: &mut S,
    timeout: Duration,
) -> Result<Option<S::Item>, RecvTimeout> {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    /// One poll-and-maybe-park step. `Break` carries [`recv_timeout`]'s
    /// answer: a ready item, or a timeout past `deadline`. `Continue`
    /// means the poll was pending and this thread parked until either
    /// woken or `deadline`; the caller polls again.
    fn step<S: PollRecv>(
        rx: &mut S,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> ControlFlow<Result<Option<S::Item>, RecvTimeout>> {
        if let Poll::Ready(item) = rx.poll_recv(cx) {
            return ControlFlow::Break(Ok(item));
        }
        let now = Instant::now();
        if now >= deadline {
            return ControlFlow::Break(Err(RecvTimeout));
        }
        std::thread::park_timeout(deadline - now);
        ControlFlow::Continue(())
    }

    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + timeout;
    loop {
        if let ControlFlow::Break(result) = step(rx, &mut cx, deadline) {
            return result;
        }
    }
}
