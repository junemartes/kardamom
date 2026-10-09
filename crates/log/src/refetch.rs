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

use std::path::PathBuf;
use std::time::Duration;

use kardamom_types::{BPosition, Deposit, ExecTxRecord, TxDataLoc, TxEnvelope};
use rusteron_archive::AeronArchiveReplayParams;
use rusteron_archive::bindings::{AERON_NULL_COUNTER_ID, AERON_NULL_VALUE};
use tracing::{info, warn};

use tokio::sync::watch;

use crate::aeron_live::{AeronRuntime, PollRecv};
use crate::archive_catalog::ArchiveCatalog;
use crate::config::{AeronConfig, ChannelUri};
use crate::discovery::{ArchiveRecord, Membership, Topic};
use crate::error::LogError;
use crate::recorder::{ArchiveSession, connect_archive_with_timeout};
use crate::term_layout::TermLayout;

mod drain;
mod recording;
#[cfg(any(test, feature = "testing"))]
pub use recording::FakeArchiveCatalog;
use recording::{FoundRecording, Located, RecordedLimit, ReplayFrom, Unresolved, Wanted};

/// How long a single archive control connect may take before the endpoint is
/// declared down and rotated. This is short, because it runs inside a
/// join-timeout budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Replay drain: stop when this much silence follows the last fragment. At
/// that point the bounded replay has delivered everything it will. A hard
/// cap still applies.
const DRAIN_IDLE: Duration = Duration::from_millis(500);
const DRAIN_CAP: Duration = Duration::from_secs(8);
/// The first replay stream id. Stream ids are scoped to a channel, and the
/// replay endpoint is this client's own, so the range only has to stay
/// clear of the live stream ids on the same driver, which sit far below.
const REPLAY_STREAM_BASE: i32 = 1 << 30;

/// The channel one replay lands on: the replay endpoint, pinned to the
/// recorded session id. The subscription and the archive's replay
/// publication use the same string.
fn replay_sub_uri(endpoint: &str, session_id: i32) -> String {
    format!("aeron:udp?endpoint={endpoint}|session-id={session_id}")
}

/// Where the refetch client reads the archive control endpoints of one
/// topic: a static list from the config, or the live archive records the
/// catalog lists. Read at every refetch, so a discovered archive that
/// joins or leaves changes the next attempt, never the current one.
pub enum EndpointSource {
    Static(Vec<String>),
    Discovered {
        topic: Topic,
        archives: watch::Receiver<Membership>,
    },
}

impl EndpointSource {
    /// The endpoints to try now, as `host:port`.
    #[must_use]
    pub fn current(&self) -> Vec<String> {
        match self {
            Self::Static(list) => list.clone(),
            Self::Discovered { .. } => self
                .discovered()
                .into_iter()
                .map(|a| a.control.to_string())
                .collect(),
        }
    }

    /// The control endpoint of the archive `archive_id`, as `host:port`.
    /// Only a discovered source names its archives, so a static list
    /// gives `None`.
    #[must_use]
    pub fn archive(&self, archive_id: &str) -> Option<String> {
        self.discovered()
            .into_iter()
            .find(|a| a.archive_id == archive_id)
            .map(|a| a.control.to_string())
    }

    /// The live archive records that record the topic. Empty for a
    /// static list.
    fn discovered(&self) -> Vec<ArchiveRecord> {
        let Self::Discovered { topic, archives } = self else {
            return Vec::new();
        };
        archives
            .borrow()
            .entries
            .values()
            .filter_map(|e| ArchiveRecord::from_entry(e).ok())
            .filter(|a| a.records(*topic))
            .collect()
    }

    /// Whether this source can ever name an endpoint: a non-empty static
    /// list, or any discovered source.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        match self {
            Self::Static(list) => !list.is_empty(),
            Self::Discovered { .. } => true,
        }
    }
}

/// Node-local transport config for the refetch client.
pub struct RefetchConfig {
    /// Remote archive control endpoints recording `tx_data`.
    pub tx_data_endpoints: EndpointSource,
    /// Remote archive control endpoints recording `tx_deposits`.
    pub tx_deposits_endpoints: EndpointSource,
    /// The archives that record an executor stream. A replay names one
    /// of them by its archive id.
    pub exec_txs_endpoints: EndpointSource,
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
    /// The replays started so far. Each one runs on its own replay
    /// stream id and its own subscription; see [`Self::replay_stream_id`].
    replays: u32,
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
    /// The limit the plan was bounded with. It goes into a replay-start
    /// error, next to the limit the archive names in its refusal.
    limit: RecordedLimit,
}

impl ReplayPlan {
    /// The archive's replay parameters for `[from_raw, from_raw + len)`.
    ///
    /// Every field that the plan does not use holds Aeron's null value,
    /// `-1`. Zero is not null. A limit counter id of zero asks the archive
    /// for a replay that counter 0 of its media driver bounds, and that
    /// counter is the driver's total of bytes sent. The total starts again at
    /// zero with the driver. After a node restart the archive then refused
    /// every range above the new total ("must be less than the limit
    /// position"), although the recording held the range.
    fn params(&self) -> Result<AeronArchiveReplayParams, LogError> {
        AeronArchiveReplayParams::new(
            AERON_NULL_COUNTER_ID,
            i32::MAX,
            self.from_raw,
            self.len,
            i64::from(AERON_NULL_VALUE),
            i64::from(AERON_NULL_VALUE),
        )
        .map_err(|e| LogError::Aeron(format!("replay params: {e}")))
    }
}

/// Where a replay of an executor stream starts: the archive that records
/// it, by its archive id, the stream, the Aeron session of the recorded
/// publication, and a raw stream position at or before the first wanted
/// record.
#[derive(Debug, Clone, Copy)]
pub struct ExecRecordsAt<'a> {
    pub archive_id: &'a str,
    pub stream_id: i32,
    pub session_id: i32,
    pub position: i64,
}

/// What one drained replay delivered, and the range it asked for.
struct Drained {
    delivered: u64,
    from_raw: i64,
    len: i64,
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
            replays: 0,
        }
    }

    /// The stream id of the next bounded replay, and the count moves on.
    ///
    /// Every replay is a new publication on the archive, pinned to the
    /// recorded session id so the frames keep their recorded session.
    /// Two replays of one recording on one stream id therefore share a
    /// publication key on the archive and an image key on this driver:
    /// the archive refuses the second while the first still lingers, and
    /// the driver folds its first frames into the lingering image, where
    /// they read as old data and are never delivered. A stream id of its
    /// own gives every replay its own publication and its own image; the
    /// archive patches the stream id into the replayed frames.
    fn replay_stream_id(&mut self) -> i32 {
        let id = REPLAY_STREAM_BASE.wrapping_add(i32::try_from(self.replays).unwrap_or(0));
        self.replays = self.replays.wrapping_add(1);
        id
    }

    /// Refetch `tx_data` envelopes for `stream_id` on publisher session
    /// `session_id`, from `from` to the recording's current recorded
    /// position. Delivers each `(loc, envelope)` to `sink`. Returns the
    /// delivered count for the whole missed range, not just one envelope.
    /// One refetch heals the entire blackout window.
    ///
    /// # Errors
    ///
    /// Returns an error if no archive endpoint is reachable, if the
    /// recording's term layout cannot decode `from`, or if starting the
    /// bounded replay fails. Returns [`LogError::RangeAbsent`] when the
    /// archive holds no byte of the range: it has no recording of the
    /// session, each recording starts after `from`, or the recording
    /// before `from` ended at or before it.
    pub fn fetch_tx_data(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        mut sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.tx_data_endpoints.current();
        let wanted = Wanted {
            stream_id,
            session_id,
            from: ReplayFrom::Fragment(from),
        };
        let Some(drained) = self.replay_session(
            &endpoints,
            wanted,
            AeronRuntime::open_tx_data_subscription_with_id,
            |(loc, env)| sink(loc, env),
        )?
        else {
            return Ok(0);
        };
        info!(
            stream_id,
            session_id,
            from_raw = drained.from_raw,
            replay_len = drained.len,
            delivered = drained.delivered,
            "tx_data refetch drained"
        );
        Ok(drained.delivered)
    }

    /// Replay the executor stream records of session `at.session_id` from
    /// the archive `at.archive_id`, from the raw position `at.position` to
    /// the recorded position. Delivers each record to `sink`, in stream
    /// order. Returns the delivered count.
    ///
    /// # Errors
    ///
    /// Returns an error when no archive record names `at.archive_id`, when
    /// the archive is not reachable, or when the replay fails to start.
    /// Returns [`LogError::RangeAbsent`] when the archive holds no byte of
    /// the range: it has no recording of the session, or the recording
    /// ended at or before the position.
    pub fn fetch_exec_records(
        &mut self,
        at: &ExecRecordsAt<'_>,
        mut sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, LogError> {
        let endpoint = self
            .cfg
            .exec_txs_endpoints
            .archive(at.archive_id)
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "refetch: no exec_txs archive record names archive {}",
                    at.archive_id
                ))
            })?;
        let wanted = Wanted {
            stream_id: at.stream_id,
            session_id: at.session_id,
            from: ReplayFrom::Raw(at.position),
        };
        let drained = self.replay_session(
            &[endpoint],
            wanted,
            AeronRuntime::open_subscription_with_id::<ExecTxRecord>,
            |(_, record)| sink(record),
        )?;
        Ok(drained.map_or(0, |d| d.delivered))
    }

    /// One bounded replay of one publisher session from the archives
    /// `endpoints`: resolve the recording that holds the start, bound the
    /// replay by the recorded position, open the replay subscription with
    /// `open`, start the replay, and drain it into `deliver`. Returns
    /// `None` when a live recording does not reach the start yet.
    ///
    /// # Errors
    ///
    /// Returns the error of the listing, the bound, the subscription or
    /// the replay start, after a rotation to the next endpoint. Returns
    /// [`LogError::RangeAbsent`] when the archive holds no byte of the
    /// range.
    fn replay_session<S: PollRecv>(
        &mut self,
        endpoints: &[String],
        wanted: Wanted,
        open: impl FnOnce(&AeronRuntime, &str, i32) -> Result<(u32, S), LogError>,
        deliver: impl FnMut(S::Item),
    ) -> Result<Option<Drained>, LogError> {
        let recs = self.list_or_rotate(endpoints, wanted.stream_id)?;
        let found = wanted.resolve(recs).map_err(|u| self.refuse(u))?;
        let Some(plan) = self.prepare_replay(endpoints, &found, wanted)? else {
            return Ok(None);
        };
        let rec = &found.rec;

        // The subscription must exist before the replay starts: Aeron does
        // not replay pre-subscription history, so a subscription opened
        // after the remote archive begins streaming can miss its earliest
        // frames. It is this replay's own, on this replay's stream id,
        // and closes once the drain ends.
        let replay_stream = self.replay_stream_id();
        let sub_uri = replay_sub_uri(&plan.endpoint, rec.session_id);
        let (sub_id, mut rx) = open(self.ensure_runtime()?, &sub_uri, replay_stream)?;

        let session = self.ensure_session(endpoints)?;
        let replay_result = Self::start_bounded_replay(session, rec, replay_stream, &plan);
        let delivered = if replay_result.is_ok() {
            Self::drain(&mut rx, deliver)
        } else {
            0
        };
        self.close_replay_subscription(sub_id);
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
                stream_id = wanted.stream_id,
                session_id = wanted.session_id,
                from_raw = plan.from_raw,
                replay_len = plan.len,
                "refetch: replay produced no fragments (corrupt recording?); rotating endpoint"
            );
            self.rotate();
        }
        Ok(Some(Drained {
            delivered,
            from_raw: plan.from_raw,
            len: plan.len,
        }))
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
        let endpoints = self.cfg.tx_deposits_endpoints.current();
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
            let limit = self.recorded_limit(&endpoints, &rec)?;
            let len = limit.replay_len(from_raw);
            plans.push((
                rec,
                ReplayPlan {
                    from_raw,
                    len,
                    endpoint: self.cfg.replay_endpoint.clone(),
                    limit,
                },
            ));
        }

        let mut delivered = 0u64;
        for (rec, plan) in plans.into_iter().filter(|(_, plan)| plan.len > 0) {
            delivered += self.replay_one_deposit_recording(&endpoints, &rec, &plan, &mut sink)?;
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
        endpoints: &[String],
        rec: &FoundRecording,
        plan: &ReplayPlan,
        sink: &mut impl FnMut(BPosition, Deposit),
    ) -> Result<u64, LogError> {
        // Subscription before replay start; see the matching comment in
        // `fetch_tx_data`. This replay's own, closed once the drain ends.
        let replay_stream = self.replay_stream_id();
        let sub_uri = replay_sub_uri(&plan.endpoint, rec.session_id);
        let (sub_id, mut rx) = self
            .ensure_runtime()?
            .open_subscription_with_id::<Deposit>(&sub_uri, replay_stream)?;

        let session = self.ensure_session(endpoints)?;
        let replay_result = Self::start_bounded_replay(session, rec, replay_stream, plan);
        let delivered = if replay_result.is_ok() {
            Self::drain(&mut rx, |(pos, dep)| sink(pos, dep))
        } else {
            0
        };
        self.close_replay_subscription(sub_id);
        if let Err(e) = replay_result {
            warn!(error = %e, "refetch: deposit replay start failed; rotating endpoint");
            self.rotate();
            return Err(e);
        }
        Ok(delivered)
    }

    // ---- internals --------------------------------------------------------

    /// Close one replay's subscription. Best effort: a close that fails
    /// leaves the row in the runtime's table, which is the state every
    /// replay left behind before subscriptions closed at all.
    fn close_replay_subscription(&self, sub_id: u32) {
        let Some(rt) = self.rt.as_ref() else {
            return;
        };
        if let Err(e) = rt.close_subscription(sub_id) {
            warn!(sub_id, error = %e, "refetch: replay subscription did not close");
        }
    }

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

    /// The error of the connected archive for a range that it cannot
    /// serve, and the rotation to the next endpoint. The error names the
    /// archive before the rotation drops the connection and its name.
    fn refuse(&mut self, why: Unresolved) -> LogError {
        let archive = self
            .live
            .as_ref()
            .map_or_else(|| "?".to_owned(), |l| l.endpoint.clone());
        let e = why.at(archive);
        warn!(error = %e, "refetch: this archive cannot serve the range; rotating endpoint");
        self.rotate();
        e
    }

    /// The `tx_data` archives to ask now. The join layer compares this list
    /// with the archives that gave [`LogError::RangeAbsent`].
    #[must_use]
    pub fn tx_data_archives(&self) -> Vec<String> {
        self.cfg.tx_data_endpoints.current()
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

    /// Bound the replay of `found` against the recorded limit on the
    /// connected archive. Returns `None` when a live recording does not
    /// reach the position yet (not an error: the caller has nothing to
    /// fetch now; this logs the position itself, since the caller then has
    /// no bound to log). On an error, rotates the endpoint before
    /// returning, matching [`Self::list_or_rotate`].
    fn prepare_replay(
        &mut self,
        endpoints: &[String],
        found: &Located,
        wanted: Wanted,
    ) -> Result<Option<ReplayPlan>, LogError> {
        let limit = match self.recorded_limit(endpoints, &found.rec) {
            Ok(limit) => limit,
            Err(e) => {
                warn!(error = %e, "refetch: replay bounds failed; rotating endpoint");
                self.rotate();
                return Err(e);
            }
        };
        let Some(len) = found.replay_len(limit).map_err(|u| self.refuse(u))? else {
            info!(
                stream_id = wanted.stream_id,
                session_id = wanted.session_id,
                from_raw = found.from_raw,
                recording_id = found.rec.recording_id,
                recorded_position = limit.position,
                "refetch: the live recording does not reach the requested position yet"
            );
            return Ok(None);
        };
        Ok(Some(ReplayPlan {
            from_raw: found.from_raw,
            len,
            endpoint: self.cfg.replay_endpoint.clone(),
            limit,
        }))
    }

    /// The recorded limit of `rec` on the connected archive.
    fn recorded_limit(
        &mut self,
        endpoints: &[String],
        rec: &FoundRecording,
    ) -> Result<RecordedLimit, LogError> {
        // `ensure_session` returns the cached session when it is fresh.
        // This call is cheap; it does not reconnect.
        let session = self.ensure_session(endpoints)?;
        RecordedLimit::read(&session.archive, rec.recording_id)
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
                    stop_position: Some(desc.stop_position()).filter(|p| *p >= 0),
                    term_layout: TermLayout::new(desc.term_buffer_length(), desc.initial_term_id()),
                });
            })?;
        Ok(recs)
    }

    /// Start a bounded replay of `[plan.from_raw, plan.from_raw +
    /// plan.len)` onto `plan.endpoint` and `replay_stream`, pinning the
    /// original session id so replayed frames keep their recorded
    /// session. Returns the replay session id.
    fn start_bounded_replay(
        session: &ArchiveSession,
        rec: &FoundRecording,
        replay_stream: i32,
        plan: &ReplayPlan,
    ) -> Result<i64, LogError> {
        let params = plan.params()?;
        let channel = crate::ffi::c_uri(
            &replay_sub_uri(&plan.endpoint, rec.session_id),
            "replay channel",
        )?;
        // The archive's refusal names the limit it holds. The plan's own
        // numbers go next to it: the two limits came from one archive
        // session, and a refusal means that they differ.
        session
            .archive
            .start_replay(rec.recording_id, &channel, replay_stream, &params)
            .map_err(|e| {
                LogError::Aeron(format!(
                    "start_replay of recording {} from {} for {} bytes (planned against limit {}, active={}): {e}",
                    rec.recording_id, plan.from_raw, plan.len, plan.limit.position, plan.limit.active
                ))
            })
    }
}

#[cfg(test)]
mod tests;
