//! Archive-backed refetch of lossy side-stream ranges (tx_data / tx_deposits).
//!
//! The multicast side streams are lossy for any subscriber that missed frames
//! (image lapse under load, publisher racing a subscriber restart, node-kill
//! blackout, process restart mid-chain) — and the durability recordings live
//! on REMOTE nodes' archives: the ingress nodes record every tx_data shard
//! stream (multicast ⇒ each ingress archive is a full mirror), the
//! da-watcher's node records tx_deposits. No consumer-local archive has them,
//! so "restart and replay from the local archive" was never a recovery path.
//! This module is the consumer-side client that makes the remote copies
//! reachable in-band: connect to a configured archive control endpoint over
//! UDP, resolve the recording for `(stream, publisher session)`, and run a
//! BOUNDED replay of `[from, recorded-position)` onto a private unicast
//! endpoint, delivering decoded records to the caller as they arrive.
//!
//! Design constraints (from the validator BAL-refetch postmortem):
//!   * **Bounded replay only**: `length = recorded_position - from`. A
//!     `length = i64::MAX` replay means FOLLOW-LIVE and never completes, and a
//!     range extending past the recording is rejected outright by the archive.
//!   * **Off the hot path**: this client owns a dedicated [`AeronRuntime`]
//!     (its own conductor thread) plus its own archive control session;
//!     nothing here shares the live subscriptions' runtime, so a slow refetch
//!     can never starve live polling.
//!   * **Session-keyed**: the replay channel pins the ORIGINAL publisher
//!     session id, so replayed frame headers carry the recorded positions and
//!     session — the caller's join keys match exactly, and a restarted
//!     publisher (its own session ⇒ its own recording) can never be confused
//!     with its predecessor. This is what makes refetch robust where a
//!     replay-from-origin merge fails on multi-recording streams.
//!
//! Failure model: any error is returned to the caller (which retries inside
//! its join budget); each failed call rotates to the next control endpoint,
//! so a dead archive node (e.g. the chaos suite killing an ingress driver)
//! costs one short attempt before the mirror serves the range.

use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::CString;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use kardamom_types::{BPosition, Deposit, TxDataLoc, TxEnvelope};
use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
    AeronArchiveReplayParams, Handler,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, warn};

use crate::aeron_live::AeronRuntime;
use crate::config::AeronConfig;
use crate::error::LogError;
use crate::recorder::{ArchiveSession, connect_archive_with_timeout};

/// How long a single archive control connect may take before the endpoint is
/// declared down and rotated. Short: this runs inside a join-timeout budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Replay drain: stop when this much silence follows the last fragment (the
/// bounded replay has then delivered everything it will), capped hard.
const DRAIN_IDLE: Duration = Duration::from_millis(500);
const DRAIN_CAP: Duration = Duration::from_secs(8);

/// Node-local transport config for the refetch client.
pub struct RefetchConfig {
    /// Remote archive control endpoints (`host:port`) recording tx_data.
    pub tx_data_endpoints: Vec<String>,
    /// Remote archive control endpoints (`host:port`) recording tx_deposits.
    pub tx_deposits_endpoints: Vec<String>,
    /// This node's UDP endpoint (`host:port`) for archive control RESPONSES.
    pub response_endpoint: String,
    /// This node's UDP endpoint (`host:port`) replayed fragments land on.
    /// (Freed by the removal of the resume replay-merge, whose destination
    /// endpoint this reuses in the deploy.)
    pub replay_endpoint: String,
    /// Media-driver dir shared with the service (same node).
    pub aeron_dir: Option<PathBuf>,
    /// Base Aeron config the control session clones its knobs from.
    pub aeron: AeronConfig,
}

/// The refetch client. One per consumer process; called from the tx_ordering
/// reader thread on join misses, so everything is lazy — no Aeron resources
/// exist until the first miss.
pub struct ArchiveRefetcher {
    cfg: RefetchConfig,
    /// Lazily-spawned dedicated runtime for the (assembled) replay
    /// subscriptions. Never the service's own runtime.
    rt: Option<AeronRuntime>,
    /// Live control session and the endpoint it is connected to.
    session: Option<ArchiveSession>,
    connected: Option<String>,
    next_endpoint: usize,
    /// Replay subscriptions per `(stream_id, publisher session)`. The runtime
    /// has no close-subscription; entries are reused across refetches (a new
    /// bounded replay onto the same channel forms a fresh image on the same
    /// subscription). Bounded by publisher restarts, i.e. tiny.
    tx_data_subs: HashMap<(i32, i32), UnboundedReceiver<(TxDataLoc, TxEnvelope)>>,
    deposit_subs: HashMap<(i32, i32), UnboundedReceiver<(BPosition, Deposit)>>,
}

/// A recording resolved on the remote archive.
struct FoundRecording {
    recording_id: i64,
    session_id: i32,
    start_position: i64,
    initial_term_id: i32,
    term_buffer_length: i32,
}

impl ArchiveRefetcher {
    pub fn new(cfg: RefetchConfig) -> Self {
        Self {
            cfg,
            rt: None,
            session: None,
            connected: None,
            next_endpoint: 0,
            tx_data_subs: HashMap::new(),
            deposit_subs: HashMap::new(),
        }
    }

    /// Refetch tx_data envelopes for `stream_id` on publisher session
    /// `session_id`, from `from` to the recording's current recorded position,
    /// delivering each `(loc, envelope)` to `sink`. Returns the delivered
    /// count (the whole missed range, not just one envelope — one refetch
    /// heals the entire blackout window).
    pub fn fetch_tx_data(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        sink: &mut dyn FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.tx_data_endpoints.clone();
        self.ensure_session(&endpoints)?;
        let recs = self.list_or_rotate(stream_id)?;
        let Some(rec) = recs
            .into_iter()
            .filter(|r| r.session_id == session_id)
            .max_by_key(|r| r.recording_id)
        else {
            self.rotate();
            return Err(LogError::Aeron(format!(
                "refetch: no recording for stream {stream_id} session {session_id} on this archive"
            )));
        };
        let from_raw = raw_position(&rec, from)?;
        let (replay_len, replay_endpoint) = self.replay_bounds(&rec, from_raw)?;
        if replay_len <= 0 {
            info!(
                stream_id,
                session_id, from_raw, "refetch: nothing recorded at/after the requested position"
            );
            return Ok(0);
        }
        self.ensure_runtime()?;
        let sub_uri = format!(
            "aeron:udp?endpoint={}|session-id={}",
            replay_endpoint, rec.session_id
        );
        // Split borrows: rt (read) / tx_data_subs (mut) / session (read) are
        // disjoint fields.
        let rt = self.rt.as_ref().expect("ensured above");
        let rx = match self.tx_data_subs.entry((stream_id, rec.session_id)) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(rt.open_tx_data_subscription(&sub_uri, stream_id)?)
            }
        };
        let session = self.session.as_ref().expect("ensured above");
        start_bounded_replay(
            session,
            &rec,
            stream_id,
            &replay_endpoint,
            from_raw,
            replay_len,
        )?;
        let mut delivered = 0u64;
        drain(rx, |(loc, env)| {
            sink(loc, env);
            delivered += 1;
        });
        info!(
            stream_id,
            session_id, from_raw, replay_len, delivered, "tx_data refetch drained"
        );
        Ok(delivered)
    }

    /// Refetch tx_deposits from `from`, best effort across ALL recordings of
    /// the stream — a `DepositRef` carries no publisher session, and deposit
    /// volume is tiny, so recordings whose position space doesn't contain
    /// `from` replay from their own start.
    pub fn fetch_deposits(
        &mut self,
        stream_id: i32,
        from: BPosition,
        sink: &mut dyn FnMut(BPosition, Deposit),
    ) -> Result<u64, LogError> {
        let endpoints = self.cfg.tx_deposits_endpoints.clone();
        self.ensure_session(&endpoints)?;
        let recs = self.list_or_rotate(stream_id)?;
        if recs.is_empty() {
            self.rotate();
            return Err(LogError::Aeron(format!(
                "refetch: no tx_deposits recordings (stream {stream_id}) on this archive"
            )));
        }
        let mut delivered = 0u64;
        for rec in recs {
            let from_raw = raw_position(&rec, from)
                .unwrap_or(rec.start_position)
                .max(rec.start_position);
            let (replay_len, replay_endpoint) = self.replay_bounds(&rec, from_raw)?;
            if replay_len <= 0 {
                continue;
            }
            self.ensure_runtime()?;
            let sub_uri = format!(
                "aeron:udp?endpoint={}|session-id={}",
                replay_endpoint, rec.session_id
            );
            let rt = self.rt.as_ref().expect("ensured above");
            let rx = match self.deposit_subs.entry((stream_id, rec.session_id)) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(rt.open_subscription::<Deposit>(&sub_uri, stream_id)?)
                }
            };
            let session = self.session.as_ref().expect("ensured above");
            start_bounded_replay(
                session,
                &rec,
                stream_id,
                &replay_endpoint,
                from_raw,
                replay_len,
            )?;
            drain(rx, |(pos, dep)| {
                sink(pos, dep);
                delivered += 1;
            });
        }
        info!(stream_id, delivered, "tx_deposits refetch drained");
        Ok(delivered)
    }

    // ---- internals --------------------------------------------------------

    fn ensure_runtime(&mut self) -> Result<(), LogError> {
        if self.rt.is_none() {
            self.rt = Some(match &self.cfg.aeron_dir {
                Some(dir) => AeronRuntime::spawn_with_dir(dir)?,
                None => AeronRuntime::spawn_default()?,
            });
        }
        Ok(())
    }

    /// Ensure a live control session against one of `endpoints`, starting at
    /// the rotation cursor. Tries each endpoint at most once per call.
    fn ensure_session(&mut self, endpoints: &[String]) -> Result<(), LogError> {
        if endpoints.is_empty() {
            return Err(LogError::Aeron(
                "refetch: no archive endpoints configured".into(),
            ));
        }
        if let (Some(_), Some(cur)) = (&self.session, &self.connected)
            && endpoints.iter().any(|e| e == cur)
        {
            return Ok(());
        }
        let mut last_err: Option<LogError> = None;
        for attempt in 0..endpoints.len() {
            let idx = (self.next_endpoint + attempt) % endpoints.len();
            let ep = &endpoints[idx];
            let mut acfg = self.cfg.aeron.clone();
            acfg.archive_control_request_channel = format!("aeron:udp?endpoint={ep}");
            acfg.archive_control_response_channel =
                format!("aeron:udp?endpoint={}", self.cfg.response_endpoint);
            match connect_archive_with_timeout(
                self.cfg.aeron_dir.as_deref(),
                &acfg,
                CONNECT_TIMEOUT,
            ) {
                Ok(s) => {
                    info!(endpoint = %ep, "refetch: archive control connected");
                    self.session = Some(s);
                    self.connected = Some(ep.clone());
                    self.next_endpoint = idx;
                    return Ok(());
                }
                Err(e) => {
                    warn!(endpoint = %ep, error = %e, "refetch: archive control connect failed");
                    last_err = Some(e);
                }
            }
        }
        self.session = None;
        self.connected = None;
        self.next_endpoint = self.next_endpoint.wrapping_add(1);
        Err(last_err.unwrap_or_else(|| LogError::Aeron("refetch: no endpoint reachable".into())))
    }

    fn rotate(&mut self) {
        self.session = None;
        self.connected = None;
        self.next_endpoint = self.next_endpoint.wrapping_add(1);
    }

    fn list_or_rotate(&mut self, stream_id: i32) -> Result<Vec<FoundRecording>, LogError> {
        let session = self.session.as_ref().expect("ensured by caller");
        match list_recordings(&session.archive, stream_id) {
            Ok(recs) => Ok(recs),
            Err(e) => {
                warn!(error = %e, "refetch: catalog listing failed; rotating endpoint");
                self.rotate();
                Err(e)
            }
        }
    }

    /// Bound the replay: `[from_raw, recorded-position)` on the connected
    /// archive. Also returns the replay destination endpoint.
    fn replay_bounds(
        &mut self,
        rec: &FoundRecording,
        from_raw: i64,
    ) -> Result<(i64, String), LogError> {
        let session = self.session.as_ref().expect("ensured by caller");
        let archive = &session.archive;
        // Active recording ⇒ current recorded position; stopped ⇒ stop position.
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
}

/// Compute the raw stream position of a fragment-start [`BPosition`] within
/// `rec`'s position space: `((term_id - initial_term_id) << log2(term_len)) +
/// term_offset` — the archive replay API addresses recordings by these raw
/// positions.
fn raw_position(rec: &FoundRecording, pos: BPosition) -> Result<i64, LogError> {
    let term_len = rec.term_buffer_length as i64;
    if term_len <= 0 || (term_len & (term_len - 1)) != 0 {
        return Err(LogError::Aeron(format!(
            "refetch: recording {} has non-power-of-two term length {term_len}",
            rec.recording_id
        )));
    }
    let bits = term_len.trailing_zeros();
    let term_count = (pos.term_id as i64) - (rec.initial_term_id as i64);
    if term_count < 0 {
        return Err(LogError::Aeron(format!(
            "refetch: position term {} precedes recording {}'s initial term {}",
            pos.term_id, rec.recording_id, rec.initial_term_id
        )));
    }
    Ok((term_count << bits) + pos.term_offset as i64)
}

/// List every recording for `stream_id` in the archive's catalog (paged).
fn list_recordings(
    archive: &rusteron_archive::AeronArchive,
    stream_id: i32,
) -> Result<Vec<FoundRecording>, LogError> {
    struct Acc {
        recs: Rc<Cell<Vec<FoundRecording>>>,
    }
    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Acc {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut v = self.recs.take();
            v.push(FoundRecording {
                recording_id: desc.recording_id(),
                session_id: desc.session_id(),
                start_position: desc.start_position(),
                initial_term_id: desc.initial_term_id(),
                term_buffer_length: desc.term_buffer_length(),
            });
            self.recs.set(v);
        }
    }
    const PAGE: i32 = 100;
    let recs: Rc<Cell<Vec<FoundRecording>>> = Rc::new(Cell::new(Vec::new()));
    let any_channel = CString::new("").expect("no NUL");
    let mut from_record_id: i64 = 0;
    loop {
        let mut handler = Handler::leak(Acc { recs: recs.clone() });
        let res = archive.list_recordings_for_uri(
            from_record_id,
            PAGE,
            any_channel.as_c_str(),
            stream_id,
            Some(&handler),
        );
        handler.release();
        let count = res.map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;
        if count < PAGE {
            break;
        }
        let v = recs.take();
        let max_id = v.iter().map(|r| r.recording_id).max();
        recs.set(v);
        match max_id {
            Some(id) => from_record_id = id + 1,
            None => break,
        }
    }
    Ok(recs.take())
}

/// Start a bounded replay of `[from_raw, from_raw + len)` onto
/// `replay_endpoint`, pinning the ORIGINAL session id so replayed frames keep
/// their recorded headers. Returns the replay session id.
fn start_bounded_replay(
    session: &ArchiveSession,
    rec: &FoundRecording,
    stream_id: i32,
    replay_endpoint: &str,
    from_raw: i64,
    len: i64,
) -> Result<i64, LogError> {
    let params = AeronArchiveReplayParams::new(0, i32::MAX, from_raw, len, 0, 0)
        .map_err(|e| LogError::Aeron(format!("replay params: {e}")))?;
    let channel = CString::new(format!(
        "aeron:udp?endpoint={replay_endpoint}|session-id={}",
        rec.session_id
    ))
    .map_err(|_| LogError::Aeron("replay channel NUL".into()))?;
    session
        .archive
        .start_replay(rec.recording_id, &channel, stream_id, &params)
        .map_err(|e| LogError::Aeron(format!("start_replay: {e}")))
}

/// Drain a replay subscription: deliver until [`DRAIN_IDLE`] of silence after
/// the last fragment (bounded replay exhausted) or the [`DRAIN_CAP`].
fn drain<T>(rx: &mut UnboundedReceiver<T>, mut deliver: impl FnMut(T)) {
    let start = Instant::now();
    let mut last = Instant::now();
    while start.elapsed() < DRAIN_CAP {
        match rx.try_recv() {
            Ok(item) => {
                deliver(item);
                last = Instant::now();
            }
            Err(_) => {
                if last.elapsed() >= DRAIN_IDLE {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
