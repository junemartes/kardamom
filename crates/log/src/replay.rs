//! Archive **replay-merge** subscriber: re-reads a recorded UDP-MDC stream from
//! its earliest recorded position and transitions seamlessly to the live
//! stream, delivering decoded `(BPosition, T)` records on an mpsc channel —
//! the same shape as the live [`crate::aeron_live::AeronRuntime::open_subscription`].
//!
//! This is the executor's crash-recovery input: on restart it must re-consume
//! the multicast `tx_data` / `tx_deposits` streams it already saw (to skip-count
//! past its durable cursor — see `kardamom_executor`'s `ResumePoint`) and then
//! continue live. (In CLUSTER-ONLY mode the canonical `tx_ordering` stream is
//! replayed by the Aeron Cluster, not this replay-merge path.) Aeron does
//! **not** replay history to a late live subscriber, so the durable copy lives
//! in the Aeron Archive (recorded by [`crate::recorder`]), and
//! [`rusteron_archive::AeronArchiveReplayMerge`] stitches "replay from the
//! recording" onto "follow the live publication" in a single multi-destination
//! subscription.
//!
//! ## Transport requirement
//!
//! Replay-merge is **UDP only**: it needs the live publication to be a UDP
//! channel and a `control-mode=manual` subscription so the merge can add the
//! replay and live destinations itself. It does not work over the single-host
//! IPC default (Aeron never replays history to an IPC subscriber).
//!
//! ## Position stability
//!
//! Archive replay is position-preserving: a replayed fragment carries the same
//! `(term_id, term_offset)` it had when first recorded. So the `BPosition` this
//! subscriber derives for a record is identical whether the record arrives via
//! replay or live — which is exactly what the executor needs, because that
//! `BPosition` becomes `Receipt.tx_idx`, the cross-replica canonical id. A
//! restarted replica therefore assigns the same `tx_idx` as its peers.
//!
//! ## Thread confinement
//!
//! `AeronArchive` and the replay-merge handle are `!Send + !Sync` (they wrap
//! `Rc` + raw pointers — the C client is thread-confined). The whole replay +
//! live poll loop therefore runs on one dedicated OS thread that the
//! [`ReplayMergeSubscriber`] owns; decoded records cross to the consumer through
//! a `tokio::sync::mpsc` channel, the stop request crosses the other way as a
//! [`CancellationToken`], and a fatal error comes back as the thread's
//! `JoinHandle<Result<(), LogError>>` return value.

use std::cell::Cell;
use std::ffi::CString;
use std::path::PathBuf;
use std::rc::Rc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
    AeronArchiveReplayMerge, Handler, Handlers,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::codec;
use crate::config::AeronConfig;
use crate::error::LogError;
use crate::recorder::connect_archive;
use kardamom_types::{BPosition, TxDataLoc};

/// How long the merge may make no progress before it is considered stuck.
const MERGE_PROGRESS_TIMEOUT_MS: i64 = 60_000;
/// Fragments polled per `poll` call. Matches the rusteron replay-merge example.
const POLL_FRAGMENT_LIMIT: i32 = 100;

/// A live replay-merge subscriber. Owns a dedicated OS thread that replays the
/// recorded stream then follows it live, delivering decoded `(BPosition, T)`
/// records. Drop (or [`shutdown`](Self::shutdown)) to stop the thread.
pub struct ReplayMergeSubscriber<T, L = BPosition> {
    rx: UnboundedReceiver<(L, T)>,
    stop: CancellationToken,
    /// The poll thread. Its return value carries the fatal error it died with
    /// (archive error, coverage gap, decode failure, stuck merge) — `Ok(())`
    /// after a clean stop. See [`take_failure`](Self::take_failure).
    join: Option<JoinHandle<Result<(), LogError>>>,
}

/// Parameters for opening a replay-merge subscriber. The channels mirror the
/// Aeron topology for the merge. `stream_id` is shared by the recording and the
/// live publication. `replay_destination_endpoint` is the local UDP endpoint the
/// archive replays TO (may be shared across streams — the media driver demuxes by
/// stream id). `live_destination` is the full Aeron channel the merge follows
/// once replay catches up — for the surviving multicast streams (tx_data /
/// tx_deposits) this is the publication's own multicast channel, e.g.
/// `aeron:udp?endpoint=239.x:port|interface=...` (the executor joins the group
/// directly).
#[derive(Clone, Debug)]
pub struct ReplayMergeParams {
    /// Aeron media-driver dir (the node-local tmpfs); `None` uses the C client
    /// default.
    pub aeron_dir: Option<PathBuf>,
    /// The stream id shared by the recording and the live publication.
    pub stream_id: i32,
    /// UDP endpoint (`host:port`) the merge binds to receive replayed fragments.
    pub replay_destination_endpoint: String,
    /// The full Aeron channel URI the merge follows live once replay catches up.
    pub live_destination: String,
}

impl<T, L> ReplayMergeSubscriber<T, L>
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    L: Send + 'static,
{
    /// Open a replay-merge subscriber, deriving each record's location `L` from
    /// its `(BPosition, session_id)` via `loc`. Resolves the recording for
    /// `params.stream_id`, replays it from the start, and follows live —
    /// delivering decoded records on the returned channel. The archive client
    /// is connected on the subscriber's own thread (it is `!Send`), so this
    /// returns as soon as the thread is spawned; connection/resolution happen
    /// on that thread and surface as a closed channel on fatal error.
    ///
    /// Archive replay is frame-faithful (see the module-level "Position
    /// stability" note): the replayed fragment header carries the SAME
    /// `(term_id, term_offset)` AND `session_id` it had when first recorded. So
    /// the `session_id` `loc` sees on replay equals the one the live path sees —
    /// which is what lets the executor's session-keyed join (`TxDataLoc`) match
    /// recorded `TxRef.tx_data_session_id` on crash-recovery.
    pub fn open_with_loc<F>(
        params: ReplayMergeParams,
        cfg: AeronConfig,
        loc: F,
    ) -> Result<Self, LogError>
    where
        F: Fn(BPosition, i32) -> L + Send + 'static,
    {
        let (msg_tx, msg_rx) = unbounded_channel::<(L, T)>();
        let stop = CancellationToken::new();
        let stop_thread = stop.clone();

        let join = thread::Builder::new()
            .name("kardamom-replay-merge".into())
            .spawn(move || {
                let res =
                    run_replay_merge::<T, L, F>(&params, &cfg, &stop_thread, loc, move |rec| {
                        // Returns Err only when the consumer has dropped its
                        // receiver — treat as a stop request.
                        msg_tx.send(rec).map_err(|_| ())
                    });
                if let Err(e) = &res {
                    // A dead replay thread is a failed crash recovery, not a
                    // clean end-of-stream: log at error level and return the
                    // failure so the consumer can distinguish the closed
                    // channel from a clean stop (and exit non-zero).
                    error!(error = %e, "replay-merge subscriber stopped with error");
                }
                res
            })
            .map_err(|e| LogError::Aeron(format!("spawn replay-merge thread: {e}")))?;

        Ok(Self {
            rx: msg_rx,
            stop,
            join: Some(join),
        })
    }

    /// Receive the next decoded record, or `None` once the stream ends / the
    /// subscriber is stopped.
    pub async fn recv(&mut self) -> Option<(L, T)> {
        self.rx.recv().await
    }

    /// Take the fatal error the poll thread died with, if any. After
    /// [`recv`](Self::recv) returns `None`, `Some(_)` here means the replay
    /// terminated abnormally (failed crash recovery) and the consumer must
    /// treat it as an error — a `None` is a clean stop/end-of-stream. The
    /// error is moved out; subsequent calls return `None`.
    ///
    /// Joins the poll thread to read its return value. Call this only after
    /// `recv` returned `None` (the thread has already dropped its sender and
    /// is exiting); calling it earlier cancels the thread first so the join
    /// cannot hang. A thread that panicked reports as an `Aeron` error.
    pub fn take_failure(&mut self) -> Option<LogError> {
        let j = self.join.take()?;
        self.stop.cancel();
        match j.join() {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(LogError::Aeron("replay-merge thread panicked".into())),
        }
    }

    /// Borrow the receiver (e.g. to bridge into a sync channel).
    pub fn receiver_mut(&mut self) -> &mut UnboundedReceiver<(L, T)> {
        &mut self.rx
    }

    /// Signal the poll thread to stop and join it.
    pub fn shutdown(mut self) {
        self.stop.cancel();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl<T> ReplayMergeSubscriber<T, BPosition>
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    /// Open a replay-merge subscriber delivering `(BPosition, T)` — the location
    /// is the fragment position alone (session id ignored). Used by the
    /// single-publisher streams (tx_ordering, tx_deposits).
    pub fn open(params: ReplayMergeParams, cfg: AeronConfig) -> Result<Self, LogError> {
        Self::open_with_loc(params, cfg, |pos, _session| pos)
    }
}

impl<T, L> Drop for ReplayMergeSubscriber<T, L> {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// The thread body: connect the archive, resolve the recording, build the
/// replay-merge, and poll it (replay then live) until stopped, delivering each
/// decoded fragment via `deliver`. `deliver` returning `Err(())` (consumer gone)
/// ends the loop.
fn run_replay_merge<T, L, F>(
    params: &ReplayMergeParams,
    cfg: &AeronConfig,
    stop: &CancellationToken,
    loc: F,
    mut deliver: impl FnMut((L, T)) -> Result<(), ()>,
) -> Result<(), LogError>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    F: Fn(BPosition, i32) -> L,
{
    let session = connect_archive(params.aeron_dir.as_deref(), cfg)?;
    let archive = &session.archive;

    // Resolve the recording (and its session id) for the stream. Waits until a
    // publisher has connected so the recording materialises in the catalog.
    let desc = match resolve_recording(archive, params.stream_id, stop)? {
        Some(d) => d,
        None => return Ok(()), // stopped before a recording appeared
    };
    info!(
        recording_id = desc.recording_id,
        session_id = desc.session_id,
        start_position = desc.start_position,
        matching_recordings = desc.matching_recordings,
        stream_id = params.stream_id,
        "replay-merge: recording resolved"
    );
    // Single-session assumption: we replay exactly the latest recording for the
    // stream, and recovery correctness depends on GAPLESS coverage from the
    // stream's origin (the executor skip-counts from record 0, so every record
    // — even in the skipped prefix — must be replayable). Two ways coverage can
    // be broken, both fatal (a warn would surface later as an opaque
    // BoundaryMisaligned / join-timeout crash-loop):
    //   1. More than one recording matches the stream: a restarted publisher
    //      creates a NEW session whose recording starts at position 0 again, so
    //      the earlier sessions' records are invisible to this replay.
    //   2. The latest recording itself does not start at position 0 (the
    //      recorder attached after the publication had already offered
    //      fragments) — Aeron also rejects a replay from before a recording's
    //      start_position.
    // Stitching multiple recordings/sessions is future work; until then, fail
    // hard with a diagnosable error instead of silently omitting records.
    if desc.matching_recordings > 1 {
        return Err(LogError::Aeron(format!(
            "replay-merge: {} recordings exist for stream {} (publisher restarted?); \
             replaying only the latest (id {}) would silently skip every earlier \
             session's records — refusing to run an incomplete recovery",
            desc.matching_recordings, params.stream_id, desc.recording_id
        )));
    }
    if desc.start_position != 0 {
        return Err(LogError::Aeron(format!(
            "replay-merge: recording {} for stream {} starts at position {} (not 0) — \
             records published before the recording began cannot be replayed; \
             refusing to run an incomplete recovery",
            desc.recording_id, params.stream_id, desc.start_position
        )));
    }

    // Manual-mode subscription filtered to the recording's session, so the
    // replay and live destinations the merge adds carry exactly this stream.
    let subscribe_channel = CString::new(format!(
        "aeron:udp?control-mode=manual|session-id={}",
        desc.session_id
    ))
    .map_err(|_| LogError::Aeron("subscribe channel NUL".into()))?;
    let subscription = session
        .aeron()
        .add_subscription(
            &subscribe_channel,
            params.stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(10),
        )
        .map_err(|e| LogError::Aeron(format!("add replay-merge subscription: {e}")))?;

    let replay_channel = CString::new(format!("aeron:udp?session-id={}", desc.session_id))
        .map_err(|_| LogError::Aeron("replay channel NUL".into()))?;
    let replay_destination = CString::new(format!(
        "aeron:udp?endpoint={}",
        params.replay_destination_endpoint
    ))
    .map_err(|_| LogError::Aeron("replay destination NUL".into()))?;
    let live_destination = CString::new(params.live_destination.clone())
        .map_err(|_| LogError::Aeron("live destination NUL".into()))?;

    let merge = AeronArchiveReplayMerge::new(
        &subscription,
        archive,
        &replay_channel,
        &replay_destination,
        &live_destination,
        desc.recording_id,
        // Replay from the recording's own start (guaranteed 0 by the coverage
        // check above; passing the descriptor's value keeps this correct if
        // that invariant ever changes rather than hardcoding an assumption).
        desc.start_position,
        rusteron_archive::Aeron::epoch_clock(),
        MERGE_PROGRESS_TIMEOUT_MS,
    )
    .map_err(|e| LogError::Aeron(format!("AeronArchiveReplayMerge::new: {e}")))?;

    // Poll replay→live until stopped. A decode/deliver failure inside the
    // handler is recorded in `handler_err`; we surface it after the poll call
    // returns (the FnMut handler cannot itself return a Result).
    let handler_err: Cell<Option<LogError>> = Cell::new(None);
    let consumer_gone = Cell::new(false);
    while !stop.is_cancelled() {
        if merge.has_failed() {
            return Err(LogError::Aeron("replay-merge failed".into()));
        }
        let n = merge
            .poll_once(
                |bytes: &[u8], header| {
                    let (pos, session) = match header_loc(&header) {
                        Some(p) => p,
                        None => {
                            handler_err.set(Some(LogError::Aeron(
                                "replay-merge header had no frame values".into(),
                            )));
                            return;
                        }
                    };
                    match codec::materialize::<T>(bytes) {
                        Ok(v) => {
                            if deliver((loc(pos, session), v)).is_err() {
                                consumer_gone.set(true);
                            }
                        }
                        Err(e) => handler_err.set(Some(e)),
                    }
                },
                POLL_FRAGMENT_LIMIT,
            )
            .map_err(|e| LogError::Aeron(format!("replay-merge poll: {e}")))?;

        if let Some(e) = handler_err.take() {
            return Err(e);
        }
        if consumer_gone.get() {
            return Ok(());
        }
        if n == 0 {
            // No fragments: surface any archive-side error, dispatch recording
            // signals (recording-stopped etc., matching the rusteron replay-merge
            // reference), then back off briefly so we don't spin the CPU during
            // the connect/merge wait.
            let err = archive
                .poll_for_error_response_as_string(4096)
                .map_err(|e| LogError::Aeron(format!("poll_for_error_response: {e}")))?;
            if !err.is_empty() {
                return Err(LogError::Aeron(format!(
                    "archive error during replay: {err}"
                )));
            }
            let _ = archive.poll_for_recording_signals();
            // Bounded back-off on this std thread: it waits on archive-side
            // state (fragments arriving), not on the stop signal, so a short
            // sleep is the right tool; the token is re-checked each lap.
            thread::sleep(Duration::from_millis(10));
        }
    }
    let _ = merge.close();
    Ok(())
}

/// A resolved recording: its id, the session id of the publication it records
/// (needed to filter the replay-merge subscription to one stream), its start
/// position, and how many recordings matched the stream in total (>1 means an
/// earlier publisher session exists — a coverage gap for replay).
struct ResolvedRecording {
    recording_id: i64,
    session_id: i32,
    start_position: i64,
    matching_recordings: usize,
}

/// Catalog page size per `list_recordings_for_uri` call. The catalog is paged
/// through to exhaustion — recording ids are archive-global across all streams
/// (10+ recorders per boot, one new recording per process restart), so the
/// newest recording for a stream can sit far beyond the first page.
const CATALOG_PAGE: i32 = 100;

/// Find the most-recent recording for `stream_id` (paging through the whole
/// archive catalog), waiting (until `stop`) for a publisher to connect so the
/// recording appears in the catalog. Mirrors
/// `recorder::active_recording_for_stream` but also captures the session id,
/// start position, and total match count.
fn resolve_recording(
    archive: &rusteron_archive::AeronArchive,
    stream_id: i32,
    stop: &CancellationToken,
) -> Result<Option<ResolvedRecording>, LogError> {
    struct Found {
        latest: Cell<Option<(i64, i32, i64)>>,
        matches: Cell<usize>,
    }
    struct Consumer {
        found: Rc<Found>,
    }
    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let id = desc.recording_id();
            let entry = (id, desc.session_id(), desc.start_position());
            self.found.matches.set(self.found.matches.get() + 1);
            let cur = self.found.latest.get();
            // Keep the highest recording id (the live one; recordings run for
            // the process lifetime with auto_stop=false).
            let next = match cur {
                Some((cid, _, _)) if cid >= id => cur,
                _ => Some(entry),
            };
            self.found.latest.set(next);
        }
    }

    let mut logged = false;
    while !stop.is_cancelled() {
        let found = Rc::new(Found {
            latest: Cell::new(None),
            matches: Cell::new(0),
        });
        let any_channel = CString::new("").expect("empty fragment has no NUL");
        // Page from record id 0 until a page comes back short: each call scans
        // the catalog in id order from `from_record_id` and delivers up to
        // CATALOG_PAGE *matching* descriptors, so a full page means more
        // catalog may remain beyond the highest id delivered.
        let mut from_record_id: i64 = 0;
        loop {
            let mut handler = Handler::leak(Consumer {
                found: found.clone(),
            });
            let res = archive.list_recordings_for_uri(
                from_record_id,
                CATALOG_PAGE,
                any_channel.as_c_str(),
                stream_id,
                Some(&handler),
            );
            handler.release();
            let count =
                res.map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;
            if count < CATALOG_PAGE {
                break; // catalog exhausted
            }
            // A full page: continue after the highest matching id seen so far
            // (the scan is in id order, so everything at or below it is done).
            match found.latest.get() {
                Some((max_id, _, _)) => from_record_id = max_id + 1,
                None => break, // defensive: full page but no match recorded
            }
        }

        if let Some((recording_id, session_id, start_position)) = found.latest.get() {
            return Ok(Some(ResolvedRecording {
                recording_id,
                session_id,
                start_position,
                matching_recordings: found.matches.get(),
            }));
        }
        if !logged {
            info!(
                stream_id,
                "replay-merge: waiting for a publisher so the recording materialises"
            );
            logged = true;
        }
        // Catalog poll cadence (std thread, waiting on archive state); the
        // stop token is re-checked every lap so cancellation lands within
        // one tick.
        thread::sleep(Duration::from_millis(500));
    }
    Ok(None)
}

/// Derive the canonical `BPosition` (`term_id`, `term_offset`) from a replay /
/// live fragment header — identical to the live subscriber's `header_pos`, so a
/// record's id is the same whether it arrives via replay or live.
/// Read the fragment-start [`BPosition`] and the original publisher `session_id`
/// from a replayed (or live) fragment header. Archive replay is frame-faithful,
/// so during replay these are the values the fragment had when first recorded —
/// not the replay transport's own session. Mirrors `aeron_live::header_loc`.
fn header_loc(h: &rusteron_archive::AeronHeader) -> Option<(BPosition, i32)> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    let pos = BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    };
    Some((pos, frame.session_id()))
}

/// Open a replay-merge subscriber for one per-sequencer `tx_data` shard,
/// decoding [`kardamom_types::TxEnvelope`]. tx_data is a **multicast**
/// publication, so the merge follows the publication's own multicast channel
/// directly (the executor joins the group) — no local live endpoint. The
/// `replay_destination_endpoint` is the local UDP endpoint the archive replays
/// to; it may be shared with other streams (the driver demuxes by stream id).
pub fn open_tx_data_replay(
    ch: &crate::config::ChannelsConfig,
    aeron: &AeronConfig,
    shard_id: u8,
    replay_destination_endpoint: String,
) -> Result<ReplayMergeSubscriber<kardamom_types::TxEnvelope, TxDataLoc>, LogError> {
    // tx_data carries the publisher (ingress) session id, which the executor's
    // join keys on (`TxDataLoc`). Replay is frame-faithful, so we recover the
    // original session from each replayed fragment header — identical to the
    // live path — and the session-keyed join matches recorded TxRefs on resume.
    ReplayMergeSubscriber::open_with_loc(
        ReplayMergeParams {
            aeron_dir: Some(aeron.aeron_dir.clone()),
            stream_id: ch.tx_data_stream_id(shard_id),
            replay_destination_endpoint,
            live_destination: ch.tx_data_channel(shard_id),
        },
        aeron.clone(),
        |pos, session| TxDataLoc::new(session, pos),
    )
}

/// Open a replay-merge subscriber for the `tx_deposits` stream, decoding
/// [`kardamom_types::Deposit`]. Like tx_data it is multicast, so the live
/// destination is the publication's own multicast channel.
pub fn open_tx_deposits_replay(
    ch: &crate::config::ChannelsConfig,
    aeron: &AeronConfig,
    replay_destination_endpoint: String,
) -> Result<ReplayMergeSubscriber<kardamom_types::Deposit>, LogError> {
    ReplayMergeSubscriber::open(
        ReplayMergeParams {
            aeron_dir: Some(aeron.aeron_dir.clone()),
            stream_id: ch.tx_deposits_stream_id,
            replay_destination_endpoint,
            live_destination: ch.tx_deposits_channel.clone(),
        },
        aeron.clone(),
    )
}
