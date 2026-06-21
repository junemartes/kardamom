//! Archive **replay-merge** subscriber: re-reads a recorded UDP-MDC stream from
//! its earliest recorded position and transitions seamlessly to the live
//! stream, delivering decoded `(BPosition, T)` records on an mpsc channel —
//! the same shape as the live [`crate::aeron_live::AeronRuntime::open_subscription`].
//!
//! This is the executor's crash-recovery input: on restart it must re-consume
//! the canonical `tx_ordering` stream it already saw (to skip-count past its
//! durable cursor — see `kardamom_executor`'s `ResumePoint`) and then continue
//! live. Aeron does **not** replay history to a late live subscriber, so the
//! durable copy lives in the Aeron Archive (recorded by [`crate::recorder`]),
//! and [`rusteron_archive::AeronArchiveReplayMerge`] stitches "replay from the
//! recording" onto "follow the live publication" in a single multi-destination
//! subscription.
//!
//! ## Transport requirement
//!
//! Replay-merge is **UDP-MDC only**: it needs the live publication to be a
//! dynamic-MDC UDP channel and a `control-mode=manual` subscription so the merge
//! can add the replay and live destinations itself. It does not work over the
//! single-host IPC default (Aeron never replays history to an IPC subscriber).
//! Callers gate on [`crate::config::ChannelsConfig::tx_ordering_mdc_enabled`].
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
//! a `tokio::sync::mpsc` channel.

use std::cell::Cell;
use std::ffi::CString;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
    AeronArchiveReplayMerge, Handler, Handlers,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::{info, warn};

use crate::codec;
use crate::config::AeronConfig;
use crate::error::LogError;
use crate::recorder::connect_archive;
use kardamom_types::BPosition;

/// How long the merge may make no progress before it is considered stuck.
const MERGE_PROGRESS_TIMEOUT_MS: i64 = 60_000;
/// Fragments polled per `poll` call. Matches the rusteron replay-merge example.
const POLL_FRAGMENT_LIMIT: i32 = 100;

/// A live replay-merge subscriber. Owns a dedicated OS thread that replays the
/// recorded stream then follows it live, delivering decoded `(BPosition, T)`
/// records. Drop (or [`shutdown`](Self::shutdown)) to stop the thread.
pub struct ReplayMergeSubscriber<T> {
    rx: UnboundedReceiver<(BPosition, T)>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

/// Parameters for opening a replay-merge subscriber. The channels mirror the
/// Aeron topology for the merge. `stream_id` is shared by the recording and the
/// live publication. `replay_destination_endpoint` is the local UDP endpoint the
/// archive replays TO (may be shared across streams — the media driver demuxes by
/// stream id). `live_destination` is the full Aeron channel the merge follows
/// once replay catches up — its shape depends on the publication transport:
/// - MDC dynamic-control (tx_ordering): `aeron:udp?endpoint=<local>|control=<ctl>`
///   (the executor receives the MDC stream on its own endpoint), built via
///   [`build_mdc_live_destination`].
/// - Multicast (tx_data / tx_deposits): the publication's own multicast channel,
///   e.g. `aeron:udp?endpoint=239.x:port|interface=...` (the executor joins the
///   group directly).
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

impl<T> ReplayMergeSubscriber<T>
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    /// Open a replay-merge subscriber. Resolves the recording for
    /// `params.stream_id`, replays it from the start, and follows live —
    /// delivering decoded records on the returned channel. The archive client
    /// is connected on the subscriber's own thread (it is `!Send`), so this
    /// returns as soon as the thread is spawned; connection/resolution happen
    /// on that thread and surface as a closed channel on fatal error.
    pub fn open(params: ReplayMergeParams, cfg: AeronConfig) -> Result<Self, LogError> {
        let (msg_tx, msg_rx) = unbounded_channel::<(BPosition, T)>();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();

        let join = thread::Builder::new()
            .name("kardamom-replay-merge".into())
            .spawn(move || {
                if let Err(e) = run_replay_merge::<T>(&params, &cfg, &stop_thread, move |rec| {
                    // Returns Err only when the consumer has dropped its
                    // receiver — treat as a stop request.
                    msg_tx.send(rec).map_err(|_| ())
                }) {
                    warn!(error = %e, "replay-merge subscriber stopped with error");
                }
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
    pub async fn recv(&mut self) -> Option<(BPosition, T)> {
        self.rx.recv().await
    }

    /// Borrow the receiver (e.g. to bridge into a sync channel).
    pub fn receiver_mut(&mut self) -> &mut UnboundedReceiver<(BPosition, T)> {
        &mut self.rx
    }

    /// Signal the poll thread to stop and join it.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl<T> Drop for ReplayMergeSubscriber<T> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// The thread body: connect the archive, resolve the recording, build the
/// replay-merge, and poll it (replay then live) until stopped, delivering each
/// decoded fragment via `deliver`. `deliver` returning `Err(())` (consumer gone)
/// ends the loop.
fn run_replay_merge<T>(
    params: &ReplayMergeParams,
    cfg: &AeronConfig,
    stop: &AtomicBool,
    mut deliver: impl FnMut((BPosition, T)) -> Result<(), ()>,
) -> Result<(), LogError>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
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
        stream_id = params.stream_id,
        "replay-merge: recording resolved"
    );
    // Single-session assumption: we replay exactly the latest recording for the
    // stream. If its start_position is non-zero, the publisher (e.g. the sealer
    // for tx_ordering) restarted and an EARLIER session holds records before
    // this one — replaying only this recording would skip them, and the
    // executor's boundary-alignment check would then crash-loop. Warn loudly so
    // the gap is diagnosable rather than surfacing as an opaque misalignment.
    if desc.start_position != 0 {
        warn!(
            recording_id = desc.recording_id,
            start_position = desc.start_position,
            stream_id = params.stream_id,
            "replay-merge: latest recording does not start at position 0 — an earlier \
             publisher session may hold records this replay will skip"
        );
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
        0, // start_position: from the earliest recorded position
        rusteron_archive::Aeron::epoch_clock(),
        MERGE_PROGRESS_TIMEOUT_MS,
    )
    .map_err(|e| LogError::Aeron(format!("AeronArchiveReplayMerge::new: {e}")))?;

    // Poll replay→live until stopped. A decode/deliver failure inside the
    // handler is recorded in `handler_err`; we surface it after the poll call
    // returns (the FnMut handler cannot itself return a Result).
    let handler_err: Cell<Option<LogError>> = Cell::new(None);
    let consumer_gone = Cell::new(false);
    while !stop.load(Ordering::Acquire) {
        if merge.has_failed() {
            return Err(LogError::Aeron("replay-merge failed".into()));
        }
        let n = merge
            .poll_once(
                |bytes: &[u8], header| {
                    let pos = match header_bposition(&header) {
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
                            if deliver((pos, v)).is_err() {
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
            thread::sleep(Duration::from_millis(10));
        }
    }
    let _ = merge.close();
    Ok(())
}

/// A resolved recording: its id, the session id of the publication it records
/// (needed to filter the replay-merge subscription to one stream), and its
/// start position (used to detect a publisher-restart session gap).
struct ResolvedRecording {
    recording_id: i64,
    session_id: i32,
    start_position: i64,
}

/// Find the most-recent recording for `stream_id`, waiting (until `stop`) for a
/// publisher to connect so the recording appears in the catalog. Mirrors
/// `recorder::active_recording_for_stream` but also captures the session id.
fn resolve_recording(
    archive: &rusteron_archive::AeronArchive,
    stream_id: i32,
    stop: &AtomicBool,
) -> Result<Option<ResolvedRecording>, LogError> {
    struct Found {
        latest: Cell<Option<(i64, i32, i64)>>,
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
    while !stop.load(Ordering::Acquire) {
        let found = Rc::new(Found {
            latest: Cell::new(None),
        });
        let mut handler = Handler::leak(Consumer {
            found: found.clone(),
        });
        let any_channel = CString::new("").expect("empty fragment has no NUL");
        let res = archive.list_recordings_for_uri(
            0,
            100,
            any_channel.as_c_str(),
            stream_id,
            Some(&handler),
        );
        handler.release();
        res.map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;

        if let Some((recording_id, session_id, start_position)) = found.latest.get() {
            return Ok(Some(ResolvedRecording {
                recording_id,
                session_id,
                start_position,
            }));
        }
        if !logged {
            info!(
                stream_id,
                "replay-merge: waiting for a publisher so the recording materialises"
            );
            logged = true;
        }
        thread::sleep(Duration::from_millis(500));
    }
    Ok(None)
}

/// Derive the canonical `BPosition` (`term_id`, `term_offset`) from a replay /
/// live fragment header — identical to the live subscriber's `header_pos`, so a
/// record's id is the same whether it arrives via replay or live.
fn header_bposition(h: &rusteron_archive::AeronHeader) -> Option<BPosition> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    Some(BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    })
}

/// Build the replay-merge live destination for the MDC `tx_ordering` stream:
/// the executor receives the MDC stream on its own `live_destination_endpoint`,
/// attached to the publisher's dynamic-control endpoint (extracted from the
/// canonical subscriber URI, e.g.
/// `aeron:udp?control=10.0.0.1:40010|control-mode=dynamic`).
fn build_mdc_live_destination(
    live_channel: &str,
    live_destination_endpoint: &str,
) -> Result<String, LogError> {
    for tok in live_channel.trim_start_matches("aeron:udp?").split('|') {
        if let Some(ctl) = tok.strip_prefix("control=") {
            return Ok(format!(
                "aeron:udp?endpoint={live_destination_endpoint}|control={ctl}"
            ));
        }
    }
    Err(LogError::Config(format!(
        "live channel {live_channel:?} has no control= endpoint (tx_ordering replay-merge needs \
         an MDC dynamic-control publication)"
    )))
}

/// Open a replay-merge subscriber for the canonical `tx_ordering` stream
/// (MDC dynamic-control), decoding [`kardamom_types::TxOrderingMessage`]. The
/// executor uses this on crash recovery instead of the live tx_ordering
/// subscriber.
pub fn open_tx_ordering_replay(
    ch: &crate::config::ChannelsConfig,
    aeron: &AeronConfig,
    replay_destination_endpoint: String,
    live_destination_endpoint: String,
) -> Result<ReplayMergeSubscriber<kardamom_types::TxOrderingMessage>, LogError> {
    let live_channel = ch.tx_ordering_canonical_subscriber_uri().ok_or_else(|| {
        LogError::Config(
            "tx_ordering replay requires MDC (tx_ordering_canonical_publisher is unset)".into(),
        )
    })?;
    let live_destination = build_mdc_live_destination(&live_channel, &live_destination_endpoint)?;
    ReplayMergeSubscriber::open(
        ReplayMergeParams {
            aeron_dir: Some(aeron.aeron_dir.clone()),
            stream_id: ch.tx_ordering_stream_id,
            replay_destination_endpoint,
            live_destination,
        },
        aeron.clone(),
    )
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
) -> Result<ReplayMergeSubscriber<kardamom_types::TxEnvelope>, LogError> {
    ReplayMergeSubscriber::open(
        ReplayMergeParams {
            aeron_dir: Some(aeron.aeron_dir.clone()),
            stream_id: ch.tx_data_stream_id(shard_id),
            replay_destination_endpoint,
            live_destination: ch.tx_data_channel(shard_id),
        },
        aeron.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mdc_live_destination_built_from_control_uri() {
        assert_eq!(
            build_mdc_live_destination(
                "aeron:udp?control=10.0.0.1:40010|control-mode=dynamic",
                "10.0.0.2:40120"
            )
            .unwrap(),
            "aeron:udp?endpoint=10.0.0.2:40120|control=10.0.0.1:40010"
        );
        // A non-MDC (endpoint-only) live channel errors — tx_ordering replay needs MDC.
        assert!(
            build_mdc_live_destination("aeron:udp?endpoint=10.0.0.1:40010", "10.0.0.2:40120")
                .is_err()
        );
    }
}
