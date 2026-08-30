//! Drives an Aeron Archive instance to record the tx_ordering stream and
//! exposes the current durable recording position.
//!
//! Topology after the move to **archive-at-the-sealer** durability: one
//! `Recorder` with `RecorderKind::TxOrdering` co-located with the **sealer**,
//! recording the sealer's tx_ordering MDC publication (see
//! [`Recorder::start_b_mdc`]); its durable position is THE watermark
//! (published by [`run_durable_watermark_loop`]). The old N-recorder Q-of-N
//! quorum aggregator has been removed.
//!
//! (unconditional dep on rusteron.)
//!
//! ## Durability model
//!
//! The Aeron Archive daemon is started with `fileSyncLevel=1` (see
//! [`crate::config::AeronConfig::file_sync_level`] and
//! [`crate::supervisor`]), which means it calls `fdatasync` on the segment
//! file after every recorded frame. As a consequence,
//! [`rusteron_archive::AeronArchive::get_recording_position`] returns a
//! position that is byte-durable on local storage — no separate fsync
//! sidecar is required. The durable-watermark loop
//! ([`run_durable_watermark_loop`]) periodically polls this position and
//! republishes it as the single [`kardamom_types::QuorumWatermark`] ingress
//! gates its must-deliver ack on.
//!
//! ## Design note: thread confinement
//!
//! `AeronArchive` is `!Send + !Sync` (it wraps `Rc` + raw pointers — the C
//! client is thread-confined). Both the recording-position poll and the
//! durable-watermark publish therefore happen on the Recorder thread;
//! cross-thread sharing of the archive handle is not supported.

use std::cell::RefCell;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{AeronConfig, ChannelsConfig, RecorderId};
use crate::error::LogError;
use crate::publisher::QuorumPublisher;
use kardamom_types::QuorumWatermark;

type Archive = rusteron_archive::AeronArchive;
type AeronClient = rusteron_client::Aeron;

/// Connect an Aeron client to the node-local Media Driver. When `aeron_dir`
/// is `Some`, the client joins the driver at that shared-memory directory
/// (the cluster's per-node tmpfs); otherwise the C client's default lookup
/// applies. Thread-confined: the returned client is `!Send` and must be used
/// on the calling thread (see the module's thread-confinement note).
pub fn connect_client(aeron_dir: Option<&Path>) -> Result<AeronClient, LogError> {
    let ctx = rusteron_client::AeronContext::new()
        .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))?;
    if let Some(dir) = aeron_dir {
        let dir_s = dir
            .to_str()
            .ok_or_else(|| LogError::Aeron(format!("aeron.dir not UTF-8: {dir:?}")))?;
        let dir_c = CString::new(dir_s)
            .map_err(|_| LogError::Aeron(format!("aeron.dir contains NUL: {dir_s}")))?;
        ctx.set_dir(dir_c.as_c_str())
            .map_err(|e| LogError::Aeron(format!("set_dir: {e}")))?;
    }
    let aeron = AeronClient::new(&ctx).map_err(|e| LogError::Aeron(format!("Aeron::new: {e}")))?;
    aeron
        .start()
        .map_err(|e| LogError::Aeron(format!("Aeron::start: {e}")))?;
    Ok(aeron)
}

/// A connected Archive control session plus the archive-side Aeron client that
/// must outlive it. `rusteron_archive` bundles its own `Aeron` type (distinct
/// from `rusteron_client::Aeron`), so the recorder runs two client conductors
/// against the same Media Driver: this one for archive control, and a
/// [`connect_client`] one for publishing the fsync watermark.
pub struct ArchiveSession {
    /// The archive's own Aeron client conductor. Kept to outlive `archive`;
    /// also exposed via [`ArchiveSession::aeron`] so a replay subscriber can
    /// open its multi-destination subscription on the same client.
    _aeron: rusteron_archive::Aeron,
    pub archive: Archive,
}

impl ArchiveSession {
    /// The archive-side Aeron client. A replay-merge subscriber opens its
    /// `control-mode=manual` subscription on this client so the subscription
    /// and the archive control session share one media-driver conductor.
    pub fn aeron(&self) -> &rusteron_archive::Aeron {
        &self._aeron
    }
}

/// Connect an `AeronArchive` control session over the configured archive
/// control request/response channels, joining the Media Driver at `aeron_dir`.
/// The recorder uses this to drive `start_recording` and poll the durable
/// recording position. Thread-confined: use the returned session only on the
/// calling thread.
pub fn connect_archive(
    aeron_dir: Option<&Path>,
    cfg: &AeronConfig,
) -> Result<ArchiveSession, LogError> {
    connect_archive_with_timeout(aeron_dir, cfg, Duration::from_secs(30))
}

/// [`connect_archive`] with a caller-chosen connect timeout. The recorder's
/// boot-time connect keeps the generous 30s default; inline callers (the
/// join-miss refetch, which runs inside a join-timeout budget and must fail
/// over to another endpoint quickly when an archive node is down) pass a
/// short one.
pub fn connect_archive_with_timeout(
    aeron_dir: Option<&Path>,
    cfg: &AeronConfig,
    connect_timeout: Duration,
) -> Result<ArchiveSession, LogError> {
    let ctx = rusteron_archive::AeronContext::new()
        .map_err(|e| LogError::Aeron(format!("archive AeronContext::new: {e}")))?;
    if let Some(dir) = aeron_dir {
        let dir_s = dir
            .to_str()
            .ok_or_else(|| LogError::Aeron(format!("aeron.dir not UTF-8: {dir:?}")))?;
        let dir_c = CString::new(dir_s)
            .map_err(|_| LogError::Aeron(format!("aeron.dir contains NUL: {dir_s}")))?;
        ctx.set_dir(dir_c.as_c_str())
            .map_err(|e| LogError::Aeron(format!("archive set_dir: {e}")))?;
    }
    let aeron = rusteron_archive::Aeron::new(&ctx)
        .map_err(|e| LogError::Aeron(format!("archive Aeron::new: {e}")))?;
    aeron
        .start()
        .map_err(|e| LogError::Aeron(format!("archive Aeron::start: {e}")))?;

    let actx = rusteron_archive::AeronArchiveContext::new()
        .map_err(|e| LogError::Aeron(format!("AeronArchiveContext::new: {e}")))?;
    actx.set_aeron(&aeron)
        .map_err(|e| LogError::Aeron(format!("archive set_aeron: {e}")))?;
    let req = CString::new(cfg.archive_control_request_channel.as_str())
        .map_err(|_| LogError::Aeron("archive control request channel NUL".into()))?;
    let resp = CString::new(cfg.archive_control_response_channel.as_str())
        .map_err(|_| LogError::Aeron("archive control response channel NUL".into()))?;
    actx.set_control_request_channel(req.as_c_str())
        .map_err(|e| LogError::Aeron(format!("set_control_request_channel: {e}")))?;
    actx.set_control_response_channel(resp.as_c_str())
        .map_err(|e| LogError::Aeron(format!("set_control_response_channel: {e}")))?;
    // Control-message timeout scales with the connect timeout (never below the
    // recorder's historical 60s when connecting patiently; a short-timeout
    // inline caller gets equally snappy per-operation failure).
    let message_timeout_ns: u64 = if connect_timeout >= Duration::from_secs(30) {
        60_000_000_000
    } else {
        (connect_timeout.as_nanos() as u64).max(1_000_000_000)
    };
    actx.set_message_timeout_ns(message_timeout_ns)
        .map_err(|e| LogError::Aeron(format!("set_message_timeout_ns: {e}")))?;
    let archive = rusteron_archive::AeronArchiveAsyncConnect::new_with_aeron(&actx, &aeron)
        .map_err(|e| LogError::Aeron(format!("archive async connect: {e}")))?
        .poll_blocking(connect_timeout)
        .map_err(|e| LogError::Aeron(format!("archive connect poll: {e}")))?;
    Ok(ArchiveSession {
        _aeron: aeron,
        archive,
    })
}

/// Which logical stream a recorder is tailing.
///
/// `TxOrdering` (recorded once, at the sealer) feeds the single durable
/// watermark. `TxData` / `TxDeposits` are recorded so the executor can replay
/// the full transaction/deposit envelopes on crash recovery (see
/// [`crate::replay`]) — without them, only the canonical order survives a
/// restart, not the bytes needed to re-execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecorderKind {
    /// TxOrdering canonical-orderer recorder (carries tiny TxRefs).
    TxOrdering,
    /// Per-sequencer TxData recorder (carries full `TxEnvelope` bytes).
    TxData { sequencer_id: u8 },
    /// TxDeposits recorder (carries full `Deposit` envelopes from the DA watcher).
    TxDeposits,
}

impl RecorderKind {
    /// Stream label used in operator-facing failure messages
    /// (`"start tx_data recording: ..."`).
    pub fn label(&self) -> &'static str {
        match self {
            RecorderKind::TxOrdering => "tx_ordering",
            RecorderKind::TxData { .. } => "tx_data",
            RecorderKind::TxDeposits => "tx_deposits",
        }
    }
}

/// Body of a dedicated stream-recorder thread — the recorder-thread +
/// ready-barrier pattern shared by the producer binaries (`kardamom-ingress`
/// records tx_data per shard; `kardamom-da-watcher` records tx_deposits):
/// connect a thread-confined archive session, start recording
/// `(channel, stream_id)`, report the startup outcome exactly once through
/// `ready`, and hold the recording (and its archive session) alive until
/// `stop` is cancelled. The recording itself runs in the ArchivingMediaDriver;
/// this thread only keeps the session connected and re-adopts an existing
/// recording on restart.
///
/// `ready` receives `Ok(recording_id)` once the recording is confirmed
/// active, or `Err(reason)` on any failure — including a `stop` during
/// startup — so a waiting barrier never hangs. The callers block on that
/// barrier BEFORE publishing anything (the F13.2 rule): crash recovery
/// replays from record 0 and needs every envelope, so a birth-of-stream gap
/// would permanently break executor crash recovery.
///
/// ## Stop signal
///
/// `stop` is a [`CancellationToken`] — the one seam primitive between the
/// tokio shell and this std thread. The archive session is `!Send`, so the
/// function stays a blocking thread body rather than an async fn. Once the
/// recording is active the thread has nothing to do but wait, so it parks on
/// `stop.cancelled()` via `futures::executor::block_on` — the token's future
/// needs no tokio timer or reactor, so this works with or without a runtime
/// handle on the thread and wakes the instant the token is cancelled (no
/// sleep-poll). During startup the catalog wait still polls the archive on a
/// bounded 500ms cadence (it is waiting on archive state, not on the stop
/// signal), checking `stop.is_cancelled()` each tick.
pub fn record_stream_until_stopped(
    aeron_dir: Option<&Path>,
    aeron_cfg: &AeronConfig,
    channel: &str,
    stream_id: i32,
    kind: RecorderKind,
    stop: &CancellationToken,
    ready: impl FnOnce(Result<i64, String>),
) -> Result<(), LogError> {
    let session = match connect_archive(aeron_dir, aeron_cfg) {
        Ok(s) => s,
        Err(e) => {
            ready(Err(format!("connect archive: {e}")));
            return Err(e);
        }
    };
    let mut should_stop = || stop.is_cancelled();
    let recorder =
        match Recorder::start_stream(session.archive, channel, stream_id, kind, &mut should_stop) {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Stopped before the recording materialised (shutdown during
                // startup); report it so a waiting barrier doesn't hang.
                ready(Err("stopped before the recording materialised".into()));
                return Ok(());
            }
            Err(e) => {
                ready(Err(format!("start {} recording: {e}", kind.label())));
                return Err(e);
            }
        };
    ready(Ok(recorder.recording_id()));
    // Hold the recording (and its archive session) alive until shutdown.
    futures::executor::block_on(stop.cancelled());
    Ok(())
}

pub struct Recorder {
    /// Owned by the Recorder thread. `AeronArchive` is `!Send + !Sync`, so
    /// the field is intentionally not exposed as `Arc<Archive>`; the
    /// recording-position poll and the durable-watermark publish in
    /// [`run_durable_watermark_loop`] both run on this thread.
    archive: Archive,
    recording_id: i64,
    term_buffer_length: i32,
}

impl Recorder {
    /// Start recording the sealer's tx_ordering **MDC** publication — the
    /// archive-at-the-sealer durability path (the locked durability decision:
    /// "archive once at the sealer"). The recording subscribes to the sealer's
    /// own MDC control endpoint (`control_uri`, e.g.
    /// `aeron:udp?control=<sealer-ip>:<port>|control-mode=dynamic`) on
    /// `tx_ordering_stream_id`; its byte-durable `get_recording_position()`
    /// becomes THE durable watermark ingress gates its must-deliver ack on.
    ///
    /// `recorder_id` and `archive_dir` are retained in the signature for
    /// caller compatibility (there is exactly one archive, conventionally
    /// recorder 0); the durable-watermark path no longer needs either.
    pub fn start_b_mdc(
        archive: Archive,
        control_uri: &str,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        archive_dir: PathBuf,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Self>, LogError> {
        let _ = (recorder_id, archive_dir);
        Self::start_inner(
            archive,
            control_uri,
            ch.tx_ordering_stream_id,
            RecorderKind::TxOrdering,
            "B-MDC",
            should_stop,
        )
    }

    /// Start recording an arbitrary `(channel, stream_id)` — the generic entry
    /// point used by the per-sequencer `tx_data` recorder (in the sequencer
    /// process) and the `tx_deposits` recorder (in the DA watcher), so the
    /// executor can replay full transaction / deposit envelopes on crash
    /// recovery. `kind` selects the log label; the channel transport (IPC vs
    /// UDP) chooses the archive source location automatically. Returns
    /// `Ok(None)` if `should_stop` fires before the recording materialises.
    pub fn start_stream(
        archive: Archive,
        channel: &str,
        stream_id: i32,
        kind: RecorderKind,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Self>, LogError> {
        Self::start_inner(archive, channel, stream_id, kind, "stream", should_stop)
    }

    /// Returns `Ok(None)` if `should_stop` fired before a recording appeared
    /// (clean shutdown during startup), `Ok(Some(recorder))` once recording.
    fn start_inner(
        archive: Archive,
        channel: &str,
        stream_id: i32,
        kind: RecorderKind,
        ctx: &str,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Self>, LogError> {
        let channel_c = CString::new(channel)
            .map_err(|e| LogError::Aeron(format!("{ctx} channel contains NUL: {e}")))?;

        // SourceLocation selects HOW the archive subscribes to record the
        // stream. LOCAL records via a "spy" subscription that taps a publication
        // on the SAME media driver — correct for IPC (the publisher is always
        // co-located) and what the single-host e2e relies on. But a spy never
        // opens a network subscription and never joins the multicast group, so
        // for the UDP channels in the multi-host cluster — where the publisher
        // is on ANOTHER node (e.g. tx_ordering is published by the sealer; the
        // recorders run on separate hosts) — LOCAL means the recording never
        // materializes (the recorder logs "waiting for a publisher …" forever).
        // Record UDP channels with REMOTE so the archive opens a real network
        // subscription that joins the group; multicast loopback means REMOTE
        // also works when the UDP publisher happens to be co-located. (Passed as
        // a bool because rusteron's SourceLocation enum type is not public — the
        // SOURCE_LOCATION_* consts are, so the value is chosen inside.)
        let record_remote = !channel.trim_start().starts_with("aeron:ipc");

        let recording_id = match Self::find_or_start_recording(
            &archive,
            channel_c.as_c_str(),
            stream_id,
            kind,
            record_remote,
            should_stop,
        )? {
            Some(id) => id,
            None => return Ok(None), // shutdown before a recording appeared
        };

        // Pull the descriptor once at startup so the term buffer length is
        // available to decode positions without a control-channel round-trip
        // on every watermark tick.
        let term_buffer_length = fetch_descriptor(&archive, recording_id)?;

        Ok(Some(Self {
            archive,
            recording_id,
            term_buffer_length,
        }))
    }

    /// Resolve the recording id for `stream_id`: initiate the recording, then
    /// wait for it to appear in the archive catalog and return its id. Returns
    /// `Ok(None)` if `should_stop` fires first.
    ///
    /// A recording started with auto_stop=false outlives the client that
    /// started it, so a recorder that restarts (or a fresh client against a
    /// long-lived ArchivingMediaDriver, as in the cluster) ADOPTs the existing
    /// recording — a second start_recording on the same (channel, stream) is
    /// rejected, which is fine.
    ///
    /// The catalog descriptor only materializes once a PUBLISHER connects to
    /// the stream (Aeron lists in-progress recordings, not idle ones). In a
    /// cluster the recorders come up BEFORE the sealer/sequencers publish
    /// tx_ordering, so this WAITS (indefinitely, until `should_stop`) rather
    /// than timing out — the process staying alive is what keeps the Nomad
    /// alloc "running" so the rest of the pipeline can be deployed and start
    /// publishing. Discovery uses `list_recordings_for_uri` (matches by stream,
    /// no sessionId — which the recorder doesn't know — unlike
    /// `find_last_matching_recording`).
    fn find_or_start_recording(
        archive: &Archive,
        channel: &std::ffi::CStr,
        stream_id: i32,
        kind: RecorderKind,
        record_remote: bool,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<i64>, LogError> {
        // Initiate the recording. The first caller wins; a second start on the
        // same (channel, stream) is rejected — harmless, the recording exists.
        //
        // NOTE: start_recording returns the *subscription* id, NOT the recording
        // id. The recording id is assigned by the archive and must be looked up
        // from the catalog (below). Using the subscription id with
        // get_recording_position would silently never advance.
        let source_location = if record_remote {
            rusteron_archive::SOURCE_LOCATION_REMOTE
        } else {
            rusteron_archive::SOURCE_LOCATION_LOCAL
        };
        match archive.start_recording(channel, stream_id, source_location, false) {
            Ok(sub_id) => info!(subscription_id = sub_id, ?kind, "recording initiated"),
            Err(e) => {
                info!(error = %e, ?kind, "start_recording rejected (another recorder owns this stream)")
            }
        }

        let mut logged_waiting = false;
        while !should_stop() {
            match active_recording_for_stream(archive, stream_id) {
                Ok(Some(id)) => {
                    info!(recording_id = id, ?kind, "recording ready");
                    return Ok(Some(id));
                }
                Ok(None) => {
                    if !logged_waiting {
                        info!(
                            ?kind,
                            "waiting for a publisher on the stream so the recording materializes"
                        );
                        logged_waiting = true;
                    }
                }
                Err(e) => warn!(error = %e, ?kind, "list_recordings_for_uri failed; retrying"),
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        Ok(None)
    }

    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Current (committed-to-archive-buffer) position for this recording.
    /// Maps to `aeron_archive_get_recording_position` in the C client.
    ///
    /// With `fileSyncLevel=1` configured on the archive daemon, the returned
    /// position is byte-durable on local storage — every byte up to it has
    /// been `fdatasync`'d before the position was published.
    pub fn current_position(&self) -> Result<i64, LogError> {
        self.archive
            .get_recording_position(self.recording_id)
            .map_err(|e| LogError::Aeron(format!("get_recording_position: {e}")))
    }

    /// Decompose an absolute Aeron stream position into the
    /// `(term_id, term_offset)` pair `BPosition` carries, using this
    /// recording's term buffer length.
    fn to_bposition(&self, pos: i64) -> kardamom_types::BPosition {
        let term_len = self.term_buffer_length as i64;
        kardamom_types::BPosition {
            term_id: (pos / term_len) as i32,
            term_offset: (pos % term_len) as i32,
        }
    }
}

/// Poll the sealer's tx_ordering archive recording position and republish it
/// as the single **durable watermark** (`QuorumWatermark` — repurposed to
/// carry the one archive-at-the-sealer durable position, NOT a Q-of-N
/// aggregate) whenever it advances. This is the producer ingress's
/// `on-quorum` ack gate consumes after the custom recorders + quorum
/// aggregator were removed.
///
/// Runs on the calling thread because `AeronArchive` and the publisher are
/// thread-confined.
pub fn run_durable_watermark_loop(
    recorder: &Recorder,
    publisher: &QuorumPublisher,
    poll_interval: Duration,
    mut should_stop: impl FnMut() -> bool,
) -> Result<(), LogError> {
    let mut last_pos: i64 = -1;
    while !should_stop() {
        match recorder.current_position() {
            Ok(pos) if pos > last_pos => {
                let wm = QuorumWatermark {
                    position: recorder.to_bposition(pos),
                };
                if let Err(e) = publisher.publish(&wm) {
                    warn!(error = %e, "durable watermark publish failed");
                } else {
                    last_pos = pos;
                }
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "get_recording_position failed"),
        }
        std::thread::sleep(poll_interval);
    }
    Ok(())
}

/// One-shot descriptor fetch via `list_recording`, returning the recording's
/// term buffer length (needed to decode absolute positions into `BPosition`).
/// We implement the `AeronArchiveRecordingDescriptorConsumerFuncCallback`
/// trait on a small `Rc<RefCell<Captured>>` shim. Single-thread access is
/// enforced by the fact that `AeronArchive` itself is `!Send + !Sync`.
fn fetch_descriptor(archive: &Archive, recording_id: i64) -> Result<i32, LogError> {
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
        Handler,
    };

    #[derive(Default)]
    struct Captured {
        term_buffer_length: i32,
        seen: bool,
    }

    struct Consumer {
        captured: Rc<RefCell<Captured>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut g = self.captured.borrow_mut();
            g.term_buffer_length = desc.term_buffer_length();
            g.seen = true;
        }
    }

    let captured: Rc<RefCell<Captured>> = Rc::new(RefCell::new(Captured::default()));
    let mut handler = Handler::leak(Consumer {
        captured: captured.clone(),
    });

    // `list_recording` invokes the consumer synchronously and does not retain
    // the callback pointer once it returns, so release the leaked handler
    // immediately afterwards — on both the ok and error paths (release before
    // `?`), else every call leaks the boxed `Consumer` and the rusteron `Drop`
    // guard logs a "release() was never called" error.
    let res = archive.list_recording(recording_id, Some(&handler));
    handler.release();
    res.map_err(|e| LogError::Aeron(format!("list_recording: {e}")))?;

    let g = captured.borrow();
    if !g.seen {
        return Err(LogError::Aeron(format!(
            "list_recording({recording_id}) returned no descriptor"
        )));
    }
    Ok(g.term_buffer_length)
}

/// Return the id of the most recent recording for `stream_id`, if any. Used to
/// adopt the recording another recorder already started for a shared stream
/// (several recorders on one archive recording tx_ordering, or a restart
/// against a long-lived archive). Lists by stream + empty channel fragment
/// (matches any channel) and takes the highest recording id — recordings run
/// for the process lifetime (auto_stop=false), so the newest is the live one.
/// Pages through the WHOLE catalog: recording ids are archive-global across
/// all streams, so the newest recording for this stream can sit beyond any
/// single page (adopting a stale id would poll a dead recording's position).
/// Aeron only lists recordings that have an in-progress image, so this returns
/// `None` until a publisher has connected to the stream.
fn active_recording_for_stream(archive: &Archive, stream_id: i32) -> Result<Option<i64>, LogError> {
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
        Handler,
    };

    struct Found {
        /// Highest recording id seen for the stream.
        latest: Option<i64>,
    }

    struct Consumer {
        found: Rc<RefCell<Found>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let id = desc.recording_id();
            let mut g = self.found.borrow_mut();
            g.latest = Some(g.latest.map_or(id, |cur| cur.max(id)));
        }
    }

    const PAGE: i32 = 100;
    let found: Rc<RefCell<Found>> = Rc::new(RefCell::new(Found { latest: None }));
    // Empty channel fragment matches any channel; stream_id narrows to ours.
    let any_channel = CString::new("").expect("empty fragment has no NUL");
    // Page from record id 0 until a page comes back short (each call delivers
    // up to PAGE matching descriptors, scanning the catalog in id order).
    let mut from_record_id: i64 = 0;
    loop {
        // This runs every poll tick in `find_or_start_recording`'s wait loop;
        // `list_recordings_for_uri` calls the consumer synchronously and drops
        // the pointer on return, so release the leaked handler right away
        // (before `?`, so the error path frees it too). Without this each tick
        // leaked a boxed `Consumer` and the rusteron `Drop` guard logged a
        // "release() was never called" error at ~2 Hz, drowning the recorder's
        // logs.
        let mut handler = Handler::leak(Consumer {
            found: found.clone(),
        });
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
            break; // catalog exhausted
        }
        match found.borrow().latest {
            Some(max_id) => from_record_id = max_id + 1,
            None => break, // defensive: full page but no match recorded
        }
    }

    let g = found.borrow();
    Ok(g.latest)
}
