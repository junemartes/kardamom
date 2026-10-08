//! Drives an Aeron Archive instance to record a stream and exposes the
//! current durable recording position.
//!
//! Topology: the ingress records the `TxData` lanes and the DA watcher
//! records `TxDeposits`, so the executor can replay full transaction and
//! deposit envelopes on crash recovery (see [`Recorder::start_stream`]).
//! Each executor records its own `ExecTxs` publication on the archive of
//! its node, and reads the recording position back
//! ([`record_stream_reporting`]).
//! A producer binary spawns those through [`RecorderThreads`], which
//! stops and joins them when it drops.
//!
//! This module has an unconditional dependency on rusteron.
//!
//! ## Durability model
//!
//! The cluster deploy starts the Aeron Archive with
//! `aeron.archive.file.sync.level=1` (the `archive_file_sync_level`
//! variable of the `aeron` job). This makes it call `fdatasync` on the
//! segment file after every recorded write batch. At level 1,
//! [`rusteron_archive::AeronArchive::get_recording_position`] returns a
//! position that is byte-durable on local storage. No separate fsync
//! sidecar is needed.
//!
//! ## Design note: thread confinement
//!
//! `AeronArchive` is `!Send + !Sync` (it wraps `Rc` and raw pointers; the C
//! client is thread-confined). Every call on the archive handle runs on the
//! Recorder thread. Cross-thread sharing of the archive handle is not
//! supported.

use std::cell::RefCell;
use std::ops::ControlFlow;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::archive_catalog::ArchiveCatalog;
use crate::config::AeronConfig;
use crate::driver_budget::DriverBudget;
use crate::error::LogError;

type Archive = rusteron_archive::AeronArchive;

/// A connected Archive control session plus the archive-side Aeron client
/// that must outlive it. `rusteron_archive` bundles its own `Aeron` type,
/// distinct from `rusteron_client::Aeron`.
pub struct ArchiveSession {
    /// The archive's own Aeron client conductor. This field keeps the
    /// conductor alive for as long as `archive`. It is also exposed through
    /// [`ArchiveSession::aeron`], so a replay subscriber can open its
    /// multi-destination subscription on the same client.
    aeron_client: rusteron_archive::Aeron,
    pub archive: Archive,
    /// The wait for a quiet archive, from the driver timeout of this
    /// client. It is read once, when the session connects.
    driver_budget: DriverBudget,
}

impl ArchiveSession {
    /// The archive-side Aeron client. A replay-merge subscriber opens its
    /// `control-mode=manual` subscription on this client, so the
    /// subscription and the archive control session share one
    /// media-driver conductor.
    #[must_use]
    pub fn aeron(&self) -> &rusteron_archive::Aeron {
        &self.aeron_client
    }
}

/// Connect an `AeronArchive` control session over the configured archive
/// control request/response channels, joining the Media Driver at
/// `aeron_dir`. The recorder uses this to drive `start_recording` and poll
/// the durable recording position. The returned session is thread-confined:
/// use it only on the calling thread.
///
/// # Errors
///
/// Returns an error if the archive control session fails to connect
/// within 30 s (see
/// [`connect_archive_with_timeout`]).
pub fn connect_archive(
    aeron_dir: Option<&Path>,
    cfg: &AeronConfig,
) -> Result<ArchiveSession, LogError> {
    connect_archive_with_timeout(aeron_dir, cfg, Duration::from_secs(30))
}

/// [`connect_archive`] with a caller-chosen connect timeout. The recorder's
/// boot-time connect keeps the generous 30 second default. Inline callers
/// pass a short timeout instead. One example is the join-miss refetch,
/// which runs inside a join-timeout budget and must fail over quickly to
/// another endpoint when an archive node is down.
///
/// # Errors
///
/// Returns an error if `aeron_dir` is not UTF-8 or contains a NUL
/// byte, if any of the archive channel strings contain a NUL byte,
/// or if the archive control session fails to connect within
/// `connect_timeout`.
pub fn connect_archive_with_timeout(
    aeron_dir: Option<&Path>,
    cfg: &AeronConfig,
    connect_timeout: Duration,
) -> Result<ArchiveSession, LogError> {
    let ctx = rusteron_archive::AeronContext::new()
        .map_err(|e| LogError::Aeron(format!("archive AeronContext::new: {e}")))?;
    let driver_budget = DriverBudget::of_archive_client(&ctx)?;
    if let Some(dir) = aeron_dir {
        let dir_c = crate::ffi::dir_cstring(dir)?;
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
    let req = crate::ffi::c_uri(
        &cfg.archive_control_request_channel,
        "archive control request channel",
    )?;
    let resp = crate::ffi::c_uri(
        &cfg.archive_control_response_channel,
        "archive control response channel",
    )?;
    actx.set_control_request_channel(req.as_c_str())
        .map_err(|e| LogError::Aeron(format!("set_control_request_channel: {e}")))?;
    actx.set_control_response_channel(resp.as_c_str())
        .map_err(|e| LogError::Aeron(format!("set_control_response_channel: {e}")))?;
    // The control-message timeout scales with the connect timeout. The
    // floor is 60 s when connecting patiently. A short-timeout inline
    // caller gets an equally quick per-operation failure.
    let message_timeout_ns: u64 = if connect_timeout >= Duration::from_secs(30) {
        60_000_000_000
    } else {
        // Floor at 1 s in the `Duration` domain, before converting to
        // nanoseconds, so a very short caller timeout still gives the
        // archive a sane per-operation budget.
        let floored = connect_timeout.max(Duration::from_secs(1));
        u64::try_from(floored.as_nanos()).map_err(|_| {
            LogError::Aeron(format!(
                "connect_timeout {floored:?} does not fit u64 nanoseconds"
            ))
        })?
    };
    actx.set_message_timeout_ns(message_timeout_ns)
        .map_err(|e| LogError::Aeron(format!("set_message_timeout_ns: {e}")))?;
    let archive = rusteron_archive::AeronArchiveAsyncConnect::new_with_aeron(&actx, &aeron)
        .map_err(|e| LogError::Aeron(format!("archive async connect: {e}")))?
        .poll_blocking(connect_timeout)
        .map_err(|e| LogError::Aeron(format!("archive connect poll: {e}")))?;
    Ok(ArchiveSession {
        aeron_client: aeron,
        archive,
        driver_budget,
    })
}

/// Which logical stream a recorder is tailing.
///
/// `TxData` and `TxDeposits` are recorded so the executor can replay the
/// full transaction and deposit envelopes on crash recovery (see
/// [`crate::refetch`]). Without them, only the canonical order survives a
/// restart, not the bytes needed to re-execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecorderKind {
    /// Per-sequencer `TxData` recorder (carries full `TxEnvelope` bytes).
    TxData { sequencer_id: u8 },
    /// `TxDeposits` recorder (carries full `Deposit` envelopes from the DA watcher).
    TxDeposits,
    /// `ExecTxs` recorder of one executor publication (carries one
    /// `ExecTxRecord` for each joined transaction). The recorder adopts
    /// only the recording of `session_id`, so a restarted executor never
    /// reads the position of the recording of its earlier session.
    ExecTxs { session_id: i32 },
}

impl RecorderKind {
    /// Stream label used in operator-facing failure messages
    /// (`"start tx_data recording: ..."`).
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            RecorderKind::TxData { .. } => "tx_data",
            RecorderKind::TxDeposits => "tx_deposits",
            RecorderKind::ExecTxs { .. } => "exec_txs",
        }
    }

    /// The publisher session whose recording this kind adopts, or `None`
    /// to adopt the newest recording of the stream.
    fn session_id(self) -> Option<i32> {
        match self {
            RecorderKind::ExecTxs { session_id } => Some(session_id),
            RecorderKind::TxData { .. } | RecorderKind::TxDeposits => None,
        }
    }
}

/// The stream that [`record_stream_reporting`] records.
#[derive(Clone, Copy, Debug)]
pub struct RecordedStream<'a> {
    pub channel: &'a str,
    pub stream_id: i32,
    pub kind: RecorderKind,
}

/// Body of a dedicated stream-recorder thread. This is the recorder-thread
/// plus ready-barrier pattern shared by the producer binaries
/// (`kardamom-ingress` records `tx_data` per shard; `kardamom-da-watcher`
/// records `tx_deposits`). It connects a thread-confined archive session,
/// starts recording `(channel, stream_id)`, reports the startup outcome
/// exactly once through `ready`, and holds the recording (and its archive
/// session) alive until `stop` is cancelled. The recording itself runs in the
/// `ArchivingMediaDriver`. This thread only keeps the session connected and
/// re-adopts an existing recording on restart.
///
/// `ready` receives `Ok(recording_id)` once the recording is confirmed
/// active, or `Err(reason)` on any failure, including a `stop` during
/// startup, so a waiting barrier never hangs. Callers must block on that
/// barrier before they publish anything. Crash recovery replays from record
/// 0 and needs every envelope, so a gap at the birth of the stream would
/// permanently break executor crash recovery.
///
/// ## Stop signal
///
/// `stop` is a [`CancellationToken`]. It is the one seam primitive between
/// the tokio shell and this std thread. The archive session is `!Send`. So
/// the function stays a blocking thread body, not an async fn. Once the
/// recording is active, the thread only waits. It parks on
/// `stop.cancelled()` through `futures::executor::block_on`. The token's
/// future needs no tokio timer or reactor. So this works with or without a
/// runtime handle on the thread. It wakes the instant the token is
/// cancelled; it does not sleep-poll. During startup, the catalog wait
/// still polls the archive on a bounded 500 ms cadence, because it waits
/// on archive state, not on the stop signal. It checks
/// `stop.is_cancelled()` on each tick.
///
/// # Errors
///
/// Returns an error if the archive control session fails to connect,
/// or if starting the recording fails (see
/// [`Recorder::start_stream`]). `ready` still receives the failure
/// reason on every error path.
pub fn record_stream_until_stopped(
    aeron_dir: Option<&Path>,
    aeron_cfg: &AeronConfig,
    channel: &str,
    stream_id: i32,
    kind: RecorderKind,
    stop: &CancellationToken,
    ready: impl FnOnce(Result<i64, String>),
) -> Result<(), LogError> {
    let stream = RecordedStream {
        channel,
        stream_id,
        kind,
    };
    if Recorder::start_reporting(aeron_dir, aeron_cfg, stream, stop, ready)?.is_some() {
        // Hold the recording (and its archive session) alive until shutdown.
        futures::executor::block_on(stop.cancelled());
    }
    Ok(())
}

/// [`record_stream_until_stopped`], and while the recording runs, hand its
/// recording position to `report.send` every `report.every`, until `stop`
/// is cancelled. A position is the raw stream position up to which the
/// archive has written the recording. At archive file sync level 1 it is
/// also durable on local storage. A failed read sends nothing, so a reader
/// of the reports never sees a position that the archive did not confirm.
///
/// The function returns an error when no read succeeds for the
/// [`DriverBudget`] of the archive client: the driver timeout
/// (`AERON_DRIVER_TIMEOUT`, the stall tolerance of the deploy) plus a
/// margin. A stall that every Aeron party survives therefore never ends
/// the recorder. A longer silence means that the recording ended (the
/// publication or the archive is gone) or that the archive no longer
/// answers. The caller then has no recorded copy and must stop
/// publishing.
///
/// # Errors
///
/// Returns an error as [`record_stream_until_stopped`] does, or
/// [`LogError::Aeron`] when the recording is lost.
pub fn record_stream_reporting(
    aeron_dir: Option<&Path>,
    aeron_cfg: &AeronConfig,
    stream: RecordedStream<'_>,
    stop: &CancellationToken,
    ready: impl FnOnce(Result<i64, String>),
    report: PositionReport<impl FnMut(i64)>,
) -> Result<(), LogError> {
    let Some((recorder, budget)) =
        Recorder::start_reporting(aeron_dir, aeron_cfg, stream, stop, ready)?
    else {
        return Ok(());
    };
    let mut reporter = PositionReporter {
        recorder,
        report,
        lost_after: budget.duration(),
        last_read: std::time::Instant::now(),
    };
    while !stop.is_cancelled() {
        reporter.report_then_wait()?;
    }
    Ok(())
}

/// Where and how often [`record_stream_reporting`] reports the recording
/// position.
pub struct PositionReport<F> {
    pub every: Duration,
    pub send: F,
}

/// The reporting loop of [`record_stream_reporting`]: the recorder, where
/// its positions go, and the time of the last good read.
struct PositionReporter<F> {
    recorder: Recorder,
    report: PositionReport<F>,
    /// A run of failed reads this long ends the reporter.
    lost_after: Duration,
    last_read: std::time::Instant,
}

impl<F: FnMut(i64)> PositionReporter<F> {
    /// Read the recording position once and send it, then wait.
    ///
    /// # Errors
    ///
    /// Returns an error when no read succeeded for `lost_after`.
    fn report_then_wait(&mut self) -> Result<(), LogError> {
        match self.recorder.position() {
            Ok(position) => {
                self.last_read = std::time::Instant::now();
                (self.report.send)(position);
            }
            Err(e) if self.last_read.elapsed() >= self.lost_after => {
                return Err(LogError::Aeron(format!(
                    "the recording {} is lost: no position for {:?}: {e}",
                    self.recorder.recording_id, self.lost_after
                )));
            }
            Err(_) => (),
        }
        std::thread::sleep(self.report.every);
        Ok(())
    }
}

/// The stream-recorder threads one producer binary spawned, and the one
/// stop token they share. Dropping this value stops the threads: it
/// cancels the token, then joins every thread. A thread that already
/// records wakes at once from its park on the token. A thread still in
/// startup returns at its next catalog poll, within about 500 ms, or
/// when its archive connect times out. So a caller on a tokio worker
/// joins through `spawn_blocking` on the normal exit path, and lets a
/// `?` return drop the value in place.
pub struct RecorderThreads {
    stop: CancellationToken,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl RecorderThreads {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stop: CancellationToken::new(),
            threads: Vec::new(),
        }
    }

    /// Spawn one recorder thread named `name`. `body` receives a clone of
    /// the shared stop token, and runs [`record_stream_until_stopped`]
    /// with it.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the thread cannot be spawned.
    pub fn spawn(
        &mut self,
        name: String,
        body: impl FnOnce(CancellationToken) + Send + 'static,
    ) -> std::io::Result<()> {
        let stop = self.stop.clone();
        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || body(stop))?;
        self.threads.push(handle);
        Ok(())
    }

    /// The shared stop token, for a recorder body that holds it as a
    /// field instead of taking it from [`RecorderThreads::spawn`].
    #[must_use]
    pub fn stop_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    /// Stop every thread, then join it. Dropping the value does the same;
    /// this method names the point at which the threads end, so a caller
    /// can move the blocking join off its async runtime.
    pub fn join(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        self.stop.cancel();
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Default for RecorderThreads {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RecorderThreads {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

pub struct Recorder {
    /// Owned by the Recorder thread. `AeronArchive` is `!Send + !Sync`, so
    /// this field is deliberately not `Arc<Archive>`. Dropping it closes
    /// the archive session.
    archive: Archive,
    recording_id: i64,
}

impl Recorder {
    /// Connect an archive session and start recording `stream`. Report the
    /// startup outcome exactly once through `ready`: `Ok(recording_id)`
    /// once the recording is active, or `Err(reason)` on any failure,
    /// including a `stop` during startup. Returns the recorder and the
    /// driver budget of its session, or `Ok(None)` when `stop` cancels
    /// before the recording appears.
    fn start_reporting(
        aeron_dir: Option<&Path>,
        aeron_cfg: &AeronConfig,
        stream: RecordedStream<'_>,
        stop: &CancellationToken,
        ready: impl FnOnce(Result<i64, String>),
    ) -> Result<Option<(Self, DriverBudget)>, LogError> {
        let session = match connect_archive(aeron_dir, aeron_cfg) {
            Ok(s) => s,
            Err(e) => {
                ready(Err(format!("connect archive: {e}")));
                return Err(e);
            }
        };
        let RecordedStream {
            channel,
            stream_id,
            kind,
        } = stream;
        let budget = session.driver_budget;
        match Recorder::start_stream(session.archive, channel, stream_id, kind, stop) {
            Ok(Some(r)) => {
                ready(Ok(r.recording_id()));
                Ok(Some((r, budget)))
            }
            Ok(None) => {
                // Stopped before the recording appeared (shutdown during
                // startup). Report it so a waiting barrier does not hang.
                ready(Err("stopped before the recording materialised".into()));
                Ok(None)
            }
            Err(e) => {
                ready(Err(format!("start {} recording: {e}", kind.label())));
                Err(e)
            }
        }
    }

    /// The recording position of an active recording.
    ///
    /// # Errors
    ///
    /// Returns an error when the read fails, or when the recording is not
    /// active.
    fn position(&self) -> Result<i64, LogError> {
        let position = self
            .archive
            .get_recording_position(self.recording_id)
            .map_err(|e| LogError::Aeron(format!("get_recording_position: {e}")))?;
        if position < 0 {
            return Err(LogError::Aeron(format!(
                "recording {} is not active",
                self.recording_id
            )));
        }
        Ok(position)
    }

    /// Start recording an arbitrary `(channel, stream_id)`. This is the
    /// generic entry point used by the per-sequencer `tx_data` recorder (in
    /// the sequencer process) and the `tx_deposits` recorder (in the DA
    /// watcher), so the executor can replay full transaction or deposit
    /// envelopes on crash recovery. `kind` selects the log label. The
    /// channel transport (IPC or UDP) picks the archive source location
    /// automatically. Returns `Ok(None)` if `stop` cancels before the
    /// recording appears.
    ///
    /// # Errors
    ///
    /// Returns an error if `channel` contains a NUL byte, or if the
    /// archive rejects `start_recording` for a reason other than the
    /// stream already being recorded, or if fetching the recording's
    /// descriptor fails once it appears.
    pub fn start_stream(
        // RAII: dropping this closes the archive session and stops the
        // recording.
        archive: Archive,
        channel: &str,
        stream_id: i32,
        kind: RecorderKind,
        stop: &CancellationToken,
    ) -> Result<Option<Self>, LogError> {
        Self::start_inner(archive, channel, stream_id, kind, "stream", stop)
    }

    /// Returns `Ok(None)` if `stop` cancels before a recording appeared
    /// (clean shutdown during startup). Returns `Ok(Some(recorder))` once
    /// recording.
    fn start_inner(
        // RAII: dropping this closes the archive session and stops the
        // recording.
        archive: Archive,
        channel: &str,
        stream_id: i32,
        kind: RecorderKind,
        ctx: &str,
        stop: &CancellationToken,
    ) -> Result<Option<Self>, LogError> {
        let channel_c = crate::ffi::c_uri(channel, &format!("{ctx} channel"))?;

        // SourceLocation selects how the archive subscribes to record the
        // stream. LOCAL records through a "spy" subscription that taps a
        // publication on the same media driver. This is correct for IPC
        // (the publisher is always co-located) and is what the single-host
        // e2e test relies on. But a spy never opens a network subscription
        // and never joins the multicast group. For the UDP channels in the
        // multi-host cluster, where the publisher can be on another node,
        // LOCAL means the recording never appears. The recorder then logs
        // "waiting for a publisher..." forever.
        // Record UDP channels with REMOTE, so the archive opens a real
        // network subscription that joins the group. Multicast loopback
        // means REMOTE also works when the UDP publisher happens to be
        // co-located. This is passed as a bool because rusteron's
        // SourceLocation enum type is not public. The SOURCE_LOCATION_*
        // constants are public, so this function picks the value inline.
        let record_remote = !channel.trim_start().starts_with("aeron:ipc");

        let Some(recording_id) = Self::find_or_start_recording(
            &archive,
            channel_c.as_c_str(),
            stream_id,
            kind,
            record_remote,
            stop,
        ) else {
            return Ok(None); // shutdown before a recording appeared
        };

        // Fetch the descriptor once at startup. The fetch validates that the
        // recording is live.
        let _ = Self::fetch_descriptor(&archive, recording_id)?;

        Ok(Some(Self {
            archive,
            recording_id,
        }))
    }

    /// Resolve the recording id for `stream_id`: start the recording, then
    /// wait for it to appear in the archive catalog and return its id.
    /// Returns `None` if `stop` cancels first.
    ///
    /// A recording started with `auto_stop=false` outlives the client that
    /// started it. A recorder that restarts, or a fresh client against a
    /// long-lived `ArchivingMediaDriver` (as in the cluster), then adopts the
    /// existing recording. A second `start_recording` on the same (channel,
    /// stream) is rejected, which is fine.
    ///
    /// The catalog descriptor only appears once a publisher connects to the
    /// stream (Aeron lists in-progress recordings, not idle ones). A
    /// recorder can come up before its publisher, so this waits
    /// indefinitely, until `stop` cancels, instead of timing out. The process staying alive keeps the
    /// Nomad alloc "running", so the rest of the pipeline can deploy and
    /// start publishing. Discovery uses `list_recordings_for_uri`, which
    /// matches by stream and no session id (the recorder does not know it),
    /// unlike `find_last_matching_recording`.
    fn find_or_start_recording(
        archive: &Archive,
        channel: &std::ffi::CStr,
        stream_id: i32,
        kind: RecorderKind,
        record_remote: bool,
        stop: &CancellationToken,
    ) -> Option<i64> {
        // Start the recording. The first caller wins. A second start on the
        // same (channel, stream) is rejected, which is harmless: the
        // recording already exists.
        //
        // start_recording returns the subscription id, not the recording
        // id. The archive assigns the recording id, and this code must look
        // it up from the catalog (below). Using the subscription id with
        // get_recording_position would silently never advance.
        let source_location = if record_remote {
            rusteron_archive::SOURCE_LOCATION_REMOTE
        } else {
            rusteron_archive::SOURCE_LOCATION_LOCAL
        };
        match archive.start_recording(channel, stream_id, source_location, false) {
            Ok(sub_id) => info!(subscription_id = sub_id, ?kind, "recording initiated"),
            Err(e) => {
                info!(error = %e, ?kind, "start_recording rejected (another recorder owns this stream)");
            }
        }

        let mut logged_waiting = false;
        while !stop.is_cancelled() {
            if let ControlFlow::Break(id) =
                Self::poll_recording(archive, stream_id, kind, &mut logged_waiting)
            {
                return Some(id);
            }
        }
        None
    }

    /// One [`Self::find_or_start_recording`] poll step. `Break` carries the
    /// recording id once the catalog lists it. `Continue` means it does
    /// not exist yet; the caller waits and polls again.
    fn poll_recording(
        archive: &Archive,
        stream_id: i32,
        kind: RecorderKind,
        logged_waiting: &mut bool,
    ) -> ControlFlow<i64> {
        match Self::active_recording_for_stream(archive, stream_id, kind.session_id()) {
            Ok(Some(id)) => {
                info!(recording_id = id, ?kind, "recording ready");
                return ControlFlow::Break(id);
            }
            Ok(None) => Self::log_waiting_once(logged_waiting, kind),
            Err(e) => warn!(error = %e, ?kind, "list_recordings_for_uri failed; retrying"),
        }
        std::thread::sleep(Duration::from_millis(500));
        ControlFlow::Continue(())
    }

    /// Log the "waiting for a publisher" message once, then latch
    /// `logged`, so [`Self::poll_recording`]'s retry loop stays quiet on
    /// later polls.
    fn log_waiting_once(logged: &mut bool, kind: RecorderKind) {
        if *logged {
            return;
        }
        info!(
            ?kind,
            "waiting for a publisher on the stream so the recording materializes"
        );
        *logged = true;
    }

    #[must_use]
    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// One-shot descriptor fetch through `list_recording`, returning the
    /// recording's term buffer length (needed to decode absolute
    /// positions into `BPosition`). This implements the
    /// `AeronArchiveRecordingDescriptorConsumerFuncCallback` trait on a
    /// small `Rc<RefCell<Captured>>` shim. `AeronArchive` itself is
    /// `!Send + !Sync`, which enforces single-thread access.
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

        // `list_recording` calls the consumer synchronously and drops the
        // callback pointer once it returns. Release the leaked handler
        // right after, on both the ok and error paths (release before
        // `?`). Otherwise every call leaks the boxed `Consumer`, and the
        // rusteron `Drop` guard logs a "release() was never called"
        // error.
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

    /// Return the id of the most recent recording for `stream_id`, if
    /// any. This adopts the recording that another recorder already
    /// started for a shared stream (a restart against a long-lived
    /// archive). It lists by stream plus an empty channel fragment
    /// (matches any channel) and takes the highest recording id.
    /// Recordings run for the process lifetime (`auto_stop=false`), so
    /// the newest one is the live one. This pages through the whole
    /// catalog: recording ids are archive-global across all streams, so
    /// the newest recording for this stream can sit beyond any single
    /// page. Adopting a stale id would poll a dead recording's position.
    /// Aeron only lists recordings with an in-progress image, so this
    /// returns `None` until a publisher has connected to the stream. With
    /// `session_id`, only a recording of that publisher session counts.
    fn active_recording_for_stream(
        archive: &Archive,
        stream_id: i32,
        session_id: Option<i32>,
    ) -> Result<Option<i64>, LogError> {
        // This runs on every poll tick in `find_or_start_recording`'s
        // wait loop. `for_each_recording_of_stream` releases its leaked
        // handler before returning, on both the ok and error paths, so a
        // tick never leaks a boxed consumer.
        let mut latest: Option<i64> = None;
        archive.for_each_recording_of_stream(stream_id, |desc| {
            if session_id.is_some_and(|s| s != desc.session_id()) {
                return;
            }
            let id = desc.recording_id();
            latest = Some(latest.map_or(id, |cur| cur.max(id)));
        })?;
        Ok(latest)
    }
}
