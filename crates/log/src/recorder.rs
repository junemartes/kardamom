//! Drives an Aeron Archive instance to record a single stream (tx_ordering
//! *or* a tx_data[i] per) and exposes the current durable
//! recording position.
//!
//! Topology after:
//!   - One `Recorder` with `RecorderKind::TxOrdering` per tx_ordering recorder host
//!     (N total; quorum-fsynced via the `QuorumAggregator`).
//!   - One `Recorder` with `RecorderKind::TxData { sequencer_id }` per sequencer
//!     host (M total; single-host fsync each).
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
//! sidecar is required. The per-recorder watermark loop
//! ([`run_watermark_loop`]) periodically polls this position and republishes
//! it as a [`kardamom_types::FsyncWatermark`] for the quorum aggregator
//! ([`crate::watermark::QuorumAggregator`]) to combine.
//!
//! ## Design note: thread confinement
//!
//! `AeronArchive` is `!Send + !Sync` (it wraps `Rc` + raw pointers — the C
//! client is thread-confined). Both the recording-position poll and the
//! watermark publish therefore happen on the Recorder thread; cross-thread
//! sharing of the archive handle is not supported.
//!
//! ## Segment file path
//!
//! The Aeron Archive stores its segment files at
//! `<archive_dir>/<recording_id>-<segmentBasePosition>.rec`. For the active
//! segment the base position is the largest multiple of
//! `segment_file_length` that is `<=` `start_position`. We learn
//! `start_position`, `term_buffer_length`, and `segment_file_length` from
//! the recording descriptor (one call to `list_recording` at startup).

use std::cell::RefCell;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use tracing::{info, warn};

use crate::config::{AeronConfig, ChannelsConfig, RecorderId};
use crate::error::LogError;
use crate::publisher::WatermarkPublisher;
use kardamom_types::FsyncWatermark;

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
    /// Held only to keep the archive's client conductor alive; never used
    /// directly (the `archive` drives all control calls).
    _aeron: rusteron_archive::Aeron,
    pub archive: Archive,
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
    actx.set_message_timeout_ns(60_000_000_000)
        .map_err(|e| LogError::Aeron(format!("set_message_timeout_ns: {e}")))?;
    let archive = rusteron_archive::AeronArchiveAsyncConnect::new_with_aeron(&actx, &aeron)
        .map_err(|e| LogError::Aeron(format!("archive async connect: {e}")))?
        .poll_blocking(Duration::from_secs(30))
        .map_err(|e| LogError::Aeron(format!("archive connect poll: {e}")))?;
    Ok(ArchiveSession {
        _aeron: aeron,
        archive,
    })
}

/// Which logical tx_data recorder is tailing. TxOrdering feeds the
/// quorum aggregator (N recorders, Q-of-N watermark). TxData[i] feeds
/// the per-sequencer single-host fsync (no quorum by default — see
/// rationale).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecorderKind {
    /// Per-sequencer tx_data recorder (carries full TxEnvelopes).
    TxData { sequencer_id: u8 },
    /// TxOrdering canonical-orderer recorder (carries tiny TxRefs).
    TxOrdering,
}

pub struct Recorder {
    /// Owned by the Recorder thread. `AeronArchive` is `!Send + !Sync`, so
    /// the field is intentionally not exposed as `Arc<Archive>`; the
    /// recording-position poll and watermark publish in
    /// [`run_watermark_loop`] both run on this thread.
    archive: Archive,
    recorder_id: RecorderId,
    kind: RecorderKind,
    recording_id: i64,
    /// Cached archive directory (where segment files live).
    archive_dir: PathBuf,
    /// Recording descriptor fields we need to compute segment file paths.
    start_position: i64,
    term_buffer_length: i32,
    segment_file_length: i32,
}

impl Recorder {
    /// Start recording tx_ordering on this host. Used by the N tx_ordering
    /// recorder hosts that participate in the quorum.
    pub fn start_b(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        archive_dir: PathBuf,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Self>, LogError> {
        Self::start_inner(
            archive,
            &ch.tx_ordering_channel,
            ch.tx_ordering_stream_id,
            recorder_id,
            RecorderKind::TxOrdering,
            archive_dir,
            "B",
            should_stop,
        )
    }

    /// Start recording tx_data[sequencer_id]. Per, each sequencer
    /// host runs one of these recording its own exclusive-publisher stream.
    pub fn start_a(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        sequencer_id: u8,
        archive_dir: PathBuf,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Self>, LogError> {
        Self::start_inner(
            archive,
            &ch.tx_data_channel(sequencer_id),
            ch.tx_data_stream_id(sequencer_id),
            recorder_id,
            RecorderKind::TxData { sequencer_id },
            archive_dir,
            "A",
            should_stop,
        )
    }

    /// Returns `Ok(None)` if `should_stop` fired before a recording appeared
    /// (clean shutdown during startup), `Ok(Some(recorder))` once recording.
    #[allow(clippy::too_many_arguments)] // private helper; start_b/start_a are the public surface
    fn start_inner(
        archive: Archive,
        channel: &str,
        stream_id: i32,
        recorder_id: RecorderId,
        kind: RecorderKind,
        archive_dir: PathBuf,
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

        // Pull the descriptor once at startup so we can compute segment file
        // paths without a control-channel round-trip on every fsync tick.
        let (start_position, term_buffer_length, segment_file_length) =
            fetch_descriptor(&archive, recording_id)?;

        Ok(Some(Self {
            archive,
            recorder_id,
            kind,
            recording_id,
            archive_dir,
            start_position,
            term_buffer_length,
            segment_file_length,
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

    pub fn recorder_id(&self) -> RecorderId {
        self.recorder_id
    }

    pub fn kind(&self) -> RecorderKind {
        self.kind
    }

    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Borrow the underlying archive handle. The borrow stays on the
    /// Recorder thread; cross-thread sharing is intentionally not supported
    /// — the watermark publish runs on this same thread (see
    /// [`run_watermark_loop`]).
    pub fn archive(&self) -> &Archive {
        &self.archive
    }

    /// Path to the active segment file on disk. Returned for diagnostics
    /// and for offline consumers (e.g. the L1 batcher) that prefer to read
    /// segment files directly instead of via the Archive replay API.
    ///
    /// We rely on Aeron's canonical naming convention:
    ///   `<archive_dir>/<recording_id>-<segmentBasePosition>.rec`
    /// where `segmentBasePosition` comes from the static utility
    /// [`rusteron_archive::AeronArchive::segment_file_base_position`].
    pub fn active_segment_path(&self) -> Result<PathBuf, LogError> {
        let cur = self.current_position()?;
        let base = Archive::segment_file_base_position(
            self.start_position,
            cur,
            self.term_buffer_length,
            self.segment_file_length,
        );
        Ok(self
            .archive_dir
            .join(format!("{}-{}.rec", self.recording_id, base)))
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

/// Poll the archive's recording position on a fixed cadence and republish it
/// as an `FsyncWatermark` whenever it advances. Runs on the calling thread
/// because `AeronArchive` and the publisher are both thread-confined.
///
/// `poll_interval` controls the watermark cadence — higher = lower CPU, more
/// tail latency on quorum advancement. 1ms is a reasonable default.
pub fn run_watermark_loop(
    recorder: &Recorder,
    publisher: &WatermarkPublisher,
    poll_interval: Duration,
    mut should_stop: impl FnMut() -> bool,
) -> Result<(), LogError> {
    let mut last_pos: i64 = -1;
    while !should_stop() {
        match recorder.current_position() {
            Ok(pos) if pos > last_pos => {
                let wm = FsyncWatermark {
                    recorder_id: recorder.recorder_id,
                    position: recorder.to_bposition(pos),
                };
                if let Err(e) = publisher.publish(&wm) {
                    warn!(error = %e, "watermark publish failed");
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

/// One-shot descriptor fetch via `list_recording`. We implement the
/// `AeronArchiveRecordingDescriptorConsumerFuncCallback` trait on a small
/// `Rc<RefCell<Captured>>` shim. Single-thread access is enforced by the
/// fact that `AeronArchive` itself is `!Send + !Sync`.
fn fetch_descriptor(archive: &Archive, recording_id: i64) -> Result<(i64, i32, i32), LogError> {
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
        Handler,
    };

    #[derive(Default)]
    struct Captured {
        start_position: i64,
        term_buffer_length: i32,
        segment_file_length: i32,
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
            g.start_position = desc.start_position();
            g.term_buffer_length = desc.term_buffer_length();
            g.segment_file_length = desc.segment_file_length();
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
    Ok((
        g.start_position,
        g.term_buffer_length,
        g.segment_file_length,
    ))
}

/// Return the id of the most recent recording for `stream_id`, if any. Used to
/// adopt the recording another recorder already started for a shared stream
/// (several recorders on one archive recording tx_ordering, or a restart
/// against a long-lived archive). Lists by stream + empty channel fragment
/// (matches any channel) and takes the highest recording id — recordings run
/// for the process lifetime (auto_stop=false), so the newest is the live one.
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

    let found: Rc<RefCell<Found>> = Rc::new(RefCell::new(Found { latest: None }));
    let mut handler = Handler::leak(Consumer {
        found: found.clone(),
    });
    // Empty channel fragment matches any channel; stream_id narrows to ours.
    let any_channel = CString::new("").expect("empty fragment has no NUL");
    // This runs every poll tick in `find_or_start_recording`'s wait loop;
    // `list_recordings_for_uri` calls the consumer synchronously and drops the
    // pointer on return, so release the leaked handler right away (before `?`,
    // so the error path frees it too). Without this each tick leaked a boxed
    // `Consumer` and the rusteron `Drop` guard logged a "release() was never
    // called" error at ~2 Hz, drowning the recorder's logs.
    let res =
        archive.list_recordings_for_uri(0, 100, any_channel.as_c_str(), stream_id, Some(&handler));
    handler.release();
    res.map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;

    let g = found.borrow();
    Ok(g.latest)
}
