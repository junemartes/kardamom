//! Drives an Aeron Archive instance to record a single stream (channel B
//! *or* a channel A[i] per D-Sh12) and exposes the current durable position
//! the fsync sidecar tails.
//!
//! Topology after D-Sh12:
//!   - One `Recorder` with `RecorderKind::B` per channel-B recorder host
//!     (N total; quorum-fsynced).
//!   - One `Recorder` with `RecorderKind::A { sequencer_id }` per sequencer
//!     host (M total; single-host fsync each).
//!
//! Gated behind the `aeron-live` cargo feature.
//!
//! ## Design note: position polling, not counter handle
//!
//! Earlier drafts threaded an Aeron `counter_id` through to the sidecar so
//! it could read the recording-position via the shared counters reader.
//! The 0.1.16x `rusteron-archive` bindings don't expose a "find the
//! recording-position counter id" helper, but they do expose
//! [`rusteron_archive::AeronArchive::get_recording_position`], which is
//! semantically equivalent for our purposes (we poll once per fsync tick;
//! the overhead of a control-channel round-trip is negligible compared to
//! the disk fsync that follows).
//!
//! ## Design note: thread confinement
//!
//! `AeronArchive` is `!Send + !Sync` (it wraps `Rc` + raw pointers — the C
//! client is thread-confined). The Recorder owns the archive on its own
//! thread and refreshes a shared
//! [`crate::fsync_sidecar::SharedPosition`] atomic that the fsync sidecar
//! thread reads. See `fsync_sidecar::SharedPosition` / `refresh_from_archive`.
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
use std::path::PathBuf;
use std::rc::Rc;

use tracing::info;

use crate::config::{ChannelsConfig, RecorderId};
use crate::error::LogError;

type Archive = rusteron_archive::AeronArchive;

/// Which logical channel a recorder is tailing. Channel B feeds the
/// quorum aggregator (N recorders, Q-of-N watermark). Channel A[i] feeds
/// the per-sequencer single-host fsync (no quorum by default — see D-Sh12
/// rationale).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecorderKind {
    /// Per-sequencer channel-A recorder (carries full TxEnvelopes).
    A { sequencer_id: u8 },
    /// Channel-B canonical-orderer recorder (carries tiny TxRefs).
    B,
}

pub struct Recorder {
    /// Owned by the Recorder thread. `AeronArchive` is `!Send + !Sync`, so
    /// the field is intentionally not exposed as `Arc<Archive>`; instead,
    /// callers learn the recording position via a
    /// [`crate::fsync_sidecar::SharedPosition`] (an `AtomicI64`) that the
    /// Recorder refreshes from its own thread.
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
    /// Start recording channel B on this host. Used by the N channel-B
    /// recorder hosts that participate in the quorum.
    pub fn start_b(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        archive_dir: PathBuf,
    ) -> Result<Self, LogError> {
        Self::start_inner(
            archive,
            &ch.b_channel,
            ch.b_stream_id,
            recorder_id,
            RecorderKind::B,
            archive_dir,
            "B",
        )
    }

    /// Start recording channel A[sequencer_id]. Per D-Sh12, each sequencer
    /// host runs one of these recording its own exclusive-publisher stream.
    pub fn start_a(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        sequencer_id: u8,
        archive_dir: PathBuf,
    ) -> Result<Self, LogError> {
        let channel = ch
            .a_channel_template
            .replace("{sid}", &sequencer_id.to_string());
        let stream_id = ch.a_stream_id_base + sequencer_id as i32;
        Self::start_inner(
            archive,
            &channel,
            stream_id,
            recorder_id,
            RecorderKind::A { sequencer_id },
            archive_dir,
            "A",
        )
    }

    /// Back-compat alias: original API recorded channel B.
    #[deprecated(note = "use start_b for channel-B recording or start_a for channel A[i]")]
    pub fn start(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        archive_dir: PathBuf,
    ) -> Result<Self, LogError> {
        Self::start_b(archive, ch, recorder_id, archive_dir)
    }

    fn start_inner(
        archive: Archive,
        channel: &str,
        stream_id: i32,
        recorder_id: RecorderId,
        kind: RecorderKind,
        archive_dir: PathBuf,
        ctx: &str,
    ) -> Result<Self, LogError> {
        let channel_c = CString::new(channel)
            .map_err(|e| LogError::Aeron(format!("{ctx} channel contains NUL: {e}")))?;

        // start_recording: (channel, stream_id, source_location, auto_stop)
        // SOURCE_LOCATION_LOCAL -> we are co-located with the publisher.
        let recording_id = archive
            .start_recording(
                channel_c.as_c_str(),
                stream_id,
                rusteron_archive::SOURCE_LOCATION_LOCAL,
                false,
            )
            .map_err(|e| LogError::Aeron(format!("start_recording {ctx}: {e}")))?;
        info!(recording_id, ?kind, "started recording");

        // Pull the descriptor once at startup so we can compute segment file
        // paths without a control-channel round-trip on every fsync tick.
        let (start_position, term_buffer_length, segment_file_length) =
            fetch_descriptor(&archive, recording_id)?;

        Ok(Self {
            archive,
            recorder_id,
            kind,
            recording_id,
            archive_dir,
            start_position,
            term_buffer_length,
            segment_file_length,
        })
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
    /// — see the struct doc and `fsync_sidecar::SharedPosition` for the
    /// approved cross-thread pattern.
    pub fn archive(&self) -> &Archive {
        &self.archive
    }

    /// Path to the active segment file on disk. The fsync sidecar mirrors
    /// bytes from here into its `O_DIRECT` file.
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
    pub fn current_position(&self) -> Result<i64, LogError> {
        self.archive
            .get_recording_position(self.recording_id)
            .map_err(|e| LogError::Aeron(format!("get_recording_position: {e}")))
    }
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
    let handler = Handler::leak(Consumer {
        captured: captured.clone(),
    });

    archive
        .list_recording(recording_id, Some(&handler))
        .map_err(|e| LogError::Aeron(format!("list_recording: {e}")))?;

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
