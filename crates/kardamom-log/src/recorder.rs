//! Drives an Aeron Archive instance to record channel B and exposes the
//! current durable position the fsync sidecar tails.
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
//! the disk fsync that follows). The sidecar holds an `Arc<AeronArchive>`
//! and calls `get_recording_position(recording_id)` on every tick.
//!
//! ## Segment file path
//!
//! The Aeron Archive stores its segment files at
//! `<archive_dir>/<recording_id>-<segmentBasePosition>.rec`. For the active
//! segment the base position is the largest multiple of
//! `segment_file_length` that is `<=` `start_position`. We learn
//! `start_position`, `term_buffer_length`, and `segment_file_length` from
//! the recording descriptor (one call to `list_recording` at startup).

use std::ffi::CString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tracing::info;

use crate::config::{ChannelsConfig, RecorderId};
use crate::error::LogError;

type Archive = rusteron_archive::AeronArchive;

pub struct Recorder {
    archive: Arc<Archive>,
    recorder_id: RecorderId,
    recording_id: i64,
    /// Cached archive directory (where segment files live).
    archive_dir: PathBuf,
    /// Recording descriptor fields we need to compute segment file paths.
    start_position: i64,
    term_buffer_length: i32,
    segment_file_length: i32,
}

impl Recorder {
    pub fn start(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
        archive_dir: PathBuf,
    ) -> Result<Self, LogError> {
        let archive = Arc::new(archive);
        let b_channel_c = CString::new(ch.b_channel.as_str())
            .map_err(|e| LogError::Aeron(format!("b_channel contains NUL: {e}")))?;

        // start_recording: (channel, stream_id, source_location, auto_stop)
        // SOURCE_LOCATION_LOCAL -> we are co-located with the publisher.
        let recording_id = archive
            .start_recording(
                b_channel_c.as_c_str(),
                ch.b_stream_id,
                rusteron_archive::SOURCE_LOCATION_LOCAL,
                false,
            )
            .map_err(|e| LogError::Aeron(format!("start_recording: {e}")))?;
        info!(recording_id, "started B recording");

        // Pull the descriptor once at startup so we can compute segment file
        // paths without a control-channel round-trip on every fsync tick.
        let (start_position, term_buffer_length, segment_file_length) =
            fetch_descriptor(&archive, recording_id)?;

        Ok(Self {
            archive,
            recorder_id,
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

    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Clone the underlying archive handle so the fsync sidecar can poll the
    /// recording position independently of the recorder.
    pub fn archive(&self) -> Arc<Archive> {
        self.archive.clone()
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
/// `Arc<Mutex<Captured>>` shim so the closure-style trait method can stash
/// the fields we need into shared state the caller reads after the call.
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
        captured: Arc<Mutex<Captured>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut g = self.captured.lock().expect("descriptor mutex");
            g.start_position = desc.start_position();
            g.term_buffer_length = desc.term_buffer_length();
            g.segment_file_length = desc.segment_file_length();
            g.seen = true;
        }
    }

    let captured: Arc<Mutex<Captured>> = Arc::new(Mutex::new(Captured::default()));
    let handler = Handler::leak(Consumer {
        captured: captured.clone(),
    });

    archive
        .list_recording(recording_id, Some(&handler))
        .map_err(|e| LogError::Aeron(format!("list_recording: {e}")))?;

    let g = captured.lock().expect("descriptor mutex");
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
