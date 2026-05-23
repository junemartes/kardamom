//! Drives an Aeron Archive instance to record channel B and exposes the
//! recording-position counter the fsync sidecar tails.
//!
//! Gated behind the `aeron-live` cargo feature. See publisher.rs for the
//! API-drift caveat — the bodies below target `rusteron-archive` 0.1.16x.

use tracing::info;

use crate::config::{ChannelsConfig, RecorderId};
use crate::error::LogError;

type Archive = rusteron_archive::AeronArchive;
type SourceLocation = rusteron_archive::SourceLocation;

pub struct Recorder {
    archive: Archive,
    recorder_id: RecorderId,
    recording_id: i64,
    counter_id: i32,
}

impl Recorder {
    pub fn start(
        archive: Archive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
    ) -> Result<Self, LogError> {
        // start_recording: (channel, stream_id, source_location, auto_stop)
        // SourceLocation::Local -> we are co-located with the publisher.
        let recording_id = archive
            .start_recording(&ch.b_channel, ch.b_stream_id, SourceLocation::Local, false)
            .map_err(|e| LogError::Aeron(format!("start_recording: {e}")))?;
        info!(recording_id, "started B recording");

        // The Archive exposes a `recording-pos` counter per recording.
        let counter_id = archive
            .find_recording_position_counter(recording_id)
            .map_err(|e| LogError::Aeron(format!("find_recording_position_counter: {e}")))?;

        Ok(Self {
            archive,
            recorder_id,
            recording_id,
            counter_id,
        })
    }

    pub fn recorder_id(&self) -> RecorderId {
        self.recorder_id
    }

    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Counter id the fsync sidecar tails to learn how much data has been
    /// committed to Aeron's internal buffer (i.e. is *available* for fsync).
    pub fn position_counter_id(&self) -> i32 {
        self.counter_id
    }

    /// Path to the active segment file on disk. The fsync sidecar mirrors
    /// bytes from here into its `O_DIRECT` file.
    ///
    /// **TODO:** the precise `rusteron-archive` API for resolving the active
    /// segment file may differ; verify against
    /// <https://docs.rs/rusteron-archive> when wiring this up to a live
    /// Archive. The convention used by Aeron Archive is
    /// `archive_dir/<recording_id>-<segment_basename>.rec`.
    pub fn active_segment_path(&self) -> Result<std::path::PathBuf, LogError> {
        self.archive
            .recording_segment_file(self.recording_id)
            .map(Into::into)
            .map_err(|e| LogError::Aeron(format!("segment_file: {e}")))
    }
}
