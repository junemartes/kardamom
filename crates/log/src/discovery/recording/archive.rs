//! The archive operations used by the recorder's reconciliation state.

use tracing::warn;

use super::Started;
use crate::archive_catalog::ArchiveCatalog;
use crate::error::LogError;

pub(super) trait RecorderArchive {
    fn start(&self, uri: &str, stream: i32) -> Result<i64, LogError>;
    fn stop(&self, subscription: i64) -> Result<(), LogError>;
    /// The newest live recording that matches `started`, or the error of
    /// the catalog listing.
    fn live(&self, started: &Started) -> Result<Option<i64>, LogError>;

    /// [`Self::live`], with a failed listing logged and read as none.
    fn latest(&self, started: &Started) -> Option<i64> {
        self.live(started).unwrap_or_else(|e| {
            warn!(fragment = %started.fragment, error = %e, "discovered recorder: catalog listing failed; retrying");
            None
        })
    }
}

impl RecorderArchive for rusteron_archive::AeronArchive {
    fn start(&self, uri: &str, stream: i32) -> Result<i64, LogError> {
        let channel = crate::ffi::c_uri(uri, "recording channel")?;
        self.start_recording(
            &channel,
            stream,
            rusteron_archive::SOURCE_LOCATION_REMOTE,
            false,
        )
        .map_err(|e| LogError::Aeron(format!("start recording: {e}")))
    }

    fn stop(&self, subscription: i64) -> Result<(), LogError> {
        self.stop_recording_subscription(subscription)
            .map(|_| ())
            .map_err(|e| LogError::Aeron(format!("stop recording: {e}")))
    }

    fn live(&self, started: &Started) -> Result<Option<i64>, LogError> {
        let mut latest = None;
        let listed =
            self.for_each_recording_of_channel(started.stream_id, &started.fragment, |d| {
                if !started.matches_recording(d.session_id(), d.stop_position()) {
                    return;
                }
                let id = d.recording_id();
                latest = Some(latest.map_or(id, |cur: i64| cur.max(id)));
            });
        listed.map(|()| latest)
    }
}
