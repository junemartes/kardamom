//! Archive-backed bootstrap for Aeron subscribers.
//!
//! See `docs/superpowers/specs/2026-05-29-aeron-archive-bootstrap-design.md`.

use std::time::Duration;

/// Policy controlling how a subscription is brought online.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapPolicy {
    /// Replay from a recent archive position, merge into the live image,
    /// then continue live. Handlers MUST tolerate duplicates from the
    /// replay/live overlap.
    Replay {
        window: ReplayWindow,
        catch_up_timeout: Duration,
    },
    /// Wait for the live image to attach. No history is replayed.
    Connected { timeout: Duration },
}

/// How far back from the archive head a replay should start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayWindow {
    /// For `tx_ordering`: anchor at the last `BoundaryStart` whose archived
    /// position is <= `latest_recording_position`, then walk back `safety`
    /// additional boundaries.
    BlockBoundaries { safety: u32 },
    /// For `tx_data[i]` and `tx_deposits`: anchor at
    /// `max(0, latest_recording_position - bytes)`. Block-boundary alignment
    /// is not a property of these streams.
    PositionBytes { bytes: u64 },
}

/// Errors surfaced by bootstrap. Each is fatal for the affected subscription.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("anchor {anchor} below archive min {archive_min}")]
    AnchorBelowArchiveHead { anchor: i64, archive_min: i64 },
    #[error("start_replay failed: {0}")]
    ReplayFailed(String),
    #[error("catch-up did not complete within configured timeout")]
    CatchUpTimeout,
    #[error("live image did not attach within configured timeout")]
    ConnectTimeout,
    #[error("archive client error: {0}")]
    ArchiveClient(String),
}

impl BootstrapPolicy {
    /// Default policy for `tx_data[i]` subscribers: replay 16 MiB back,
    /// catch up within 30 s.
    pub fn default_tx_data() -> Self {
        Self::Replay {
            window: ReplayWindow::PositionBytes { bytes: 16 * 1024 * 1024 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for `tx_ordering`: 4 block boundaries back with no extra
    /// safety, 30 s catch-up budget.
    pub fn default_tx_ordering() -> Self {
        Self::Replay {
            window: ReplayWindow::BlockBoundaries { safety: 4 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for `tx_deposits`: 4 MiB back, 30 s catch-up.
    pub fn default_tx_deposits() -> Self {
        Self::Replay {
            window: ReplayWindow::PositionBytes { bytes: 4 * 1024 * 1024 },
            catch_up_timeout: Duration::from_secs(30),
        }
    }

    /// Default for read-only "tail" streams (`tx_receipts`, `tx_errors`,
    /// watermarks, block boundaries): wait up to 10 s for the live image
    /// to attach.
    pub fn default_connected() -> Self {
        Self::Connected { timeout: Duration::from_secs(10) }
    }
}

/// Position of a frame on its Aeron stream (or replay session). Newtype so
/// the state-machine code can't accidentally mix it with other integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamPosition(pub i64);

/// Source of frames for the state machine — abstracts over a live Aeron
/// subscription so we can drive the state machine against a `FakeBus` in
/// unit tests.
pub trait LiveImage: Send {
    /// Current end position of the live image. None if the image has not
    /// attached yet.
    fn current_position(&self) -> Option<StreamPosition>;
    /// Position at which this image first became attached. None until then.
    fn join_position(&self) -> Option<StreamPosition>;
    /// Pull all currently-available frames, calling `f` for each.
    fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>));
}

/// Source of replayed frames — abstracts over a rusteron replay session.
pub trait ReplaySource: Send {
    /// Current position of the replay cursor. Equal to anchor at start;
    /// advances as `poll` is called.
    fn current_position(&self) -> StreamPosition;
    /// Pull all currently-available frames, calling `f` for each.
    fn poll(&mut self, f: &mut dyn FnMut(StreamPosition, Vec<u8>));
    /// Stop the replay session.
    fn stop(&mut self);
}

/// Bootstrap-time queries against the archive: position lookup and replay
/// session creation. The state machine calls these once at startup; live
/// frames flow through `LiveImage` and `ReplaySource`.
pub trait ArchiveSource {
    /// `latest_recording_position` for the recording mapped to this stream.
    /// Returns `None` if no recording exists yet (cold-start fallback).
    fn latest_recording_position(&self) -> Result<Option<StreamPosition>, BootstrapError>;
    /// Earliest position still archived for this stream (used to detect
    /// `AnchorBelowArchiveHead`).
    fn earliest_recording_position(&self) -> Result<StreamPosition, BootstrapError>;
    /// Walk back from `at` to the last `BoundaryStart` <= `at`, then back
    /// `safety` additional boundaries. Returns `None` if no boundaries
    /// exist before `at`. Only meaningful for `tx_ordering`.
    fn last_block_boundary(
        &self,
        at: StreamPosition,
        safety: u32,
    ) -> Result<Option<StreamPosition>, BootstrapError>;
    /// Start a replay anchored at `start` and return the session.
    fn start_replay(&self, start: StreamPosition) -> Result<Box<dyn ReplaySource>, BootstrapError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policies_are_replay_or_connected() {
        match BootstrapPolicy::default_tx_data() {
            BootstrapPolicy::Replay { .. } => {}
            _ => panic!("expected Replay"),
        }
        match BootstrapPolicy::default_connected() {
            BootstrapPolicy::Connected { .. } => {}
            _ => panic!("expected Connected"),
        }
    }

    #[test]
    fn replay_window_variants_are_distinct() {
        assert_ne!(
            ReplayWindow::BlockBoundaries { safety: 0 },
            ReplayWindow::PositionBytes { bytes: 0 },
        );
    }

    #[test]
    fn stream_position_ordered() {
        assert!(StreamPosition(10) < StreamPosition(11));
        assert!(StreamPosition(0) <= StreamPosition(0));
    }
}
