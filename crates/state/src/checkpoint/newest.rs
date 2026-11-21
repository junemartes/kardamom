//! [`NewestCheckpoint`]: the newest restore point of a checkpoint
//! directory, and the two gauges that show it.
//!
//! A restore copies the newest base, then applies the block log after
//! it. So the newest restore point is the end of the log when the log
//! continues the base with no gap, and the base otherwise. The gauges
//! hold its block and the unix time at which the directory got it. An
//! alert computes the age as `time()` minus that time, so the age also
//! grows while the process is down.

use std::num::NonZeroU64;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use super::block_log::SegmentSize;
use super::replay::LOG_DIR;
use super::{BlockRange, CheckpointInfo, latest_checkpoint};
use crate::error::StateError;

/// The last block that a restore of the newest checkpoint reaches.
pub const NEWEST_BLOCK: &str = "kardamom_checkpoint_newest_block";
/// The unix time, in seconds, at which the checkpoint directory got the
/// block of [`NEWEST_BLOCK`].
pub const NEWEST_TIMESTAMP: &str = "kardamom_checkpoint_newest_timestamp_seconds";

/// The newest restore point of a checkpoint directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewestCheckpoint {
    /// The last block that a restore reaches.
    pub block: u64,
    /// The time of the write that put that block in the directory.
    pub written: SystemTime,
}

/// The end of the block log, as the log thread knows it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogEnd {
    /// The last block in the log.
    pub(crate) block: u64,
    /// The time of the sync that made that block durable.
    pub(crate) written: SystemTime,
    /// The size at which the writer ends a segment.
    pub(crate) segment_bytes: NonZeroU64,
}

impl NewestCheckpoint {
    /// Register the descriptions of the two gauges.
    pub fn describe() {
        metrics::describe_gauge!(
            NEWEST_BLOCK,
            "the last block that a restore of the newest checkpoint reaches: the base and its block log"
        );
        metrics::describe_gauge!(
            NEWEST_TIMESTAMP,
            "the unix time in seconds at which the checkpoint directory got the newest restore point"
        );
    }

    /// The newest base of `checkpoints_dir`, with no block log. `None`
    /// with no base.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or the base does not read.
    pub fn base(checkpoints_dir: &Path) -> Result<Option<Self>, StateError> {
        latest_checkpoint(checkpoints_dir, BlockRange::ANY)?
            .map(|base| Self::of(&base))
            .transpose()
    }

    /// Show the newest base of `checkpoints_dir` in the gauges.
    pub fn publish_base(checkpoints_dir: &Path) {
        Self::show(Self::base(checkpoints_dir));
    }

    /// Show `found` in the gauges. With no base, or with a directory
    /// that does not read, the gauges keep their values: a process with
    /// no base exports no sample.
    pub(crate) fn show(found: Result<Option<Self>, StateError>) {
        match found {
            Ok(point) => point.into_iter().for_each(Self::publish),
            Err(e) => warn!(
                error = %e,
                "the newest checkpoint does not read; its gauges keep their values"
            ),
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "block numbers stay far below 2^52"
    )]
    fn publish(self) {
        // A write time before 1970 shows as 0. The age is then the whole
        // unix time, so the alert fires on the wrong clock.
        let written = self
            .written
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        metrics::gauge!(NEWEST_BLOCK).set(self.block as f64);
        metrics::gauge!(NEWEST_TIMESTAMP).set(written);
    }

    /// The point of `base`: its block, and the modification time of its
    /// directory. The copy writes the manifest into it as its last step.
    fn of(base: &CheckpointInfo) -> Result<Self, StateError> {
        Ok(Self {
            block: base.block,
            written: std::fs::metadata(&base.path)?.modified()?,
        })
    }
}

impl LogEnd {
    /// The newest restore point of `checkpoints_dir`: the end of the log
    /// when the log continues the newest base with no gap, and that base
    /// otherwise. `None` with no base.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory, the log or the base does not
    /// read.
    pub(crate) fn newest(
        &self,
        checkpoints_dir: &Path,
    ) -> Result<Option<NewestCheckpoint>, StateError> {
        let Some(base) = latest_checkpoint(checkpoints_dir, BlockRange::ANY)? else {
            return Ok(None);
        };
        if self.block > base.block && self.continues(checkpoints_dir, base.block)? {
            return Ok(Some(NewestCheckpoint {
                block: self.block,
                written: self.written,
            }));
        }
        NewestCheckpoint::of(&base).map(Some)
    }

    /// Whether the log holds each block after `base`, by the gap rule of
    /// the sizes of its segments.
    fn continues(&self, checkpoints_dir: &Path, base: u64) -> Result<bool, StateError> {
        let segments = SegmentSize::list(&checkpoints_dir.join(LOG_DIR))?;
        Ok(!segments.is_empty() && !SegmentSize::gap_after(&segments, base, self.segment_bytes))
    }
}

#[cfg(test)]
#[path = "newest_tests.rs"]
mod tests;
