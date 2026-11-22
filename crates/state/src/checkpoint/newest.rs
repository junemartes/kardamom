//! [`NewestCheckpoint`]: the newest restore point of a checkpoint
//! directory, and the two gauges that show it.
//!
//! A restore copies the newest base, then applies the block log after it
//! up to the first gap. [`RestorePoint`] gives that reach. The gauges hold
//! the reach and the unix time at which the directory got it. An alert
//! computes the age as `time()` minus that time, so the age also grows
//! while the process is down.
//!
//! The time of the base is the modification time of its directory. The
//! time of a reach past the base is the time of the sync that made it
//! durable, when the log thread knows it. Otherwise the gauges take the
//! time of the base: an older time than the true one, so the age never
//! hides an old checkpoint.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use super::{RestorePoint, checkpoint_name};
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

    /// The newest restore point of `checkpoints_dir`. `end` is the end of
    /// the log that the log thread synced, when one runs. `None` with no
    /// base.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory, the log or the base does not
    /// read.
    pub(crate) fn read(
        checkpoints_dir: &Path,
        end: Option<LogEnd>,
    ) -> Result<Option<Self>, StateError> {
        let Some(point) = RestorePoint::newest(checkpoints_dir)? else {
            return Ok(None);
        };
        let synced = end
            .filter(|end| point.reach > point.base && end.block == point.reach)
            .map(|end| end.written);
        let written = match synced {
            Some(written) => written,
            None => {
                std::fs::metadata(checkpoints_dir.join(checkpoint_name(point.base)))?.modified()?
            }
        };
        Ok(Some(Self {
            block: point.reach,
            written,
        }))
    }

    /// The newest restore point of `checkpoints_dir`, with no log thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory, the log or the base does not
    /// read.
    pub fn on_disk(checkpoints_dir: &Path) -> Result<Option<Self>, StateError> {
        Self::read(checkpoints_dir, None)
    }

    /// Show the newest restore point of `checkpoints_dir` in the gauges,
    /// with no log thread.
    pub fn publish_on_disk(checkpoints_dir: &Path) {
        Self::show(Self::on_disk(checkpoints_dir));
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
}

#[cfg(test)]
#[path = "newest_tests.rs"]
mod tests;
