//! [`BaseRule`]: when the checkpointer starts a new base, and which bases
//! and log segments it keeps.
//!
//! A base is a full image, so it costs as much as the state. The block log
//! after it costs as much as the blocks. A new base pays off when the log
//! after the newest base is larger than that base: then a restore reads
//! less with the new base. A new base also starts when the newest one is
//! old, when the log has a gap after it (the blocks after the gap have no
//! restore point), and when no base exists.
//!
//! A gap shows in the sizes of the segments. The log writer ends a segment
//! when it is full or at a gap. So a segment that is smaller than the
//! segment size, and that another segment follows, ended at a gap.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use super::block_log::{BlockLogWriter, DEFAULT_SEGMENT_BYTES, LogIdentity, SegmentSize};
use super::copy_rate::CopyRate;
use super::replay::LOG_DIR;
use super::{
    BlockRange, CheckpointInfo, checkpoint_data_file, checkpoints_newest_first,
    create_checkpoint_at, latest_checkpoint, prune_checkpoints, read_manifest,
};
use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::SCHEMA_VERSION;

/// Why a new base is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseDue {
    /// No base exists.
    First,
    /// The log after the newest base is larger than the rule allows.
    LogSize,
    /// The newest base is older than the rule allows.
    Age,
    /// The log has a gap after the newest base.
    Gap,
}

impl BaseDue {
    /// The `reason` label of the base counter.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::LogSize => "log-size",
            Self::Age => "age",
            Self::Gap => "gap",
        }
    }
}

/// The rule for a new base.
#[derive(Clone, Copy, Debug)]
pub struct BaseRule {
    /// A new base when the log after the newest base is larger than this
    /// percent of the size of that base.
    pub log_percent: NonZeroU64,
    /// A new base when the newest base is older than this.
    pub max_age: Duration,
    /// The size at which the log writer ends a segment.
    pub segment_bytes: NonZeroU64,
}

impl BaseRule {
    /// Why a new base is due in `checkpoints_dir`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory, the log or the newest base
    /// does not read.
    pub fn due(&self, checkpoints_dir: &Path) -> Result<Option<BaseDue>, StateError> {
        let Some(base) = latest_checkpoint(checkpoints_dir, BlockRange::ANY)? else {
            return Ok(Some(BaseDue::First));
        };
        let segments = SegmentSize::list(&checkpoints_dir.join(LOG_DIR))?;
        let due = [
            (self.has_gap(&segments, base.block), BaseDue::Gap),
            (self.log_too_large(&segments, &base)?, BaseDue::LogSize),
            (self.too_old(&base)?, BaseDue::Age),
        ]
        .into_iter()
        .find_map(|(due, why)| due.then_some(why));
        Ok(due)
    }

    /// Whether the log has a gap after `base`: no segment holds the block
    /// after it while later segments exist, or a segment of the chain
    /// after it ended before it was full.
    fn has_gap(&self, segments: &[SegmentSize], base: u64) -> bool {
        let Some(chain) = SegmentSize::chain_after(segments, base) else {
            return !segments.is_empty();
        };
        chain
            .iter()
            .take(chain.len().saturating_sub(1))
            .any(|s| s.bytes < self.segment_bytes.get())
    }

    /// Whether the segments after `base` hold more bytes than the rule
    /// allows for the size of its image.
    fn log_too_large(
        &self,
        segments: &[SegmentSize],
        base: &CheckpointInfo,
    ) -> Result<bool, StateError> {
        let log: u128 = segments
            .iter()
            .filter(|s| s.first_block > base.block)
            .map(|s| u128::from(s.bytes))
            .sum();
        let image = std::fs::metadata(checkpoint_data_file(&base.path)?)?.len();
        Ok(log * 100 > u128::from(image) * u128::from(self.log_percent.get()))
    }

    fn too_old(&self, base: &CheckpointInfo) -> Result<bool, StateError> {
        let made = std::fs::metadata(&base.path)?.modified()?;
        Ok(made.elapsed().is_ok_and(|age| age >= self.max_age))
    }
}

/// The bases that the rule made, by the `reason` of [`BaseDue::label`].
const BASES_TOTAL: &str = "kardamom_checkpoint_bases_total";

/// One round of the checkpointer with the block log: the rule, the rate of
/// the copy, and the bases to keep.
#[derive(Clone, Copy, Debug)]
pub struct BaseRound {
    pub rule: BaseRule,
    pub rate: CopyRate,
    pub keep: NonZeroUsize,
}

impl BaseRound {
    /// Make a base of `env` in `checkpoints_dir` when one is due, then
    /// prune. Returns why a base was made, or `None`.
    ///
    /// # Errors
    ///
    /// Returns an error when the rule, the copy or the prune fails.
    pub fn run(
        &self,
        env: &StateEnv,
        checkpoints_dir: &Path,
    ) -> Result<Option<BaseDue>, StateError> {
        let due = self.rule.due(checkpoints_dir)?;
        if let Some(reason) = due {
            create_checkpoint_at(env, checkpoints_dir, self.rate)?;
            metrics::counter!(BASES_TOTAL, "reason" => reason.label()).increment(1);
        }
        prune_bases(checkpoints_dir, self.keep)?;
        Ok(due)
    }
}

/// Cut the block log of `checkpoints_dir` after `block`. A rollback drops
/// the blocks of the stream that the cluster lost, so no restore and no
/// peer takes them. The chain of the log is the chain of the newest base.
/// With no base or no log, nothing is cut: the log thread cuts the log at
/// its next start.
///
/// # Errors
///
/// Returns an error when the newest base or the log does not read, or the
/// cut fails.
pub fn cut_log_after(checkpoints_dir: &Path, block: u64) -> Result<(), StateError> {
    let log = checkpoints_dir.join(LOG_DIR);
    let Some(base) = latest_checkpoint(checkpoints_dir, BlockRange::ANY)?.filter(|_| log.exists())
    else {
        return Ok(());
    };
    let identity = LogIdentity {
        genesis: read_manifest(&base.path)?.genesis_digest,
        schema: SCHEMA_VERSION,
    };
    BlockLogWriter::open(&log, identity, DEFAULT_SEGMENT_BYTES)?.cut_after(block)
}

/// Keep the newest `keep` bases, and the log segments that they need.
/// Returns how many bases and segments were deleted.
///
/// # Errors
///
/// Returns an error when the directory does not read, or a base or a
/// segment does not delete.
pub fn prune_bases(
    checkpoints_dir: &Path,
    keep: NonZeroUsize,
) -> Result<(usize, usize), StateError> {
    let bases = prune_checkpoints(checkpoints_dir, keep)?;
    let Some(oldest) = checkpoints_newest_first(checkpoints_dir)?.pop() else {
        return Ok((bases, 0));
    };
    let segments = SegmentSize::prune_below(&checkpoints_dir.join(LOG_DIR), oldest.block)?;
    Ok((bases, segments))
}

#[cfg(test)]
#[path = "bases_tests.rs"]
mod tests;
