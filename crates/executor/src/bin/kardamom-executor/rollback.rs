//! The automatic rollback after a divergence of the canonical stream.
//!
//! The reader of the member feed ends a run with the divergence point when
//! the stream differs from the committed history: the cluster continued
//! from a shorter log after a power loss on a majority of the members.
//! Between two runs, [`RollbackRepair`] removes the local checkpoints at and
//! above the divergence block, chooses the newest checkpoint below it (a
//! local one first, then one of a peer), and parks the diverged state DB.
//! The next run restores a checkpoint below the block, publishes the
//! rollback record of the marker before its first receipt, and executes
//! the new stream from the checkpoint. With no checkpoint below the block,
//! the executor stops as before, and the operator restores it.

use std::path::Path;

use alloy_primitives::B256;
use anyhow::{Context, Result};
use kardamom_engine::ExecutorError;
use kardamom_state::BlockRange;

use crate::marker::{Cause, Markers};

/// Where the checkpoint of a rollback is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    Local,
    Peer,
}

impl Found {
    /// The `outcome` label of the rollback counter.
    fn label(self) -> &'static str {
        match self {
            Self::Local => "local-checkpoint",
            Self::Peer => "peer-checkpoint",
        }
    }
}

/// What the rollback between two runs needs.
pub(crate) struct RollbackRepair<'a> {
    pub(crate) checkpoint_dir: Option<&'a Path>,
    pub(crate) checkpoint_peers: &'a [String],
    pub(crate) state_dir: &'a Path,
    pub(crate) expected_genesis: Option<B256>,
    pub(crate) markers: &'a Markers,
}

impl RollbackRepair<'_> {
    /// Stage the rollback for `err`, when it is a divergence. Returns
    /// whether a rollback is staged: `false` for every other error, and
    /// when no checkpoint below the divergence block exists.
    ///
    /// The rollback marker is on disk before the park, so a crash after
    /// the park still publishes the record, and the next start restores
    /// only a checkpoint at or below its block.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the removal of a diverged checkpoint, the write
    /// of the marker, or the park of the state DB fails.
    pub(crate) fn stage(&self, err: Option<&ExecutorError>) -> Result<bool> {
        let Some(ExecutorError::StreamDiverged {
            block,
            index,
            stream,
        }) = err
        else {
            return Ok(false);
        };
        tracing::warn!(
            block,
            index,
            "the canonical stream diverges from the committed history; rolling back"
        );
        let (Some(dir), Some(restore)) = (self.checkpoint_dir, BlockRange::below(*block)) else {
            return Ok(Self::unrecoverable(*block));
        };
        let diverged = BlockRange {
            min: *block,
            ..BlockRange::ANY
        };
        let removed = kardamom_state::remove_checkpoints(dir, diverged)
            .context("remove the diverged checkpoints")?;
        let Some(found) = self.find(dir, restore) else {
            return Ok(Self::unrecoverable(*block));
        };
        let record = Cause::Diverged { stream: *stream }.record(restore.max);
        self.markers.raise(record)?;
        kardamom_state::park_state_db(self.state_dir).context("park the diverged state DB")?;
        metrics::counter!(kardamom_engine::metrics::ROLLBACK_TOTAL, "outcome" => found.label())
            .increment(1);
        tracing::warn!(
            block,
            removed_checkpoints = removed,
            checkpoint = found.label(),
            "rollback prepared: diverged state parked; the next run restores a checkpoint below the block"
        );
        Ok(true)
    }

    /// Where a checkpoint in `restore` is: on the local disk, else fetched
    /// from a peer into `dir`.
    fn find(&self, dir: &Path, restore: BlockRange) -> Option<Found> {
        let local = kardamom_state::latest_checkpoint(dir, restore)
            .inspect_err(|e| tracing::warn!(error = %e, "the checkpoint listing failed"))
            .ok()
            .flatten();
        if local.is_some() {
            return Some(Found::Local);
        }
        kardamom_state::fetch_best_checkpoint(
            self.checkpoint_peers,
            dir,
            restore,
            self.expected_genesis,
        )
        .map(|_| Found::Peer)
    }

    /// Count and log a divergence with no checkpoint below it. The executor
    /// stops, and the operator restores it.
    fn unrecoverable(block: u64) -> bool {
        metrics::counter!(kardamom_engine::metrics::ROLLBACK_TOTAL, "outcome" => "no-checkpoint")
            .increment(1);
        tracing::error!(
            block,
            "rollback failed: no checkpoint below the divergence block, locally or from a \
             peer — operator action required (restore a checkpoint into --checkpoint-dir, or \
             rebuild-from-L1 with kardamom-reconstruct into --state-dir)"
        );
        false
    }
}

#[cfg(test)]
#[path = "rollback_tests.rs"]
mod tests;
