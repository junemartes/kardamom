//! The automatic rollback after a divergence of the canonical stream.
//!
//! The reader of the member feed ends a run with the divergence point when
//! the stream differs from the committed history, or from the receipts that
//! the executor published above it: the cluster continued from a shorter
//! log after a power loss on a majority of the members. Between two runs,
//! [`RollbackRepair`] writes the rollback record, and removes the void
//! receipts from the receipt ledger: every receipt after a restore, else
//! the receipts of the divergence block and above.
//!
//! - A divergence of the committed history also needs an older state. The
//!   repair removes the local checkpoints at and above the divergence
//!   block, chooses the newest checkpoint below it (a local one first, then
//!   one of a peer), cuts the block log after the block before the
//!   divergence, and parks the diverged state DB. With the block log, the
//!   restore then reaches the block just before the divergence. The next run restores
//!   that checkpoint. With no checkpoint below the block, the executor
//!   stops as before, and the operator restores it.
//! - A divergence of the published receipts keeps the state: the committed
//!   history is equal.
//!
//! The next run publishes the rollback record before its first receipt, and
//! executes the new stream.

use std::path::Path;

use alloy_primitives::B256;
use anyhow::{Context, Result};
use kardamom_engine::ExecutorError;
use kardamom_state::BlockRange;
use kardamom_types::ReceiptRollback;

use crate::ledger::ReceiptLedger;
use crate::marker::Void;

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

/// What a divergence voids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    /// The committed history: the state rolls back to a checkpoint.
    Committed,
    /// Only the receipts published above the committed history.
    Published,
}

/// What the rollback between two runs needs.
pub(crate) struct RollbackRepair<'a> {
    pub(crate) checkpoint_dir: Option<&'a Path>,
    pub(crate) checkpoint_peers: &'a [String],
    pub(crate) state_dir: &'a Path,
    pub(crate) expected_genesis: Option<B256>,
    pub(crate) ledger: &'a ReceiptLedger,
}

impl RollbackRepair<'_> {
    /// Stage the rollback for `err`, when it is a divergence. Returns
    /// whether a rollback is staged: `false` for every other error, and
    /// when no checkpoint below a divergence of the committed history
    /// exists.
    ///
    /// The rollback marker is on disk before the park, so a crash after
    /// the park still publishes the record, and the next start restores
    /// only a checkpoint at or below its block.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the removal of a diverged checkpoint, the write
    /// of the marker, the void of the ledger, or the park of the state DB
    /// fails.
    pub(crate) fn stage(&self, err: Option<&ExecutorError>) -> Result<bool> {
        let (reach, block, index, stream) = match err {
            Some(ExecutorError::StreamDiverged {
                block,
                index,
                stream,
            }) => (Reach::Committed, *block, *index, *stream),
            Some(ExecutorError::PublishedDiverged {
                block,
                index,
                stream,
            }) => (Reach::Published, *block, *index, *stream),
            _ => return Ok(false),
        };
        tracing::warn!(
            block,
            index,
            ?reach,
            "the canonical stream diverges from this executor; rolling back"
        );
        let Some(kept) = BlockRange::below(block) else {
            return Ok(Self::unrecoverable(block));
        };
        let record = Void {
            kept: kept.max,
            stream,
        }
        .record();
        match reach {
            Reach::Committed => self.restore(block, kept, record),
            Reach::Published => {
                self.ledger.markers.raise(record)?;
                self.ledger.void(block)?;
                metrics::counter!(kardamom_engine::metrics::ROLLBACK_TOTAL, "outcome" => "receipts-only")
                    .increment(1);
                Ok(true)
            }
        }
    }

    /// Stage a checkpoint in `restore` for a divergence of the committed
    /// history at `block`, and park the diverged state DB.
    fn restore(&self, block: u64, restore: BlockRange, record: ReceiptRollback) -> Result<bool> {
        let Some(dir) = self.checkpoint_dir else {
            return Ok(Self::unrecoverable(block));
        };
        let diverged = BlockRange {
            min: block,
            ..BlockRange::ANY
        };
        let removed = kardamom_state::remove_checkpoints(dir, diverged)
            .context("remove the diverged checkpoints")?;
        let Some(found) = self.find(dir, restore) else {
            return Ok(Self::unrecoverable(block));
        };
        kardamom_state::checkpoint::bases::cut_log_after(dir, restore.max)
            .context("cut the block log after the kept block")?;
        self.ledger.markers.raise(record)?;
        self.ledger.clear()?;
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
