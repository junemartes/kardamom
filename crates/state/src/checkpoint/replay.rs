//! [`LogReplay`]: a restore point from a base checkpoint and the block log
//! after it.
//!
//! A restore copies a base checkpoint, then applies the batches of the
//! block log after the base through a state writer. So the restored state
//! is the state that the writer made, block by block. The replay commits
//! with no sync for each block, and syncs once at the end.
//!
//! A replay that stops early leaves the database at the last applied
//! block: each commit is atomic. The normal resume path then executes the
//! rest from the canonical stream. So a fault of the log never sets the
//! base aside.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use super::block_log::{BlockLog, LogIdentity, Record};
use crate::env::{Durability, StateEnvBuilder};
use crate::error::StateError;
use crate::meta::SCHEMA_VERSION;
use crate::recovery::read_recovery_point;
use crate::writer::{StateWriter, WriteBatch, WriterHandle};

/// The directory of the block log inside the checkpoint directory.
pub const LOG_DIR: &str = "log";

/// The block log of one checkpoint directory, and the last block that a
/// restore may reach.
pub(crate) struct LogReplay {
    log_dir: PathBuf,
    identity: LogIdentity,
    to: u64,
}

impl LogReplay {
    /// The replay of the log next to `checkpoint`, for the chain of
    /// `genesis`, up to block `to`.
    pub(crate) fn new(checkpoint: &Path, genesis: alloy_primitives::B256, to: u64) -> Self {
        let dir = checkpoint.parent().unwrap_or(checkpoint);
        Self {
            log_dir: dir.join(LOG_DIR),
            identity: LogIdentity {
                genesis,
                schema: SCHEMA_VERSION,
            },
            to,
        }
    }

    /// The last block that the log reaches from `base`, up to the limit
    /// of the replay.
    pub(crate) fn reach(&self, base: u64) -> Result<u64, StateError> {
        let log = BlockLog::open(&self.log_dir, self.identity)?;
        Ok(log.after(base, self.to).last().map_or(base, |r| r.block))
    }

    /// Open the restored database in `state_dir`, apply the log after its
    /// block, and sync. Returns the last committed block.
    ///
    /// # Errors
    ///
    /// Returns an error when the database does not open, read or sync, or
    /// when the log directory does not read.
    pub(crate) fn apply(&self, state_dir: &Path) -> Result<u64, StateError> {
        let env = StateEnvBuilder::new(state_dir)
            .durability(Durability::SafeNoSync)
            .open()?;
        let base = read_recovery_point(&env)?.last_committed_block;
        let log = BlockLog::open(&self.log_dir, self.identity)?;
        let writer = StateWriter::spawn(env.clone())?;
        Self::feed(writer, log.after(base, self.to));
        env.raw().sync(true)?;
        let block = read_recovery_point(&env)?.last_committed_block;
        info!(
            base,
            block,
            log = %self.log_dir.display(),
            "applied the block log after the base checkpoint"
        );
        Ok(block)
    }

    /// Send each batch of `records` to `writer`, then stop it. The writer
    /// applies every batch that it took before it stops. A batch that
    /// does not decode ends the records, and a batch that does not apply
    /// stops the writer at the block before it.
    fn feed(mut writer: WriterHandle, records: impl Iterator<Item = Record>) {
        let sent = records
            .map_while(|r| {
                r.batch()
                    .inspect_err(|e| warn!(block = r.block, error = %e, "a block log record does not decode; the replay ends before it"))
                    .ok()
            })
            // A stopped writer returns the batch, and the replay needs only
            // the fact that it stopped.
            .try_for_each(|batch: WriteBatch| writer.delta_tx.send(batch).map_err(|_| ()));
        if sent.is_err() {
            warn!("the state writer of the replay stopped early");
        }
        if let Err(e) = writer.shutdown() {
            warn!(error = %e, "the replay of the block log stopped at a block that does not apply");
        }
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
