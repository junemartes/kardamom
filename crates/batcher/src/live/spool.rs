//! The spool: every closed block the batcher has consumed and not yet
//! posted, on its own disk.
//!
//! The sealers retain egress frames in memory, and a sealer that
//! restarts keeps only the frames since its last snapshot. A batcher that
//! groups blocks for an hour would, after such a restart, ask for a replay
//! the sealers cannot serve, and fail-stop with a permanent DA gap. With
//! the spool the batcher needs the sealers only for the frames it has not
//! read yet: a restart reloads the pending group from disk and resumes the
//! stream just past it.
//!
//! One file per block, written whole and renamed into place; removed once
//! the block's post is confirmed. The spool is the batcher's own memory,
//! not a source of truth: the cursor file still binds the last confirmed
//! post to L1, and a spool that does not continue the confirmed cursor is
//! dropped.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};

use crate::batch::ClosedBlock;

/// The spool directory.
#[derive(Clone, Debug)]
pub(crate) struct Spool {
    dir: PathBuf,
}

/// What the spool holds at start: the blocks in order, and when the
/// oldest was written, for the group's timers.
pub(crate) struct Restored {
    pub(crate) blocks: Vec<ClosedBlock>,
    pub(crate) oldest_written: Option<SystemTime>,
}

impl Spool {
    /// Open the spool at `dir`; create it.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be created.
    pub(crate) fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).with_context(|| format!("create spool {}", dir.display()))?;
        Ok(Self { dir })
    }

    fn path_of(&self, block_number: u64) -> PathBuf {
        self.dir.join(format!("{block_number:020}.block"))
    }

    /// Keep `block`.
    ///
    /// # Errors
    /// Returns an error when the block does not serialize or the write fails.
    pub(crate) fn append(&self, block: &ClosedBlock) -> Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(block)
            .with_context(|| format!("serialize block {}", block.block_number))?;
        let path = self.path_of(block.block_number);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Drop every block at or below `block_number`: its post is confirmed.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read or a file removed.
    pub(crate) fn clear_through(&self, block_number: u64) -> Result<()> {
        self.entries()?
            .into_iter()
            .filter(|(number, _)| *number <= block_number)
            .try_for_each(|(_, path)| fs::remove_file(path))?;
        Ok(())
    }

    /// Every block, in block order.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read or a block does
    /// not deserialize.
    pub(crate) fn load(&self) -> Result<Restored> {
        let entries = self.entries()?;
        let oldest_written = entries
            .first()
            .map(|(_, path)| fs::metadata(path).and_then(|m| m.modified()))
            .transpose()?;
        let blocks = entries
            .iter()
            .map(|(number, path)| {
                let bytes = fs::read(path)?;
                rkyv::from_bytes::<ClosedBlock, rkyv::rancor::Error>(&bytes)
                    .with_context(|| format!("deserialize spooled block {number}"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Restored {
            blocks,
            oldest_written,
        })
    }

    /// The block files, sorted by block number. A stray temporary file is
    /// not one.
    fn entries(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut entries: Vec<(u64, PathBuf)> = fs::read_dir(&self.dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let number = path.file_stem()?.to_str()?.parse::<u64>().ok()?;
                (path.extension()? == "block").then_some((number, path))
            })
            .collect();
        entries.sort_by_key(|(number, _)| *number);
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    fn block(number: u64) -> ClosedBlock {
        ClosedBlock {
            block_number: number,
            l2_timestamp: 1_700_000_000 + number,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l1_origin: 7,
            remote_epochs: Vec::new(),
            txs: Vec::new(),
        }
    }

    #[test]
    fn blocks_round_trip_in_order_and_clear_through_a_post() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        assert!(spool.load().unwrap().blocks.is_empty());
        for n in [12, 10, 11] {
            spool.append(&block(n)).unwrap();
        }
        let restored = spool.load().unwrap();
        assert_eq!(
            restored
                .blocks
                .iter()
                .map(|b| b.block_number)
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert_eq!(restored.blocks[2], block(12));
        assert!(restored.oldest_written.is_some());
        spool.clear_through(11).unwrap();
        let left = spool.load().unwrap().blocks;
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].block_number, 12);
    }
}
