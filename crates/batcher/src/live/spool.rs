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
//!
//! The files of one layout version live in their own directory,
//! `spool/v<N>`. A release reads only the layout it knows. A `v<N>`
//! directory of another version, and a block file of the unversioned
//! layout before `v1`, are spools of another release: each is dropped,
//! with a warning and a count, and the range is read again from the
//! sealer, which keeps every frame above the posted head. Every other
//! entry of the spool root stays. A block file that does not decode
//! drops the spool the same way. A spool never stops the batcher: a
//! spool that cannot be removed stays, with a warning.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use metrics::counter;
use tracing::warn;

use crate::batch::ClosedBlock;

use super::live_metric_names;

/// The version of the spool layout: one rkyv `ClosedBlock` per file. The
/// spool of this version lives in `spool/v<SPOOL_VERSION>`.
pub(crate) const SPOOL_VERSION: u32 = 1;

/// The spool directory of this release's layout version.
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

impl Restored {
    /// No block: the reader resumes at the confirmed cursor.
    pub(crate) fn empty() -> Self {
        Self {
            blocks: Vec::new(),
            oldest_written: None,
        }
    }
}

impl Spool {
    /// Count one dropped spool. `reason` is the metric label.
    pub(crate) fn count_dropped(reason: &'static str) {
        counter!(live_metric_names::SPOOL_DROPPED, "reason" => reason).increment(1);
    }

    /// Open the spool of this release under `root`; create it. Drop every
    /// spool of another version under `root`; keep every other entry.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be created or `root`
    /// cannot be listed.
    pub(crate) fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let spool = Self {
            dir: root.join(format!("v{SPOOL_VERSION}")),
        };
        fs::create_dir_all(&spool.dir)
            .with_context(|| format!("create spool {}", spool.dir.display()))?;
        fs::read_dir(root)
            .with_context(|| format!("list spool root {}", root.display()))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| spool.is_other_version(path))
            .for_each(|path| Self::drop_other_version(&path));
        Ok(spool)
    }

    /// True when `path`, an entry of the spool root, is a spool of another
    /// version: a `v<N>` directory that is not this release's, or a block
    /// file of the unversioned layout before `v1`.
    fn is_other_version(&self, path: &Path) -> bool {
        *path != self.dir && (Self::is_version_dir(path) || Self::is_unversioned_block(path))
    }

    /// True when `path` is a directory named `v<N>`, the spool of layout
    /// version `N`.
    fn is_version_dir(path: &Path) -> bool {
        path.is_dir()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix('v'))
                .is_some_and(|version| version.parse::<u32>().is_ok())
    }

    /// True when `path` is a block file, or its temporary file, of the
    /// unversioned spool layout: `<block number>.block` or `.tmp`.
    fn is_unversioned_block(path: &Path) -> bool {
        path.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext == "block" || ext == "tmp")
            && path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.parse::<u64>().is_ok())
    }

    /// Remove `path`, a spool of another version, with a warning and a
    /// count. Its blocks are read again from the sealer. A failed remove
    /// leaves the spool in place with a warning: it never stops the start.
    fn drop_other_version(path: &Path) {
        warn!(
            path = %path.display(),
            spool_version = SPOOL_VERSION,
            "spool of another version; dropping it; the range is read again from the sealer"
        );
        Self::count_dropped("other-version");
        let removed = if path.is_dir() {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        };
        if let Err(error) = removed {
            warn!(
                path = %path.display(),
                %error,
                "cannot remove the spool of another version; it stays"
            );
        }
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

    /// Every block, in block order. A spool that does not read back is
    /// dropped, with a warning and a count, and the result is empty: the
    /// reader then resumes at the confirmed cursor.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read or an unreadable
    /// spool cannot be removed.
    pub(crate) fn load(&self) -> Result<Restored> {
        let entries = self.entries()?;
        match Self::decode(&entries) {
            Ok(restored) => Ok(restored),
            Err(error) => {
                warn!(
                    spool = %self.dir.display(),
                    error = %format!("{error:#}"),
                    "spool does not read back; dropping it; the range is read again from the sealer"
                );
                Self::count_dropped("unreadable");
                self.clear_through(u64::MAX)?;
                Ok(Restored::empty())
            }
        }
    }

    /// The blocks behind `entries`, and the write time of the oldest.
    fn decode(entries: &[(u64, PathBuf)]) -> Result<Restored> {
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

    fn numbers(spool: &Spool) -> Vec<u64> {
        spool
            .load()
            .unwrap()
            .blocks
            .iter()
            .map(|b| b.block_number)
            .collect()
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
        assert_eq!(numbers(&spool), vec![12]);
    }

    #[test]
    fn the_spool_lives_in_its_version_directory() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        spool.append(&block(3)).unwrap();
        let versioned = dir.path().join(format!("v{SPOOL_VERSION}"));
        assert!(versioned.join("00000000000000000003.block").is_file());
        let reopened = Spool::open(dir.path()).unwrap();
        assert_eq!(numbers(&reopened), vec![3]);
    }

    #[test]
    fn a_spool_of_another_version_is_dropped_at_open() {
        let dir = tempfile::tempdir().unwrap();
        // A spool with no version directory, one of a later version, and
        // two entries that are not spools.
        std::fs::write(dir.path().join("00000000000000000005.block"), b"old").unwrap();
        std::fs::write(dir.path().join("cursor.json"), b"{}").unwrap();
        std::fs::create_dir_all(dir.path().join("vault")).unwrap();
        let later = dir.path().join(format!("v{}", SPOOL_VERSION + 1));
        std::fs::create_dir_all(&later).unwrap();
        std::fs::write(later.join("00000000000000000006.block"), b"new").unwrap();

        let spool = Spool::open(dir.path()).unwrap();
        assert!(spool.load().unwrap().blocks.is_empty());
        let mut left: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec![
                "cursor.json".to_owned(),
                format!("v{SPOOL_VERSION}"),
                "vault".to_owned()
            ]
        );
    }

    #[test]
    fn a_block_that_does_not_decode_drops_the_spool() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        spool.append(&block(10)).unwrap();
        std::fs::write(
            dir.path()
                .join(format!("v{SPOOL_VERSION}"))
                .join("00000000000000000011.block"),
            b"not a block",
        )
        .unwrap();
        let restored = spool.load().unwrap();
        assert!(restored.blocks.is_empty());
        assert!(restored.oldest_written.is_none());
        assert_eq!(
            numbers(&spool),
            Vec::<u64>::new(),
            "the spool is empty after the drop"
        );
    }

    /// A spool of another version that cannot be removed stays, with a
    /// warning. The open still succeeds.
    #[test]
    fn a_spool_that_cannot_be_removed_does_not_stop_the_open() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let later = dir.path().join(format!("v{}", SPOOL_VERSION + 1));
        std::fs::create_dir_all(&later).unwrap();
        std::fs::write(later.join("00000000000000000006.block"), b"new").unwrap();
        std::fs::set_permissions(&later, std::fs::Permissions::from_mode(0o500)).unwrap();

        let opened = Spool::open(dir.path());
        std::fs::set_permissions(&later, std::fs::Permissions::from_mode(0o700)).unwrap();
        let spool = opened.expect("a stuck old spool never stops the open");
        spool.append(&block(7)).unwrap();
        assert_eq!(numbers(&spool), vec![7]);
    }
}
