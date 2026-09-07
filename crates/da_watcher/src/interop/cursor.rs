//! Durable per-pair cursor: the interop watcher's resume position, persisted
//! to a file with atomic replace (temp + rename).
//!
//! ## The write-ordering invariant, and why staleness is the SAFE side
//!
//! The cursor is persisted AFTER each successful publish, never before. The
//! two possible failure windows are wildly asymmetric
//! ([`crate::interop::publisher`] states the same rule for the publish
//! report itself):
//!
//! * **Stale cursor** (crash after publish, before persist): HARMLESS. The
//!   restarted watcher re-reads the feed from the old cursor and re-derives
//!   the same batch; re-derivation is byte-identical
//!   (`derive_remote_epoch` is a pure function of the feed prefix), so the
//!   re-published record carries the same `canonical_id` and cluster
//!   first-seen dedup absorbs it. Cost: one duplicate offer.
//! * **Ahead cursor** (persisted before the publish it describes): a
//!   PERMANENT lane hole. The record between old and new cursor was never
//!   published, no retry will ever re-derive it (the cursor has moved past
//!   it), and the destination's no-skip verifier halts the pair on the gap —
//!   which is why no code path here writes the file before the publish it
//!   records.
//!
//! ## One watcher per cursor file
//!
//! [`CursorFile::open`] takes an advisory lock on a sibling `<path>.lock`
//! file and holds it for the life of the process. Two watchers on one cursor
//! file would race the temp-file rename: each could publish the lane from a
//! different position, and the file would hold whichever rename landed
//! last. The second watcher fails at startup instead. The lock is an OS
//! file lock, so a crash releases it; nothing stale is left behind.
//!
//! ## Corruption is a hard error
//!
//! A cursor file that exists but does not parse is never treated as 0 or as
//! absent: silently restarting a long-lived pair from seq 0 would replay the
//! entire lane history against the feed's retention window (`Lagged` →
//! stall), and — worse — LOOK like a fresh first boot while actually being
//! evidence of disk corruption or operator error. The operator must decide
//! (restore the file, or deliberately delete it to re-seed via
//! `--interop-start-seq`).

use std::fs::{File, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Why the cursor file could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("cursor file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The file exists but is not a bare decimal seq. Deliberately NOT
    /// recovered from — see the module docs.
    #[error(
        "cursor file {path} is corrupt (contents {contents:?}); refusing to guess a resume \
         position — restore the file, or delete it to re-seed from --interop-start-seq"
    )]
    Corrupt { path: PathBuf, contents: String },
    /// Another process holds the lock on this cursor file. See the module
    /// docs: two watchers on one file is a misconfiguration, never a race
    /// to tolerate.
    #[error(
        "cursor file {path} is locked by another watcher (lock file {lock}); run one watcher \
         per cursor file — stop the other process, or use a different --interop-cursor-file"
    )]
    Locked { path: PathBuf, lock: PathBuf },
}

/// One pair's persisted cursor. The value stored is the FIRST SEQ NOT YET
/// PUBLISHED (the same convention as the in-memory cursor and the
/// `REMOTE_CURSOR_SEQ` gauge).
#[derive(Debug, Clone)]
pub struct CursorFile {
    path: PathBuf,
    /// The advisory lock on `<path>.lock`. Every clone shares the handle, so
    /// the lock lives as long as the last clone. The OS releases it when the
    /// process exits, cleanly or not.
    _lock: Arc<File>,
}

impl CursorFile {
    /// Open the cursor at `path` and take its lock. Fails with
    /// [`CursorError::Locked`] when another watcher holds the lock. The
    /// cursor file itself is not created here; `load` reports it absent
    /// until the first `persist`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CursorError> {
        let path = path.into();
        let lock_path = lock_path_of(&path);
        let io = |source| CursorError::Io {
            path: lock_path.clone(),
            source,
        };
        let lock = File::options()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(io)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(CursorError::Locked {
                    path,
                    lock: lock_path,
                });
            }
            Err(TryLockError::Error(e)) => return Err(io(e)),
        }
        Ok(Self {
            path,
            _lock: Arc::new(lock),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the persisted cursor. `Ok(None)` when the file does not exist —
    /// the first-boot case, where the CLI seed applies. A file that exists
    /// but does not parse is [`CursorError::Corrupt`], never a silent 0.
    pub fn load(&self) -> Result<Option<u64>, CursorError> {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(CursorError::Io {
                    path: self.path.clone(),
                    source: e,
                });
            }
        };
        raw.trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| CursorError::Corrupt {
                path: self.path.clone(),
                contents: raw.chars().take(128).collect(),
            })
    }

    /// Persist `next_seq` atomically: write a sibling temp file, fsync it,
    /// rename over the target. A crash at any point leaves either the old
    /// complete value or the new complete value — never a torn write, which
    /// `load` would otherwise reject as corruption.
    pub fn persist(&self, next_seq: u64) -> Result<(), CursorError> {
        let io = |source| CursorError::Io {
            path: self.path.clone(),
            source,
        };
        let dir = self.path.parent().filter(|p| !p.as_os_str().is_empty());
        // Unique-per-process temp name: two watchers misconfigured onto one
        // file must not interleave partial writes into each other's temp.
        let tmp = self
            .path
            .with_extension(format!("tmp.{}", std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            f.write_all(format!("{next_seq}\n").as_bytes())
                .map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        std::fs::rename(&tmp, &self.path).map_err(io)?;
        // Best-effort directory fsync so the rename itself is durable; on
        // filesystems/platforms where opening a directory for sync is not
        // supported this degrades to rename-durability-on-journal, which
        // still can only lose the LAST update (stale — the harmless side).
        if let Some(dir) = dir
            && let Ok(d) = std::fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

/// `<path>.lock`, appended to the full file name so `pair.cursor` and
/// `pair` never share a lock file.
fn lock_path_of(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kardamom-cursor-test-{}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("cursor")
    }

    #[test]
    fn missing_file_is_first_boot_not_zero() {
        let c = CursorFile::open(temp_path("missing")).unwrap();
        let _ = std::fs::remove_file(c.path());
        assert!(matches!(c.load(), Ok(None)));
    }

    #[test]
    fn roundtrip_and_overwrite() {
        let c = CursorFile::open(temp_path("roundtrip")).unwrap();
        c.persist(7).unwrap();
        assert_eq!(c.load().unwrap(), Some(7));
        c.persist(12_345).unwrap();
        assert_eq!(c.load().unwrap(), Some(12_345));
        // The temp file must not linger after a successful rename.
        let tmp = c
            .path()
            .with_extension(format!("tmp.{}", std::process::id()));
        assert!(!tmp.exists(), "temp file left behind at {}", tmp.display());
    }

    #[test]
    fn corrupt_file_is_a_hard_error_never_silent_zero() {
        let c = CursorFile::open(temp_path("corrupt")).unwrap();
        std::fs::write(c.path(), "not-a-seq\n").unwrap();
        let err = c.load().unwrap_err();
        assert!(matches!(err, CursorError::Corrupt { .. }), "got {err:?}");

        // Empty is corrupt too: a truncated file is evidence, not a seed.
        std::fs::write(c.path(), "").unwrap();
        assert!(matches!(c.load(), Err(CursorError::Corrupt { .. })));
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        // The file is written with a trailing newline; hand-edits with an
        // editor may add one more. Both are the same value, not corruption.
        let c = CursorFile::open(temp_path("whitespace")).unwrap();
        std::fs::write(c.path(), " 42\n\n").unwrap();
        assert_eq!(c.load().unwrap(), Some(42));
    }

    #[test]
    fn a_second_watcher_on_one_cursor_file_is_refused() {
        let path = temp_path("locked");
        let first = CursorFile::open(&path).unwrap();
        let err = CursorFile::open(&path).unwrap_err();
        assert!(matches!(err, CursorError::Locked { .. }), "got {err:?}");
        assert!(
            lock_path_of(&path).exists(),
            "lock file is a sibling of the cursor"
        );

        // A clone shares the lock: dropping the original keeps it held.
        let clone = first.clone();
        drop(first);
        assert!(matches!(
            CursorFile::open(&path),
            Err(CursorError::Locked { .. })
        ));

        // The last handle releases it, so a restart can take it again.
        drop(clone);
        CursorFile::open(&path).unwrap();
    }

    #[test]
    fn lock_file_name_keeps_the_full_cursor_name() {
        assert_eq!(
            lock_path_of(Path::new("/var/lib/kardamom/pair.cursor")),
            PathBuf::from("/var/lib/kardamom/pair.cursor.lock")
        );
    }
}
