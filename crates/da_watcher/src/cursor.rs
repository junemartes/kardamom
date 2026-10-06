//! Durable cursor: a watcher's resume position, persisted to a file with
//! atomic replace (temp + rename). The L1 watcher stores the last L1 block
//! it published ([`crate::L1Cursor`]); the interop watcher stores the first
//! seq of its pair that it did not publish yet (`u64`).
//!
//! ## The write-ordering invariant, and why staleness is the SAFE side
//!
//! The cursor is persisted AFTER each successful publish, never before. The
//! two possible failure windows are wildly asymmetric
//! ([`crate::interop::publisher`] states the same rule for the publish
//! report itself):
//!
//! * **Stale cursor** (crash after publish, before persist): HARMLESS. The
//!   restarted watcher derives the same records again from the old cursor.
//!   Derivation is byte-identical, so a record published again carries the
//!   same `canonical_id`, and the sealer's first-seen dedup absorbs it. An
//!   L1 epoch whose id left the dedup window meets the sealer's origin
//!   guard instead, which drops an origin at or below its own. Cost: one
//!   duplicate offer per record.
//! * **Ahead cursor** (persisted before the publish it describes): a
//!   PERMANENT hole. The record between the old and the new cursor was
//!   never published, no retry derives it again (the cursor has moved past
//!   it), and the downstream no-skip rule halts on the gap. This is why no
//!   code path here writes the file before the publish it records.
//!
//! ## One watcher per cursor file
//!
//! [`CursorFile::open`] takes an advisory lock on a sibling `<path>.lock`
//! file and holds it for the life of the process. Two watchers on one cursor
//! file would race the temp-file rename: each could publish from a
//! different position, and the file would hold whichever rename landed
//! last. The second watcher fails at startup instead. The lock is an OS
//! file lock, so a crash releases it; nothing stale is left behind.
//!
//! ## Corruption is a hard error
//!
//! A cursor file that exists but does not parse is never treated as absent:
//! a watcher that silently restarts from its seed position LOOKS like a
//! fresh first boot while it actually holds evidence of disk corruption or
//! operator error, and the seed position can skip records or replay a whole
//! history. The operator decides: restore the file, or delete it to start
//! from the seed position.

use std::fmt::Display;
use std::fs::{File, TryLockError};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Why the cursor file could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("cursor file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The file exists but does not parse as the cursor value. Deliberately
    /// NOT recovered from — see the module docs.
    #[error(
        "cursor file {path} is corrupt (contents {contents:?}); refusing to guess a resume \
         position — restore the file, or delete it to start from the seed position"
    )]
    Corrupt { path: PathBuf, contents: String },
    /// Another process holds the lock on this cursor file. See the module
    /// docs: two watchers on one file is a misconfiguration, never a race
    /// to tolerate.
    #[error(
        "cursor file {path} is locked by another watcher (lock file {lock}); run one watcher \
         per cursor file — stop the other process, or use a different cursor file"
    )]
    Locked { path: PathBuf, lock: PathBuf },
}

/// One watcher's persisted cursor of type `V`. The file holds the value's
/// `Display` form and one newline; [`CursorFile::load`] parses it back
/// with `FromStr`.
#[derive(Debug)]
pub struct CursorFile<V> {
    path: PathBuf,
    /// The advisory lock on `<path>.lock`. Held for the value's lifetime;
    /// dropping it (or exiting the process) releases the lock.
    _lock: File,
    value: PhantomData<fn() -> V>,
}

impl<V: Display + FromStr> CursorFile<V> {
    /// Open the cursor at `path` and take its lock. Fails with
    /// [`CursorError::Locked`] when another watcher holds the lock. The
    /// cursor file itself is not created here; `load` reports it absent
    /// until the first `persist`.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError::Io`] if the lock file cannot be opened, or
    /// [`CursorError::Locked`] if another watcher already holds the lock.
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
            _lock: lock,
            value: PhantomData,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the persisted cursor. `Ok(None)` when the file does not exist —
    /// the first-boot case, where the seed position applies. A file that
    /// exists but does not parse is [`CursorError::Corrupt`], never a
    /// silent seed.
    ///
    /// # Errors
    /// Returns an error when the file exists but cannot be read, or its
    /// contents do not parse as a `V`.
    pub fn load(&self) -> Result<Option<V>, CursorError> {
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
            .parse::<V>()
            .map(Some)
            .map_err(|_| CursorError::Corrupt {
                path: self.path.clone(),
                contents: raw.chars().take(128).collect(),
            })
    }

    /// Persist `value` atomically: write a sibling temp file, fsync it,
    /// rename over the target. A crash at any point leaves either the old
    /// complete value or the new complete value — never a torn write, which
    /// `load` would otherwise reject as corruption.
    ///
    /// # Errors
    /// Returns an error when the temp file cannot be written, synced, or
    /// renamed into place.
    pub fn persist(&self, value: &V) -> Result<(), CursorError> {
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
            f.write_all(format!("{value}\n").as_bytes()).map_err(io)?;
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

    type SeqCursor = CursorFile<u64>;

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
        let c = SeqCursor::open(temp_path("missing")).unwrap();
        let _ = std::fs::remove_file(c.path());
        assert!(matches!(c.load(), Ok(None)));
    }

    #[test]
    fn roundtrip_and_overwrite() {
        let c = SeqCursor::open(temp_path("roundtrip")).unwrap();
        c.persist(&7).unwrap();
        assert_eq!(c.load().unwrap(), Some(7));
        c.persist(&12_345).unwrap();
        assert_eq!(c.load().unwrap(), Some(12_345));
        // The temp file must not linger after a successful rename.
        let tmp = c
            .path()
            .with_extension(format!("tmp.{}", std::process::id()));
        assert!(!tmp.exists(), "temp file left behind at {}", tmp.display());
    }

    #[test]
    fn corrupt_file_is_a_hard_error_never_silent_zero() {
        let c = SeqCursor::open(temp_path("corrupt")).unwrap();
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
        let c = SeqCursor::open(temp_path("whitespace")).unwrap();
        std::fs::write(c.path(), " 42\n\n").unwrap();
        assert_eq!(c.load().unwrap(), Some(42));
    }

    #[test]
    fn a_second_watcher_on_one_cursor_file_is_refused() {
        let path = temp_path("locked");
        {
            let _first = SeqCursor::open(&path).unwrap();
            let err = SeqCursor::open(&path).unwrap_err();
            assert!(matches!(err, CursorError::Locked { .. }), "got {err:?}");
            assert!(
                lock_path_of(&path).exists(),
                "lock file is a sibling of the cursor"
            );
            // `_first` is still held here; a second open still fails.
        }
        // The block above released the lock, so a restart can take it again.
        SeqCursor::open(&path).unwrap();
    }

    #[test]
    fn lock_file_name_keeps_the_full_cursor_name() {
        assert_eq!(
            lock_path_of(Path::new("/var/lib/kardamom/pair.cursor")),
            PathBuf::from("/var/lib/kardamom/pair.cursor.lock")
        );
    }
}
