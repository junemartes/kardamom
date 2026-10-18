//! The markers of the executor process in its state directory. A park of
//! the state DB leaves them in place (`kardamom_state::MARKER_PREFIX`).
//! Only an executor that reads the member feed (`--carry-bytes`) keeps
//! them: only that executor compares the stream with its history.
//!
//! - The rollback marker holds the rollback record that a run must publish
//!   before its first receipt. The process writes it, with a disk sync,
//!   when the stream differs from the committed history or from the
//!   receipts that a run published above it. The commit thread removes it
//!   only after the publish of the record succeeds. So a crash, a
//!   shutdown, or a run that fails before the publish leaves it for the
//!   next start.
//! - The running marker holds the boot id of the host while a run is live.
//!   The run writes it at its start and removes it at its end, after a
//!   disk sync of the receipt ledger (see [`crate::ledger`]). A start that
//!   finds it with another boot id follows a restart of the host during a
//!   run: the ledger can be short.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use alloy_primitives::{B256, keccak256};
use anyhow::{Context, Result};
use kardamom_state::MARKER_PREFIX;
use kardamom_types::ReceiptRollback;

/// The file that holds the boot id of the host.
const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";

/// A divergence of the stream: the receipts above `kept` are void.
/// `stream` is the fingerprint of the first frame of the next block in the
/// stream, so every executor that finds one divergence names it with one
/// identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Void {
    pub(crate) kept: u64,
    pub(crate) stream: B256,
}

impl Void {
    /// The rollback record of this divergence.
    pub(crate) fn record(self) -> ReceiptRollback {
        let id = keccak256(
            [
                b"diverged".as_slice(),
                &self.kept.to_be_bytes(),
                self.stream.as_slice(),
            ]
            .concat(),
        );
        ReceiptRollback {
            to_block: self.kept,
            id,
        }
    }
}

/// The marker files of one state directory.
#[derive(Debug, Clone)]
pub(crate) struct Markers {
    dir: PathBuf,
}

impl Markers {
    pub(crate) fn new(state_dir: &Path) -> Self {
        Self {
            dir: state_dir.to_path_buf(),
        }
    }

    fn rollback_path(&self) -> PathBuf {
        self.dir.join(format!("{MARKER_PREFIX}rollback"))
    }

    fn running_path(&self) -> PathBuf {
        self.dir.join(format!("{MARKER_PREFIX}running"))
    }

    /// The path of the receipt ledger. Its name has the marker prefix, so
    /// a park leaves it in place.
    pub(crate) fn ledger_path(&self) -> PathBuf {
        self.dir.join(format!("{MARKER_PREFIX}receipts"))
    }

    /// The rollback record that the next run must publish, if any.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the marker exists but does not read or parse.
    pub(crate) fn pending(&self) -> Result<Option<ReceiptRollback>> {
        let path = self.rollback_path();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        Self::parse(&text)
            .map(Some)
            .with_context(|| format!("parse {}", path.display()))
    }

    /// Parse the text `<to_block> <id>` of the rollback marker.
    fn parse(text: &str) -> Result<ReceiptRollback> {
        let mut fields = text.split_whitespace();
        let to_block = fields.next().context("no block")?.parse()?;
        let id = B256::from_str(fields.next().context("no id")?)?;
        Ok(ReceiptRollback { to_block, id })
    }

    /// The text of the rollback marker.
    fn text(rollback: ReceiptRollback) -> String {
        format!("{} {:#x}\n", rollback.to_block, rollback.id)
    }

    /// Record that the next run must publish `rollback`. A pending record
    /// with a lower block stays: it voids more receipts. A pending record
    /// with the same block gives way, so the marker holds the identity of
    /// the latest divergence.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the marker does not read, or the write or the
    /// disk sync fails.
    pub(crate) fn raise(&self, rollback: ReceiptRollback) -> Result<()> {
        if self
            .pending()?
            .is_some_and(|pending| pending.to_block < rollback.to_block)
        {
            return Ok(());
        }
        self.write(&self.rollback_path(), Self::text(rollback).as_bytes())?;
        tracing::warn!(
            to_block = rollback.to_block,
            id = %rollback.id,
            "rollback marker written: a run publishes the record before its first receipt"
        );
        Ok(())
    }

    /// Remove the rollback marker after the publish of `published`. A
    /// marker that holds another record stays.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the marker does not read, or the removal fails.
    pub(crate) fn clear(&self, published: ReceiptRollback) -> Result<()> {
        if self.pending()? != Some(published) {
            return Ok(());
        }
        std::fs::remove_file(self.rollback_path()).context("remove the rollback marker")?;
        self.sync_dir()
    }

    /// Record that a run starts, and tell whether the receipt ledger of the
    /// runs before is whole.
    ///
    /// - No running marker: the last run synced the ledger at its end, or
    ///   no run of this release ran in the state directory.
    /// - A running marker with the boot id of this boot: the process of the
    ///   last run crashed or was killed. The page cache keeps the writes of
    ///   the ledger.
    /// - A running marker with another boot id: the host restarted during
    ///   the last run, and the writes of the ledger can be gone.
    /// - A running marker, and no boot id: the start cannot tell, so it
    ///   counts as a restart of the host.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the running marker exists but does not read, or
    /// its write fails.
    pub(crate) fn start_run(&self) -> Result<bool> {
        let boot = Self::boot_id();
        let whole = match std::fs::read_to_string(self.running_path()) {
            Ok(last) => boot.as_deref() == Some(last.as_str()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(e).context("read the running marker"),
        };
        if !whole {
            tracing::warn!(
                "the host restarted during the last run: every receipt it published above the committed block can be void"
            );
        }
        self.write(&self.running_path(), boot.unwrap_or_default().as_bytes())?;
        Ok(whole)
    }

    /// The boot id of the host. In a sandbox runtime, such as gVisor or
    /// Kata Containers, it is the boot id of the sandbox: a restart of the
    /// sandbox also drops its page cache, so the rule of
    /// [`Self::start_run`] holds. `None`, with a warning, when the file
    /// does not read.
    fn boot_id() -> Option<String> {
        std::fs::read_to_string(BOOT_ID)
            .inspect_err(|e| {
                tracing::warn!(
                    error = %e,
                    "the boot id of the host does not read: a start after a run that did not end counts as a restart of the host"
                );
            })
            .ok()
    }

    /// Record that a run ended, after the disk sync of the ledger.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the removal or the disk sync fails.
    pub(crate) fn end_run(&self) -> Result<()> {
        match std::fs::remove_file(self.running_path()) {
            Ok(()) => self.sync_dir(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context("remove the running marker"),
        }
    }

    /// Write `bytes` to `path` with a disk sync: a temporary file, a sync,
    /// a rename, and a sync of the directory.
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        std::fs::create_dir_all(&self.dir).context("create the state dir")?;
        let tmp = path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp).context("create a marker")?;
        file.write_all(bytes).context("write a marker")?;
        file.sync_all().context("sync a marker")?;
        std::fs::rename(&tmp, path).context("rename a marker")?;
        self.sync_dir()
    }

    fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .context("sync the state dir")
    }
}

#[cfg(test)]
#[path = "marker_tests.rs"]
mod tests;
