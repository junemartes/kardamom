//! The markers of the executor process in its state directory. A park of
//! the state DB leaves them in place (`kardamom_state::MARKER_PREFIX`).
//!
//! - The rollback marker holds the rollback record that a run must publish
//!   before its first receipt. The process writes it, with a disk sync,
//!   before it parks a diverged state DB, and when a run published
//!   receipts above its last committed block. The commit thread removes it
//!   only after the publish of the record succeeds. So a crash, a shutdown,
//!   or a run that fails before the publish leaves it for the next start.
//! - The clean marker exists when the last run of the process ended
//!   through its end-of-run bookkeeping. A start without it follows a crash:
//!   that run could have published receipts above its last committed
//!   block, so the start publishes a rollback to its last committed block.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use alloy_primitives::{B256, keccak256};
use anyhow::{Context, Result};
use kardamom_state::MARKER_PREFIX;
use kardamom_types::ReceiptRollback;

/// Why a run publishes a rollback record. The cause gives the identity of
/// the event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cause {
    /// The canonical stream differs from the committed history. `stream`
    /// is the fingerprint of the stream at the divergence, so every
    /// executor that finds one divergence names it with one identity.
    Diverged { stream: B256 },
    /// A run of this executor published receipts above its last committed
    /// block and ended, or crashed. Only this executor knows the event, so
    /// the identity holds the executor and the time.
    Restart { recorder: u32 },
}

impl Cause {
    /// The rollback record of this cause for the blocks above `to_block`.
    pub(crate) fn record(self, to_block: u64) -> ReceiptRollback {
        let id = match self {
            Self::Diverged { stream } => keccak256(
                [
                    b"diverged".as_slice(),
                    &to_block.to_be_bytes(),
                    stream.as_slice(),
                ]
                .concat(),
            ),
            Self::Restart { recorder } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_nanos());
                keccak256(
                    [
                        b"restart".as_slice(),
                        &recorder.to_be_bytes(),
                        &to_block.to_be_bytes(),
                        &now.to_be_bytes(),
                    ]
                    .concat(),
                )
            }
        };
        ReceiptRollback { to_block, id }
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

    fn clean_path(&self) -> PathBuf {
        self.dir.join(format!("{MARKER_PREFIX}clean"))
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

    /// Record that the next run must publish `rollback`. A pending record
    /// with a lower or equal block stays: it voids at least the same
    /// receipts.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the marker does not read, or the write or the
    /// disk sync fails.
    pub(crate) fn raise(&self, rollback: ReceiptRollback) -> Result<()> {
        if self
            .pending()?
            .is_some_and(|pending| pending.to_block <= rollback.to_block)
        {
            return Ok(());
        }
        let text = format!("{} {:#x}\n", rollback.to_block, rollback.id);
        self.write(&self.rollback_path(), text.as_bytes())?;
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

    /// The record that a run publishes before its first receipt. The first
    /// run of a process that finds no clean marker follows a crash: that
    /// run could have published receipts above `committed`, so a record of
    /// `cause` covers them.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a marker does not read or write.
    pub(crate) fn open(
        &self,
        first: bool,
        committed: u64,
        cause: Cause,
    ) -> Result<Option<ReceiptRollback>> {
        if first && !self.take_clean()? {
            tracing::warn!(
                committed,
                "no clean end of the last run; its receipts above the committed block can be void"
            );
            self.raise(cause.record(committed))?;
        }
        self.pending()
    }

    /// Whether the last run ended through its bookkeeping. The marker is
    /// gone after the call: a crash of this run must not find it.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the removal fails.
    pub(crate) fn take_clean(&self) -> Result<bool> {
        match std::fs::remove_file(self.clean_path()) {
            Ok(()) => self.sync_dir().map(|()| true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).context("remove the clean marker"),
        }
    }

    /// Record that this run ended through its bookkeeping.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the write or the disk sync fails.
    pub(crate) fn mark_clean(&self) -> Result<()> {
        self.write(&self.clean_path(), b"")
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

/// What the receipt publication of one run tells the process: the highest
/// block of a published receipt, and the rollback marker that it clears
/// after the publish of the record. The commit thread writes it, and the
/// process reads it after the run.
#[derive(Debug, Clone)]
pub(crate) struct RunLedger {
    published: std::sync::Arc<std::sync::atomic::AtomicU64>,
    markers: Markers,
}

impl RunLedger {
    pub(crate) fn new(markers: Markers) -> Self {
        Self {
            published: std::sync::Arc::default(),
            markers,
        }
    }

    /// The highest block of a receipt that this run published, 0 for none.
    pub(crate) fn published_through(&self) -> u64 {
        self.published.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Note the receipts of one published batch.
    pub(crate) fn published(&self, items: &[kardamom_types::ReceiptRows]) {
        let highest = items
            .iter()
            .map(|item| item.receipt.block_number)
            .max()
            .unwrap_or(0);
        self.published
            .fetch_max(highest, std::sync::atomic::Ordering::AcqRel);
    }

    /// After the run: receipts published above `committed`, for example of
    /// the open block, can be gone from the chain, so the next run
    /// publishes a record of `cause` to `committed`.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the marker does not write.
    pub(crate) fn settle(&self, committed: u64, cause: Cause) -> Result<()> {
        if self.published_through() <= committed {
            return Ok(());
        }
        self.markers.raise(cause.record(committed))
    }

    /// The rollback record went out: the next run need not publish it.
    pub(crate) fn rollback_published(&self, rollback: ReceiptRollback) {
        if let Err(e) = self.markers.clear(rollback) {
            tracing::warn!(error = %e, "the rollback marker stays; the next run publishes it again");
        }
    }
}

#[cfg(test)]
#[path = "marker_tests.rs"]
mod tests;
