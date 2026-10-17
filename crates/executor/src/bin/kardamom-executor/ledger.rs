//! The ledger of the receipts that the executor published above its
//! committed block, in its state directory.
//!
//! The commit thread publishes the receipts of a block before the state
//! writer commits the block. The stream can lose such a block, for example
//! after a power loss on a majority of the members. So the commit thread
//! writes each receipt here before it publishes it, and the comparison at
//! the next start reads the stream against the receipts above the
//! committed block (`PublishedTail`). A difference makes them void.
//!
//! - The file is a ring of fixed slots, one for each canonical index
//!   modulo [`SLOTS`]. A slot is 48 bytes: the canonical index and the
//!   block, as little-endian `u64`, then the transaction hash. Block 0
//!   marks an empty slot: genesis has no receipt.
//! - A write goes to the page cache with no disk sync, so it survives a
//!   crash or a kill of the process. The process syncs the file at the end
//!   of a run. The running marker tells the next start whether a restart
//!   of the host can have dropped writes (see `Markers::start_run`).

use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use alloy_primitives::B256;
use anyhow::{Context, Result};
use kardamom_engine::reader::carried::PublishedTail;
use kardamom_types::{Receipt, ReceiptRollback, ReceiptRows};

use crate::marker::Markers;

/// The slots of the ring. The receipts above the committed block must fit:
/// the state writer commits a block a few blocks after its receipts.
const SLOTS: u64 = 1 << 16;
/// The bytes of one slot.
const SLOT_LEN: usize = 48;
/// The bytes of the ledger file.
const LEDGER_BYTES: u64 = SLOTS * SLOT_LEN as u64;

/// One receipt in the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    index: u64,
    block: u64,
    hash: B256,
}

impl Slot {
    fn of(receipt: &Receipt) -> Self {
        Self {
            index: receipt.tx_idx.as_index(),
            block: receipt.block_number,
            hash: receipt.tx_hash,
        }
    }

    /// The offset of the slot of `index` in the file. The slot number is
    /// below [`SLOTS`], so the offset is below [`LEDGER_BYTES`].
    fn offset(index: u64) -> u64 {
        index % SLOTS * SLOT_LEN as u64
    }

    fn encode(self) -> [u8; SLOT_LEN] {
        let mut bytes = [0u8; SLOT_LEN];
        bytes[..8].copy_from_slice(&self.index.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.block.to_le_bytes());
        bytes[16..].copy_from_slice(self.hash.as_slice());
        bytes
    }

    /// The receipt in one slot. `None` for an empty slot.
    fn decode(bytes: &[u8; SLOT_LEN]) -> Option<Self> {
        let (index, rest) = bytes.split_first_chunk::<8>()?;
        let (block, hash) = rest.split_first_chunk::<8>()?;
        let slot = Self {
            index: u64::from_le_bytes(*index),
            block: u64::from_le_bytes(*block),
            hash: B256::try_from(hash).ok()?,
        };
        (slot.block != 0).then_some(slot)
    }
}

/// The receipt ledger of one state directory, and the markers that the
/// commit thread clears.
#[derive(Debug, Clone)]
pub(crate) struct ReceiptLedger {
    file: Arc<File>,
    pub(crate) markers: Markers,
}

impl ReceiptLedger {
    /// Open the ledger, and create it when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the file does not open or does not take its
    /// length.
    pub(crate) fn open(markers: Markers) -> Result<Self> {
        let path = markers.ledger_path();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        file.set_len(LEDGER_BYTES)
            .context("size the receipt ledger")?;
        Ok(Self {
            file: Arc::new(file),
            markers,
        })
    }

    /// Write the receipts of one batch, before their publish. A write that
    /// fails stops the publish: a published receipt must be in the ledger.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a write fails.
    pub(crate) fn record(&self, items: &[ReceiptRows]) -> std::io::Result<()> {
        items.iter().try_for_each(|item| {
            let slot = Slot::of(&item.receipt);
            self.file
                .write_all_at(&slot.encode(), Slot::offset(slot.index))
        })
    }

    /// Every receipt in the ledger.
    fn slots(&self) -> Result<Vec<Slot>> {
        let mut bytes = vec![0u8; usize::try_from(LEDGER_BYTES)?];
        self.file
            .read_exact_at(&mut bytes, 0)
            .context("read the receipt ledger")?;
        Ok(bytes
            .as_chunks::<SLOT_LEN>()
            .0
            .iter()
            .filter_map(Slot::decode)
            .collect())
    }

    /// The receipts published at and above `cursor_index`. When `whole` is
    /// false, the writes of the ledger can be gone, and the tail is
    /// unknown. It is unknown too when the receipts span more than the
    /// ring: a later write can have taken the slot of an earlier one.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the ledger does not read.
    pub(crate) fn tail(&self, cursor_index: u64, whole: bool) -> Result<PublishedTail> {
        if !whole {
            return Ok(PublishedTail::Unknown);
        }
        let published: BTreeMap<u64, (u64, B256)> = self
            .slots()?
            .into_iter()
            .filter(|slot| slot.index >= cursor_index)
            .map(|slot| (slot.index, (slot.block, slot.hash)))
            .collect();
        let spans = published
            .last_key_value()
            .and_then(|(last, _)| last.checked_sub(cursor_index))
            .is_some_and(|span| span >= SLOTS);
        if spans {
            tracing::warn!(
                cursor_index,
                "the published receipts span more than the receipt ledger; every receipt above the committed block can be void"
            );
            return Ok(PublishedTail::Unknown);
        }
        Ok(PublishedTail::Known(published))
    }

    /// Remove the receipts of `from_block` and above, with a disk sync: a
    /// rollback record voids them, and the next run publishes them again.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the ledger does not read, write or sync.
    pub(crate) fn void(&self, from_block: u64) -> Result<()> {
        self.slots()?
            .into_iter()
            .filter(|slot| slot.block >= from_block)
            .try_for_each(|slot| {
                self.file
                    .write_all_at(&[0u8; SLOT_LEN], Slot::offset(slot.index))
            })
            .context("void the receipt ledger")?;
        self.file.sync_data().context("sync the receipt ledger")
    }

    /// End the run: sync the ledger, then remove the running marker. The
    /// next start then trusts the ledger.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the sync or the removal fails.
    pub(crate) fn end_run(&self) -> Result<()> {
        self.file.sync_data().context("sync the receipt ledger")?;
        self.markers.end_run()
    }

    /// The rollback record went out: the next run need not publish it.
    pub(crate) fn rollback_published(&self, rollback: ReceiptRollback) {
        if let Err(e) = self.markers.clear(rollback) {
            tracing::warn!(error = %e, "the rollback marker stays; the next run publishes it again");
        }
    }
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod tests;
