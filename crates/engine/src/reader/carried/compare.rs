//! [`Compare`]: the check of the canonical stream against the committed
//! history of this consumer, before the reader delivers a frame.
//!
//! A member can lose the unsynced tail of its log, for example after a
//! power loss on a majority of the members. The cluster then continues
//! from a shorter log, and the next entries take the lost indexes. A
//! paired executor can have committed entries of the lost tail. So at each
//! start, and at each new connection, the reader replays the source from a
//! floor below its cursor. It compares each frame below the cursor with
//! its committed history: the transaction hash of each receipt by index,
//! and the end, the timestamp and the L1 origin of each header. The first
//! difference is the divergence point. With no difference, delivery starts
//! at the cursor.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;

use alloy_primitives::B256;
use kardamom_state::{StateError, StateSnapshot};
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

use crate::ExecutorError;
use crate::reader::Ordered;
use crate::reader::cluster::{BoundaryMetrics, Reorder, ReplayCursor};

/// The first block that the canonical stream carries. Block 0 is genesis.
const FIRST_BLOCK: u64 = 1;

/// The committed history that the comparison reads: the headers and the
/// receipt hashes from the floor up to the delivery cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalHistory {
    /// The first point of the comparison: a canonical index and the block
    /// that starts at it.
    floor: (u64, u64),
    /// The delivery cursor: the end of the committed history.
    cursor: (u64, u64),
    /// The header of each block from the floor to the last committed
    /// block, by block number.
    headers: BTreeMap<u64, BlockBoundaryStart>,
    /// The transaction hash of each receipt from the floor to the cursor,
    /// by canonical index. A skipped transaction has no receipt.
    receipts: HashMap<u64, B256>,
}

impl LocalHistory {
    /// Read the history of the last `window` committed blocks of
    /// `snapshot`. `cursor` is the delivery cursor of the snapshot: the
    /// next canonical index and the next block. Returns `None` at genesis:
    /// there is nothing to compare.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a read fails, or if a header in the window
    /// is absent.
    pub fn read(
        snapshot: &StateSnapshot,
        cursor: (u64, u64),
        window: NonZeroU64,
    ) -> Result<Option<Self>, StateError> {
        let Some(last) = cursor.1.checked_sub(1).filter(|b| *b >= FIRST_BLOCK) else {
            return Ok(None);
        };
        // A window longer than the chain starts at the first block.
        let floor_block = cursor.1.saturating_sub(window.get()).max(FIRST_BLOCK);
        let headers = (floor_block..=last)
            .map(|block| Self::header_of(snapshot, block))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let floor_index = match floor_block.checked_sub(1).filter(|b| *b >= FIRST_BLOCK) {
            Some(before) => Self::header_of(snapshot, before)?.1.end_tx_idx.as_index(),
            None => 0,
        };
        let receipts = snapshot
            .receipt_hashes(
                BPosition::from_index(floor_index),
                BPosition::from_index(cursor.0),
            )?
            .into_iter()
            .map(|(pos, hash)| (pos.as_index(), hash))
            .collect();
        Ok(Some(Self {
            floor: (floor_index, floor_block),
            cursor,
            headers,
            receipts,
        }))
    }

    /// The committed header of `block`, as the boundary that sealed it.
    fn header_of(
        snapshot: &StateSnapshot,
        block: u64,
    ) -> Result<(u64, BlockBoundaryStart), StateError> {
        let header = snapshot.header(block)?.ok_or_else(|| {
            StateError::Recovery(format!("block {block} has no committed header"))
        })?;
        Ok((
            block,
            BlockBoundaryStart {
                block_number: block,
                end_tx_idx: header.end_tx_idx,
                l2_timestamp: header.l2_timestamp,
                l1_origin: header.l1_origin,
            },
        ))
    }

    /// A history from its parts, for a test.
    #[cfg(test)]
    pub(crate) fn of(
        floor: (u64, u64),
        cursor: (u64, u64),
        headers: impl IntoIterator<Item = BlockBoundaryStart>,
        receipts: impl IntoIterator<Item = (u64, B256)>,
    ) -> Self {
        Self {
            floor,
            cursor,
            headers: headers.into_iter().map(|h| (h.block_number, h)).collect(),
            receipts: receipts.into_iter().collect(),
        }
    }

    /// The delivery cursor: the end of the committed history.
    pub(crate) fn cursor(&self) -> (u64, u64) {
        self.cursor
    }
}

/// The first difference between the stream and the committed history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Divergence {
    pub(crate) block: u64,
    pub(crate) index: u64,
}

impl From<Divergence> for ExecutorError {
    fn from(d: Divergence) -> Self {
        Self::StreamDiverged {
            block: d.block,
            index: d.index,
        }
    }
}

/// Where the comparison stands after the frames that it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// It needs more frames.
    Pending,
    /// The stream equals the history up to the delivery cursor.
    Equal,
    /// The source holds no frame of the window that is left: its floor is
    /// at or above the delivery cursor. Nothing is compared.
    Skipped,
    Diverged(Divergence),
}

/// The comparison of one run of the reader.
pub(crate) struct Compare {
    history: LocalHistory,
    /// The cursor and the pending set of the compared frames. It starts at
    /// the floor, and it delivers nothing downstream.
    reorder: Reorder,
}

impl Compare {
    pub(crate) fn new(history: LocalHistory) -> Self {
        let reorder = Self::reorder_at(history.floor);
        Self { history, reorder }
    }

    fn reorder_at((index, block): (u64, u64)) -> Reorder {
        let mut reorder = Reorder::new(ReplayCursor::new(index, block));
        reorder.metrics = BoundaryMetrics::Off;
        reorder.restart();
        reorder
    }

    /// The next frame that the comparison needs.
    pub(crate) fn cursor(&self) -> (u64, u64) {
        self.reorder.next()
    }

    /// The pending set of the compared frames.
    pub(crate) fn reorder(&mut self) -> &mut Reorder {
        &mut self.reorder
    }

    /// Start again from the floor. A new connection can serve another
    /// stream, so the frames compared on the old one prove nothing.
    pub(crate) fn rewind(&mut self) {
        self.reorder = Self::reorder_at(self.history.floor);
    }

    /// The source refused the cursor with its retained floor.
    ///
    /// - A cursor at or above the retained floor on both axes is a cursor
    ///   that names no point of the stream of the source: the blocks before
    ///   it differ. The divergence is at the block before the cursor.
    /// - A cursor below the retained floor moves up to the first committed
    ///   block that starts at or above the floor.
    pub(crate) fn refused(&mut self, oldest_index: u64, oldest_block: u64) -> Progress {
        let (index, block) = self.cursor();
        if oldest_index <= index && oldest_block <= block {
            // Block 0 is genesis, which no stream changes.
            return Progress::Diverged(Divergence {
                block: block.saturating_sub(1),
                index,
            });
        }
        let raised = self
            .history
            .headers
            .range(block..)
            .find(|(b, h)| {
                b.saturating_add(1) >= oldest_block && h.end_tx_idx.as_index() >= oldest_index
            })
            .map(|(b, h)| (h.end_tx_idx.as_index(), b.saturating_add(1)))
            .filter(|cursor| cursor.1 < self.history.cursor.1);
        let Some(raised) = raised else {
            return Progress::Skipped;
        };
        self.history.floor = raised;
        self.rewind();
        Progress::Pending
    }

    /// Compare every frame that the pending set can deliver.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the pending set fails: a cursor overflow, or a
    /// boundary that seals below the cursor.
    pub(crate) fn drain(&mut self) -> Result<Progress, ExecutorError> {
        loop {
            let Some(progress) = self.step()? else {
                continue;
            };
            return Ok(progress);
        }
    }

    /// Compare one frame. `None` when the frame is equal and the cursor is
    /// not reached yet.
    fn step(&mut self) -> Result<Option<Progress>, ExecutorError> {
        if self.reorder.next() == self.history.cursor {
            return Ok(Some(Progress::Equal));
        }
        let Some((pos, item)) = self.reorder.try_deliver()? else {
            return Ok(Some(Progress::Pending));
        };
        Ok(self.check(pos, &item).map(Progress::Diverged))
    }

    /// The divergence at one delivered frame, if any. A record compares its
    /// hash with the receipt at its index. A boundary compares itself with
    /// the header of its block. Epochs and deposits have no hash to compare
    /// here: the L1 origin of the header covers them.
    fn check(&self, pos: BPosition, item: &Ordered) -> Option<Divergence> {
        match item {
            Ordered::Tx(tx) => {
                let index = pos.as_index();
                let local = self.history.receipts.get(&index)?;
                (*local != tx.canonical_id).then(|| Divergence {
                    block: self.reorder.next().1,
                    index,
                })
            }
            Ordered::Message(TxOrderingMessage::BoundaryStart(b)) => {
                (self.history.headers.get(&b.block_number) != Some(b)).then_some(Divergence {
                    block: b.block_number,
                    index: b.end_tx_idx.as_index(),
                })
            }
            Ordered::Message(_) => None,
        }
    }
}
