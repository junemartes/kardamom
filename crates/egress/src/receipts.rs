//! [`Receipts`]: the receipts of every executor as one stream, and an index
//! of the newest receipts by hash.
//!
//! Every executor publishes the receipts of the canonical order, in batches
//! that each executor can cut differently. The merge takes a batch when it
//! reaches past the last receipt that it took, and passes on only the
//! receipts past that point, with the account rows of the batch. The rows
//! are absolute values at the end of their batch, and a reader keeps the
//! row of the higher position, so the rows of a trimmed batch stay right.
//!
//! A rollback of an executor voids the receipts above its block. Each
//! executor that rolls back sends the same rollback, with one id, so the
//! merge passes on each id once. It then forgets the receipts above the
//! block, and takes the receipts of the new history from the end of the
//! block.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;

use alloy_primitives::B256;
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::{BPosition, Receipt, ReceiptBatch, ReceiptRollback};

/// The merged receipts and the index.
pub struct Receipts {
    capacity: NonZeroUsize,
    /// The position of the last receipt taken.
    last: Option<BPosition>,
    /// The newest receipts, oldest first: their hash, block and position.
    order: VecDeque<(B256, u64, BPosition)>,
    by_hash: HashMap<B256, Receipt>,
    /// The id of the last rollback passed on.
    rolled_back: Option<B256>,
}

impl Receipts {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            last: None,
            order: VecDeque::new(),
            by_hash: HashMap::new(),
            rolled_back: None,
        }
    }

    /// Take one frame of an executor. Returns the frame to pass on: the new
    /// part of a batch, or a rollback that no copy passed on yet.
    pub fn take(&mut self, frame: ReceiptFrame) -> Option<ReceiptFrame> {
        match frame {
            ReceiptFrame::Batch(batch) => self.batch(batch).map(ReceiptFrame::Batch),
            ReceiptFrame::Rollback(rollback) => self.rollback(rollback).map(ReceiptFrame::Rollback),
        }
    }

    /// The receipt of `tx_hash`, while the index keeps it.
    #[must_use]
    pub fn receipt(&self, tx_hash: &B256) -> Option<&Receipt> {
        self.by_hash.get(tx_hash)
    }

    /// The block of the last receipt taken, 0 before the first.
    #[must_use]
    pub fn block(&self) -> u64 {
        self.order.back().map_or(0, |(_, block, _)| *block)
    }

    fn batch(&mut self, mut batch: ReceiptBatch) -> Option<ReceiptBatch> {
        let last = self.last;
        batch
            .receipts
            .retain(|r| last.is_none_or(|last| r.tx_idx > last));
        let newest = batch.receipts.last()?.tx_idx;
        self.last = Some(newest);
        batch.receipts.iter().for_each(|r| self.index(r));
        Some(batch)
    }

    fn index(&mut self, receipt: &Receipt) {
        self.order
            .push_back((receipt.tx_hash, receipt.block_number, receipt.tx_idx));
        self.by_hash.insert(receipt.tx_hash, receipt.clone());
        if self.order.len() > self.capacity.get()
            && let Some((old, _, _)) = self.order.pop_front()
        {
            self.by_hash.remove(&old);
        }
    }

    /// Pass a rollback on once, forget the receipts above its block, and
    /// take receipts again from the last one at or below it.
    fn rollback(&mut self, rollback: ReceiptRollback) -> Option<ReceiptRollback> {
        if self.rolled_back == Some(rollback.id) {
            return None;
        }
        self.rolled_back = Some(rollback.id);
        let keep = self
            .order
            .iter()
            .rposition(|(_, block, _)| *block <= rollback.to_block)
            .map_or(0, |at| at + 1);
        self.order.drain(keep..).for_each(|(hash, _, _)| {
            self.by_hash.remove(&hash);
        });
        self.last = self.order.back().map(|(_, _, tx_idx)| *tx_idx);
        Some(rollback)
    }
}

#[cfg(test)]
#[path = "receipts_tests.rs"]
mod tests;
