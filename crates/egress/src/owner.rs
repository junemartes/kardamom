//! [`Owner`]: the one thread that owns the merge and the store.
//!
//! Everything that touches the stream comes to this thread on one channel:
//! the messages and the receipts of the executors, the range reads of the
//! readers that resume, and the receipt reads. So the merges, the store and
//! the live publications need no lock, and a read sees the state as the
//! thread left it after its last input.
//!
//! The thread appends each delivered frame to the store, then publishes it
//! live. It forces the store to disk on a fixed period, and deletes the
//! segments below the retention after each sync.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tokio::sync::oneshot;

use alloy_primitives::B256;
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::Receipt;

use crate::merge::Merge;
use crate::receipts::Receipts;
use crate::store::{Range, Store, StoreError};

/// Where the delivered frames go live.
pub trait FramePublisher: Send + 'static {
    fn publish(&self, frame: &[u8]);
}

/// One input of the owner thread.
pub enum Input {
    /// One `[seq:u64][frame]` message of the executor copy `session`.
    Frame { session: i32, message: Vec<u8> },
    /// A range read of a reader that resumes.
    Read(ReadRequest),
    /// One receipt frame of an executor.
    Receipts(ReceiptFrame),
    /// A receipt read: the receipt of a hash, and the block of the newest
    /// receipt that the egress holds.
    Receipt {
        tx_hash: B256,
        reply: oneshot::Sender<(Option<Receipt>, u64)>,
    },
}

/// A range read: the frames after the cursor, up to about `limit` bytes.
pub struct ReadRequest {
    pub from_index: u64,
    pub from_block: u64,
    pub limit: usize,
    pub reply: oneshot::Sender<Result<Range, StoreError>>,
}

/// The settings of the owner thread.
#[derive(Clone, Copy, Debug)]
pub struct OwnerConfig {
    /// The period of the sync of the store.
    pub sync_every: Duration,
    /// The blocks the store keeps below its head.
    pub keep_blocks: NonZeroU64,
}

/// What the owner thread owns.
pub struct Parts<P: FramePublisher> {
    pub merge: Merge,
    pub store: Store,
    pub receipts: Receipts,
    /// The live publication of the merged stream.
    pub live: P,
    /// The live publication of the merged receipts.
    pub receipts_live: P,
}

/// The owner of the merges, the store and the live publications.
pub struct Owner<P: FramePublisher> {
    cfg: OwnerConfig,
    parts: Parts<P>,
    delivered: Vec<Vec<u8>>,
    synced_at: Instant,
}

impl<P: FramePublisher> Owner<P> {
    #[must_use]
    pub fn new(cfg: OwnerConfig, parts: Parts<P>) -> Self {
        Self {
            cfg,
            parts,
            delivered: Vec::new(),
            synced_at: Instant::now(),
        }
    }

    /// Run until every sender of `inputs` is gone.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails: the egress then stops, and
    /// its readers use another instance.
    pub fn run(mut self, inputs: &Receiver<Input>) -> Result<(), StoreError> {
        loop {
            if let ControlFlow::Break(end) = self.step(inputs) {
                return end;
            }
        }
    }

    /// Take the next input, or the sync period. `Break` carries the end of
    /// the run: every sender is gone, or the store failed.
    fn step(&mut self, inputs: &Receiver<Input>) -> ControlFlow<Result<(), StoreError>> {
        let taken = match inputs.recv_timeout(self.cfg.sync_every) {
            Ok(input) => self.on(input),
            Err(RecvTimeoutError::Timeout) => Ok(()),
            Err(RecvTimeoutError::Disconnected) => {
                return ControlFlow::Break(self.parts.store.sync());
            }
        };
        match taken.and_then(|()| self.sync_if_due()) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(Err(e)),
        }
    }

    /// Take one input.
    ///
    /// # Errors
    ///
    /// Returns an error when the store fails to append.
    pub fn on(&mut self, input: Input) -> Result<(), StoreError> {
        match input {
            Input::Frame { session, message } => self.take(session, &message),
            Input::Read(read) => {
                let range =
                    self.parts
                        .store
                        .read_from(read.from_index, read.from_block, read.limit);
                // A reader that left takes no answer.
                let _ = read.reply.send(range);
                Ok(())
            }
            Input::Receipts(frame) => {
                self.take_receipts(frame);
                Ok(())
            }
            Input::Receipt { tx_hash, reply } => {
                let receipts = &self.parts.receipts;
                let _ = reply.send((receipts.receipt(&tx_hash).cloned(), receipts.block()));
                Ok(())
            }
        }
    }

    fn take(&mut self, session: i32, message: &[u8]) -> Result<(), StoreError> {
        self.parts.merge.take(session, message, &mut self.delivered);
        self.parts.store.append(&self.delivered)?;
        self.delivered
            .drain(..)
            .for_each(|frame| self.parts.live.publish(&frame));
        Ok(())
    }

    /// Merge one receipt frame, and publish what is new in it.
    fn take_receipts(&mut self, frame: ReceiptFrame) {
        let Some(frame) = self.parts.receipts.take(frame) else {
            return;
        };
        let encoded = match &frame {
            ReceiptFrame::Batch(batch) => kardamom_log::codec::encode(batch),
            ReceiptFrame::Rollback(rollback) => kardamom_log::codec::encode(rollback),
        };
        match encoded {
            Ok(bytes) => self.parts.receipts_live.publish(&bytes),
            Err(e) => tracing::error!(error = %e, "egress: a receipt frame did not encode"),
        }
    }

    fn sync_if_due(&mut self) -> Result<(), StoreError> {
        if self.synced_at.elapsed() < self.cfg.sync_every {
            return Ok(());
        }
        self.parts.store.sync()?;
        self.synced_at = Instant::now();
        let floor = self
            .parts
            .store
            .head()
            .next_block
            .saturating_sub(self.cfg.keep_blocks.get());
        self.parts.store.prune_below(floor)
    }
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
