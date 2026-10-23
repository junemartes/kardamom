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
//! segments below the retention after each sync. After each sync it also
//! sets the distance of the stored head from the newest record that any
//! executor copy sent. The sealer keeps only its replay window, so a
//! reader below the window recovers from this store, and the distance
//! must stay well inside the window.
//!
//! After a start, the copies of the executors chain only behind a frame
//! that another source delivers (see [`crate::merge`]). The thread joins
//! them in one of two ways:
//! - with peers, the frames of a peer egress fill the place between the
//!   head and the copies, in order. A peer whose floor is above the head
//!   gives its floor as the new start;
//! - with no peer, or when the peers do not fill the place in time, the
//!   stream starts again after the next boundary of a copy. The store then
//!   holds no frame before that boundary, and a reader of an older cursor
//!   reads another instance.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tokio::sync::oneshot;

use alloy_primitives::B256;
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::Receipt;

use crate::merge::{Head, Merge};
use crate::metrics;
use crate::receipts::Receipts;
use crate::ref_mark::RefMark;
use crate::store::{Range, Store, StoreError};

/// Where the delivered frames go live.
pub trait FramePublisher: Send + 'static {
    fn publish(&self, frame: &[u8]);
}

/// How the owner joins the copies of the executors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Join {
    /// The merge follows the copies.
    Joined,
    /// A peer egress fills the frames between the head and the copies.
    FromPeer,
    /// The stream starts again after the next boundary of a copy.
    AtBoundary,
}

/// Where the stream stands for the task that reads a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinState {
    pub head: Head,
    pub joined: bool,
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
    /// Frames of a peer egress, in order, from the head of this one.
    Caught(Vec<Vec<u8>>),
    /// The floor of a peer egress, above the head of this one: no peer
    /// holds the frames below it. `mark` is the reference mark of the peer.
    Floor { floor: Head, mark: RefMark },
    /// No peer filled the place in time: start again after a boundary.
    GiveUp,
    /// Where the stream stands, for the task that reads a peer.
    State(oneshot::Sender<JoinState>),
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
    /// How the owner joins the copies at the start: [`Join::FromPeer`]
    /// with peers, [`Join::AtBoundary`] with none.
    pub join: Join,
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
    join: Join,
}

impl<P: FramePublisher> Owner<P> {
    #[must_use]
    pub fn new(cfg: OwnerConfig, parts: Parts<P>) -> Self {
        Self {
            join: cfg.join,
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
            Input::Caught(frames) => self.take_caught(&frames),
            Input::Floor { floor, mark } => self.start_at_floor(floor, mark),
            Input::GiveUp => {
                self.give_up();
                Ok(())
            }
            Input::State(reply) => {
                let _ = reply.send(JoinState {
                    head: self.parts.merge.head(),
                    joined: self.join == Join::Joined,
                });
                Ok(())
            }
        }
    }

    /// Merge one message of a copy. Before the join with no peer, a
    /// boundary of a copy at or after the head starts the stream again.
    fn take(&mut self, session: i32, message: &[u8]) -> Result<(), StoreError> {
        self.parts.merge.take(session, message, &mut self.delivered);
        self.note_join();
        if self.join == Join::AtBoundary {
            self.start_after_boundary(message)?;
        }
        self.flush()
    }

    /// Merge the frames of a peer, in order.
    fn take_caught(&mut self, frames: &[Vec<u8>]) -> Result<(), StoreError> {
        metrics::count_caught(frames.len());
        for frame in frames {
            self.parts.merge.take_ordered(frame, &mut self.delivered);
        }
        self.note_join();
        self.flush()
    }

    /// Append the delivered frames to the store, then publish them live.
    fn flush(&mut self) -> Result<(), StoreError> {
        self.parts.store.append(&self.delivered)?;
        self.delivered
            .drain(..)
            .for_each(|frame| self.parts.live.publish(&frame));
        Ok(())
    }

    fn note_join(&mut self) {
        if self.join != Join::Joined && self.parts.merge.joined() {
            tracing::info!(head = ?self.parts.merge.head(), "egress: joined the copies of the executors");
            self.join = Join::Joined;
        }
    }

    fn give_up(&mut self) {
        if self.join == Join::FromPeer {
            tracing::warn!(
                head = ?self.parts.merge.head(),
                "egress: no peer filled the place in time; the stream starts again after the next boundary"
            );
            self.join = Join::AtBoundary;
        }
    }

    /// Start the stream again after the boundary in `message`, when it is
    /// at or after the head.
    fn start_after_boundary(&mut self, message: &[u8]) -> Result<(), StoreError> {
        let next_block = self.parts.merge.head().next_block;
        let Some(start) = message
            .get(8..)
            .and_then(Head::after_boundary)
            .filter(|start| start.next_block > next_block)
        else {
            return Ok(());
        };
        tracing::warn!(
            ?start,
            "egress: the stream starts again after a boundary of a copy"
        );
        metrics::count_rebased("boundary");
        self.rebase(start, RefMark::default())?;
        self.join = Join::Joined;
        Ok(())
    }

    /// Start the stream again at the floor of a peer, when it is above the
    /// head and the copies did not join yet. The reference mark of the
    /// store becomes the higher of its own and `mark`, the mark of the peer.
    fn start_at_floor(&mut self, floor: Head, mark: RefMark) -> Result<(), StoreError> {
        if self.join == Join::Joined || floor.next_block <= self.parts.merge.head().next_block {
            return Ok(());
        }
        tracing::warn!(
            ?floor,
            "egress: the floor of a peer is above the head; the stream starts there"
        );
        metrics::count_rebased("peer-floor");
        self.rebase(floor, mark)
    }

    fn rebase(&mut self, start: Head, peer: RefMark) -> Result<(), StoreError> {
        self.parts.store.reset(start, peer)?;
        self.parts.merge.rebase(start);
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
        self.show_distance();
        let floor = self
            .parts
            .store
            .head()
            .next_block
            .saturating_sub(self.cfg.keep_blocks.get());
        self.parts.store.prune_below(floor)
    }

    /// The canonical indexes between the stored head and the newest record
    /// that any copy sent. A copy behind the stored head counts 0, so the
    /// subtraction saturates.
    #[must_use]
    pub fn distance(&self) -> u64 {
        self.parts
            .merge
            .seen()
            .saturating_sub(self.parts.store.head().next_index)
    }

    fn show_distance(&self) {
        crate::metrics::set_head_distance(self.distance());
    }
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
