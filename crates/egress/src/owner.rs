//! [`Owner`]: the one thread that owns the merge and the store.
//!
//! Everything that touches the stream comes to this thread on one channel:
//! the messages of the executors, and the range reads of the readers that
//! resume. So the merge, the store and the live publication need no lock,
//! and a read sees the store as the thread left it after its last frame.
//!
//! The thread appends each delivered frame to the store, then publishes it
//! live. It forces the store to disk on a fixed period, and deletes the
//! segments below the retention after each sync.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tokio::sync::oneshot;

use crate::merge::Merge;
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

/// The owner of the merge, the store and the live publication.
pub struct Owner<P: FramePublisher> {
    cfg: OwnerConfig,
    merge: Merge,
    store: Store,
    live: P,
    delivered: Vec<Vec<u8>>,
    synced_at: Instant,
}

impl<P: FramePublisher> Owner<P> {
    #[must_use]
    pub fn new(cfg: OwnerConfig, merge: Merge, store: Store, live: P) -> Self {
        Self {
            cfg,
            merge,
            store,
            live,
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
            Err(RecvTimeoutError::Disconnected) => return ControlFlow::Break(self.store.sync()),
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
                let range = self
                    .store
                    .read_from(read.from_index, read.from_block, read.limit);
                // A reader that left takes no answer.
                let _ = read.reply.send(range);
                Ok(())
            }
        }
    }

    fn take(&mut self, session: i32, message: &[u8]) -> Result<(), StoreError> {
        self.merge.take(session, message, &mut self.delivered);
        self.store.append(&self.delivered)?;
        self.delivered
            .drain(..)
            .for_each(|frame| self.live.publish(&frame));
        Ok(())
    }

    fn sync_if_due(&mut self) -> Result<(), StoreError> {
        if self.synced_at.elapsed() < self.cfg.sync_every {
            return Ok(());
        }
        self.store.sync()?;
        self.synced_at = Instant::now();
        let floor = self
            .store
            .head()
            .next_block
            .saturating_sub(self.cfg.keep_blocks.get());
        self.store.prune_below(floor)
    }
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
