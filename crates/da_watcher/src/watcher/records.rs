//! Taking the records of `l1_blocks`, in order, and publishing their
//! epochs on `tx_deposits`.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use kardamom_obs::halt;
use kardamom_types::l1_block::Admit;
use kardamom_types::{EpochRecord, L1Block};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::{L1Watcher, MonitorError, Position};
use crate::feed::BlockFeed;
use crate::l1_cursor::L1Cursor;
use crate::metrics;
use crate::publisher::{EpochPublisher, PublishError};

/// What the watcher does with one record.
enum Step {
    /// The record is done with: anchored, published, or dropped. `true`
    /// when its epoch was published.
    Taken(bool),
    /// The record must wait: the window is full, or a publish failed. It
    /// stays first in the inbox.
    Held,
    /// The records from this block on are missing here: the inbox is
    /// dropped, and the archives give them.
    Seek(u64),
}

impl<F: BlockFeed, P: EpochPublisher> L1Watcher<F, P> {
    /// Take every record that waits, in order, up to the first that must
    /// wait. Returns how many epochs were published. Nothing is taken
    /// while an operator halt stands or a failed publish waits for its
    /// retry.
    pub(super) fn process(&mut self) -> Result<usize, MonitorError> {
        if self.held || self.retry_at.is_some_and(|at| at > Instant::now()) {
            return Ok(0);
        }
        self.retry_at = None;
        let mut published = 0usize;
        while let ControlFlow::Continue(one) = self.process_one()? {
            published = published.saturating_add(usize::from(one));
        }
        Ok(published)
    }

    /// Take the first record of the inbox. `Continue` with whether an
    /// epoch was published; `Break` once the inbox is empty or a record
    /// must wait.
    fn process_one(&mut self) -> Result<ControlFlow<(), bool>, MonitorError> {
        let Some(record) = self.inbox.pop_front() else {
            return Ok(ControlFlow::Break(()));
        };
        match self.handle(&record) {
            Ok(Step::Taken(published)) => Ok(ControlFlow::Continue(published)),
            Ok(Step::Held) => {
                self.inbox.push_front(record);
                Ok(ControlFlow::Break(()))
            }
            Ok(Step::Seek(number)) => {
                self.inbox.clear();
                self.want(number);
                Ok(ControlFlow::Break(()))
            }
            Err(e) => Err(self.stop_on(e)),
        }
    }

    /// The state a record that stops the watcher leaves: the inbox is
    /// dropped. A chain break reads the records after the head again from
    /// the archives; a disagreement holds every record until an operator
    /// clears the halt.
    fn stop_on(&mut self, error: MonitorError) -> MonitorError {
        self.inbox.clear();
        match &error {
            MonitorError::Disagreement { .. } | MonitorError::ContentDisagreement { .. } => {
                self.held = true;
            }
            MonitorError::ChainBreak { number, .. } => self.want(*number),
            MonitorError::PublisherClosed => {}
        }
        error
    }

    /// One record against the position.
    /// Before the anchor, the record of the resume block anchors the
    /// watcher (with no position, the first record does); an earlier
    /// record is dropped, and a later one means the resume block's record
    /// is missing here.
    fn handle(&mut self, record: &L1Block) -> Result<Step, MonitorError> {
        let resume = match &self.position {
            Position::Anchored(window, dedup) => {
                let admit = dedup.admit(record);
                let full = window.is_full();
                return self.on_admit(record, admit, full);
            }
            Position::Tip => record.number,
            Position::After(after) => after.block(),
        };
        match record.number.cmp(&resume) {
            Ordering::Less => Ok(Step::Taken(false)),
            Ordering::Greater => Ok(Step::Seek(resume)),
            Ordering::Equal => {
                self.anchor_on(record);
                Ok(Step::Taken(false))
            }
        }
    }

    /// Anchor at a record: the resume block's record, or the first record
    /// of a watcher with no position.
    fn anchor_on(&mut self, record: &L1Block) {
        let cursor = L1Cursor {
            number: record.number,
            hash: record.hash,
        };
        info!(
            target: "da_watcher",
            l1_number = cursor.number,
            l1_hash = %cursor.hash,
            "anchored the L1 cursor"
        );
        self.anchor_at(cursor);
        self.taken();
    }

    /// A record taken in order: the watcher no longer waits, and a chain
    /// break that stood is over.
    fn taken(&mut self) {
        self.wanted = None;
        self.history_at = None;
        if halt::current().is_some() {
            halt::clear();
        }
    }

    /// One record after the anchor: the consumer rule of the two follower
    /// instances (`admit`), then the window (`full`), then the publish.
    fn on_admit(
        &mut self,
        record: &L1Block,
        admit: Admit,
        full: bool,
    ) -> Result<Step, MonitorError> {
        match admit {
            Admit::Duplicate => Ok(Step::Taken(false)),
            Admit::Ahead { expected } => Ok(Step::Seek(expected)),
            Admit::ParentMismatch {
                number,
                parent,
                expected,
            } => Err(MonitorError::ChainBreak {
                number,
                expected,
                parent,
            }),
            Admit::Disagreement {
                number,
                first,
                second,
            } => Err(MonitorError::Disagreement {
                number,
                first,
                second,
            }),
            Admit::ContentDisagreement { number } => {
                Err(MonitorError::ContentDisagreement { number })
            }
            Admit::Next if full => {
                debug!(
                    target: "da_watcher",
                    l1_number = record.number,
                    "the publish window is full; waiting for a boundary"
                );
                Ok(Step::Held)
            }
            Admit::Next => self.publish(record),
        }
    }

    /// Publish the record's epoch. On a publish, the window and the
    /// consumer rule both advance to the block.
    fn publish(&mut self, record: &L1Block) -> Result<Step, MonitorError> {
        match self.publisher.publish(&record.epoch) {
            Ok(pos) => {
                Self::count_published(&record.epoch);
                debug!(
                    target: "da_watcher",
                    l1_number = record.number,
                    deposits = record.epoch.deposits.len(),
                    ?pos,
                    "published epoch"
                );
                self.keep_record(record);
                Ok(Step::Taken(true))
            }
            Err(PublishError::Closed) => Err(MonitorError::PublisherClosed),
            Err(e @ (PublishError::Backpressure | PublishError::Transport(_))) => {
                // An epoch must never be skipped: a hole in the origin
                // sequence is a chain a verifier rejects. The record
                // stays first, and the publish is retried after a tick.
                warn!(
                    target: "da_watcher",
                    l1_number = record.number,
                    error = %e,
                    "epoch publish failed; retrying after a tick (an epoch must never be skipped)"
                );
                self.retry_at = Instant::now().checked_add(self.tick);
                Ok(Step::Held)
            }
        }
    }

    /// The published record is the new head.
    fn keep_record(&mut self, record: &L1Block) {
        if let Position::Anchored(_, dedup) = &mut self.position {
            dedup.take(record);
        }
        self.keep_published(record.epoch.clone());
        self.taken();
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "metric value; never nears 2^52 for an L1 block number"
    )]
    fn count_published(epoch: &EpochRecord) {
        ::metrics::counter!(metrics::EPOCHS_PUBLISHED_TOTAL).increment(1);
        ::metrics::counter!(metrics::DEPOSITS_DETECTED_TOTAL)
            .increment(epoch.deposits.len() as u64);
        ::metrics::gauge!(metrics::EPOCH_ORIGIN).set(epoch.l1_number as f64);
    }
}
