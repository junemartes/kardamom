//! The wait for a void record: the reader's side of the void rule.
//!
//! An entry whose `tx_data` every archive refuses cannot execute on this
//! consumer. The consumer does not drop the entry on its own clock: a peer
//! with other archives could execute it, and the two states would then
//! differ. The consumer asks the sealer to void the entry, and stops at the
//! entry until the canonical order carries the void record. The sealer
//! appends the record only after every configured voter asked for the same
//! entry, so every consumer drops the entry at the same canonical index.
//!
//! The void record comes after the entry in the order, so the reader reads
//! ahead to find it. The read-ahead keeps every message, and the reader
//! dispatches them in order after the wait. Nothing reaches the executor
//! during the wait, so no block closes across an undecided entry.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use kardamom_cluster_adapter::OfferOutcome;
use kardamom_types::{BPosition, TxOrderingMessage, VoidRecord};

use crate::error::ExecutorError;

use super::ports::TxOrderingSubscription;

/// The read-ahead bound, in messages. It equals the sealer's default void
/// window. The sealer refuses a vote for an entry that is more than one
/// window behind its head, so no void record can come after this many
/// messages.
pub(super) const MAX_READ_AHEAD: usize = 65_536;

/// The reader sends its vote again after this interval. Each vote is one
/// entry in the sealer's log, so a short interval costs the sealer work. A
/// repeat covers a first vote that the session did not accept. The reader
/// reads the clock at each message, and the sealer emits a boundary on a
/// timer also with no traffic, so the interval holds on an idle chain.
const REVOTE_INTERVAL: Duration = Duration::from_secs(5);

/// Messages read from the canonical order, not yet dispatched.
pub(super) type ReadAhead = VecDeque<(BPosition, TxOrderingMessage)>;

/// How a wait for a void record ended.
pub(super) enum ParkOutcome {
    /// The order carries the void record. Drop the entry.
    Voided,
    /// The wait or the read-ahead bound ended first. The sealer keeps the
    /// vote, so the process can stop and a restart continues the count.
    GaveUp,
}

/// One wait for the void record of one entry. `backlog` holds messages that
/// an earlier wait read ahead; they come before anything new on `sub`.
pub(super) struct VoidPark<'a, O> {
    sub: &'a mut O,
    backlog: &'a mut ReadAhead,
    voter_id: u8,
    void: VoidRecord,
    deadline: Instant,
    ahead: ReadAhead,
    pub(super) revote_after: Duration,
    last_vote: Instant,
    vote_refusal_logged: bool,
}

impl<'a, O: TxOrderingSubscription> VoidPark<'a, O> {
    pub(super) fn new(
        sub: &'a mut O,
        backlog: &'a mut ReadAhead,
        voter_id: u8,
        void: VoidRecord,
        wait: Duration,
    ) -> Self {
        Self {
            sub,
            backlog,
            voter_id,
            void,
            deadline: Instant::now() + wait,
            ahead: ReadAhead::new(),
            revote_after: REVOTE_INTERVAL,
            last_vote: Instant::now(),
            vote_refusal_logged: false,
        }
    }

    /// Vote, then read ahead until the void record arrives or the wait ends.
    /// On return, the backlog holds every message read, in canonical order,
    /// the void record included: that record has a slot of its own, which
    /// the reader counts when it dispatches the record.
    ///
    /// # Errors
    ///
    /// Returns the subscription's error, which includes a clean close.
    pub(super) fn run(mut self) -> Result<ParkOutcome, ExecutorError> {
        info!(
            target: "kardamom_executor::reader",
            voter_id = self.voter_id,
            index = self.void.index,
            tx_hash = ?self.void.tx_hash,
            "every archive refused the entry's tx_data: voting to void the entry"
        );
        self.vote();
        let outcome = loop {
            if let Some(outcome) = self.step()? {
                break outcome;
            }
        };
        self.ahead.append(self.backlog);
        *self.backlog = self.ahead;
        Ok(outcome)
    }

    /// Read one message ahead. Returns the outcome when the wait is over.
    fn step(&mut self) -> Result<Option<ParkOutcome>, ExecutorError> {
        let now = Instant::now();
        if now >= self.deadline || self.ahead.len() >= MAX_READ_AHEAD {
            return Ok(Some(ParkOutcome::GaveUp));
        }
        if now.duration_since(self.last_vote) >= self.revote_after {
            self.vote();
        }
        let (position, msg) = match self.backlog.pop_front() {
            Some(read) => read,
            None => self.sub.next()?,
        };
        let voided = msg.as_void() == Some(&self.void);
        self.ahead.push_back((position, msg));
        Ok(voided.then_some(ParkOutcome::Voided))
    }

    /// Send the vote. A session that does not accept it is not an error:
    /// the next repeat sends the vote again.
    fn vote(&mut self) {
        self.last_vote = Instant::now();
        let outcome = self.sub.vote(self.voter_id, &self.void);
        if outcome == OfferOutcome::Accepted || self.vote_refusal_logged {
            return;
        }
        self.vote_refusal_logged = true;
        warn!(
            target: "kardamom_executor::reader",
            voter_id = self.voter_id,
            index = self.void.index,
            ?outcome,
            "the cluster session did not accept the void vote; the reader sends it again"
        );
    }
}
