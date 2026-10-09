//! The wait for a void record: the reader's side of the void rule.
//!
//! An entry whose `tx_data` every archive fails cannot execute from those
//! sources. The consumer does not drop the entry on its own clock: a peer
//! with other sources could execute it, and the two states would then
//! differ. The consumer parks at the entry. It asks its peer executors
//! first ([`PeerFetch`]): a peer's archive that holds the entry ends the
//! park, and the reader joins the entry with no vote. The reader votes to
//! void the entry only when every archive refused the range and every peer
//! answered that it holds no record. The sealer appends the void record
//! only after every configured voter asked for the same entry, so every
//! consumer drops the entry at the same canonical index.
//!
//! A reader that sent a vote does not execute the entry in the same run:
//! it waits for the void record. A peer that executed the entry and lost
//! its record blocks the vote for good. The reader then stops for the
//! peer-checkpoint repair at the block that holds the entry.
//!
//! The void record comes after the entry in the order, so the reader reads
//! ahead to find it. The read-ahead keeps every message, and the reader
//! dispatches them in order after the wait. Nothing reaches the executor
//! during the wait, so no block closes across an undecided entry. The
//! reader also reads ahead while it waits for a peer: a vote of an earlier
//! run of this consumer can complete the void, and the void record ends
//! the wait then.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use kardamom_cluster_adapter::OfferOutcome;
use kardamom_types::{BPosition, TxEnvelope, TxOrderingMessage, VoidRecord};

use crate::error::ExecutorError;

use super::peer_fetch::{PeerFetch, PeerPoll};
use super::ports::{ExecRecordReplay, TxOrderingSubscription};

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
    /// A peer's archive served the entry. Join it.
    Fetched(TxEnvelope),
    /// The order carries the void record. Drop the entry.
    Voided,
    /// A peer executed the entry and lost its record, so no void can come.
    /// `block` is the block that holds the entry.
    Lost { block: u64 },
    /// The wait or the read-ahead bound ended first. The sealer keeps a
    /// vote, so the process can stop and a restart continues the count.
    GaveUp,
}

/// What one park is for: the entry's void record, the voter id that asks
/// for it, the bound of the wait, and the asks to the peers.
pub(super) struct ParkPlan<'a, R> {
    /// The voter id, when this reader may vote: it votes only when every
    /// archive refused the entry's range. `None` waits with no vote.
    pub(super) voter_id: Option<u8>,
    pub(super) void: VoidRecord,
    pub(super) wait: Duration,
    pub(super) peers: PeerFetch<'a, R>,
}

/// One wait for the void record of one entry. `backlog` holds messages that
/// an earlier wait read ahead; they come before anything new on `sub`.
pub(super) struct VoidPark<'a, O, R> {
    sub: &'a mut O,
    backlog: &'a mut ReadAhead,
    voter_id: Option<u8>,
    void: VoidRecord,
    deadline: Instant,
    ahead: ReadAhead,
    pub(super) revote_after: Duration,
    /// `None` until the first vote.
    last_vote: Option<Instant>,
    vote_refusal_logged: bool,
    pub(super) peers: PeerFetch<'a, R>,
}

impl<'a, O: TxOrderingSubscription, R: ExecRecordReplay> VoidPark<'a, O, R> {
    pub(super) fn new(sub: &'a mut O, backlog: &'a mut ReadAhead, plan: ParkPlan<'a, R>) -> Self {
        let now = Instant::now();
        Self {
            sub,
            backlog,
            voter_id: plan.voter_id,
            void: plan.void,
            // A wait too long for the clock ends at once. The reader then
            // stops and restarts, as after any wait.
            deadline: now.checked_add(plan.wait).unwrap_or(now),
            ahead: ReadAhead::new(),
            revote_after: REVOTE_INTERVAL,
            last_vote: None,
            vote_refusal_logged: false,
            peers: plan.peers,
        }
    }

    /// Ask the peers, vote when the rule allows it, and read ahead until
    /// the void record arrives, a peer serves the entry, a lost record
    /// ends the wait, or the wait ends. On return, the backlog holds every
    /// message read, in canonical order, the void record included: that
    /// record has a slot of its own, which the reader counts when it
    /// dispatches the record.
    ///
    /// # Errors
    ///
    /// Returns the subscription's error, which includes a clean close.
    pub(super) fn run(mut self) -> Result<ParkOutcome, ExecutorError> {
        let outcome = match self.decide(Instant::now()) {
            Some(outcome) => outcome,
            None => self.read_ahead()?,
        };
        self.log_unanswered(&outcome);
        self.ahead.append(self.backlog);
        *self.backlog = self.ahead;
        Ok(outcome)
    }

    fn read_ahead(&mut self) -> Result<ParkOutcome, ExecutorError> {
        loop {
            if let Some(outcome) = self.step()? {
                return Ok(outcome);
            }
        }
    }

    /// The peer step, then the vote. Returns the outcome when the wait is
    /// over without a void record. After a vote, the peer step is over:
    /// every peer answered `not_held`, and the reader executes nothing of
    /// the entry in this run.
    fn decide(&mut self, now: Instant) -> Option<ParkOutcome> {
        if self.last_vote.is_some() {
            return self.vote_when_due(now);
        }
        match self.peers.poll(now) {
            PeerPoll::Fetched(envelope) => Some(ParkOutcome::Fetched(envelope)),
            PeerPoll::Waiting => None,
            PeerPoll::NoneHolds => self.vote_when_due(now),
            PeerPoll::Lost => self.lost_block().map(|block| ParkOutcome::Lost { block }),
        }
    }

    /// Every peer answered `not_held`. A reader that may vote votes, and
    /// again after the interval. Any other reader only waits for the void
    /// record.
    fn vote_when_due(&mut self, now: Instant) -> Option<ParkOutcome> {
        let voter_id = self.voter_id?;
        let due = self
            .last_vote
            .is_none_or(|last| now.duration_since(last) >= self.revote_after);
        if due {
            self.vote(voter_id);
        }
        None
    }

    /// The block that holds the entry: the first boundary read ahead whose
    /// end passes the entry. `None` until the read-ahead reaches it.
    fn lost_block(&self) -> Option<u64> {
        self.ahead
            .iter()
            .filter_map(|(_, msg)| msg.as_boundary())
            .find(|b| b.end_tx_idx.as_index() > self.void.index)
            .map(|b| b.block_number)
    }

    /// Read one message ahead. Returns the outcome when the wait is over.
    fn step(&mut self) -> Result<Option<ParkOutcome>, ExecutorError> {
        let now = Instant::now();
        if now >= self.deadline || self.ahead.len() >= MAX_READ_AHEAD {
            return Ok(Some(ParkOutcome::GaveUp));
        }
        if let Some(outcome) = self.decide(now) {
            return Ok(Some(outcome));
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
    fn vote(&mut self, voter_id: u8) {
        if self.last_vote.is_none() {
            info!(
                target: "kardamom_executor::reader",
                voter_id,
                index = self.void.index,
                tx_hash = ?self.void.tx_hash,
                peer_answers = ?self.peers.answers(),
                "every archive refused the entry's tx_data: voting to void the entry"
            );
        }
        self.last_vote = Some(Instant::now());
        let outcome = self.sub.vote(voter_id, &self.void);
        if outcome == OfferOutcome::Accepted || self.vote_refusal_logged {
            return;
        }
        self.vote_refusal_logged = true;
        warn!(
            target: "kardamom_executor::reader",
            voter_id,
            index = self.void.index,
            ?outcome,
            "the cluster session did not accept the void vote; the reader sends it again"
        );
    }

    /// A wait that ended with a peer still unanswered, or with a lost
    /// record, sent no vote. Say which peers kept the vote back.
    fn log_unanswered(&self, outcome: &ParkOutcome) {
        let unanswered = self.peers.unanswered();
        let lost = self.peers.lost();
        let quiet = unanswered.is_empty() && lost.is_empty();
        if quiet || !matches!(outcome, ParkOutcome::GaveUp | ParkOutcome::Lost { .. }) {
            return;
        }
        warn!(
            target: "kardamom_executor::reader",
            index = self.void.index,
            tx_hash = ?self.void.tx_hash,
            ?unanswered,
            ?lost,
            "peer fetch: the wait ended with a peer unanswered or lost; no vote was sent"
        );
    }
}
