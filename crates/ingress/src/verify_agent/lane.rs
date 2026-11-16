//! The lanes of the verify agent: the `tx_data` publications that it
//! offers on, and the frames that wait for their lane.

use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_log::LogError;
use kardamom_log::aeron_live::LentPublication;
use kardamom_log::parked::{Offered, Parked};
use kardamom_types::BPosition;
use metrics::Counter;
use rkyv::util::AlignedVec;
use tokio::sync::oneshot;

use crate::error::IngressError;
use crate::stage_latency::StageStamps;

/// One `tx_data` lane, as the agent offers on it: one offer, on the thread
/// of the agent, that never waits.
pub trait LaneOffer: Send + 'static {
    /// One offer of the encoded envelope `bytes`.
    fn offer(&self, bytes: &[u8]) -> Offered;
}

impl LaneOffer for LentPublication {
    fn offer(&self, bytes: &[u8]) -> Offered {
        LentPublication::offer(self, bytes)
    }
}

/// What the agent takes from the publication of the ingress: one lane for
/// each `tx_data` lane, in lane order, the stage stamps of the published
/// transactions, and the time cap of an idle park.
pub struct AgentLanes<L> {
    pub lanes: Vec<L>,
    /// `None` when no clock observes the stages (the in-memory channels).
    pub stamps: Option<StageStamps>,
    pub idle_cap: Duration,
}

/// A transaction that passed the checks of the agent: its hash and the
/// recovered sender. The handler uses them for the duplicate cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    pub tx_hash: B256,
    pub sender: Address,
}

/// The answer to one submit: the admitted transaction, or the error that
/// the client gets.
pub type Answer = Result<Admitted, IngressError>;

/// An encoded envelope for one lane, with the answer to its submit. It
/// waits in the retry queue of the agent while its lane refuses it.
pub(crate) struct LaneFrame {
    lane: u32,
    bytes: AlignedVec,
    deadline: Instant,
    admitted: Admitted,
    /// `None` once the frame answered.
    answer: Option<oneshot::Sender<Answer>>,
    /// The count of the `txpool-full` refusals.
    full: Counter,
}

impl LaneFrame {
    pub(crate) fn new(
        lane: u32,
        bytes: AlignedVec,
        deadline: Instant,
        admitted: Admitted,
        answer: oneshot::Sender<Answer>,
        full: Counter,
    ) -> Self {
        Self {
            lane,
            bytes,
            deadline,
            admitted,
            answer: Some(answer),
            full,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// One offer on its lane among `lanes`.
    pub(crate) fn offer_on<L: LaneOffer>(&self, lanes: &[L]) -> Offered {
        usize::try_from(self.lane)
            .ok()
            .and_then(|lane| lanes.get(lane))
            .map_or(Offered::UnknownPublication, |lane| lane.offer(self.bytes()))
    }

    /// Answer the submit. A handler that went away dropped its receiver,
    /// and the answer then has no reader.
    fn answer(&mut self, answer: Answer) {
        if let Some(tx) = self.answer.take() {
            let _ = tx.send(answer);
        }
    }
}

impl Parked for LaneFrame {
    fn publication(&self) -> u32 {
        self.lane
    }

    fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The lane took the frame. A position that does not decode still
    /// answers `txpool is full`, as the publish of the tokio path does: the
    /// client sends again, and the window of the sealer drops the copy.
    fn delivered(&mut self, position: Result<BPosition, LogError>) {
        match position {
            Ok(_) => self.answer(Ok(self.admitted)),
            Err(error) => self.failed(error.to_string()),
        }
    }

    /// The lane refused the frame until its deadline: `txpool is full`.
    fn failed(&mut self, reason: String) {
        self.full.increment(1);
        tracing::debug!(
            sender = %self.admitted.sender,
            tx_hash = %self.admitted.tx_hash,
            reason,
            "verify agent: tx_data offer failed"
        );
        self.answer(Err(IngressError::PoolFull));
    }
}
