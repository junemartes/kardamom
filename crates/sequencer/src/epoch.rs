//! Epoch-side of the sequencer.
//!
//! The DA watcher publishes one [`types::EpochRecord`] per finalized L1
//! block onto the dedicated `tx_deposits` Aeron channel. This includes
//! blocks with no deposits. Each of the M sequencers subscribes to that
//! channel and forwards the epoch verbatim onto the canonical orderer
//! `tx_ordering`, as an origin-advancing record.
//!
//! All M sequencers race on the multi-publisher `tx_ordering` stream, so
//! each epoch is offered M times. The cluster's first-seen dedup collapses
//! them on `canonical_id = keccak(l1_hash)`. Racing producers derive this
//! id identically from the same L1 block.
//!
//! The deposits travel inside the record itself, so there is no
//! ref-to-envelope join for a consumer to time out on: a lost
//! `tx_deposits` fragment cannot strand a deposit.
//!
//! Epochs are not nonce-gated. They carry OP `source_hash` values and have
//! no state-machine interaction in the sequencer. The epoch pump runs
//! independently of the nonce-gated tx_data-to-TxRef path in
//! [`crate::sequencer`].
//!
//! An accepted offer is not an ordered epoch. "Accepted" means only that
//! the frame entered the ingress publication buffer, and cluster ingress
//! is at-most-once across a leader kill or a quorum loss. The da-watcher
//! publishes each epoch once, and `tx_deposits` is live-only. So the pump
//! keeps every epoch it took until a boundary confirms it:
//!
//! * a boundary whose `l1_origin` is at or past the epoch's L1 block
//!   confirms the epoch, and the pump forgets it;
//! * the sealer refuses an epoch that skips an L1 block, and answers the
//!   offering session with the origin it expects. The pump offers its
//!   unconfirmed epochs again from that origin, in order. The sealer's
//!   dedup absorbs a copy it already ordered;
//! * when the pump does not hold the expected epoch (it restarted, and
//!   lost its queue), it stops offering and waits. The da-watcher
//!   publishes every epoch that no boundary confirms again, and a twin
//!   can order the epoch too. When the gap stays for
//!   [`ORIGIN_GAP_GRACE`], the pump reports an error that raises the
//!   `origin_gap` halt;
//! * when the pump holds [`MAX_UNCONFIRMED_EPOCHS`] that no boundary
//!   confirms, it stops and reports that error at once.
//!
//! The pump never skips an epoch.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use kardamom_log::aeron_live::TxDepositsSubscriberHandle;
use kardamom_types::epoch_delivery::ORIGIN_GAP_GRACE;
use kardamom_types::{BPosition, EpochRecord};

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;
use crate::pump::OriginLane;

/// The most epochs the pump keeps without a boundary that confirms them.
///
/// A live sealer confirms an epoch within one boundary tick (seconds), and
/// the next epoch's forced boundary confirms the one before it, so a
/// burst of L1 catch-up stays within one ingress round trip. While the
/// cluster has no leader, the offers back-pressure, and the pump takes no
/// new epoch. So the queue grows only while the sealer takes the offers
/// and orders none of them. 4096 L1 blocks are 13.6 hours of L1 at
/// 12 seconds a block: an overflow is a fault, not load. An epoch is a
/// few hundred bytes plus its deposits, so the bound also caps the memory.
pub const MAX_UNCONFIRMED_EPOCHS: usize = 4096;

/// What the cluster egress tells the epoch pump about the L1 origin. The
/// egress feed sends these in egress order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginSignal {
    /// A boundary carries this L1 origin, so the sealer ordered every
    /// epoch up to it. The feed sends it only when the origin grows.
    Confirmed(u64),
    /// The sealer refused an epoch of this session: the next L1 origin it
    /// accepts is `expected`.
    Gap { expected: u64 },
}

/// The sending half of the pump's signal channel. The egress feed holds
/// it.
pub type OriginSignalTx = Sender<OriginSignal>;

/// Subscription surface that the epoch pump reads from. Production wiring
/// binds this to the real `log::TxDepositsSubscriber`. Tests use the
/// in-memory fake in [`fakes`].
pub trait EpochSubscriber: Send {
    /// Poll for one epoch. Returns:
    /// * `Ok(Some((pos, epoch)))`: an epoch was available.
    /// * `Ok(None)`: no fragment is ready now. The caller should back off.
    /// * `Err(SequencerError::IngressDisconnected)`: the subscription is closed.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::IngressDisconnected`] when the
    /// subscription is closed.
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError>;
}

/// The live adapter: a miss is not an error, the pump backs off.
impl EpochSubscriber for TxDepositsSubscriberHandle {
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
        Ok(self.try_recv())
    }
}

/// An origin the sealer expects that the pump does not hold, and when the
/// pump first lacked an expected origin.
#[derive(Debug, Clone, Copy)]
struct Gap {
    expected: u64,
    since: Instant,
}

/// The epoch lane: the epochs it took off `tx_deposits` and that no
/// boundary confirmed yet, and the cursor of the next one to offer.
pub struct EpochPump {
    /// Ascending by L1 block, one epoch per block.
    unconfirmed: VecDeque<EpochRecord>,
    /// The index in `unconfirmed` of the next epoch to offer. The pump
    /// offered every epoch before it at least once.
    next: usize,
    /// The highest L1 origin a boundary carried.
    confirmed: Option<u64>,
    /// The origin the sealer expects that this pump does not hold.
    missing: Option<Gap>,
    /// How long a gap may stand before the pump reports it.
    grace: Duration,
    signals: Receiver<OriginSignal>,
}

impl EpochPump {
    /// A pump, and the sender the egress feed signals it through.
    #[must_use]
    pub fn new() -> (OriginSignalTx, Self) {
        let (tx, signals) = crossbeam_channel::unbounded();
        let pump = Self {
            unconfirmed: VecDeque::new(),
            next: 0,
            confirmed: None,
            missing: None,
            grace: ORIGIN_GAP_GRACE,
            signals,
        };
        (tx, pump)
    }

    /// The same pump with another gap grace. Tests use a zero grace to
    /// see the halt at once.
    #[must_use]
    pub fn with_gap_grace(self, grace: Duration) -> Self {
        Self { grace, ..self }
    }

    /// Apply every signal the egress feed sent since the last call.
    fn take_signals(&mut self) {
        while let Ok(signal) = self.signals.try_recv() {
            self.apply(signal);
        }
    }

    fn apply(&mut self, signal: OriginSignal) {
        match signal {
            OriginSignal::Confirmed(origin) => self.confirm(origin),
            OriginSignal::Gap { expected } => self.rewind(expected),
        }
    }

    /// Forget every epoch at or below `origin`.
    fn confirm(&mut self, origin: u64) {
        let done = self.unconfirmed.partition_point(|e| e.l1_number <= origin);
        self.unconfirmed.drain(..done);
        // An epoch that a twin's offer ordered before this pump offered it
        // needs no offer, so the cursor stops at the first epoch left.
        self.next = self.next.saturating_sub(done);
        self.confirmed = Some(origin);
        self.missing = self.missing.filter(|gap| gap.expected > origin);
        crate::metrics::record_epochs_unconfirmed(self.unconfirmed.len());
    }

    /// Offer again from `expected`: the sealer refused every epoch above
    /// it. When the pump does not hold `expected`, the gap stands until a
    /// boundary confirms it or the epoch arrives. An origin the boundaries
    /// confirmed already is not missing. A gap that moves to another
    /// origin keeps its start time: the lane stalls from the first one.
    fn rewind(&mut self, expected: u64) {
        let at = self.unconfirmed.partition_point(|e| e.l1_number < expected);
        self.next = self.next.min(at);
        let held = self
            .unconfirmed
            .get(at)
            .is_some_and(|e| e.l1_number == expected);
        let since = self.missing.map_or_else(Instant::now, |gap| gap.since);
        self.missing = Some(Gap { expected, since })
            .filter(|gap| !held && Some(gap.expected) > self.confirmed);
    }

    /// Keep `epoch` in L1 order. An epoch the sealer ordered already, or
    /// one the pump holds, is a copy. An epoch that lands before the
    /// cursor (a da-watcher that publishes again from an older block) is
    /// offered next.
    fn insert(&mut self, epoch: EpochRecord) {
        if self.confirmed.is_some_and(|c| epoch.l1_number <= c) {
            return;
        }
        let Err(at) = self
            .unconfirmed
            .binary_search_by_key(&epoch.l1_number, |e| e.l1_number)
        else {
            return;
        };
        self.unconfirmed.insert(at, epoch);
        self.next = self.next.min(at);
        crate::metrics::record_epochs_unconfirmed(self.unconfirmed.len());
        if let Some(gap) = self.missing {
            self.rewind(gap.expected);
        }
    }

    /// The next epoch to offer: none while the gap stands.
    fn to_offer(&self) -> Option<&EpochRecord> {
        if self.missing.is_some() {
            return None;
        }
        self.unconfirmed.get(self.next)
    }

    /// Poll one epoch when the pump has none left to offer. Returns
    /// whether it took one. While a gap stands for less than the grace,
    /// the pump waits: it polls, and it offers nothing.
    ///
    /// # Errors
    ///
    /// [`SequencerError::EpochQueueFull`] when the queue is full, before
    /// it polls; [`SequencerError::OriginGapUnfilled`] when the gap stood
    /// for the grace, after it polls; and the subscription's error.
    fn fill<S: EpochSubscriber>(&mut self, sub: &mut S) -> Result<bool, SequencerError> {
        if self.to_offer().is_some() {
            return Ok(false);
        }
        if self.unconfirmed.len() >= MAX_UNCONFIRMED_EPOCHS {
            return Err(SequencerError::EpochQueueFull {
                held: self.unconfirmed.len(),
                oldest: self.unconfirmed.front().map_or(0, |e| e.l1_number),
            });
        }
        let took = sub.poll()?.map(|(_, epoch)| self.insert(epoch)).is_some();
        match self.missing {
            Some(gap) if gap.since.elapsed() >= self.grace => {
                Err(SequencerError::OriginGapUnfilled {
                    expected: gap.expected,
                })
            }
            _ => Ok(took),
        }
    }

    /// Whether an epoch waits for its first offer, mid-retry after a
    /// `Backpressure` result. Test-only.
    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        self.next < self.unconfirmed.len()
    }
}

/// The epoch lane: apply the egress signals, take one epoch off the
/// subscription when nothing is left to offer, and offer the next epoch.
///
/// On `SequencerError::Backpressure` the cursor stays, and the next call
/// offers the SAME epoch again before it polls a new one. `Backpressure`
/// includes "not connected", so a leader election does not drain the
/// backlog.
impl<S, P> OriginLane<S, P> for EpochPump
where
    S: EpochSubscriber,
    P: TxOrderingRefPublisher,
{
    fn relay(&mut self, sub: &mut S, publ: &mut P) -> Result<bool, SequencerError> {
        self.take_signals();
        let took = self.fill(sub)?;
        let Some(epoch) = self.to_offer() else {
            return Ok(took);
        };
        publ.try_publish_epoch(epoch)?;
        // `next` indexes the queue, which holds at most
        // `MAX_UNCONFIRMED_EPOCHS`.
        self.next += 1;
        Ok(true)
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use super::{BPosition, EpochRecord, EpochSubscriber, SequencerError};
    use crate::fakes::ScriptedQueue;

    /// In-memory [`EpochSubscriber`] driven by a scripted queue. Push test
    /// inputs with `ScriptedEpochs::push`. The sequencer drains them in
    /// first-in-first-out order.
    pub type ScriptedEpochs = ScriptedQueue<EpochRecord>;

    impl EpochSubscriber for ScriptedEpochs {
        fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
            self.poll_next()
        }
    }
}

#[cfg(test)]
#[path = "epoch_tests.rs"]
mod tests;
