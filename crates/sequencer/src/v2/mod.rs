//! Sequencer v2: the publish loop around the admission filter
//! ([`crate::admission`]).
//!
//! The loop takes three inputs and has three outputs:
//! - `tx_data` envelopes go through the wrong-shard guard, the fee checks
//!   and the filter;
//! - account events (the rows of each block, the answers of lookups, and
//!   rollbacks) update the filter;
//! - a timer runs the filter's sweep.
//! - A forwarded transaction goes to the sealer as a reference.
//! - A refused one goes to the client on `tx_errors` and `tx_status`, and
//!   to the egress on `tx_drops`.
//! - A sender with no state goes to the lookup task.
//! - With verdicts on, the answer to each transaction goes to the ingress
//!   that published it ([`verdicts`]).
//!
//! The loop keeps no ledger of offers and no resync: the sealer orders a
//! reference once, and the filter forgets a forwarded run that no row
//! confirms within its loss window. Offers wait in an outbox while the
//! sealer is back-pressured; a full outbox stops the intake.
//!
//! The filter keeps each forwarded transaction until a row confirms it.
//! The loop offers it again after recovery, so the sealer never orders a
//! later nonce of a sender in place of a lost one:
//! - The cluster session changed (a leader change or a reconnect). The
//!   old session can have lost the offers that it took. The publisher
//!   refuses the first offer on the new session, and the loop then puts
//!   every unconfirmed run in the outbox again, in nonce order, before any
//!   new offer. The sweep also observes the session generation, so an idle
//!   sender needs no new offer to retry. The sealer absorbs a copy of an
//!   ordered offer.
//! - The contiguity guard of the sealer rejects a nonce. The loop offers
//!   the transaction at the expected nonce and the rejected one again,
//!   when the filter still holds the expected one.
//! - The lifecycle clears a pause. The sealer can have refused offers
//!   during its upstream halt. The loop retries its unconfirmed runs
//!   before it reads new input, also when the whole pause passes between
//!   turns. Empty blocks during a pause do not expire those runs. Rows
//!   still confirm them, and recovery starts a new loss window.
//!
//! The loop builds each offer when it goes to the outbox, in the format of
//! the decision version that the sealer reports at that time. So the
//! offers of one sender change format once, in nonce order, at a version
//! switch, also for a transaction that waited for a nonce gap.
//!
//! The loop records the counters of the v1 loop under the same names, and
//! the parked depth of each vslot ([`depth`]) at each sweep.

mod depth;
mod offers;
mod refusals;
use offers::OfferReplay;
mod runner;
mod verdicts;

pub use verdicts::VerdictOut;

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_obs::pipeline::Queue;
use kardamom_types::shard_map::{VslotSet, vslot_for};
use kardamom_types::{DropReason, TxDataLoc, TxDrop, TxEnvelope, TxError, TxErrorReason, TxStatus};

use crate::admission::{
    AccountState, Ack, Admission, AdmissionConfig, Candidate, ImagePos, Refusal, Verdict,
};
use crate::error::SequencerError;
use crate::fees::FeeGate;
use crate::inbound::{Inbound, TxDataSubscriber};
use crate::metrics::HotMetrics;
use crate::outbound::{RefOffer, SideChannelPublisher, TxOrderingRefPublisher};
use crate::pause_gate::PauseGate;
use crate::pipeline::PublishMeters;
use crate::sealer_rules::SealerVersion;
use crate::sender::sender_of;
use crate::sequencer::SequencerPorts;
use crate::tx_decode::decode_fields;

/// An Aeron position unit: frames are aligned to 32 bytes.
const UNIT_SHIFT: u32 = 5;

/// The `[admission]` config section. Off, the sequencer runs the v1 loop.
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct AdmissionSettings {
    /// Run the v2 loop in place of the v1 loop.
    pub enabled: bool,
    /// Send the answer to each transaction to the ingress that published
    /// it, on `tx_verdicts`: accepted into the mempool, or rejected with the
    /// reason. Only the v2 loop sends verdicts.
    pub verdicts: bool,
    /// The most transactions of one sender that wait for a nonce gap.
    pub max_parked: NonZeroUsize,
    /// The most forwarded transactions of one sender that no row confirmed.
    pub max_forwarded: NonZeroUsize,
    /// The blocks after which an unconfirmed forwarded transaction is lost.
    /// It must exceed the blocks from an offer to the rows of its block.
    pub loss_blocks: NonZeroU64,
    /// The 32-byte units of an image after which a parked transaction
    /// expires. Half the sealer window, so a released transaction is still
    /// inside the window of the sealer.
    pub park_width_units: NonZeroU64,
    /// The senders above which a sweep forgets the idle ones.
    pub max_senders: NonZeroUsize,
    /// The time between two sweeps, in milliseconds.
    pub sweep_every_ms: u64,
    /// The time after which a sender whose state came from a lookup asks
    /// again, while a parked transaction waits for a gap over it, in
    /// milliseconds.
    pub relook_ms: u64,
    /// The offers that may wait for the sealer.
    pub outbox_cap: NonZeroUsize,
}

impl Default for AdmissionSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            verdicts: false,
            max_parked: NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN),
            max_forwarded: NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
            loss_blocks: NonZeroU64::new(16).unwrap_or(NonZeroU64::MIN),
            park_width_units: NonZeroU64::new(1 << 17).unwrap_or(NonZeroU64::MIN),
            max_senders: NonZeroUsize::new(1 << 20).unwrap_or(NonZeroUsize::MIN),
            sweep_every_ms: 50,
            relook_ms: 500,
            outbox_cap: NonZeroUsize::new(4096).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

impl AdmissionSettings {
    /// The settings of the loop of `partition`. A parked transaction
    /// expires after `tx_ttl`, the lifetime of the sequencer.
    #[must_use]
    pub fn loop_config(&self, tx_ttl: Duration, partition: u32) -> V2Config {
        V2Config {
            partition,
            admission: AdmissionConfig {
                max_parked: self.max_parked,
                max_forwarded: self.max_forwarded,
                loss_blocks: self.loss_blocks,
                park_width: self.park_width_units,
                park_ttl: tx_ttl,
                relook_after: Duration::from_millis(self.relook_ms),
                max_senders: self.max_senders,
            },
            sweep_every: Duration::from_millis(self.sweep_every_ms),
            outbox_cap: self.outbox_cap,
        }
    }
}

/// The settings of the loop.
#[derive(Clone, Copy, Debug)]
pub struct V2Config {
    /// The partition label of the metrics.
    pub partition: u32,
    pub admission: AdmissionConfig,
    /// The time between two sweeps of the filter.
    pub sweep_every: Duration,
    /// The offers that may wait for the sealer before the loop stops taking
    /// envelopes.
    pub outbox_cap: NonZeroUsize,
}

/// One change of the chain state that the filter holds.
#[derive(Clone, Debug)]
pub enum AccountEvent {
    /// The account rows of a block, for the senders of this replica.
    Rows {
        block: u64,
        rows: Vec<(Address, AccountState)>,
    },
    /// The answer of a lookup.
    Answer {
        address: Address,
        state: AccountState,
    },
    /// The executor rolled back.
    Rollback,
    /// The sealer refused a reference without bytes (egress kind 18).
    /// Only the named transaction leaves; its suffix waits for a resend.
    Refused(Box<kardamom_cluster_adapter::wire::RefRefused>),
    /// A guard refusal retains the run for replay. A past-deadline refusal
    /// removes the named copy and parks its suffix behind the missing nonce.
    Refusal(Box<crate::resync::SealerRefusal>),
    /// Backpressure at the sealer requests another offer of a held run.
    Retry { sender: Address, nonce: u64 },
    /// The contiguity guard of the sealer rejected the forwarded
    /// transaction of `sender` at `nonce`, because it expects `expected`
    /// (egress kind 5).
    Rejected {
        sender: Address,
        nonce: u64,
        expected: u64,
    },
}

/// What the loop needs to offer one forwarded transaction.
#[derive(Clone, Debug)]
pub struct RefPayload {
    /// The vslot of the sender, for the parked depth.
    vslot: u8,
    lane: u8,
    loc: TxDataLoc,
    /// The deadline of the envelope, for the deadline field below decision
    /// version 4.
    max_inclusion_block: u64,
    /// The position of the transaction in its image, for the deadline field
    /// from decision version 4 on.
    stream_position: u64,
    tip: u128,
    /// The archived envelope, when the sealer carried the bytes at
    /// admission. A parked or forwarded transaction keeps it until it goes
    /// out or a row confirms it: the filter bounds the parked and the
    /// forwarded transactions of each sender, and the park width, the time
    /// to live and the loss window bound how long they stay.
    envelope: Option<bytes::Bytes>,
}

/// The publish loop of sequencer v2 for one replica.
pub struct AdmissionLoop {
    cfg: V2Config,
    admission: Admission<RefPayload>,
    vslots: VslotSet,
    sealer_version: SealerVersion,
    session_generation: u64,
    fees: FeeGate,
    pause: PauseGate,
    accounts: crossbeam_channel::Receiver<AccountEvent>,
    lookups: tokio::sync::mpsc::Sender<Address>,
    verdicts: Vec<Verdict<RefPayload>>,
    outbox: VecDeque<RefOffer>,
    last_sweep: Instant,
    hot: HotMetrics,
    depth: depth::ParkDepth,
    to_ingress: verdicts::Verdicts,
    pipeline: PublishMeters,
    /// The offers in the outbox.
    outbox_depth: Queue,
    /// The account events that wait in `accounts`.
    accounts_depth: Queue,
}

impl AdmissionLoop {
    #[must_use]
    pub fn new(
        cfg: V2Config,
        vslots: VslotSet,
        accounts: crossbeam_channel::Receiver<AccountEvent>,
        lookups: tokio::sync::mpsc::Sender<Address>,
    ) -> Self {
        Self {
            admission: Admission::new(cfg.admission, Instant::now()),
            cfg,
            vslots,
            sealer_version: SealerVersion::default(),
            session_generation: 0,
            fees: FeeGate::off(),
            pause: PauseGate::default(),
            accounts,
            lookups,
            verdicts: Vec::new(),
            outbox: VecDeque::new(),
            last_sweep: Instant::now(),
            hot: HotMetrics::new(cfg.partition),
            depth: depth::ParkDepth::default(),
            to_ingress: verdicts::Verdicts::off(cfg.partition),
            pipeline: PublishMeters::new(),
            outbox_depth: Queue::new("sequencer.outbox"),
            accounts_depth: Queue::new("sequencer.accounts"),
        }
    }

    /// Send the answer to each transaction to the ingress that published
    /// it, through `out`.
    pub fn enable_verdicts(&mut self, out: tokio::sync::mpsc::Sender<VerdictOut>) {
        self.to_ingress.to(out);
    }

    /// Follow the decision version that the sealer reports. See
    /// [`crate::sequencer::Sequencer::enable_positioned_refs`].
    pub fn enable_positioned_refs(&mut self, version: SealerVersion) {
        self.session_generation = version.session_generation();
        self.sealer_version = version;
    }

    /// The fee checks of the tip and of the base fee. The filter checks the
    /// balance, so the gate must hold no account layer.
    pub fn enable_fees(&mut self, gate: FeeGate) {
        self.fees = gate;
    }

    /// Offer nothing and leave envelopes in the archive while the
    /// process follows a lifecycle pause.
    pub fn enable_pause(
        &mut self,
        rx: tokio::sync::watch::Receiver<kardamom_obs::lifecycle::Slots>,
    ) {
        self.pause = PauseGate::new(rx);
    }

    /// One turn: apply the account events, sweep when due, publish what
    /// waits, then take one envelope. Returns whether it did any work.
    /// A lifecycle pause holds publication and intake. Account events
    /// still drain so the feed never waits on the pause.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::Backpressure`] while the sealer takes no
    /// offer, and the errors of the input and of the decode.
    pub fn run_once<P: SequencerPorts>(&mut self, ports: &mut P) -> Result<bool, SequencerError> {
        let applied = self.apply_accounts();
        let (input, refs, side) = ports.split();
        side.flush_events();
        let generation = self.sealer_version.session_generation();
        let renewed = generation != self.session_generation;
        if renewed {
            self.session_generation = generation;
            self.offer_again_unconfirmed("sealer session changed");
        }
        let paused = self.pause.read_with(|paused| {
            if !paused {
                OfferReplay::new(
                    &mut self.admission,
                    &mut self.outbox,
                    &self.sealer_version,
                    self.cfg.partition,
                )
                .resume();
            }
        });
        self.sweep_if_due(Instant::now(), paused);
        self.dispatch(side);
        if paused {
            return Ok(applied || renewed);
        }
        self.flush(refs, side)?;
        if self.outbox.len() >= self.cfg.outbox_cap.get() {
            return Ok(true);
        }
        let Some(inbound) = input.poll()? else {
            return Ok(applied || renewed);
        };
        self.admit(&inbound, side)?;
        self.dispatch(side);
        self.flush(refs, side)?;
        Ok(true)
    }

    /// Apply the account events that wait. Returns whether there was one.
    fn apply_accounts(&mut self) -> bool {
        let events: Vec<AccountEvent> = self.accounts.try_iter().collect();
        let applied = !events.is_empty();
        // Every event applies, including rows after a replay request.
        let replay = events
            .into_iter()
            .fold(false, |replay, event| self.apply(event) | replay);
        if replay {
            self.replay_after_refusals();
        }
        applied
    }

    fn apply(&mut self, event: AccountEvent) -> bool {
        match event {
            AccountEvent::Rows { block, rows } => {
                self.admission.on_rows(block, rows, &mut self.verdicts);
            }
            AccountEvent::Answer { address, state } => {
                self.admission.on_lookup(address, state, &mut self.verdicts);
            }
            AccountEvent::Rollback => self.admission.on_rollback(),
            AccountEvent::Refused(refused) => {
                return self.on_reference_refused(*refused);
            }
            AccountEvent::Refusal(refusal) => return self.on_sealer_refusal(*refusal),
            AccountEvent::Retry { sender, nonce } => {
                return self.admission.unconfirmed_at(sender, nonce).is_some();
            }
            AccountEvent::Rejected {
                sender,
                nonce,
                expected,
            } => self.offer_rejected(sender, nonce, expected),
        }
        false
    }

    /// The guard of the sealer expects `expected` of `sender`, and rejected
    /// `nonce`. Offer the transactions at both nonces again, when the
    /// filter holds the one at `expected`. Each such reject moves the
    /// expected nonce up by at least one, so the offers end. A guard that
    /// expects a nonce that the filter does not hold changes nothing: the
    /// loss window frees the run.
    fn offer_rejected(&mut self, sender: Address, nonce: u64, expected: u64) {
        if self.admission.unconfirmed_at(sender, expected).is_none() {
            return;
        }
        let offers: Vec<RefOffer> = [expected, nonce]
            .into_iter()
            .filter_map(|n| self.admission.unconfirmed_at(sender, n))
            .map(|c| c.ref_offer(&self.sealer_version))
            .collect();
        self.outbox.extend(offers);
    }

    fn offer_again_unconfirmed(&mut self, reason: &str) {
        OfferReplay::new(
            &mut self.admission,
            &mut self.outbox,
            &self.sealer_version,
            self.cfg.partition,
        )
        .queue(reason);
    }

    fn sweep_if_due(&mut self, now: Instant, paused: bool) {
        if now.duration_since(self.last_sweep) < self.cfg.sweep_every {
            return;
        }
        self.last_sweep = now;
        if paused {
            self.admission.sweep_while_paused(now, &mut self.verdicts);
        } else {
            self.admission.sweep(now, &mut self.verdicts);
        }
        self.depth.publish(self.cfg.partition);
    }

    /// Guard, decode and check one envelope, and give it to the filter.
    fn admit<R: SideChannelPublisher>(
        &mut self,
        inbound: &Inbound,
        side: &mut R,
    ) -> Result<(), SequencerError> {
        let sender = sender_of(&inbound.envelope);
        let vslot = vslot_for(sender);
        if !self.vslots.contains(vslot) {
            self.hot.wrong_shard.increment(1);
            return Ok(());
        }
        self.hot.ingest.increment(1);
        let fields = decode_fields(&inbound.envelope.raw_tx)?;
        let ingress = TxEnvelope::ingress_id_of(inbound.envelope.correlation_id);
        let tip = match self.fees.admit(sender, &fields.fees) {
            Ok(tip) => tip,
            Err(reason) => {
                self.hot.fee_rejected.increment(1);
                let ack = Ack {
                    ingress,
                    sender,
                    nonce: fields.nonce,
                    tx_hash: inbound.envelope.tx_hash,
                };
                self.to_ingress.reject(ack, reason.clone());
                reject(side, ack.tx_hash, sender, fields.nonce, reason);
                return Ok(());
            }
        };
        let candidate = Candidate {
            sender,
            nonce: fields.nonce,
            tx_hash: inbound.envelope.tx_hash,
            cost: fields.fees.fees.worst_case_cost(fields.fees.value),
            image: ImagePos {
                lane: inbound.lane,
                session: inbound.loc.session_id,
                unit: inbound.stream_position >> UNIT_SHIFT,
            },
            ingress,
            answered: false,
            payload: RefPayload {
                vslot,
                lane: inbound.lane,
                loc: inbound.loc,
                max_inclusion_block: inbound.envelope.max_inclusion_block,
                stream_position: inbound.stream_position,
                tip,
                envelope: self.carried(&inbound.envelope)?,
            },
        };
        self.depth.took(vslot);
        let started = Instant::now();
        self.admission.offer(candidate, &mut self.verdicts);
        self.hot
            .nonce_check_seconds
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }

    /// The archived envelope for the offer, when the sealer carries the
    /// bytes.
    fn carried(&self, envelope: &TxEnvelope) -> Result<Option<bytes::Bytes>, SequencerError> {
        if !self.sealer_version.carries_bytes() {
            return Ok(None);
        }
        kardamom_log::codec::encode(envelope)
            .map(|archived| Some(bytes::Bytes::from(archived.into_vec())))
            .map_err(|e| SequencerError::EncodeFailed(format!("envelope: {e}")))
    }

    /// Turn the verdicts into offers, client errors and lookups.
    fn dispatch<R: SideChannelPublisher>(&mut self, side: &mut R) {
        // The vector goes back after the drain, so its capacity stays.
        let mut verdicts = std::mem::take(&mut self.verdicts);
        for verdict in verdicts.drain(..) {
            self.dispatch_one(verdict, side);
        }
        self.verdicts = verdicts;
    }

    fn dispatch_one<R: SideChannelPublisher>(
        &mut self,
        verdict: Verdict<RefPayload>,
        side: &mut R,
    ) {
        match verdict {
            Verdict::Forward(mut c, _) => {
                self.depth.gave(c.payload.vslot);
                if let Some(ack) = c.answer() {
                    self.to_ingress.accept(ack);
                }
                self.outbox.push_back(c.ref_offer(&self.sealer_version));
            }
            Verdict::Reject(mut c, refusal) => {
                self.depth.gave(c.payload.vslot);
                self.count_refusal(refusal);
                let reason = reason_of(refusal);
                if let Some(ack) = c.answer() {
                    self.to_ingress.reject(ack, reason.clone());
                }
                reject(side, c.tx_hash, c.sender, c.nonce, reason);
            }
            Verdict::SealerRefused(mut c, reason) => {
                if let Some(ack) = c.answer() {
                    self.to_ingress.reject(ack, reason.clone());
                }
                reject(side, c.tx_hash, c.sender, c.nonce, reason);
            }
            Verdict::DropReference(c) => side.publish_drop(TxDrop {
                tx_hash: c.tx_hash,
                sender: c.sender,
                nonce: c.nonce,
                reason: DropReason::SealerRefusedReference,
            }),
            Verdict::Accept(ack) => self.to_ingress.accept(ack),
            Verdict::Lookup(address) => {
                self.hot.lookup_requests.increment(1);
                if self.lookups.try_send(address).is_err() {
                    crate::metrics::record_nonce_lookup(self.cfg.partition, "shed");
                    tracing::warn!(%address, "the lookup queue is full; the sender waits for its rows");
                }
            }
        }
    }

    /// Offer what waits in the outbox, in order, and report each offer
    /// that went out.
    fn flush<B, R>(&mut self, refs: &mut B, side: &mut R) -> Result<(), SequencerError>
    where
        B: TxOrderingRefPublisher,
        R: SideChannelPublisher,
    {
        if self.outbox.is_empty() {
            return Ok(());
        }
        let (published, err) = self.pipeline.offer(refs, self.outbox.make_contiguous());
        self.hot.publish.increment(published as u64);
        for offer in self.outbox.drain(..published) {
            side.publish_status(TxStatus::offered(
                offer.tx_ref.tx_hash,
                offer.guard.sender,
                offer.guard.nonce,
            ));
            side.note_sent(&offer.tx_ref.tx_hash);
        }
        match err {
            Some(SequencerError::SessionChanged) => {
                self.offer_again_unconfirmed("sealer session changed");
                Ok(())
            }
            Some(SequencerError::Backpressure) => {
                self.hot.backpressure.increment(1);
                Err(SequencerError::Backpressure)
            }
            other => other.map_or(Ok(()), Err),
        }
    }

    /// Count the refusals that the v1 loop counts: the parked transactions
    /// that expired, and those that the bound of a sender evicted.
    fn count_refusal(&self, refusal: Refusal) {
        match refusal {
            Refusal::Expired { .. } => self.hot.expired.increment(1),
            Refusal::TooManyPending { .. } => self.hot.evictions.increment(1),
            Refusal::NonceTooLow { .. }
            | Refusal::InFlight { .. }
            | Refusal::InsufficientFunds { .. } => {}
        }
    }
}

/// The client error of a refusal of the filter.
fn reason_of(refusal: Refusal) -> TxErrorReason {
    match refusal {
        Refusal::NonceTooLow { chain_nonce } => TxErrorReason::DuplicatedTx {
            expected_nonce: chain_nonce,
        },
        Refusal::InFlight { expected_nonce } => TxErrorReason::AlreadyKnown { expected_nonce },
        Refusal::InsufficientFunds { available, cost } => TxErrorReason::InsufficientFunds {
            have: available.saturating_to(),
            want: cost.saturating_to(),
        },
        Refusal::TooManyPending { expected_nonce } => TxErrorReason::Evicted { expected_nonce },
        Refusal::Expired { expected_nonce } => TxErrorReason::Expired { expected_nonce },
    }
}

/// Tell the client on both side channels that its transaction is refused,
/// and the egress that it dropped.
fn reject<R: SideChannelPublisher>(
    side: &mut R,
    tx_hash: B256,
    sender: Address,
    nonce: u64,
    reason: TxErrorReason,
) {
    let err = TxError {
        sender,
        nonce,
        reason,
    };
    side.publish_status(TxStatus::rejected(tx_hash, &err));
    side.publish_drop(TxDrop {
        tx_hash,
        sender,
        nonce,
        reason: err.reason.drop_reason(),
    });
    side.publish_error(err);
}

#[cfg(test)]
mod resend_tests;
#[cfg(test)]
mod tests;
