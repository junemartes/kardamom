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
//! - A refused one goes to the client on `tx_errors` and `tx_status`.
//! - A sender with no state goes to the lookup task.
//!
//! The loop keeps no ledger of offers and no resync: the sealer orders a
//! reference once, and the filter forgets a forwarded run that no row
//! confirms within its loss window. Offers wait in an outbox while the
//! sealer is back-pressured; a full outbox stops the intake.
//!
//! The loop records the counters of the v1 loop under the same names, and
//! the parked depth of each vslot ([`depth`]) at each sweep.

mod depth;
mod verdicts;

pub use verdicts::VerdictOut;

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::{GuardHeader, position_deadline};
use kardamom_log::aeron_live::{IdleBackoff, IdleCap};
use kardamom_types::shard_map::{VslotSet, vslot_for};
use kardamom_types::{
    AccountRow, Receipt, ReceiptRollback, TxDataLoc, TxEnvelope, TxError, TxErrorReason, TxStatus,
};

use crate::admission::{
    AccountState, Admission, AdmissionConfig, Candidate, ImagePos, Refusal, Verdict,
};
use crate::error::SequencerError;
use crate::fees::FeeGate;
use crate::inbound::{Inbound, TxDataSubscriber};
use crate::metrics::HotMetrics;
use crate::outbound::{RefOffer, SideChannelPublisher, TxOrderingRefPublisher};
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
    /// Send the receipt of each forwarded transaction to the ingress that
    /// published it, on `tx_verdicts`. Only the v2 loop sends verdicts.
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
    /// The receipts and the rows of a batch for the senders of this
    /// replica, for the verdicts. They come before the `Rows` event of the
    /// same batch, which confirms their transactions.
    Receipts {
        receipts: Vec<Receipt>,
        rows: Vec<AccountRow>,
    },
    /// The executor rolled back.
    Rollback(ReceiptRollback),
}

/// What the loop needs to offer one forwarded transaction.
#[derive(Debug)]
pub struct RefPayload {
    tx_hash: B256,
    /// The vslot of the sender, for the parked depth.
    vslot: u8,
    lane: u8,
    loc: TxDataLoc,
    /// The deadline field of the guard header.
    deadline: u64,
    tip: u128,
    /// The archived envelope, when the sealer carries the bytes. A parked
    /// transaction keeps it until it goes out: the filter bounds the parked
    /// transactions of each sender, and the park width and time to live
    /// bound how long they stay.
    envelope: Option<bytes::Bytes>,
}

/// The publish loop of sequencer v2 for one replica.
pub struct AdmissionLoop {
    cfg: V2Config,
    admission: Admission<RefPayload>,
    vslots: VslotSet,
    sealer_version: SealerVersion,
    fees: FeeGate,
    accounts: crossbeam_channel::Receiver<AccountEvent>,
    lookups: tokio::sync::mpsc::Sender<Address>,
    verdicts: Vec<Verdict<RefPayload>>,
    outbox: VecDeque<RefOffer>,
    last_sweep: Instant,
    hot: HotMetrics,
    depth: depth::ParkDepth,
    to_ingress: verdicts::Verdicts,
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
            fees: FeeGate::off(),
            accounts,
            lookups,
            verdicts: Vec::new(),
            outbox: VecDeque::new(),
            last_sweep: Instant::now(),
            hot: HotMetrics::new(cfg.partition),
            depth: depth::ParkDepth::default(),
            to_ingress: verdicts::Verdicts::off(cfg.partition),
        }
    }

    /// Send the receipt of each forwarded transaction to the ingress that
    /// published it, through `out`.
    pub fn enable_verdicts(&mut self, out: tokio::sync::mpsc::Sender<VerdictOut>) {
        self.to_ingress.to(out);
    }

    /// Follow the decision version that the sealer reports. See
    /// [`crate::sequencer::Sequencer::enable_positioned_refs`].
    pub fn enable_positioned_refs(&mut self, version: SealerVersion) {
        self.sealer_version = version;
    }

    /// The fee checks of the tip and of the base fee. The filter checks the
    /// balance, so the gate must hold no account layer.
    pub fn enable_fees(&mut self, gate: FeeGate) {
        self.fees = gate;
    }

    /// One turn: apply the account events, sweep when due, publish what
    /// waits, then take one envelope. Returns whether it did any work.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::Backpressure`] while the sealer takes no
    /// offer, and the errors of the input and of the decode.
    pub fn run_once<P: SequencerPorts>(&mut self, ports: &mut P) -> Result<bool, SequencerError> {
        let applied = self.apply_accounts();
        self.sweep_if_due(Instant::now());
        let (input, refs, side) = ports.split();
        self.dispatch(side);
        self.flush(refs, side)?;
        if self.outbox.len() >= self.cfg.outbox_cap.get() {
            return Ok(true);
        }
        let Some(inbound) = input.poll()? else {
            return Ok(applied);
        };
        self.admit(&inbound, side)?;
        self.dispatch(side);
        self.flush(refs, side)?;
        Ok(true)
    }

    /// Run until `shutdown` or until the input closes. An idle turn waits
    /// with the same backoff as the v1 loop; a back-pressured one waits
    /// 10 µs.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::run_once`] other than back-pressure
    /// and a closed input.
    pub fn run<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        shutdown: &crate::sequencer::Shutdown,
        idle_cap: IdleCap,
    ) -> Result<(), SequencerError> {
        let mut backoff = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1)
            .under(idle_cap, Duration::from_micros(1));
        while !shutdown.is_signaled() && self.run_tick(ports, &mut backoff)? {}
        Ok(())
    }

    /// One turn of [`Self::run`]. Returns whether to go on.
    fn run_tick<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        backoff: &mut IdleBackoff,
    ) -> Result<bool, SequencerError> {
        match self.run_once(ports) {
            Ok(true) => {
                backoff.reset();
                Ok(true)
            }
            Ok(false) => {
                std::thread::sleep(backoff.idle_wait());
                Ok(true)
            }
            Err(SequencerError::Backpressure) => {
                std::thread::sleep(Duration::from_micros(10));
                Ok(true)
            }
            Err(SequencerError::IngressDisconnected) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Apply the account events that wait. Returns whether there was one.
    fn apply_accounts(&mut self) -> bool {
        let events: Vec<AccountEvent> = self.accounts.try_iter().collect();
        let applied = !events.is_empty();
        for event in events {
            self.apply(event);
        }
        applied
    }

    fn apply(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Rows { block, rows } => {
                self.admission.on_rows(block, rows, &mut self.verdicts);
            }
            AccountEvent::Answer { address, state } => {
                self.admission.on_lookup(address, state, &mut self.verdicts);
            }
            AccountEvent::Receipts { receipts, rows } => {
                self.to_ingress.on_receipts(&self.admission, receipts, rows);
            }
            AccountEvent::Rollback(rollback) => {
                self.admission.on_rollback();
                self.to_ingress.on_rollback(rollback);
            }
        }
    }

    fn sweep_if_due(&mut self, now: Instant) {
        if now.duration_since(self.last_sweep) < self.cfg.sweep_every {
            return;
        }
        self.last_sweep = now;
        self.admission.sweep(now, &mut self.verdicts);
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
        let tip = match self.fees.admit(sender, &fields.fees) {
            Ok(tip) => tip,
            Err(reason) => {
                self.hot.fee_rejected.increment(1);
                reject(side, inbound.envelope.tx_hash, sender, fields.nonce, reason);
                return Ok(());
            }
        };
        let deadline = self.guard_deadline(
            inbound.envelope.max_inclusion_block,
            inbound.stream_position,
        );
        let candidate = Candidate {
            sender,
            nonce: fields.nonce,
            cost: fields.fees.fees.worst_case_cost(fields.fees.value),
            image: ImagePos {
                lane: inbound.lane,
                session: inbound.loc.session_id,
                unit: inbound.stream_position >> UNIT_SHIFT,
            },
            ingress: TxEnvelope::ingress_id_of(inbound.envelope.correlation_id),
            payload: RefPayload {
                tx_hash: inbound.envelope.tx_hash,
                vslot,
                lane: inbound.lane,
                loc: inbound.loc,
                deadline,
                tip,
                envelope: self.carried(&inbound.envelope)?,
            },
        };
        self.depth.took(vslot);
        self.admission.offer(candidate, &mut self.verdicts);
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

    /// The deadline field: the tagged position under decision version 4,
    /// the envelope's deadline otherwise.
    fn guard_deadline(&self, max_inclusion_block: u64, stream_position: u64) -> u64 {
        if !self.sealer_version.orders_by_position() {
            return max_inclusion_block;
        }
        position_deadline(stream_position).unwrap_or(max_inclusion_block)
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
            Verdict::Forward(c) => {
                self.depth.gave(c.payload.vslot);
                self.outbox.push_back(offer_of(&c));
            }
            Verdict::Reject(c, refusal) => {
                self.depth.gave(c.payload.vslot);
                self.count_refusal(refusal);
                reject(
                    side,
                    c.payload.tx_hash,
                    c.sender,
                    c.nonce,
                    reason_of(refusal),
                );
            }
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
        let (published, err) = refs.try_publish_ref_batch(self.outbox.make_contiguous());
        self.hot.publish.increment(published as u64);
        if matches!(err, Some(SequencerError::Backpressure)) {
            self.hot.backpressure.increment(1);
        }
        for offer in self.outbox.drain(..published) {
            side.publish_status(TxStatus::offered(
                offer.tx_ref.tx_hash,
                offer.guard.sender,
                offer.guard.nonce,
            ));
        }
        err.map_or(Ok(()), Err)
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

fn offer_of(c: &Candidate<RefPayload>) -> RefOffer {
    RefOffer {
        tx_ref: kardamom_types::TxRef::new(
            c.payload.tx_hash,
            c.payload.lane,
            c.payload.loc.position,
            c.payload.loc.session_id,
        ),
        guard: GuardHeader {
            sender: c.sender,
            nonce: c.nonce,
            max_inclusion_block: c.payload.deadline,
            tip: c.payload.tip,
        },
        envelope: c.payload.envelope.clone(),
    }
}

/// The client error of a refusal of the filter.
fn reason_of(refusal: Refusal) -> TxErrorReason {
    match refusal {
        Refusal::NonceTooLow { chain_nonce } => TxErrorReason::DuplicatedTx {
            expected_nonce: chain_nonce,
        },
        Refusal::InFlight { expected_nonce } => TxErrorReason::DuplicatedTx { expected_nonce },
        Refusal::InsufficientFunds { available, cost } => TxErrorReason::InsufficientFunds {
            have: available.saturating_to(),
            want: cost.saturating_to(),
        },
        Refusal::TooManyPending { expected_nonce } => TxErrorReason::Evicted { expected_nonce },
        Refusal::Expired { expected_nonce } => TxErrorReason::Expired { expected_nonce },
    }
}

/// Tell the client on both side channels that its transaction is refused.
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
    side.publish_error(err);
}

#[cfg(test)]
mod tests;
