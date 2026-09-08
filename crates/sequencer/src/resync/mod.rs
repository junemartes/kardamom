//! Lag detection and receipt-floor resync.
//!
//! A replica that falls far enough behind its twin can drain re-offers past the
//! cluster's first-seen dedup horizon. This orders the same transaction
//! canonically twice, which is fatal to the executor and poisons recovery
//! replay. The guard splits into a provably safe response, and cheap
//! triggers:
//!
//! - Response ([`ResyncController::floor`], read through
//!   `Sequencer::proven_executed`): while in resync mode, skip a publish
//!   only if the sender's executed-truth floor (derived from the
//!   `tx_receipts` stream, [`FloorUpdate`]) proves the nonce already
//!   executed. A skip backed by a receipt needs no
//!   dedup-window guarantee at all. Everything unproven is published, so
//!   every degraded mode (missed receipts, late subscribe) degrades
//!   toward publish, the side the layered dedup windows guard, and never
//!   toward skip (a canonical nonce gap, which nothing guards; see
//!   [`crate::state::PartitionState::advance_floor`]'s note).
//! - Triggers: the primary signal is the canonical-count watermark. The
//!   cluster broadcasts every boundary (`end_tx_idx`, the global
//!   canonical count) to publisher sessions too, so the horizon is
//!   measured in its native units, with no clocks and no wire change. A
//!   watermark jump larger than `enter_fraction × dedup_capacity` means
//!   this process was blind while that many records were ordered (a
//!   freeze or pause). Watermark silence means it is partitioned from
//!   egress. A sustained publish stall and startup round out the
//!   triggers. False positives only cost floor lookups, which is what
//!   lets the triggers stay twitchy and local.

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

use crate::metrics;

/// One executed-truth observation from the `tx_receipts` stream.
/// `sender`'s transaction at `executed_nonce` produced a receipt, so the
/// sender's floor is at least `executed_nonce + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorUpdate {
    pub sender: Address,
    pub executed_nonce: u64,
    /// An L1-originated deposit. It consumes no L2 nonce, so it is
    /// neither floor evidence nor a publish confirmation. This is
    /// explicit because a deposit carries `Receipt::tx_type ==
    /// TX_TYPE_DEPOSIT`. The nonce-0 heuristic this replaces could not
    /// tell a deposit apart from a genuine nonce-0 transaction.
    pub deposit: bool,
    /// A marker receipt: the transaction was ordered (canonical-log
    /// commitment is proven, so it confirms publishes), but it consumed
    /// no nonce, so it is not floor evidence. `Some` carries the typed
    /// cause. The floor logic only asks "is this a skip?" today. Reason
    /// specific handling (drop on `NonceTooLow`, evict on `NonceTooHigh`)
    /// is a future step.
    pub skip_reason: Option<kardamom_types::SkipReason>,
}

impl FloorUpdate {
    /// A non-skip execution receipt for `sender` at `nonce`: confirms the
    /// publish, and is floor evidence (raises the floor to `nonce + 1`).
    #[must_use]
    pub fn executed(sender: Address, nonce: u64) -> Self {
        Self {
            sender,
            executed_nonce: nonce,
            deposit: false,
            skip_reason: None,
        }
    }

    /// A skip receipt for `sender` at `nonce`: confirms the publish, but
    /// is not floor evidence (it consumed no nonce).
    #[must_use]
    pub fn skip(sender: Address, nonce: u64, reason: kardamom_types::SkipReason) -> Self {
        Self {
            sender,
            executed_nonce: nonce,
            deposit: false,
            skip_reason: Some(reason),
        }
    }

    /// A deposit receipt for `sender`: carries the filler nonce 0, and is
    /// neither a confirmation nor floor evidence (see the `deposit` field
    /// doc for why).
    #[must_use]
    pub fn deposit(sender: Address) -> Self {
        Self {
            sender,
            executed_nonce: 0,
            deposit: true,
            skip_reason: None,
        }
    }
}

/// Shared state between the egress-watermark FEED thread and the publish
/// loop's controller:
///
/// - `count`: the latest boundary `end_tx_idx` (the global canonical
///   count).
/// - `lag_gap_ms`: a sticky lag flag. The feed thread sets it when it
///   sees a boundary inter-arrival gap past the silence threshold, and
///   the controller consumes it (swaps it to 0) on its next iteration.
///   This flag is sticky because the publish loop can be blocked for long
///   stretches exactly when lag happens (`LiveIngress::offer` waits on
///   the session thread, which may be mid-reconnect after a process
///   freeze). A point-in-time check that needs the loop running to catch
///   it can miss a freeze entirely. Boundary arrivals are the liveness
///   signal, not count changes: idle traffic emits boundaries every tick
///   with an unchanged count, which a value-change tracker would mistake
///   for silence.
#[derive(Clone, Default)]
pub struct SharedWatermark {
    count: Arc<AtomicU64>,
    lag_gap_ms: Arc<AtomicU64>,
}

impl SharedWatermark {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn store(&self, count: u64) {
        self.count.store(count, Ordering::Release);
    }
    #[must_use]
    pub fn load(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }
    /// Feed thread: flag an observed boundary-arrival gap, in
    /// milliseconds. Keeps the largest unconsumed gap, so a later,
    /// smaller gap cannot hide a freeze.
    pub fn flag_lag(&self, gap_ms: u64) {
        self.lag_gap_ms.fetch_max(gap_ms, Ordering::AcqRel);
    }
    /// Controller: consume the pending lag flag, if any.
    #[must_use]
    pub fn take_lag(&self) -> Option<u64> {
        match self.lag_gap_ms.swap(0, Ordering::AcqRel) {
            0 => None,
            gap => Some(gap),
        }
    }
}

/// `[resync]` TOML section and CLI settings. All defaults follow the spec.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ResyncConfig {
    /// Must equal the cluster's `-Dkardamom.cluster.dedupCapacity`: the
    /// horizon this mechanism protects. Logged at startup for the
    /// contract check. Never zero: a zero-capacity dedup window is not a
    /// valid deployment, so serde rejects a `0` at TOML-parse time, the
    /// same parse-once boundary `SequencerConfig::partition_count` uses.
    pub dedup_capacity: NonZeroU64,
    /// Watermark-jump and gap enter threshold, as a percent of the
    /// capacity. This is an integer so the config stays `Eq` (the spec's
    /// 0.25 fraction is 25 here). Never zero: a zero percent would mean
    /// "always resync", which is never the intended setting.
    pub enter_percent: NonZeroU64,
    /// Boundary-silence trigger. No watermark change for this long
    /// enters resync. Sized as the spec's `boundary_silence_ticks ×
    /// cluster tick interval` (5 × 2000 ms deploy tick).
    pub boundary_silence_ms: u64,
    /// A publish stall (continuous backpressure, or no successful publish
    /// while work is pending) longer than this enters resync. This is the
    /// fallback trigger for the no-egress-signal case.
    pub publish_stall_ms: u64,
    /// How long conditions must stay calm before exiting resync (hysteresis).
    pub exit_hold_ms: u64,
    /// How long a published ref may stay unconfirmed (no receipt at or
    /// above its nonce) before it is rewound and republished. This must
    /// comfortably exceed the order-execute-receipt round trip under load.
    /// Republishing early is harmless, since dedup absorbs it. Republishing
    /// late leaves voided refs unrecovered longer.
    pub confirm_timeout_ms: u64,
}

impl Default for ResyncConfig {
    fn default() -> Self {
        Self {
            dedup_capacity: NonZeroU64::new(1 << 17).expect("1 << 17 != 0"),
            enter_percent: NonZeroU64::new(25).expect("25 != 0"),
            boundary_silence_ms: 10_000,
            publish_stall_ms: 10_000,
            exit_hold_ms: 2_000,
            confirm_timeout_ms: 15_000,
        }
    }
}

/// A `[resync]` setting combination this process refuses to run with.
#[derive(Debug, thiserror::Error)]
pub enum ResyncConfigError {
    #[error(
        "resync: dedup_capacity {dedup_capacity} * enter_percent {enter_percent} overflows u64"
    )]
    ThresholdOverflow {
        dedup_capacity: u64,
        enter_percent: u64,
    },
    #[error(
        "resync: dedup_capacity {dedup_capacity} * enter_percent {enter_percent} / 100 rounds \
         down to 0; raise enter_percent or dedup_capacity"
    )]
    ThresholdTooSmall {
        dedup_capacity: u64,
        enter_percent: u64,
    },
    #[error("resync: dedup_capacity {dedup_capacity} does not fit in usize on this target")]
    CapacityNotUsize { dedup_capacity: u64 },
}

impl ResyncConfig {
    /// Watermark jump/gap threshold in records, computed once (by
    /// [`ResyncController::new`]) rather than on every [`ResyncController::observe`]
    /// call.
    ///
    /// # Errors
    ///
    /// Returns [`ResyncConfigError::ThresholdOverflow`] if
    /// `dedup_capacity * enter_percent` overflows `u64`, or
    /// [`ResyncConfigError::ThresholdTooSmall`] if the product rounds down
    /// to a threshold of 0 records (which would mean "resync on every
    /// watermark tick"). Neither is silently clamped: a threshold that
    /// must be at least 1 is a `NonZeroU64`, not a `.max(1)` fixup.
    pub fn enter_threshold(&self) -> Result<NonZeroU64, ResyncConfigError> {
        let product = self
            .dedup_capacity
            .get()
            .checked_mul(self.enter_percent.get())
            .ok_or(ResyncConfigError::ThresholdOverflow {
                dedup_capacity: self.dedup_capacity.get(),
                enter_percent: self.enter_percent.get(),
            })?;
        NonZeroU64::new(product / 100).ok_or(ResyncConfigError::ThresholdTooSmall {
            dedup_capacity: self.dedup_capacity.get(),
            enter_percent: self.enter_percent.get(),
        })
    }

    /// Check this `[resync]` section at the config parse-once boundary,
    /// before anything is built from it: [`Self::enter_threshold`] must
    /// compute, and `dedup_capacity` must fit in a `usize` on this
    /// target. [`ResyncController::new`] and [`ResyncChannel::open`]
    /// repeat these same checks (they can be called directly, without
    /// going through [`SequencerConfig::validate`]), so a caller that
    /// already validated pays only for the redundant check, never for a
    /// bypassed one.
    ///
    /// # Errors
    ///
    /// Returns [`ResyncConfigError`] for the same reasons as
    /// [`Self::enter_threshold`] and [`ResyncChannel::open`].
    pub fn validate(&self) -> Result<(), ResyncConfigError> {
        self.enter_threshold()?;
        usize::try_from(self.dedup_capacity.get()).map_err(|_| {
            ResyncConfigError::CapacityNotUsize {
                dedup_capacity: self.dedup_capacity.get(),
            }
        })?;
        Ok(())
    }
}

/// Why resync mode was entered or re-entered. Carried in the enter log
/// line, which the chaos suite greps for (`sequencer RESYNC enter`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterReason {
    Startup,
    WatermarkJump { gap: u64 },
    BoundarySilence { silent_ms: u64 },
    PublishStall { stalled_ms: u64 },
}

/// Trigger-and-exit state machine, plus executed-truth floors. The
/// publish loop thread owns this. It is fed by the receipts thread
/// ([`FloorUpdate`] mpsc) and the egress-watermark thread
/// ([`SharedWatermark`]).
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent flags (resync active, each receiver dead, a boundary has been seen), not a state machine to collapse into one enum"
)]
pub struct ResyncController {
    cfg: ResyncConfig,
    /// `cfg.enter_threshold()`, computed once at construction rather than
    /// on every [`Self::observe`] call.
    enter_threshold: NonZeroU64,
    partition: u32,
    active: bool,
    floors: HashMap<Address, u64>,
    floor_rx: Receiver<FloorUpdate>,
    /// The drain sets this when it sees `Disconnected`. It logs the event
    /// once. It does not treat the channel as "empty forever".
    floor_rx_dead: bool,
    /// `(sender, nonce, expected)` contiguity rejects, forwarded by the
    /// egress-watermark thread. The sealer refused a ref whose nonce was
    /// not the sender's expected next one. `nonce >= expected` means a
    /// gap: rewind the unconfirmed ledger from `expected` and republish.
    /// `nonce < expected` means the ref already committed (the guard's
    /// expected nonce advanced past it), and its dedup entry aged out.
    /// This confirms by reject, dropping the ledger entry.
    reject_rx: Receiver<(Address, u64, u64)>,
    reject_rx_dead: bool,
    watermark: SharedWatermark,
    last_watermark: u64,
    /// Set once the first boundary has been seen. A jump before the
    /// session ever delivered a boundary looks the same as startup.
    watermark_seen: bool,
    stall_since: Option<Instant>,
    calm_since: Option<Instant>,
}

/// Bound on floor updates drained per loop iteration, so a receipts burst
/// cannot starve the publish path.
const FLOOR_DRAIN_PER_ITER: usize = 1024;

/// Drain up to `max` items off a bounded `try_recv` channel. Stops early
/// on `Empty`. On `Disconnected`, latches `*dead` and warns once (repeat
/// disconnects on later calls stay silent), instead of once per drained
/// batch. Shared by every bounded drain in this controller.
fn drain_bounded<T>(
    rx: &Receiver<T>,
    dead: &mut bool,
    max: usize,
    partition: u32,
    on_dead: &str,
) -> Vec<T> {
    let mut out = Vec::new();
    for _ in 0..max {
        match rx.try_recv() {
            Ok(item) => out.push(item),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                if !*dead {
                    *dead = true;
                    tracing::warn!(partition, "{}", on_dead);
                }
                break;
            }
        }
    }
    out
}

/// One drain of the receipts channel: `(raised_floors, confirmations)`.
/// Both are `(sender, nonce-or-floor)` lists. See
/// [`ResyncController::drain_floor_updates`].
pub type ReceiptDrain = (Vec<(Address, u64)>, Vec<(Address, u64)>);

/// One drain of the contiguity-reject channel:
/// `(committed_drops, gap_rewinds)`. See
/// [`ResyncController::drain_contiguity_rejects`].
pub type RejectDrain = (Vec<(Address, u64)>, Vec<(Address, u64)>);

impl ResyncController {
    /// # Errors
    ///
    /// Returns [`ResyncConfigError`] if `cfg`'s watermark-jump threshold
    /// cannot be computed (see [`ResyncConfig::enter_threshold`]).
    pub fn new(
        cfg: ResyncConfig,
        partition: u32,
        floor_rx: Receiver<FloorUpdate>,
        reject_rx: Receiver<(Address, u64, u64)>,
        watermark: SharedWatermark,
    ) -> Result<Self, ResyncConfigError> {
        let enter_threshold = cfg.enter_threshold()?;
        let mut c = Self {
            cfg,
            enter_threshold,
            partition,
            active: false,
            floors: HashMap::new(),
            floor_rx,
            floor_rx_dead: false,
            reject_rx,
            reject_rx_dead: false,
            watermark,
            last_watermark: 0,
            watermark_seen: false,
            stall_since: None,
            calm_since: None,
        };
        // Startup trigger. A restarted replica cannot know what its twin
        // ordered while it was away. Begin filtered until calm.
        c.enter(EnterReason::Startup);
        Ok(c)
    }

    #[must_use]
    pub fn active(&self) -> bool {
        self.active
    }

    #[must_use]
    pub fn floor(&self, sender: Address) -> Option<u64> {
        self.floors.get(&sender).copied()
    }

    /// Drain pending receipt updates (bounded per iteration). Returns
    /// `(raised_floors, confirmations)`:
    ///
    /// - `raised_floors`: senders whose executed-truth floor rose, for
    ///   [`crate::state::PartitionState::advance_floor`]. Skip receipts
    ///   and deposit receipts are not floor evidence: a skip consumed no
    ///   nonce, and a deposit has no L2 nonce at all.
    /// - `confirmations`: `(sender, nonce)` for each receipt, including
    ///   skip receipts (ordering in the canonical log is exactly what a
    ///   publish confirmation needs, since an Aeron offer is not a
    ///   commit and only a receipt proves the ref survived into the
    ///   committed stream) and including nonce 0.
    ///
    ///   A deposit receipt (filler nonce 0) must not confirm a genuine
    ///   nonce-0 transaction. `Receipt::tx_type` tells the two apart at
    ///   the source, so the exclusion is exactly "is this a deposit?",
    ///   not "is the nonce 0?".
    pub fn drain_floor_updates(&mut self) -> ReceiptDrain {
        let mut raised = Vec::new();
        let mut confirmations = Vec::new();
        let updates = drain_bounded(
            &self.floor_rx,
            &mut self.floor_rx_dead,
            FLOOR_DRAIN_PER_ITER,
            self.partition,
            "floor-update producer disconnected; resync floors are frozen",
        );
        for u in updates {
            // A deposit consumes no L2 nonce. It is neither a
            // confirmation (it never corresponds to a published TxRef)
            // nor floor evidence.
            if u.deposit {
                continue;
            }
            confirmations.push((u.sender, u.executed_nonce));
            if u.skip_reason.is_some() {
                continue;
            }
            let floor = u.executed_nonce.saturating_add(1);
            let e = self.floors.entry(u.sender).or_insert(0);
            if floor > *e {
                *e = floor;
                raised.push((u.sender, floor));
            }
        }
        if !raised.is_empty() {
            metrics::record_floor_senders(self.partition, self.floors.len());
        }
        (raised, confirmations)
    }

    /// Drain pending contiguity rejects into `(committed_drops,
    /// gap_rewinds)`:
    ///
    /// - `committed_drops`: `(sender, nonce)` rejects with `nonce <
    ///   expected`. The guard's expected nonce is already past this ref,
    ///   which proves it committed (per-sender contiguity means the guard
    ///   accepted it on the way up). Its dedup entry merely aged out. The
    ///   ledger entry is dropped, like a receipt confirmation would drop
    ///   it. Not dropping it would re-offer the ref every confirm timeout
    ///   forever (the nonce-0 case has no confirming receipt at all,
    ///   since nonce-0 receipts cannot be told apart from deposits and
    ///   never confirm). Caveat: if the sealer evicted and re-seeded this
    ///   sender above a genuinely voided ref, this drops a ref that never
    ///   sealed. But republishing can never seal it either, since the
    ///   guard rejects it forever. So the drop only trades an infinite
    ///   reject loop for an honest, bounded degradation, in the same
    ///   eviction-floor class the guard itself documents.
    /// - `gap_rewinds`: `(sender, expected)` rejects with `nonce >=
    ///   expected`, deduplicated per sender to the lowest expected (a
    ///   rejected batch produces one reject per entry, and one rewind to
    ///   the lowest covers them all). Refs for `expected..nonce`
    ///   vanished, so rewind the unconfirmed ledger and republish.
    ///
    /// Bounded per iteration, like the floor drain.
    pub fn drain_contiguity_rejects(&mut self) -> RejectDrain {
        let mut drops: Vec<(Address, u64)> = Vec::new();
        let mut lowest: HashMap<Address, u64> = HashMap::new();
        let rejects = drain_bounded(
            &self.reject_rx,
            &mut self.reject_rx_dead,
            FLOOR_DRAIN_PER_ITER,
            self.partition,
            "contiguity-reject producer disconnected",
        );
        for (sender, nonce, expected) in rejects {
            if nonce < expected {
                drops.push((sender, nonce));
            } else {
                lowest
                    .entry(sender)
                    .and_modify(|e| *e = (*e).min(expected))
                    .or_insert(expected);
            }
        }
        (drops, lowest.into_iter().collect())
    }

    /// Per-iteration trigger evaluation. `now` is injected for testability.
    ///
    /// Silence detection does not happen here. This method only runs when
    /// the publish loop is running, and the loop can be blocked in a
    /// session offer exactly while lag is happening. The egress FEED
    /// thread is the silence authority: it flags boundary-arrival gaps
    /// into the sticky [`SharedWatermark::flag_lag`], consumed here
    /// whenever the loop next turns. The jump check stays here as a
    /// second, loop-local signal.
    pub fn observe(&mut self, now: Instant) {
        let w = self.watermark.load();
        if let Some(gap_ms) = self.watermark.take_lag() {
            self.enter(EnterReason::BoundarySilence { silent_ms: gap_ms });
        }
        if w != self.last_watermark {
            // Record the gauge only on change. observe runs every loop
            // iteration, and the metrics macro allocates its label each call.
            metrics::record_canonical_watermark(self.partition, w);
            if w >= self.last_watermark {
                let jump = w - self.last_watermark;
                if self.watermark_seen && jump >= self.enter_threshold.get() {
                    self.enter(EnterReason::WatermarkJump { gap: jump });
                }
            } else {
                // The canonical count must never go backwards. Log this
                // instead of silently clamping the jump to 0: a
                // regression is a sealer fault, not a routine event.
                tracing::warn!(
                    partition = self.partition,
                    previous = self.last_watermark,
                    observed = w,
                    "canonical watermark regressed; this should never happen (sealer fault?)"
                );
            }
            self.last_watermark = w;
            self.watermark_seen = true;
        }
        self.maybe_exit(now);
    }

    /// The publish path hit backpressure (or a not-connected session) with
    /// work pending.
    pub fn note_publish_stall(&mut self, now: Instant) {
        let since = *self.stall_since.get_or_insert(now);
        let stalled_ms = u64::try_from(now.duration_since(since).as_millis()).unwrap_or(u64::MAX);
        if stalled_ms >= self.cfg.publish_stall_ms {
            self.enter(EnterReason::PublishStall { stalled_ms });
            self.stall_since = Some(now); // re-arm, to avoid re-enter spam
        }
    }

    /// The publish path made progress (a successful publish, or idle
    /// with no work).
    pub fn note_publish_ok(&mut self) {
        self.stall_since = None;
    }

    fn enter(&mut self, reason: EnterReason) {
        self.calm_since = None;
        if self.active {
            return;
        }
        self.active = true;
        metrics::record_resync_enter(self.partition);
        // This is a stable grep target for the chaos suite. Keep the
        // "sequencer RESYNC enter" prefix in lockstep with
        // deploy/cluster/scripts/chaos.sh.
        tracing::info!(
            partition = self.partition,
            reason = ?reason,
            watermark = self.last_watermark,
            floors = self.floors.len(),
            "sequencer RESYNC enter"
        );
    }

    fn maybe_exit(&mut self, now: Instant) {
        if !self.active {
            return;
        }
        // Calm means the feed thread has raised no unconsumed lag flag
        // (checked just above in observe), and the publish path is not
        // stalled. The watermark does not need to advance: an idle but
        // healthy cluster emits boundaries with an unchanged count.
        let calm = self.watermark_seen && self.stall_since.is_none();
        if !calm {
            self.calm_since = None;
            return;
        }
        let since = *self.calm_since.get_or_insert(now);
        let calm_ms = u64::try_from(now.duration_since(since).as_millis()).unwrap_or(u64::MAX);
        if calm_ms >= self.cfg.exit_hold_ms {
            self.active = false;
            self.calm_since = None;
            tracing::info!(
                partition = self.partition,
                watermark = self.last_watermark,
                "sequencer RESYNC exit"
            );
        }
        metrics::record_resync_mode(self.partition, self.active);
    }
}

/// What [`ResyncChannel::open`] hands back: the controller (publish
/// loop), the floor-update sender (receipts thread), the `(sender,
/// nonce, expected)` contiguity-reject sender (egress-watermark thread),
/// and the shared watermark (egress-watermark thread). A named struct,
/// not a positional tuple: the four fields have four different owners on
/// the sequencer binary side, so a `.0`/`.1`/`.2`/`.3` call site would
/// carry no information about which is which.
pub struct ResyncChannel {
    pub controller: ResyncController,
    pub floor_tx: Sender<FloorUpdate>,
    pub reject_tx: Sender<(Address, u64, u64)>,
    pub watermark: SharedWatermark,
}

impl ResyncChannel {
    /// Build the controller, plus the sender halves of the floor-update
    /// channel (handed to the receipts thread) and the contiguity-reject
    /// channel (handed to the egress-watermark thread, alongside the
    /// shared watermark).
    ///
    /// Both channels are bounded to `cfg.dedup_capacity`: the cluster's
    /// first-seen dedup horizon, the same resync window the rest of this
    /// module protects. A backlog past that many entries already means
    /// the publish loop has stalled longer than the window this
    /// mechanism covers, so the producer side drops on overflow instead
    /// of growing without bound (see [`crate::feeds`] on the sequencer
    /// binary side).
    ///
    /// Each channel is `crossbeam_channel::bounded`, which allocates
    /// every slot up front rather than growing on demand: at the default
    /// `dedup_capacity` (2^17), each channel's backing ring buffer is
    /// about 6 MB, so this allocates about 12 MB total.
    ///
    /// # Errors
    ///
    /// Returns [`ResyncConfigError`] if `cfg`'s watermark-jump threshold
    /// cannot be computed (see [`ResyncConfig::enter_threshold`]), or if
    /// `cfg.dedup_capacity` does not fit in a `usize` on this target
    /// (only possible on a 32-bit target with a `dedup_capacity` above
    /// `u32::MAX`).
    pub fn open(cfg: ResyncConfig, partition: u32) -> Result<Self, ResyncConfigError> {
        let cap = usize::try_from(cfg.dedup_capacity.get()).map_err(|_| {
            ResyncConfigError::CapacityNotUsize {
                dedup_capacity: cfg.dedup_capacity.get(),
            }
        })?;
        let (floor_tx, floor_rx) = crossbeam_channel::bounded(cap);
        let (reject_tx, reject_rx) = crossbeam_channel::bounded(cap);
        let watermark = SharedWatermark::new();
        let controller =
            ResyncController::new(cfg, partition, floor_rx, reject_rx, watermark.clone())?;
        Ok(Self {
            controller,
            floor_tx,
            reject_tx,
            watermark,
        })
    }
}

#[cfg(test)]
mod tests;
