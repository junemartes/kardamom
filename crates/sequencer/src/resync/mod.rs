//! Lag detection and receipt-floor resync.
//!
//! Implements docs/agents/sequencer-lag-resync-spec.md. A replica that
//! falls far enough behind its twin can drain re-offers past the
//! cluster's first-seen dedup horizon. This orders the same transaction
//! canonically twice, which is fatal to the executor and poisons recovery
//! replay. The guard splits into a provably safe response, and cheap
//! triggers:
//!
//! - Response ([`should_skip`](ResyncController::should_skip)): while in
//!   resync mode, skip a publish only if the sender's executed-truth
//!   floor (derived from the tx_receipts stream, [`FloorUpdate`]) proves
//!   the nonce already executed. A skip backed by a receipt needs no
//!   dedup-window guarantee at all. Everything unproven is published, so
//!   every degraded mode (missed receipts, late subscribe) degrades
//!   toward publish, the side the layered dedup windows guard, and never
//!   toward skip (a canonical nonce gap, which nothing guards; see the
//!   removed fast-forward note in [`crate::state`]).
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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

use crate::metrics;

/// One executed-truth observation from the tx_receipts stream.
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn store(&self, count: u64) {
        self.count.store(count, Ordering::Release);
    }
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
    /// contract check.
    pub dedup_capacity: u64,
    /// Watermark-jump and gap enter threshold, as a percent of the
    /// capacity. This is an integer so the config stays `Eq` (the spec's
    /// 0.25 fraction is 25 here).
    pub enter_percent: u64,
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
            dedup_capacity: 1 << 17,
            enter_percent: 25,
            boundary_silence_ms: 10_000,
            publish_stall_ms: 10_000,
            exit_hold_ms: 2_000,
            confirm_timeout_ms: 15_000,
        }
    }
}

impl ResyncConfig {
    /// Watermark jump/gap threshold in records.
    pub fn enter_threshold(&self) -> u64 {
        (self.dedup_capacity.saturating_mul(self.enter_percent) / 100).max(1)
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
pub struct ResyncController {
    cfg: ResyncConfig,
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

/// One drain of the receipts channel: `(raised_floors, confirmations)`.
/// Both are `(sender, nonce-or-floor)` lists. See
/// [`ResyncController::drain_floor_updates`].
pub type ReceiptDrain = (Vec<(Address, u64)>, Vec<(Address, u64)>);

/// One drain of the contiguity-reject channel:
/// `(committed_drops, gap_rewinds)`. See
/// [`ResyncController::drain_contiguity_rejects`].
pub type RejectDrain = (Vec<(Address, u64)>, Vec<(Address, u64)>);

impl ResyncController {
    pub fn new(
        cfg: ResyncConfig,
        partition: u32,
        floor_rx: Receiver<FloorUpdate>,
        reject_rx: Receiver<(Address, u64, u64)>,
        watermark: SharedWatermark,
    ) -> Self {
        let mut c = Self {
            cfg,
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
        c
    }

    pub fn active(&self) -> bool {
        self.active
    }

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
    ///   Nonce 0 used to be excluded wholesale, because a deposit receipt
    ///   (filler nonce 0) could not be told apart from a genuine nonce-0
    ///   transaction, and must not confirm one. The cost was silent and
    ///   unbounded: a one-transaction sender's nonce-0 ref could never be
    ///   confirmed, so the ledger re-offered it every confirm timeout
    ///   forever, rewinding that sender's nonce floor on every sweep.
    ///   With `Receipt::tx_type`, the two are distinguishable at the
    ///   source, so the exclusion is now exactly "is this a deposit?".
    pub fn drain_floor_updates(&mut self) -> ReceiptDrain {
        let mut raised = Vec::new();
        let mut confirmations = Vec::new();
        for _ in 0..FLOOR_DRAIN_PER_ITER {
            match self.floor_rx.try_recv() {
                Ok(u) => {
                    // A deposit consumes no L2 nonce. It is neither a
                    // confirmation (it never corresponds to a published
                    // TxRef) nor floor evidence.
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
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if !self.floor_rx_dead {
                        self.floor_rx_dead = true;
                        tracing::warn!(
                            partition = self.partition,
                            "floor-update producer disconnected; resync floors are frozen"
                        );
                    }
                    break;
                }
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
        for _ in 0..FLOOR_DRAIN_PER_ITER {
            match self.reject_rx.try_recv() {
                Ok((sender, nonce, expected)) => {
                    if nonce < expected {
                        drops.push((sender, nonce));
                    } else {
                        lowest
                            .entry(sender)
                            .and_modify(|e| *e = (*e).min(expected))
                            .or_insert(expected);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if !self.reject_rx_dead {
                        self.reject_rx_dead = true;
                        tracing::warn!(
                            partition = self.partition,
                            "contiguity-reject producer disconnected"
                        );
                    }
                    break;
                }
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
            let jump = w.saturating_sub(self.last_watermark);
            if self.watermark_seen && jump >= self.cfg.enter_threshold() {
                self.enter(EnterReason::WatermarkJump { gap: jump });
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
        if now.duration_since(since).as_millis() as u64 >= self.cfg.publish_stall_ms {
            self.enter(EnterReason::PublishStall {
                stalled_ms: now.duration_since(since).as_millis() as u64,
            });
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
        if now.duration_since(since).as_millis() as u64 >= self.cfg.exit_hold_ms {
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

/// What [`resync_channel`] hands back: the controller (publish loop),
/// the floor-update sender (receipts thread), the `(sender, nonce,
/// expected)` contiguity-reject sender (egress-watermark thread), and
/// the shared watermark (egress-watermark thread).
pub type ResyncChannel = (
    ResyncController,
    Sender<FloorUpdate>,
    Sender<(Address, u64, u64)>,
    SharedWatermark,
);

/// Build the controller, plus the sender halves of the floor-update
/// channel (handed to the receipts thread) and the contiguity-reject
/// channel (handed to the egress-watermark thread, alongside the shared
/// watermark).
pub fn resync_channel(cfg: ResyncConfig, partition: u32) -> ResyncChannel {
    let (tx, rx) = crossbeam_channel::unbounded();
    let (reject_tx, reject_rx) = crossbeam_channel::unbounded();
    let watermark = SharedWatermark::new();
    let controller = ResyncController::new(cfg, partition, rx, reject_rx, watermark.clone());
    (controller, tx, reject_tx, watermark)
}

#[cfg(test)]
mod tests;
