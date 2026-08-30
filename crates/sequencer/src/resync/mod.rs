//! Lag detection + receipt-floor resync.
//!
//! Implements docs/agents/sequencer-lag-resync-spec.md: a replica that falls
//! far enough behind its twin can drain re-offers past the cluster's
//! first-seen dedup horizon, getting the same tx canonically ordered TWICE
//! (executor-fatal, and it poisons recovery replay — issue #92). The guard is
//! split into a **provably-safe response** and **cheap triggers**:
//!
//! - Response ([`should_skip`](ResyncController::should_skip)): while in
//!   resync mode, skip a publish iff the sender's **executed-truth floor**
//!   (derived from the tx_receipts stream, [`FloorUpdate`]) proves the nonce
//!   already executed. A skip backed by a receipt needs no dedup-window
//!   guarantee at all; everything unproven is published, so every degraded
//!   mode (missed receipts, late subscribe) degrades toward *publish* — the
//!   side guarded by the layered dedup windows — never toward *skip* (a
//!   canonical nonce gap, which nothing guards; see the removed F02.1
//!   fast-forward note in [`crate::state`]).
//! - Triggers: the primary signal is the **canonical-count watermark** — the
//!   cluster broadcasts every boundary (`end_tx_idx` = global canonical
//!   count) to publisher sessions too, so the horizon is measured in its
//!   native units, no clocks and no wire change. A watermark JUMP larger
//!   than `enter_fraction × dedup_capacity` means this process was blind
//!   while that many records were ordered (freeze/pause); watermark SILENCE
//!   means it is partitioned from egress. A sustained publish stall and
//!   startup round out the triggers. False positives only cost floor
//!   lookups, which is what lets the triggers stay twitchy and local.

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

use crate::metrics;

/// One executed-truth observation off the tx_receipts stream: `sender`'s tx
/// with `executed_nonce` produced a receipt, so the sender's floor is at
/// least `executed_nonce + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorUpdate {
    pub sender: Address,
    pub executed_nonce: u64,
    /// L1-originated deposit: consumes no L2 nonce, so it is neither floor
    /// evidence nor a publish confirmation. Explicit since deposits carry
    /// `Receipt::tx_type == TX_TYPE_DEPOSIT` — the nonce-0 heuristic this
    /// replaces could not tell a deposit from a genuine nonce-0 tx.
    pub deposit: bool,
    /// #92 marker receipt: the tx was ORDERED (canonical-log commitment is
    /// proven — it confirms publishes) but consumed no nonce (it is NOT
    /// floor evidence).
    pub invalid_skip: bool,
}

/// Shared state between the egress-watermark FEED thread and the publish
/// loop's controller:
///
/// - `count` — latest boundary `end_tx_idx` (the global canonical count).
/// - `lag_gap_ms` — a STICKY lag flag: the feed thread sets it when it
///   observes a boundary inter-arrival gap past the silence threshold, and
///   the controller consumes it (swap-to-0) on its next iteration. Sticky
///   because the publish loop can be BLOCKED for long stretches exactly when
///   lag happens (`LiveIngress::offer` waits on the session thread, which is
///   mid-reconnect after e.g. a process freeze) — a point-in-time check the
///   loop must be running to catch was observed to miss a 30 s freeze
///   entirely (sequencer-lapse CI, run 30163255470). Boundary ARRIVALS are
///   the liveness signal, not count changes: idle traffic emits boundaries
///   every tick with an unchanged count, which a value-change tracker
///   mistakes for silence (observed as enter/exit thrash every 10 s).
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
    /// Feed thread: flag an observed boundary-arrival gap (ms). Keeps the
    /// LARGEST unconsumed gap so a later smaller gap can't shadow a freeze.
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

/// `[resync]` TOML section / CLI knobs. All defaults follow the spec.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ResyncConfig {
    /// MUST equal the cluster's `-Dkardamom.cluster.dedupCapacity` — the
    /// horizon this mechanism protects. Logged at startup for the contract
    /// check.
    pub dedup_capacity: u64,
    /// Watermark-jump / gap enter threshold as a PERCENT of the capacity
    /// (integer so the config stays `Eq`; spec's 0.25 fraction = 25).
    pub enter_percent: u64,
    /// Boundary-silence trigger: no watermark change for this long ⇒ resync.
    /// Sized as spec's `boundary_silence_ticks × cluster tick interval`
    /// (5 × 2000 ms deploy tick).
    pub boundary_silence_ms: u64,
    /// A publish stall (continuous backpressure / no successful publish while
    /// work is pending) longer than this enters resync. Fallback trigger for
    /// the no-egress-signal case.
    pub publish_stall_ms: u64,
    /// How long conditions must stay calm before exiting resync (hysteresis).
    pub exit_hold_ms: u64,
    /// #85: how long a published ref may remain unconfirmed (no receipt at
    /// or above its nonce) before it is rewound and re-published. Must
    /// comfortably exceed the order→execute→receipt round trip under load;
    /// re-publishing early is harmless (dedup absorbs), late leaves voided
    /// refs unrecovered longer.
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

/// Why resync mode was (re-)entered. Carried in the enter log line, which the
/// chaos suite greps (`sequencer RESYNC enter`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterReason {
    Startup,
    WatermarkJump { gap: u64 },
    BoundarySilence { silent_ms: u64 },
    PublishStall { stalled_ms: u64 },
}

/// Trigger/exit state machine + executed-truth floors. Owned by the publish
/// loop thread; fed by the receipts thread ([`FloorUpdate`] mpsc) and the
/// egress-watermark thread ([`SharedWatermark`]).
pub struct ResyncController {
    cfg: ResyncConfig,
    partition: u32,
    active: bool,
    floors: HashMap<Address, u64>,
    floor_rx: Receiver<FloorUpdate>,
    /// Set when a drain sees `Disconnected` — logged once, not silently
    /// treated as "empty forever".
    floor_rx_dead: bool,
    /// #85 fix B: `(sender, nonce, expected)` contiguity rejects forwarded
    /// by the egress-watermark thread — the sealer refused a ref whose nonce
    /// was not the sender's expected next one. `nonce >= expected` means a
    /// gap: rewind the unconfirmed ledger from `expected` and republish.
    /// `nonce < expected` means the ref already COMMITTED (the guard's
    /// expected nonce advanced past it) and its dedup entry aged out —
    /// confirm-by-reject, dropping the ledger entry.
    reject_rx: Receiver<(Address, u64, u64)>,
    reject_rx_dead: bool,
    watermark: SharedWatermark,
    last_watermark: u64,
    /// Set once the first boundary has been observed — a jump before the
    /// session has ever delivered a boundary is indistinct from startup.
    watermark_seen: bool,
    stall_since: Option<Instant>,
    calm_since: Option<Instant>,
}

/// Bound on floor updates drained per loop iteration, so a receipts burst
/// cannot starve the publish path.
const FLOOR_DRAIN_PER_ITER: usize = 1024;

/// One drain of the receipts channel: `(raised_floors, confirmations)` —
/// both `(sender, nonce-or-floor)` lists; see
/// [`ResyncController::drain_floor_updates`].
pub type ReceiptDrain = (Vec<(Address, u64)>, Vec<(Address, u64)>);

/// One drain of the contiguity-reject channel:
/// `(committed_drops, gap_rewinds)`; see
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
        // Startup trigger: a (re)started replica cannot know what its twin
        // ordered while it was away — begin filtered until calm.
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
    /// - `raised_floors` — senders whose executed-truth floor ROSE, for
    ///   [`crate::state::PartitionState::advance_floor`]. Skip receipts (#92)
    ///   and DEPOSIT receipts are NOT floor evidence (a skip consumed no
    ///   nonce; a deposit has no L2 nonce at all).
    /// - `confirmations` — `(sender, nonce)` per receipt, INCLUDING skip
    ///   receipts (ordering in the canonical log is exactly what a publish
    ///   confirmation needs — #85: an Aeron offer is NOT a commit; only a
    ///   receipt proves the ref survived into the committed stream) and
    ///   INCLUDING nonce 0.
    ///
    ///   Nonce 0 used to be excluded wholesale, because a deposit receipt
    ///   (filler nonce 0) was indistinguishable from a genuine nonce-0 tx
    ///   and must not confirm one. The cost was silent and unbounded: a
    ///   one-tx sender's nonce-0 ref could never be confirmed, so the #85
    ///   ledger re-offered it every confirm-timeout FOREVER, rewinding that
    ///   sender's nonce floor on every sweep (observed in CI: 46 rewinds,
    ///   one every 15s, for the whole soak — issue #124). With
    ///   `Receipt::tx_type` the two are distinguishable at the source, so
    ///   the exclusion is now exactly "is this a deposit?".
    pub fn drain_floor_updates(&mut self) -> ReceiptDrain {
        let mut raised = Vec::new();
        let mut confirmations = Vec::new();
        for _ in 0..FLOOR_DRAIN_PER_ITER {
            match self.floor_rx.try_recv() {
                Ok(u) => {
                    // Deposits consume no L2 nonce: neither a confirmation
                    // (they never correspond to a published TxRef) nor floor
                    // evidence.
                    if u.deposit {
                        continue;
                    }
                    confirmations.push((u.sender, u.executed_nonce));
                    if u.invalid_skip {
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

    /// Drain pending contiguity rejects (#85 fix B) into
    /// `(committed_drops, gap_rewinds)`:
    ///
    /// - `committed_drops` — `(sender, nonce)` rejects with
    ///   `nonce < expected`: the guard's expected nonce is already past this
    ///   ref, which proves it committed (per-sender contiguity means the
    ///   guard accepted it on the way up) — its dedup entry merely aged out.
    ///   The ledger entry is dropped like a receipt confirmation would; NOT
    ///   dropping it re-offers the ref every confirm-timeout forever (the
    ///   nonce-0 case has no confirming receipt at all: nonce-0 receipts are
    ///   deposit-indistinguishable and never confirm). Caveat: if the sealer
    ///   evicted + re-seeded this sender ABOVE a genuinely-voided ref, this
    ///   drops a ref that never sealed — but republishing can never seal it
    ///   either (the guard rejects it forever), so the drop only trades an
    ///   infinite reject loop for an honest, bounded degradation, in the
    ///   same eviction-floor class the guard itself documents.
    /// - `gap_rewinds` — `(sender, expected)` rejects with
    ///   `nonce >= expected`, deduplicated per sender to the LOWEST expected
    ///   (a rejected batch produces one reject per entry; one rewind to the
    ///   lowest covers them all): refs for `expected..nonce` vanished —
    ///   rewind the unconfirmed ledger and republish.
    ///
    /// Bounded per iteration like the floor drain.
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
    /// Silence detection does NOT happen here: this method only runs when the
    /// publish loop is running, and the loop can be blocked in a session
    /// offer exactly while lag is happening. The egress FEED thread is the
    /// silence authority — it flags boundary-arrival gaps into the sticky
    /// [`SharedWatermark::flag_lag`], consumed here whenever the loop next
    /// turns. The jump check stays here as a second, loop-local signal.
    pub fn observe(&mut self, now: Instant) {
        let w = self.watermark.load();
        if let Some(gap_ms) = self.watermark.take_lag() {
            self.enter(EnterReason::BoundarySilence { silent_ms: gap_ms });
        }
        if w != self.last_watermark {
            // Gauge recorded on CHANGE only — observe runs every loop
            // iteration and the metrics macro allocates its label each call.
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
            self.stall_since = Some(now); // re-arm, avoid re-enter spam
        }
    }

    /// The publish path made progress (successful publish or idle-with-no-work).
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
        // Stable grep target for the chaos suite — keep the "sequencer RESYNC
        // enter" prefix in lockstep with deploy/cluster/scripts/chaos.sh.
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
        // Calm = the feed thread has raised no unconsumed lag flag (checked
        // just above in observe) and the publish path is not stalled. The
        // watermark advancing is NOT required: an idle-but-healthy cluster
        // emits boundaries with an unchanged count.
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

/// What [`resync_channel`] hands back: the controller (publish loop), the
/// floor-update sender (receipts thread), the `(sender, nonce, expected)`
/// contiguity-reject sender (#85 fix B, egress-watermark thread) and the
/// shared watermark (egress-watermark thread).
pub type ResyncChannel = (
    ResyncController,
    Sender<FloorUpdate>,
    Sender<(Address, u64, u64)>,
    SharedWatermark,
);

/// Build the controller plus the sender halves of the floor-update channel
/// (handed to the receipts thread) and the contiguity-reject channel (#85
/// fix B, handed to the egress-watermark thread alongside the shared
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
