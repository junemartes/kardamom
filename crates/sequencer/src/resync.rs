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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
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
    /// #85 fix B: `(sender, nonce, expected)` contiguity rejects forwarded
    /// by the egress-watermark thread — the sealer refused a ref whose nonce
    /// was not the sender's expected next one. `nonce >= expected` means a
    /// gap: rewind the unconfirmed ledger from `expected` and republish.
    /// `nonce < expected` means the ref already COMMITTED (the guard's
    /// expected nonce advanced past it) and its dedup entry aged out —
    /// confirm-by-reject, dropping the ledger entry.
    reject_rx: Receiver<(Address, u64, u64)>,
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
            reject_rx,
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
    ///   and nonce-0 receipts (deposit-indistinguishable) are NOT floor
    ///   evidence.
    /// - `confirmations` — `(sender, nonce)` per receipt, INCLUDING skip
    ///   receipts (ordering in the canonical log is exactly what a publish
    ///   confirmation needs — #85: an Aeron offer is NOT a commit; only a
    ///   receipt proves the ref survived into the committed stream). Nonce-0
    ///   receipts are excluded here too (a deposit receipt must not confirm
    ///   a same-sender nonce-0 TxRef); a genuine nonce-0 ref is confirmed
    ///   cumulatively by the sender's nonce-1 receipt, or re-offers
    ///   harmlessly until then (dedup absorbs).
    pub fn drain_floor_updates(&mut self) -> ReceiptDrain {
        let mut raised = Vec::new();
        let mut confirmations = Vec::new();
        for _ in 0..FLOOR_DRAIN_PER_ITER {
            match self.floor_rx.try_recv() {
                Ok(u) => {
                    if u.executed_nonce == 0 {
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
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
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
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
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
    let (tx, rx) = std::sync::mpsc::channel();
    let (reject_tx, reject_rx) = std::sync::mpsc::channel();
    let watermark = SharedWatermark::new();
    let controller = ResyncController::new(cfg, partition, rx, reject_rx, watermark.clone());
    (controller, tx, reject_tx, watermark)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn s(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    fn mk(cfg: ResyncConfig) -> (ResyncController, Sender<FloorUpdate>, SharedWatermark) {
        let (c, tx, _reject_tx, w) = resync_channel(cfg, 0);
        (c, tx, w)
    }

    fn calm_down(c: &mut ResyncController, w: &SharedWatermark, t: &mut Instant) {
        // Boundaries flowing (count advancing), no lag flag, no stall: the
        // exit hold elapses across a few observes.
        for i in 1..=6u64 {
            w.store(c.last_watermark + i);
            *t += Duration::from_millis(500);
            c.observe(*t);
        }
        assert!(!c.active(), "controller should have exited resync");
    }

    #[test]
    fn starts_in_resync_and_exits_when_calm() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        assert!(c.active(), "startup trigger");
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
    }

    #[test]
    fn watermark_jump_enters() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        // Jump past 25% of 2^17 = 32768.
        w.store(c.last_watermark + 40_000);
        t += Duration::from_millis(100);
        c.observe(t);
        assert!(c.active(), "jump must re-enter resync");
    }

    #[test]
    fn feed_lag_flag_enters_even_if_raised_while_loop_was_blocked() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        // The FEED thread observed a 30 s boundary-arrival gap while the
        // publish loop was blocked in a session offer; the flag is sticky
        // and consumed on the loop's next turn — however late.
        w.flag_lag(30_000);
        t += Duration::from_millis(70_000);
        c.observe(t);
        assert!(c.active(), "sticky lag flag must enter resync");
        // A second, smaller gap flagged before consumption must not shadow
        // a larger one (fetch_max).
        w.flag_lag(12_000);
        w.flag_lag(3_000);
        assert_eq!(w.take_lag(), Some(12_000));
    }

    #[test]
    fn idle_boundaries_do_not_thrash() {
        // Idle traffic: boundaries arrive but the count never advances. The
        // controller must stay OUT of resync (silence is judged by boundary
        // ARRIVAL in the feed thread, not by count changes here).
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        for _ in 0..10 {
            t += Duration::from_millis(10_000);
            c.observe(t); // count unchanged, no lag flag raised
            assert!(!c.active(), "idle must not re-enter resync");
        }
    }

    #[test]
    fn small_jump_stays_calm() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        w.store(c.last_watermark + 100);
        t += Duration::from_millis(500);
        c.observe(t);
        assert!(!c.active(), "ordinary progress must not trigger");
    }

    #[test]
    fn publish_stall_enters() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        c.note_publish_stall(t);
        assert!(!c.active(), "stall below threshold must not trigger");
        t += Duration::from_millis(10_001);
        c.note_publish_stall(t);
        assert!(c.active(), "sustained stall must trigger");
    }

    #[test]
    fn floor_updates_raise_and_report() {
        let (mut c, tx, _w) = mk(ResyncConfig::default());
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 4,
            invalid_skip: false,
        })
        .unwrap();
        let (raised, confirmations) = c.drain_floor_updates();
        assert_eq!(raised, vec![(s(1), 5)]);
        assert_eq!(confirmations, vec![(s(1), 4)], "every receipt confirms");
        assert_eq!(c.floor(s(1)), Some(5), "nonces 0..=4 proven executed");
        assert_eq!(c.floor(s(2)), None, "unknown sender has no proof");
        // Re-draining with nothing pending raises nothing.
        let (raised, confirmations) = c.drain_floor_updates();
        assert!(raised.is_empty() && confirmations.is_empty());
    }

    #[test]
    fn skip_receipts_confirm_but_never_raise_floors() {
        // #85 + #92: a skip receipt proves the ref was ORDERED (a valid
        // publish confirmation) while proving no nonce was consumed (NOT
        // floor evidence).
        let (mut c, tx, _w) = mk(ResyncConfig::default());
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 7,
            invalid_skip: true,
        })
        .unwrap();
        let (raised, confirmations) = c.drain_floor_updates();
        assert!(raised.is_empty(), "skip is not floor evidence");
        assert_eq!(c.floor(s(1)), None);
        assert_eq!(confirmations, vec![(s(1), 7)], "skip IS a confirmation");
    }

    #[test]
    fn nonce_zero_receipts_neither_confirm_nor_raise() {
        // Deposit receipts stamp a filler nonce 0 and are indistinguishable
        // on the wire — they must not confirm a same-sender nonce-0 TxRef.
        let (mut c, tx, _w) = mk(ResyncConfig::default());
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 0,
            invalid_skip: false,
        })
        .unwrap();
        let (raised, confirmations) = c.drain_floor_updates();
        assert!(raised.is_empty() && confirmations.is_empty());
    }

    #[test]
    fn contiguity_rejects_split_drops_from_rewinds() {
        let (mut c, _tx, reject_tx, _w) = resync_channel(ResyncConfig::default(), 0);
        // Gap rejects (nonce >= expected): a rejected batch produces one per
        // entry; the drain collapses them to ONE rewind per sender at the
        // lowest expected.
        reject_tx.send((s(1), 9, 7)).unwrap();
        reject_tx.send((s(1), 8, 5)).unwrap();
        reject_tx.send((s(1), 12, 9)).unwrap();
        reject_tx.send((s(2), 3, 3)).unwrap();
        // Committed-proof reject (nonce < expected): the ref sealed long ago
        // and its dedup entry aged out — confirm-by-reject, drop the entry.
        reject_tx.send((s(3), 0, 1)).unwrap();
        let (drops, mut rewinds) = c.drain_contiguity_rejects();
        rewinds.sort();
        assert_eq!(rewinds, vec![(s(1), 5), (s(2), 3)]);
        assert_eq!(drops, vec![(s(3), 0)]);
        let (drops, rewinds) = c.drain_contiguity_rejects();
        assert!(drops.is_empty() && rewinds.is_empty());
    }

    #[test]
    fn floors_are_monotonic() {
        let (mut c, tx, _w) = mk(ResyncConfig::default());
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 9,
            invalid_skip: false,
        })
        .unwrap();
        // A LOWER receipt later (late-arriving multicast frame) must not
        // regress the floor.
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 3,
            invalid_skip: false,
        })
        .unwrap();
        let (raised, _) = c.drain_floor_updates();
        assert_eq!(raised, vec![(s(1), 10)]);
        assert_eq!(c.floor(s(1)), Some(10));
    }
}
