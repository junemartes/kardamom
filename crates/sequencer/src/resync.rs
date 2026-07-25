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
}

/// Latest cluster boundary `end_tx_idx` (the global canonical count), written
/// by the egress-watermark thread, read by the publish loop. Zero until the
/// first boundary is observed.
#[derive(Clone, Default)]
pub struct SharedWatermark(Arc<AtomicU64>);

impl SharedWatermark {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn store(&self, count: u64) {
        self.0.store(count, Ordering::Release);
    }
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
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
}

impl Default for ResyncConfig {
    fn default() -> Self {
        Self {
            dedup_capacity: 1 << 17,
            enter_percent: 25,
            boundary_silence_ms: 10_000,
            publish_stall_ms: 10_000,
            exit_hold_ms: 2_000,
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
    watermark: SharedWatermark,
    last_watermark: u64,
    last_watermark_change: Instant,
    /// Set once the first boundary has been observed — silence before the
    /// session has ever delivered a boundary is indistinct from startup.
    watermark_seen: bool,
    stall_since: Option<Instant>,
    calm_since: Option<Instant>,
}

/// Bound on floor updates drained per loop iteration, so a receipts burst
/// cannot starve the publish path.
const FLOOR_DRAIN_PER_ITER: usize = 1024;

impl ResyncController {
    pub fn new(
        cfg: ResyncConfig,
        partition: u32,
        floor_rx: Receiver<FloorUpdate>,
        watermark: SharedWatermark,
    ) -> Self {
        let mut c = Self {
            cfg,
            partition,
            active: false,
            floors: HashMap::new(),
            floor_rx,
            watermark,
            last_watermark: 0,
            last_watermark_change: Instant::now(),
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

    /// Drain pending floor updates (bounded per iteration). Returns the
    /// senders whose floor ROSE this drain, for the caller to apply
    /// [`crate::state::PartitionState::advance_floor`].
    pub fn drain_floor_updates(&mut self) -> Vec<(Address, u64)> {
        let mut raised = Vec::new();
        for _ in 0..FLOOR_DRAIN_PER_ITER {
            match self.floor_rx.try_recv() {
                Ok(u) => {
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
        raised
    }

    /// Per-iteration trigger evaluation. `now` is injected for testability.
    pub fn observe(&mut self, now: Instant) {
        let w = self.watermark.load();
        metrics::record_canonical_watermark(self.partition, w);
        if w != self.last_watermark {
            // Measure the gap BEFORE resetting the change stamp: after a
            // full-process freeze the first post-resume read sees both a
            // jump and a long silent interval — either alone must trigger.
            let silent = now.duration_since(self.last_watermark_change);
            let jump = w.saturating_sub(self.last_watermark);
            if self.watermark_seen {
                if jump >= self.cfg.enter_threshold() {
                    self.enter(EnterReason::WatermarkJump { gap: jump });
                } else if silent.as_millis() as u64 >= self.cfg.boundary_silence_ms {
                    self.enter(EnterReason::BoundarySilence {
                        silent_ms: silent.as_millis() as u64,
                    });
                }
            }
            self.last_watermark = w;
            self.last_watermark_change = now;
            self.watermark_seen = true;
        } else if self.watermark_seen {
            let silent = now.duration_since(self.last_watermark_change);
            if silent.as_millis() as u64 >= self.cfg.boundary_silence_ms {
                self.enter(EnterReason::BoundarySilence {
                    silent_ms: silent.as_millis() as u64,
                });
                // Re-arm so a persistent outage does not re-enter every
                // iteration (enter() is idempotent but the log would spam).
                self.last_watermark_change = now;
            }
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
        let silent_ms = now.duration_since(self.last_watermark_change).as_millis() as u64;
        let calm = self.watermark_seen
            && silent_ms < self.cfg.boundary_silence_ms
            && self.stall_since.is_none();
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

/// Build the controller plus the sender half of the floor-update channel
/// (handed to the receipts thread) and the shared watermark (handed to the
/// egress-watermark thread).
pub fn resync_channel(
    cfg: ResyncConfig,
    partition: u32,
) -> (ResyncController, Sender<FloorUpdate>, SharedWatermark) {
    let (tx, rx) = std::sync::mpsc::channel();
    let watermark = SharedWatermark::new();
    let controller = ResyncController::new(cfg, partition, rx, watermark.clone());
    (controller, tx, watermark)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn s(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    fn mk(cfg: ResyncConfig) -> (ResyncController, Sender<FloorUpdate>, SharedWatermark) {
        resync_channel(cfg, 0)
    }

    fn calm_down(c: &mut ResyncController, w: &SharedWatermark, t: &mut Instant) {
        // Advance the watermark a little every 500ms until the exit hold
        // elapses — fresh boundaries, no stall.
        for i in 1..=8u64 {
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
    fn boundary_silence_enters() {
        let (mut c, _tx, w) = mk(ResyncConfig::default());
        let mut t = Instant::now();
        calm_down(&mut c, &w, &mut t);
        // No watermark change for > boundary_silence_ms.
        t += Duration::from_millis(10_001);
        c.observe(t);
        assert!(c.active(), "silence must re-enter resync");
        let _ = w;
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
        })
        .unwrap();
        let raised = c.drain_floor_updates();
        assert_eq!(raised, vec![(s(1), 5)]);
        assert_eq!(c.floor(s(1)), Some(5), "nonces 0..=4 proven executed");
        assert_eq!(c.floor(s(2)), None, "unknown sender has no proof");
        // Re-draining with nothing pending raises nothing.
        assert!(c.drain_floor_updates().is_empty());
    }

    #[test]
    fn floors_are_monotonic() {
        let (mut c, tx, _w) = mk(ResyncConfig::default());
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 9,
        })
        .unwrap();
        // A LOWER receipt later (late-arriving multicast frame) must not
        // regress the floor.
        tx.send(FloorUpdate {
            sender: s(1),
            executed_nonce: 3,
        })
        .unwrap();
        let raised = c.drain_floor_updates();
        assert_eq!(raised, vec![(s(1), 10)]);
        assert_eq!(c.floor(s(1)), Some(10));
    }
}
