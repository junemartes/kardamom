//! Top-level sealer supervisor.
//!
//! Bundles:
//!   - a [`crate::watermark_tracker::WatermarkTracker`] populated by an
//!     external feeder (the supervisor exposes [`Sealer::update_watermark`]
//!     so tests can pump watermarks deterministically; the CLI bin wires a
//!     real watermark subscriber that pumps the same method);
//!   - a [`crate::emitter::BoundaryEmitter`] that publishes when this
//!     process is elected leader;
//!   - a tick loop that, every `tick_interval_ms`, evaluates
//!     [`crate::election::elect`] over the tracker snapshot and emits iff
//!     this process is the winner.
//!
//! The tick loop is **driven externally** via [`Sealer::tick_once`] so
//! `tokio::time::pause` tests can deterministically advance one tick at a
//! time. The CLI binary calls [`Sealer::run_forever`] which loops with
//! `tokio::time::sleep(next_tick - now)`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use kardamom_types::BlockBoundaryStart;

use crate::clock::WallClock;
use crate::config::SealerConfig;
use crate::election::{CaughtUpSet, RecorderState, elect};
use crate::emitter::{BoundaryEmitter, BoundaryPublisher};
use crate::tick::next_tick;
use crate::watermark_tracker::WatermarkTracker;

pub struct Sealer<C: WallClock + Clone, P: BoundaryPublisher> {
    cfg: SealerConfig,
    /// Cached `cfg.host_id.to_string()` so the per-tick metric label allocation
    /// doesn't fire every 250 ms.
    host_id_str: String,
    clock: C,
    tracker: Arc<WatermarkTracker>,
    emitter: BoundaryEmitter<C, P>,
    /// Highest block_number observed on B's boundary stream from any sealer
    /// (including ourselves). Used by [`Self::tick_once`] to keep the local
    /// emitter's counter monotonic across leadership flaps.
    observed_block: Arc<AtomicU64>,
}

impl<C: WallClock + Clone, P: BoundaryPublisher> Sealer<C, P> {
    /// Construct a sealer from already-prepared parts. The `initial_block` is
    /// what the bootstrap module produced from B's tail.
    pub fn new(cfg: SealerConfig, clock: C, publisher: P, initial_block: u64) -> Result<Self> {
        cfg.validate()?;
        let tracker = Arc::new(WatermarkTracker::new(cfg.recorder_ids.clone()));
        let host_id = cfg.host_id;
        let host_id_str = host_id.to_string();
        let tick_ms = cfg.tick_interval_ms;
        let emitter =
            BoundaryEmitter::new(publisher, clock.clone(), initial_block, tick_ms, host_id);
        Ok(Self {
            cfg,
            host_id_str,
            clock,
            tracker,
            emitter,
            observed_block: Arc::new(AtomicU64::new(initial_block.saturating_sub(1))),
        })
    }

    /// Feed a recorder watermark observation into the tracker. The CLI
    /// binary's watermark-subscription task calls this; tests call it
    /// directly to drive election state.
    pub fn update_watermark(&self, state: RecorderState) {
        self.tracker.update(state);
    }

    /// Note that a boundary with `block_number` was observed on B's tail
    /// (potentially published by another sealer). The next [`Self::tick_once`]
    /// call uses `max(self, observed) + 1` for the emitted block_number so
    /// flapping leadership cannot produce duplicate block numbers.
    pub fn observe_boundary(&self, b: &BlockBoundaryStart) {
        let prev = self.observed_block.load(Ordering::Relaxed);
        if b.block_number > prev {
            self.observed_block.store(b.block_number, Ordering::Relaxed);
        }
    }

    /// One pass of the tick loop. Returns `Some(block_number)` iff this
    /// process emitted a boundary on this tick.
    ///
    /// Exposed so deterministic tests can step the supervisor one tick at a
    /// time under `tokio::time::pause`. [`Self::run_forever`] is the
    /// production driver that sleeps to the next aligned tick.
    pub async fn tick_once(&mut self) -> Result<Option<u64>> {
        let snap: CaughtUpSet = self.tracker.snapshot();
        let cur = self.emitter.publisher_tx_tail();
        let now = self.clock.unix_ms();
        let leader = elect(
            &snap,
            cur,
            now,
            self.cfg.caught_up_lag_bytes,
            self.cfg.caught_up_stale_ms,
        );
        metrics::gauge!(
            "sealer_election_winner",
            "host_id" => self.host_id_str.clone(),
        )
        .set(if leader == Some(self.cfg.host_id) {
            1.0
        } else {
            0.0
        });

        if leader != Some(self.cfg.host_id) {
            return Ok(None);
        }

        // Sync block_number to whatever's been observed on B so flapping
        // leadership can't produce duplicates. `sync_block_number` is a
        // no-op when our local counter is already ahead.
        let observed = self.observed_block.load(Ordering::Relaxed);
        self.emitter.sync_block_number(observed.saturating_add(1));
        let emitted = self.emitter.run_one_tick().await?;
        // Record our own emission so the next snapshot reflects it.
        if emitted > observed {
            self.observed_block.store(emitted, Ordering::Relaxed);
        }
        Ok(Some(emitted))
    }

    /// Run the supervisor forever, sleeping to each aligned tick boundary.
    /// Used by the CLI binary; tests prefer [`Self::tick_once`] for
    /// determinism.
    pub async fn run_forever(mut self) -> Result<()> {
        loop {
            let now = self.clock.unix_ms();
            let next = next_tick(now, self.cfg.tick_interval_ms);
            let sleep_ms = next.saturating_sub(now);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            if let Err(e) = self.tick_once().await {
                tracing::warn!(error = %e, "tick failed; continuing");
            }
        }
    }
}

// Small accessor on BoundaryEmitter for the supervisor (kept private outside
// the crate to avoid leaking the transport seam).
impl<C: WallClock, P: BoundaryPublisher> BoundaryEmitter<C, P> {
    pub(crate) fn publisher_tx_tail(&self) -> kardamom_types::BPosition {
        self.publisher_ref().current_tx_tail()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::emitter::fakes::FakeBoundaryPublisher;
    use kardamom_log::testing::FakeBus;
    use kardamom_types::BPosition;

    fn cfg(host_id: u8) -> SealerConfig {
        SealerConfig {
            host_id,
            channel_b_uri: "ch".into(),
            channel_b_tx_stream_id: 1,
            channel_b_boundary_stream_id: 2,
            watermark_channel_uri: "ch".into(),
            watermark_stream_id_base: 100,
            recorder_ids: vec![1, 2, 3],
            caught_up_lag_bytes: 64 * 1024,
            caught_up_stale_ms: 500,
            tick_interval_ms: 250,
        }
    }

    fn rs(id: u8, ts: u64) -> RecorderState {
        RecorderState {
            recorder_id: id,
            fsynced: BPosition::ZERO,
            last_seen_ms: ts,
        }
    }

    /// One-shot construction helper used by every test below.
    fn build(
        host_id: u8,
        bus: &FakeBus,
        clock: MockClock,
        initial: u64,
    ) -> (
        Sealer<MockClock, FakeBoundaryPublisher>,
        FakeBoundaryPublisher,
    ) {
        let pubh = FakeBoundaryPublisher::new(bus.clone(), "ch", 2);
        let view = pubh.clone();
        let sealer = Sealer::new(cfg(host_id), clock, pubh, initial).unwrap();
        (sealer, view)
    }

    #[tokio::test(start_paused = true)]
    async fn lowest_id_wins_and_emits() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s1, view1) = build(1, &bus, clock.clone(), 1);
        let (mut s2, _) = build(2, &bus, clock.clone(), 1);
        let (mut s3, _) = build(3, &bus, clock.clone(), 1);

        for s in [&s1, &s2, &s3] {
            for hid in [1u8, 2, 3] {
                s.update_watermark(rs(hid, 1_000));
            }
        }

        assert_eq!(s1.tick_once().await.unwrap(), Some(1));
        assert_eq!(s2.tick_once().await.unwrap(), None);
        assert_eq!(s3.tick_once().await.unwrap(), None);

        let published = view1.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].block_number, 1);
        assert_eq!(published[0].l2_timestamp, 1_000);
    }

    #[tokio::test(start_paused = true)]
    async fn no_emission_when_no_recorder_caught_up() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s, view) = build(1, &bus, clock.clone(), 1);
        // No watermarks observed yet.
        assert_eq!(s.tick_once().await.unwrap(), None);
        assert!(view.published().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn block_numbers_are_monotonic_across_ticks() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s, view) = build(1, &bus, clock.clone(), 1);
        for hid in [1u8, 2, 3] {
            s.update_watermark(rs(hid, 1_000));
        }
        for tick in 0u64..5 {
            clock.advance(250);
            // Refresh watermarks so they stay fresh.
            for hid in [1u8, 2, 3] {
                s.update_watermark(rs(hid, 1_000 + (tick + 1) * 250));
            }
            let n = s.tick_once().await.unwrap().unwrap();
            assert_eq!(n, tick + 1);
        }
        let published = view.published();
        for (i, b) in published.iter().enumerate() {
            assert_eq!(b.block_number, (i as u64) + 1);
            // l2_timestamp is exact floor of the clock at tick_once time:
            // we started at 1_000 and advanced 250 ms before each call.
            assert_eq!(b.l2_timestamp, 1_250 + (i as u64) * 250);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn observe_boundary_prevents_duplicate_block_numbers() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        // Start with initial_block=1, but pretend another sealer already
        // published block 7. Our next emission must be 8.
        let (mut s, view) = build(1, &bus, clock.clone(), 1);
        for hid in [1u8, 2, 3] {
            s.update_watermark(rs(hid, 1_000));
        }
        s.observe_boundary(&BlockBoundaryStart {
            block_number: 7,
            end_tx_idx: BPosition::ZERO,
            l2_timestamp: 0,
        });
        let n = s.tick_once().await.unwrap().unwrap();
        assert_eq!(n, 8);
        assert_eq!(view.published()[0].block_number, 8);
    }
}
