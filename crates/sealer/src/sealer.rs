//! Top-level sealer supervisor.
//!
//! Wraps a [`crate::emitter::BoundaryEmitter`] in a tick loop. Each tick
//! publishes a `BlockBoundaryStart` onto tx_ordering with `block_number =
//! prev + 1`. There is no election: this is the only sealer.
//!
//! The tick loop is **driven externally** via [`Sealer::tick_once`] so
//! `tokio::time::pause` tests can deterministically advance one tick at a
//! time. The CLI binary calls [`Sealer::run_forever`] which loops with
//! `tokio::time::sleep(next_tick - now)`.

use std::time::Duration;

use anyhow::Result;
use kardamom_types::BlockBoundaryStart;

use crate::clock::WallClock;
use crate::config::SealerConfig;
use crate::emitter::{BoundaryEmitter, BoundaryPublisher};
use crate::tick::next_tick;

pub struct Sealer<C: WallClock + Clone, P: BoundaryPublisher> {
    cfg: SealerConfig,
    clock: C,
    emitter: BoundaryEmitter<C, P>,
}

impl<C: WallClock + Clone, P: BoundaryPublisher> Sealer<C, P> {
    /// Construct a sealer from already-prepared parts. The `initial_block` is
    /// what the bootstrap module produced from tx_ordering's tail.
    pub fn new(cfg: SealerConfig, clock: C, publisher: P, initial_block: u64) -> Result<Self> {
        cfg.validate()?;
        let tick_ms = cfg.tick_interval_ms;
        let emitter = BoundaryEmitter::new(publisher, clock.clone(), initial_block, tick_ms);
        Ok(Self {
            cfg,
            clock,
            emitter,
        })
    }

    /// Note that a boundary with `block_number` was observed on tx_ordering's
    /// tail (e.g. another process restarting from a snapshot). The next
    /// [`Self::tick_once`] call uses `max(self, observed) + 1` so the local
    /// emitter never regresses or duplicates.
    pub fn observe_boundary(&mut self, b: &BlockBoundaryStart) {
        self.emitter.sync_block_number(b.block_number + 1);
    }

    /// One pass of the tick loop. Always emits one boundary and returns its
    /// block number.
    pub async fn tick_once(&mut self) -> Result<u64> {
        self.emitter.run_one_tick().await
    }

    /// Run the supervisor forever, sleeping to each aligned tick boundary.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::emitter::fakes::FakeBoundaryPublisher;
    use kardamom_log::testing::FakeBus;
    use kardamom_types::BPosition;

    fn cfg() -> SealerConfig {
        SealerConfig {
            host_id: 1,
            channel_b_uri: "ch".into(),
            channel_b_tx_stream_id: 1,
            channel_b_boundary_stream_id: 2,
            tick_interval_ms: 250,
        }
    }

    fn build(
        bus: &FakeBus,
        clock: MockClock,
        initial: u64,
    ) -> (
        Sealer<MockClock, FakeBoundaryPublisher>,
        FakeBoundaryPublisher,
    ) {
        let pubh = FakeBoundaryPublisher::new(bus.clone(), "ch", 2);
        let view = pubh.clone();
        let sealer = Sealer::new(cfg(), clock, pubh, initial).unwrap();
        (sealer, view)
    }

    #[tokio::test(start_paused = true)]
    async fn emits_one_per_tick() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s, view) = build(&bus, clock.clone(), 1);

        assert_eq!(s.tick_once().await.unwrap(), 1);
        let published = view.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].block_number, 1);
        assert_eq!(published[0].l2_timestamp, 1_000);
    }

    #[tokio::test(start_paused = true)]
    async fn block_numbers_are_monotonic() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s, view) = build(&bus, clock.clone(), 1);
        for tick in 0u64..5 {
            clock.advance(250);
            let n = s.tick_once().await.unwrap();
            assert_eq!(n, tick + 1);
        }
        let published = view.published();
        for (i, b) in published.iter().enumerate() {
            assert_eq!(b.block_number, (i as u64) + 1);
            assert_eq!(b.l2_timestamp, 1_250 + (i as u64) * 250);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn observe_boundary_advances_counter() {
        let bus = FakeBus::new();
        let clock = MockClock::new(1_000);
        let (mut s, view) = build(&bus, clock.clone(), 1);
        s.observe_boundary(&BlockBoundaryStart {
            block_number: 7,
            end_tx_idx: BPosition::ZERO,
            l2_timestamp: 0,
        });
        let n = s.tick_once().await.unwrap();
        assert_eq!(n, 8);
        assert_eq!(view.published()[0].block_number, 8);
    }
}
