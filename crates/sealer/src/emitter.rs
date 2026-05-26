//! Boundary emission loop. Run only by the elected leader.
//!
//! Each tick:
//!   1. Read the current B publication position (the boundary stream's
//!      `next_offset`, or the tx-stream's tail — see [`BoundaryPublisher`]).
//!   2. Compute `l2_timestamp = floor(now / tick_interval_ms) * tick_interval_ms`.
//!   3. Publish `BlockBoundaryStart { block_number, end_tx_idx, l2_timestamp }`.
//!   4. Increment `block_number`.
//!
//! The `BoundaryPublisher` trait abstracts the underlying transport so unit
//! tests run against `log::testing::FakePublication` and production
//! drives an Aeron concurrent publication (added in Task 13's CLI plumbing).

use std::sync::Arc;

use anyhow::Result;
use types::{BPosition, BlockBoundaryStart};

use crate::clock::WallClock;
use crate::tick::floor_to_tick;

/// Transport seam used by [`BoundaryEmitter`].
///
/// Implemented by both the testing FakeBus adapter and (in Task 13) the
/// real Aeron publisher. Returns the new tail position after the publish so
/// the next tick can read `end_tx_idx` without a separate round-trip.
pub trait BoundaryPublisher: Send {
    /// Publish a `BlockBoundaryStart`. Errors surface as the BACK_PRESSURE
    /// case (caller retries) or fatal (caller propagates).
    fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError>;

    /// Current tail position of the channel the boundary references as
    /// `end_tx_idx`. For v0 this is the tx-stream tail (matching:
    /// "every tx ≤ end_tx_idx is in this block"). The testing adapter exposes
    /// the in-memory next_offset; the real implementation will read the
    /// Aeron Publication's `position()` for the tx stream.
    fn current_tx_tail(&self) -> BPosition;
}

/// Errors a publisher can surface to the emitter. We distinguish back-pressure
/// (recoverable) from fatal (don't retry).
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("channel back-pressured; retry")]
    BackPressure,
    #[error("fatal publish error: {0}")]
    Fatal(String),
}

pub struct BoundaryEmitter<C: WallClock, P: BoundaryPublisher> {
    publisher: P,
    clock: Arc<C>,
    block_number: u64,
    tick_interval_ms: u64,
    /// Pre-formatted `host_id` for the per-emit metric labels, so we don't
    /// allocate a fresh `String` every tick.
    host_id_str: String,
}

impl<C: WallClock, P: BoundaryPublisher> BoundaryEmitter<C, P> {
    pub fn new(publisher: P, clock: C, initial_block: u64, tick_ms: u64, host_id: u8) -> Self {
        Self {
            publisher,
            clock: Arc::new(clock),
            block_number: initial_block,
            tick_interval_ms: tick_ms,
            host_id_str: host_id.to_string(),
        }
    }

    /// Adjust the local block counter forward if an observed B-tail boundary
    /// has a higher block_number than what we'd next emit. Never moves
    /// backwards (a leader does not regress its block_number; if the
    /// observation is stale we ignore it).
    pub fn sync_block_number(&mut self, candidate: u64) {
        if candidate > self.block_number {
            self.block_number = candidate;
        }
    }

    /// Current block_number the emitter would emit on the next tick.
    pub fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Emit one boundary at the current wall-clock tick. Returns the block
    /// number that was emitted, or an error if back-pressure persisted past
    /// the retry budget.
    ///
    /// On `BackPressure`, retries with bounded exponential backoff up to
    /// 50 ms total wall-clock time. Beyond that, the emitter records a
    /// skipped tick (metric) and returns; the next tick will produce a
    /// higher block_number — gaps are observable.
    pub async fn run_one_tick(&mut self) -> Result<u64> {
        let now = self.clock.unix_ms();
        let l2_ts = floor_to_tick(now, self.tick_interval_ms);
        let end_tx_idx = self.publisher.current_tx_tail();

        let msg = BlockBoundaryStart {
            block_number: self.block_number,
            end_tx_idx,
            l2_timestamp: l2_ts,
        };

        let mut backoff_ms = 1u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
        loop {
            match self.publisher.publish(&msg) {
                Ok(_pos) => {
                    let emitted = self.block_number;
                    self.block_number += 1;
                    metrics::counter!(
                        "sealer_boundaries_emitted_total",
                        "host_id" => self.host_id_str.clone(),
                    )
                    .increment(1);
                    metrics::gauge!(
                        "sealer_block_number",
                        "host_id" => self.host_id_str.clone(),
                    )
                    .set(emitted as f64);
                    return Ok(emitted);
                }
                Err(PublishError::BackPressure) => {
                    if std::time::Instant::now() >= deadline {
                        metrics::counter!(
                            "sealer_tick_skipped_total",
                            "host_id" => self.host_id_str.clone(),
                            "reason" => "backpressure",
                        )
                        .increment(1);
                        anyhow::bail!(
                            "backpressure on tx_ordering persisted >50 ms; skipping tick"
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(8);
                }
                Err(PublishError::Fatal(e)) => anyhow::bail!("fatal publish: {e}"),
            }
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    //! In-memory `BoundaryPublisher` over [`log::testing::FakeBus`].
    //!
    //! Exposed under `testing` so integration tests (single_emitter,
    //! failover, chaos_isolation) can re-use the same adapter the unit
    //! tests do.

    use std::sync::{Arc, Mutex};

    use log::testing::{FakeBus, FakePublication, FakeTypedSubscription};
    use types::{BPosition, BlockBoundaryStart};

    use super::{BoundaryPublisher, PublishError};

    /// Two-stream view of the in-memory bus: the tx tail (whose `next_offset`
    /// becomes `end_tx_idx`) and the boundary stream (where we publish).
    #[derive(Clone)]
    pub struct FakeBoundaryPublisher {
        bus: FakeBus,
        channel: String,
        boundary_stream: i32,
        /// Atomically-updated synthetic tail for the tx stream. Tests can poke
        /// this to simulate channel growth without actually publishing tx
        /// envelopes.
        synthetic_tx_tail: Arc<Mutex<BPosition>>,
    }

    impl FakeBoundaryPublisher {
        pub fn new(bus: FakeBus, channel: impl Into<String>, boundary_stream: i32) -> Self {
            Self {
                bus,
                channel: channel.into(),
                boundary_stream,
                synthetic_tx_tail: Arc::new(Mutex::new(BPosition::ZERO)),
            }
        }

        pub fn set_tx_tail(&self, pos: BPosition) {
            *self.synthetic_tx_tail.lock().unwrap() = pos;
        }

        /// Collect every boundary published so far (for assertions). Each call
        /// opens a fresh subscriber at offset 0.
        pub fn published(&self) -> Vec<BlockBoundaryStart> {
            let mut sub: FakeTypedSubscription<BlockBoundaryStart> =
                FakeTypedSubscription::open(&self.bus, &self.channel, self.boundary_stream);
            let mut out = Vec::new();
            loop {
                let mut got = 0;
                let delivered = sub.poll(
                    |v: BlockBoundaryStart, _pos| {
                        out.push(v);
                        got += 1;
                    },
                    64,
                );
                if delivered == 0 || got == 0 {
                    break;
                }
            }
            out
        }
    }

    impl BoundaryPublisher for FakeBoundaryPublisher {
        fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError> {
            let p = FakePublication::open(&self.bus, &self.channel, self.boundary_stream);
            p.publish(msg)
                .map_err(|e| PublishError::Fatal(e.to_string()))
        }

        fn current_tx_tail(&self) -> BPosition {
            *self.synthetic_tx_tail.lock().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use log::testing::FakeBus;

    fn emitter_with_fake(
        clock: MockClock,
        initial: u64,
    ) -> (
        BoundaryEmitter<MockClock, fakes::FakeBoundaryPublisher>,
        fakes::FakeBoundaryPublisher,
    ) {
        let bus = FakeBus::new();
        let publisher = fakes::FakeBoundaryPublisher::new(bus, "ch", 1);
        let cloned = publisher.clone();
        let emitter = BoundaryEmitter::new(publisher, clock, initial, 250, 1);
        (emitter, cloned)
    }

    #[tokio::test(start_paused = true)]
    async fn emits_one_boundary_per_tick() {
        let clock = MockClock::new(1_000);
        let (mut emitter, view) = emitter_with_fake(clock.clone(), 42);

        emitter.run_one_tick().await.unwrap();
        let obs = view.published();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].block_number, 42);
        assert_eq!(obs[0].l2_timestamp, 1_000);

        clock.set(1_300);
        emitter.run_one_tick().await.unwrap();
        let obs = view.published();
        assert_eq!(obs.len(), 2);
        assert_eq!(obs[1].block_number, 43);
        // floor(1_300 / 250) * 250 = 1_250
        assert_eq!(obs[1].l2_timestamp, 1_250);
    }

    #[tokio::test(start_paused = true)]
    async fn end_tx_idx_reflects_publisher_tail() {
        let clock = MockClock::new(0);
        let (mut emitter, view) = emitter_with_fake(clock.clone(), 1);
        view.set_tx_tail(BPosition {
            term_id: 0,
            term_offset: 9_999,
        });
        emitter.run_one_tick().await.unwrap();
        let obs = view.published();
        assert_eq!(obs[0].end_tx_idx.term_offset, 9_999);
    }

    #[tokio::test(start_paused = true)]
    async fn sync_block_number_only_moves_forward() {
        let clock = MockClock::new(0);
        let (mut emitter, _view) = emitter_with_fake(clock, 5);
        emitter.sync_block_number(3);
        assert_eq!(emitter.block_number(), 5);
        emitter.sync_block_number(10);
        assert_eq!(emitter.block_number(), 10);
    }
}
