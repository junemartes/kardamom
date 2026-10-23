//! The shared metrics that locate a bottleneck on the transaction path.
//!
//! Every service records the same four names. The global `service` label
//! tells the services apart, so one dashboard puts all stages in path
//! order:
//!
//! - [`LOOP_NANOSECONDS`]: the time that a thread loop spends in each
//!   [`LoopState`]. The busy share of a loop is busy divided by the sum of
//!   the three states. A busy share near 1 means that the loop is
//!   saturated.
//! - [`QUEUE_DEPTH`]: the items that wait in a queue between two loops. A
//!   queue that grows sits in front of the bottleneck.
//! - [`STEP_SECONDS`]: the duration of one step of work. A hot loop
//!   records it per batch, not per transaction.
//! - [`BATCH_ITEMS`]: the items in one batch.

use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram};

/// The time of a loop in each state, in nanoseconds. Labels: `loop`,
/// `state`.
pub const LOOP_NANOSECONDS: &str = "kardamom_loop_nanoseconds_total";

/// The items in a queue. Label: `queue`.
pub const QUEUE_DEPTH: &str = "kardamom_queue_depth";

/// The duration of one step. Label: `step`.
pub const STEP_SECONDS: &str = "kardamom_step_seconds";

/// The items in one batch. Label: `batch`.
pub const BATCH_ITEMS: &str = "kardamom_batch_items";

/// The buckets of [`STEP_SECONDS`]: 1 µs to 10 s. A step of a hot loop
/// takes some microseconds, below the 100 µs floor of the shared buckets.
pub(crate) const STEP_BUCKETS: &[f64] = &[
    0.000_001,
    0.000_002_5,
    0.000_005,
    0.000_01,
    0.000_025,
    0.000_05,
    0.000_1,
    0.000_25,
    0.000_5,
    0.001,
    0.002_5,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
];

/// The buckets of a count histogram: the powers of 2 from 1 to 65,536.
pub(crate) const COUNT_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0,
    16384.0, 32768.0, 65536.0,
];

/// The buckets of a size histogram: the powers of 4 from 64 B to 64 MiB.
pub(crate) const BYTE_BUCKETS: &[f64] = &[
    64.0,
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
];

/// A [`LoopMeter`] adds its time to the counters at most this often.
const FLUSH_EVERY: Duration = Duration::from_millis(100);

/// What a loop does in a span of time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoopState {
    /// The loop works on an item.
    Busy,
    /// The loop has no input. It polls, parks or waits on a receive.
    Idle,
    /// The loop has work but waits on its output: a full queue or a
    /// back-pressured publication.
    Blocked,
}

impl LoopState {
    const ALL: [LoopState; 3] = [LoopState::Busy, LoopState::Idle, LoopState::Blocked];

    fn label(self) -> &'static str {
        match self {
            LoopState::Busy => "busy",
            LoopState::Idle => "idle",
            LoopState::Blocked => "blocked",
        }
    }
}

/// The time of one loop in each [`LoopState`]. The loop owns its meter.
/// It calls [`LoopMeter::mark`] at each change of state. The meter keeps
/// the time in plain fields, and adds it to the shared counters at most
/// once in [`FLUSH_EVERY`], so a mark costs one clock read.
pub struct LoopMeter {
    counters: [Counter; 3],
    pending: [u64; 3],
    mark: Instant,
    flushed: Instant,
}

impl LoopMeter {
    /// The meter of the loop `name`, from now.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        let now = Instant::now();
        Self {
            counters: LoopState::ALL
                .map(|state| metrics::counter!(LOOP_NANOSECONDS, "loop" => name, "state" => state.label())),
            pending: [0; 3],
            mark: now,
            flushed: now,
        }
    }

    /// Charge the time since the last mark to `state`.
    pub fn mark(&mut self, state: LoopState) {
        let now = Instant::now();
        // The value holds at most the time since the last flush plus one
        // span. A u64 of nanoseconds holds 584 years, so it cannot overflow.
        self.pending[state as usize] += Self::nanos(now.duration_since(self.mark));
        self.mark = now;
        if now.duration_since(self.flushed) >= FLUSH_EVERY {
            self.flush(now);
        }
    }

    /// Charge the time since the last mark to one pass of a polling loop:
    /// busy when the pass did work, idle when it did not.
    pub fn pass(&mut self, worked: bool) {
        self.mark(if worked {
            LoopState::Busy
        } else {
            LoopState::Idle
        });
    }

    fn flush(&mut self, now: Instant) {
        self.counters
            .iter()
            .zip(&mut self.pending)
            .for_each(|(counter, pending)| counter.increment(std::mem::take(pending)));
        self.flushed = now;
    }

    /// A span in nanoseconds. A span above 584 years saturates.
    fn nanos(span: Duration) -> u64 {
        u64::try_from(span.as_nanos()).unwrap_or(u64::MAX)
    }
}

/// The duration of one step of work.
#[derive(Clone)]
pub struct Step(Histogram);

impl Step {
    /// The step `name`.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(metrics::histogram!(STEP_SECONDS, "step" => name))
    }

    /// Record the time from `started` to now.
    pub fn since(&self, started: Instant) {
        self.0.record(started.elapsed());
    }
}

/// The items that wait in one queue.
#[derive(Clone)]
pub struct Queue(Gauge);

impl Queue {
    /// The queue `name`.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(metrics::gauge!(QUEUE_DEPTH, "queue" => name))
    }

    /// Set the depth to `items`.
    pub fn set(&self, items: usize) {
        self.0.set(Count(items).value());
    }
}

/// The items in each batch of one kind.
#[derive(Clone)]
pub struct Batch(Histogram);

impl Batch {
    /// The batch kind `name`.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(metrics::histogram!(BATCH_ITEMS, "batch" => name))
    }

    /// Record a batch of `items`.
    pub fn record(&self, items: usize) {
        self.0.record(Count(items).value());
    }
}

/// A number of items, as a metric value.
struct Count(usize);

impl Count {
    /// The value. A count above `u32::MAX` saturates: no queue or batch
    /// holds that many items.
    fn value(self) -> f64 {
        f64::from(u32::try_from(self.0).unwrap_or(u32::MAX))
    }
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
