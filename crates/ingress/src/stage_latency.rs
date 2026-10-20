//! The stage latencies of a transaction, from its publish on `tx_data`:
//!
//! - [`Stage::Sealed`]: to the cluster egress frame that gives it a
//!   canonical index.
//! - [`Stage::Executed`]: to the first executor receipt for it.
//!
//! The submit path and the two observers run on different threads. The
//! submit path holds the [`StageStamps`] and sends the hash and the
//! publish instant of each transaction to each stage over a bounded
//! channel. The egress tap owns the sealed [`StageClock`], and the
//! receipt pump owns the executed one. A clock moves the stamps into its
//! own map, and records one histogram sample for each hash that it has a
//! stamp for. A transaction that a different ingress published has no
//! stamp here, so it gives no sample. A later receipt copy from another
//! executor finds no stamp either, so the executed latency is the time
//! to the first receipt.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

use alloy_primitives::B256;

use crate::metrics::{
    EXECUTE_LATENCY_SECONDS, EXECUTE_STAMPS_DROPPED_TOTAL, EXECUTE_STAMPS_EXPIRED_TOTAL,
    SEAL_LATENCY_SECONDS, SEAL_STAMPS_DROPPED_TOTAL, SEAL_STAMPS_EXPIRED_TOTAL,
};

/// The stamps a channel holds between two observations. Only
/// [`StageClock::observe`] empties the channel, so the channel fills only
/// while no transaction of this ingress reaches the stage. At 6,000 tx/s
/// this capacity holds about 10 s of stamps.
const STAMP_CAPACITY: usize = 64 * 1024;

/// A clock removes a stamp older than this, and counts it in the
/// stage's expired counter. Most such stamps are of transactions that
/// the sequencer refused. An observation that comes later gives no
/// sample, so the histogram has no value above this age: a non-zero
/// counter under load means that the histogram undercounts the tail. The
/// age is above the 90 s receipt drain of `kardamom-load`.
const STAMP_TTL: Duration = Duration::from_secs(180);

/// A clock removes old stamps at most this often.
const SWEEP_EVERY: Duration = Duration::from_secs(5);

/// The step of the pipeline that a clock times.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Sealed,
    Executed,
}

impl Stage {
    /// The histogram of the stage's latency.
    fn histogram(self) -> &'static str {
        match self {
            Stage::Sealed => SEAL_LATENCY_SECONDS,
            Stage::Executed => EXECUTE_LATENCY_SECONDS,
        }
    }

    /// The counter of the stamps that the full channel dropped.
    fn dropped(self) -> &'static str {
        match self {
            Stage::Sealed => SEAL_STAMPS_DROPPED_TOTAL,
            Stage::Executed => EXECUTE_STAMPS_DROPPED_TOTAL,
        }
    }

    /// The counter of the stamps that no observation matched in time.
    fn expired(self) -> &'static str {
        match self {
            Stage::Sealed => SEAL_STAMPS_EXPIRED_TOTAL,
            Stage::Executed => EXECUTE_STAMPS_EXPIRED_TOTAL,
        }
    }
}

/// The submit side of one stage.
#[derive(Clone)]
struct StageSender {
    stage: Stage,
    tx: SyncSender<(B256, Instant)>,
}

impl StageSender {
    /// Send the stamp. A full channel drops it and counts it: the
    /// histogram then misses one sample, and the submit never waits. A
    /// clock that is gone (no observer) drops the stamp with no count.
    fn send(&self, stamp: (B256, Instant)) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send(stamp) {
            metrics::counter!(self.stage.dropped()).increment(1);
        }
    }
}

/// The submit side: it stamps each transaction at its publish, once for
/// each stage.
#[derive(Clone)]
pub struct StageStamps {
    sealed: StageSender,
    executed: StageSender,
}

impl StageStamps {
    /// Stamp `tx_hash` with the current instant for both stages.
    pub fn stamp(&self, tx_hash: B256) {
        let stamp = (tx_hash, Instant::now());
        self.sealed.send(stamp);
        self.executed.send(stamp);
    }
}

/// The observer side of each stage.
pub struct StageClocks {
    pub sealed: StageClock,
    pub executed: StageClock,
}

/// The observer side of one stage: the open stamps, by hash.
pub struct StageClock {
    stage: Stage,
    rx: Receiver<(B256, Instant)>,
    open: HashMap<B256, Instant>,
    last_sweep: Instant,
}

impl StageClock {
    /// The submit side and the two connected clocks.
    #[must_use]
    pub fn pair() -> (StageStamps, StageClocks) {
        let (sealed, sealed_clock) = StageClock::open(Stage::Sealed);
        let (executed, executed_clock) = StageClock::open(Stage::Executed);
        (
            StageStamps { sealed, executed },
            StageClocks {
                sealed: sealed_clock,
                executed: executed_clock,
            },
        )
    }

    fn open(stage: Stage) -> (StageSender, StageClock) {
        let (tx, rx) = sync_channel(STAMP_CAPACITY);
        let clock = StageClock {
            stage,
            rx,
            open: HashMap::new(),
            last_sweep: Instant::now(),
        };
        (StageSender { stage, tx }, clock)
    }

    /// Record the stage latency of `tx_hash`, if this ingress stamped it.
    pub fn observe(&mut self, tx_hash: B256) {
        if let Some(latency) = self.latency_at(tx_hash, Instant::now()) {
            metrics::histogram!(self.stage.histogram()).record(latency.as_secs_f64());
        }
    }

    /// The time from the stamp of `tx_hash` to `now`. It first moves the
    /// waiting stamps into the map, so a stamp sent before the
    /// observation is always found.
    fn latency_at(&mut self, tx_hash: B256, now: Instant) -> Option<Duration> {
        self.open.extend(self.rx.try_iter());
        self.sweep(now);
        self.open
            .remove(&tx_hash)
            .map(|stamped| now.saturating_duration_since(stamped))
    }

    /// Remove and count the stamps older than [`STAMP_TTL`], at most
    /// once in each [`SWEEP_EVERY`].
    fn sweep(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_sweep) < SWEEP_EVERY {
            return;
        }
        self.last_sweep = now;
        // The count is at most the length of the map, so it cannot overflow.
        let mut expired = 0u64;
        self.open.retain(|_, stamped| {
            let keep = now.saturating_duration_since(*stamped) < STAMP_TTL;
            expired += u64::from(!keep);
            keep
        });
        metrics::counter!(self.stage.expired()).increment(expired);
    }
}

#[cfg(test)]
#[path = "stage_latency_tests.rs"]
mod tests;
