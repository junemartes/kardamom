//! This is a shared, thread-safe per-transaction delivery tracker. It
//! holds cumulative counters, latency histograms, gas accounting, and
//! the pending, un-receipted set that the drain and sweeper settle.
//! Every transaction is tracked, by its locally computed hash, to a
//! receipt. A leftover entry is classified as `missing`, meaning
//! accepted but never receipted, or `unlanded`, meaning the submit
//! failed and never landed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use hdrhistogram::Histogram;

const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 60_000_000;
const HIST_SIGFIGS: u8 = 3;

/// A poison-tolerant lock. A panicked submit task must not block the
/// whole run's accounting, so this reads the data through the poison.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The cumulative delivery counters. Take a snapshot with [`Tracker::counts`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    /// The submits attempted.
    pub offered: u64,
    /// The submits that returned a hash. Ingress accepted these; under
    /// on-offer acking, this also means receipted.
    pub accepted: u64,
    /// The transactions confirmed by a receipt, either inline or drained.
    pub receipted: u64,
    /// The receipts with a status other than `0x1`.
    pub bad_status: u64,
}

struct Pending {
    submit_ts: Instant,
    accepted: bool,
}

/// A shared, thread-safe delivery tracker.
pub struct Tracker {
    offered: AtomicU64,
    accepted: AtomicU64,
    receipted: AtomicU64,
    bad_status: AtomicU64,
    /// The total gas used by receipted transactions. This is the
    /// numerator for gas/s, a workload-independent throughput measure,
    /// unlike tx/s.
    gas_used: AtomicU64,
    /// The gas for the current ramp step. `take_step_gas` resets this.
    step_gas: AtomicU64,
    lat_us: Mutex<Histogram<u64>>,
    /// The latency histogram for the current ramp step.
    /// [`Tracker::take_step_latency_us`] resets this. Per-step
    /// percentiles show where in the ramp the tail latency degrades;
    /// the cumulative histogram would blend early clean steps with
    /// later ones.
    step_lat_us: Mutex<Histogram<u64>>,
    pending: Mutex<HashMap<B256, Pending>>,
    /// In subscribe mode only: a receipt whose feed notification
    /// arrived before its submit task registered in `pending`. This
    /// handles the race between the feed and the ack.
    early: Mutex<HashMap<B256, u64>>,
}

impl Tracker {
    /// Construct an empty tracker.
    ///
    /// # Errors
    /// Returns an error if the code cannot allocate the latency histogram.
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            offered: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            receipted: AtomicU64::new(0),
            bad_status: AtomicU64::new(0),
            gas_used: AtomicU64::new(0),
            step_gas: AtomicU64::new(0),
            lat_us: Mutex::new(Histogram::new_with_bounds(
                HIST_LOW_US,
                HIST_HIGH_US,
                HIST_SIGFIGS,
            )?),
            step_lat_us: Mutex::new(Histogram::new_with_bounds(
                HIST_LOW_US,
                HIST_HIGH_US,
                HIST_SIGFIGS,
            )?),
            pending: Mutex::new(HashMap::new()),
            early: Mutex::new(HashMap::new()),
        })
    }

    /// Register an attempted submit.
    pub(crate) fn note_offered(&self) {
        self.offered.fetch_add(1, Ordering::Relaxed);
    }

    /// Register a submit that ingress accepted.
    pub(crate) fn note_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Park `hash` as pending, until a receipt settles it. `accepted`
    /// records whether ingress acked the submit; this decides `missing`
    /// versus `unlanded`.
    pub(crate) fn insert_pending(&self, hash: B256, submit_ts: Instant, accepted: bool) {
        lock(&self.pending).insert(
            hash,
            Pending {
                submit_ts,
                accepted,
            },
        );
    }

    /// Take a snapshot of `(hash, submit_ts)` for pending entries at
    /// least `min_age` old.
    pub(crate) fn pending_older_than(&self, min_age: Duration) -> Vec<(B256, Instant)> {
        lock(&self.pending)
            .iter()
            .filter(|(_, v)| v.submit_ts.elapsed() >= min_age)
            .map(|(h, v)| (*h, v.submit_ts))
            .collect()
    }

    /// Remove `hash` from the pending set. Returns `true` only if this
    /// call removed it. The caller should confirm only in that case: the
    /// live feed settles entries at the same time, and a transaction it
    /// already settled must not count twice.
    pub(crate) fn remove_pending(&self, hash: &B256) -> bool {
        lock(&self.pending).remove(hash).is_some()
    }

    /// The number of entries still pending.
    pub(crate) fn pending_len(&self) -> usize {
        lock(&self.pending).len()
    }

    /// The feed-side confirmation, for subscribe mode. Settles the
    /// pending entry for `hash`, or stores the status if the submit
    /// task has not registered yet.
    pub fn confirm_from_feed(&self, hash: B256, status: u64, gas: u64) {
        self.gas_used.fetch_add(gas, Ordering::Relaxed);
        self.step_gas.fetch_add(gas, Ordering::Relaxed);
        let settled = lock(&self.pending).remove(&hash);
        match settled {
            Some(p) => self.confirm(status, p.submit_ts.elapsed()),
            None => {
                lock(&self.early).insert(hash, status);
            }
        }
    }

    /// The submit-side registration, for subscribe mode. Parks the
    /// accepted transaction until its feed notification arrives, and
    /// settles it right away if the notification won the race.
    pub(crate) fn await_feed(&self, hash: B256, submit_ts: Instant) {
        self.insert_pending(hash, submit_ts, true);
        let early = lock(&self.early).remove(&hash);
        let Some(status) = early else { return };
        if self.remove_pending(&hash) {
            self.confirm(status, submit_ts.elapsed());
        }
    }

    /// Snapshot the cumulative counters.
    #[must_use]
    pub fn counts(&self) -> Counts {
        Counts {
            offered: self.offered.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            receipted: self.receipted.load(Ordering::Relaxed),
            bad_status: self.bad_status.load(Ordering::Relaxed),
        }
    }

    /// Sample up to `n` still-pending entries as `(hash, accepted, age)`.
    /// These are the concrete identities behind `missing` and `unlanded`,
    /// for post-run forensics. Query each hash against each ingress
    /// replica directly, to tell apart per-replica stream loss, cache
    /// eviction, and harness accounting bugs.
    #[must_use]
    pub fn sample_pending(&self, n: usize) -> Vec<(B256, bool, Duration)> {
        lock(&self.pending)
            .iter()
            .take(n)
            .map(|(h, v)| (*h, v.accepted, v.submit_ts.elapsed()))
            .collect()
    }

    /// Returns `(missing_accepted, unlanded)`: the leftover pending
    /// transactions after the drain. `missing_accepted` is
    /// accepted-but-never-receipted, a durability failure. `unlanded`
    /// is an offered transaction whose submit failed and never landed.
    #[must_use]
    pub fn remaining_pending(&self) -> (u64, u64) {
        let p = lock(&self.pending);
        let mut missing = 0u64;
        let mut unlanded = 0u64;
        for v in p.values() {
            if v.accepted {
                missing += 1;
            } else {
                unlanded += 1;
            }
        }
        (missing, unlanded)
    }

    /// Drain the per-step gas counter, for per-step Mgas/s.
    #[must_use]
    pub fn take_step_gas(&self) -> u64 {
        self.step_gas.swap(0, Ordering::Relaxed)
    }

    /// The total gas used by receipted transactions.
    pub fn total_gas(&self) -> u64 {
        self.gas_used.load(Ordering::Relaxed)
    }

    /// The latency percentiles `(p50, p95, p99, max)`, in microseconds,
    /// over the confirmed set.
    pub fn latency_us(&self) -> (u64, u64, u64, u64) {
        let h = lock(&self.lat_us);
        (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.95),
            h.value_at_quantile(0.99),
            h.max(),
        )
    }

    /// Drain the per-step latency histogram. Returns `(p50, p95, p99)`
    /// in microseconds for everything confirmed since the previous call,
    /// then resets the histogram.
    #[must_use]
    pub fn take_step_latency_us(&self) -> (u64, u64, u64) {
        let mut h = lock(&self.step_lat_us);
        let out = (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.95),
            h.value_at_quantile(0.99),
        );
        h.reset();
        out
    }

    pub(crate) fn confirm(&self, status: u64, latency: Duration) {
        self.confirm_with_gas(status, latency, 0);
    }

    /// Confirm with the receipt's gasUsed value. This is the HTTP
    /// re-fetch path.
    pub(crate) fn confirm_with_gas(&self, status: u64, latency: Duration, gas: u64) {
        self.gas_used.fetch_add(gas, Ordering::Relaxed);
        self.step_gas.fetch_add(gas, Ordering::Relaxed);
        self.receipted.fetch_add(1, Ordering::Relaxed);
        if status != 1 {
            self.bad_status.fetch_add(1, Ordering::Relaxed);
        }
        let us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        if let Ok(mut h) = self.lat_us.lock() {
            let _ = h.record(us.clamp(HIST_LOW_US, HIST_HIGH_US));
        }
        if let Ok(mut h) = self.step_lat_us.lock() {
            let _ = h.record(us.clamp(HIST_LOW_US, HIST_HIGH_US));
        }
    }
}
