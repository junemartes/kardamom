//! Shared, thread-safe per-tx delivery tracker: cumulative counters, latency
//! histograms, gas accounting, and the pending (un-receipted) set the drain
//! and sweeper settle. Every tx is tracked by its locally-computed hash to a
//! receipt; leftovers classify as `missing` (accepted-but-never-receipted)
//! or `unlanded` (submit failed and never landed).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use hdrhistogram::Histogram;

const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 60_000_000;
const HIST_SIGFIGS: u8 = 3;

/// Poison-tolerant lock: a panicked submit task must not wedge the whole
/// run's accounting, so take the data through the poison.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Cumulative delivery counters (snapshot with [`Tracker::counts`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    /// Submits attempted.
    pub offered: u64,
    /// Submits that returned a hash (ingress accepted; on-offer ⇒ receipted).
    pub accepted: u64,
    /// Txs confirmed via a receipt (inline or drained).
    pub receipted: u64,
    /// Receipts with a non-`0x1` status.
    pub bad_status: u64,
}

struct Pending {
    submit_ts: Instant,
    accepted: bool,
}

/// Shared, thread-safe delivery tracker.
pub struct Tracker {
    offered: AtomicU64,
    accepted: AtomicU64,
    receipted: AtomicU64,
    bad_status: AtomicU64,
    /// Total gas consumed by receipted txs (the gas/s numerator — workload-
    /// independent throughput, unlike tx/s).
    gas_used: AtomicU64,
    /// Per-ramp-step gas, reset by `take_step_gas`.
    step_gas: AtomicU64,
    lat_us: Mutex<Histogram<u64>>,
    /// Per-ramp-step latency histogram, reset by [`Tracker::take_step_latency_us`]
    /// — per-step percentiles localize WHERE in the ramp the tail degrades
    /// (the cumulative histogram smears early clean steps over late ones).
    step_lat_us: Mutex<Histogram<u64>>,
    pending: Mutex<HashMap<B256, Pending>>,
    /// Subscribe mode only: receipts whose feed notification arrived before
    /// their submit task registered in `pending` (feed vs ack race).
    early: Mutex<HashMap<B256, u64>>,
}

impl Tracker {
    /// Construct an empty tracker.
    ///
    /// # Errors
    /// Errors if the latency histogram can't be allocated.
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

    /// Register an ingress-accepted submit.
    pub(crate) fn note_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Park `hash` as pending until a receipt settles it (`accepted` records
    /// whether ingress acked the submit — it decides missing vs unlanded).
    pub(crate) fn insert_pending(&self, hash: B256, submit_ts: Instant, accepted: bool) {
        lock(&self.pending).insert(
            hash,
            Pending {
                submit_ts,
                accepted,
            },
        );
    }

    /// Snapshot `(hash, submit_ts)` of pending entries at least `min_age` old.
    pub(crate) fn pending_older_than(&self, min_age: Duration) -> Vec<(B256, Instant)> {
        lock(&self.pending)
            .iter()
            .filter(|(_, v)| v.submit_ts.elapsed() >= min_age)
            .map(|(h, v)| (*h, v.submit_ts))
            .collect()
    }

    /// Remove `hash` from the pending set; `true` iff THIS call removed it.
    /// The caller may confirm only then — the live feed settles entries
    /// concurrently, and a tx it already settled must not double-count.
    pub(crate) fn remove_pending(&self, hash: &B256) -> bool {
        lock(&self.pending).remove(hash).is_some()
    }

    /// Number of still-pending entries.
    pub(crate) fn pending_len(&self) -> usize {
        lock(&self.pending).len()
    }

    /// Feed-side confirmation (subscribe mode): settle the pending entry for
    /// `hash`, or stash the status if the submit task hasn't registered yet.
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

    /// Submit-side registration (subscribe mode): park the accepted tx until
    /// its feed notification, settling immediately if the notification won
    /// the race.
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

    /// Sample up to `n` still-pending entries `(hash, accepted, age)` — the
    /// concrete identities behind `missing`/`unlanded`, for post-run
    /// forensics (query each hash against each ingress replica directly to
    /// distinguish per-replica stream loss from cache eviction from harness
    /// accounting bugs).
    #[must_use]
    pub fn sample_pending(&self, n: usize) -> Vec<(B256, bool, Duration)> {
        lock(&self.pending)
            .iter()
            .take(n)
            .map(|(h, v)| (*h, v.accepted, v.submit_ts.elapsed()))
            .collect()
    }

    /// `(missing_accepted, unlanded)` — leftover pending txs after the drain:
    /// accepted-but-never-receipted (a durability failure) vs offered whose
    /// submit failed and never landed.
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

    /// Drain the per-step gas counter (for per-step Mgas/s).
    #[must_use]
    pub fn take_step_gas(&self) -> u64 {
        self.step_gas.swap(0, Ordering::Relaxed)
    }

    /// Total gas consumed by receipted txs.
    pub fn total_gas(&self) -> u64 {
        self.gas_used.load(Ordering::Relaxed)
    }

    /// Latency percentiles `(p50, p95, p99, max)` in microseconds over the
    /// confirmed set.
    pub fn latency_us(&self) -> (u64, u64, u64, u64) {
        let h = lock(&self.lat_us);
        (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.95),
            h.value_at_quantile(0.99),
            h.max(),
        )
    }

    /// Drain the per-step latency histogram: `(p50, p95, p99)` in µs for
    /// everything confirmed since the previous call, then reset it.
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

    /// Confirm with the receipt's gasUsed (the HTTP-refetch path).
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
