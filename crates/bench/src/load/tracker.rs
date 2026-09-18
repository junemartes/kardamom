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

use crate::config::{HIST_HIGH_US, HIST_LOW_US};

/// A poison-tolerant lock. A panicked submit task must not block the
/// whole run's accounting, so this reads the data through the poison.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The p50/p95/p99 triple, in microseconds, from a latency histogram.
fn quantiles(h: &Histogram<u64>) -> StepLatency {
    StepLatency {
        p50: h.value_at_quantile(0.50),
        p95: h.value_at_quantile(0.95),
        p99: h.value_at_quantile(0.99),
    }
}

/// The leftover pending transactions after a drain, from
/// [`Tracker::remaining_pending`]. `missing` is accepted-but-never
/// receipted, a durability failure. `unlanded` is an offered
/// transaction whose submit failed and never landed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingCounts {
    pub(crate) missing: u64,
    pub(crate) unlanded: u64,
}

/// The latency percentiles, in microseconds, from [`Tracker::latency_us`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Latency {
    pub(crate) p50: u64,
    pub(crate) p95: u64,
    pub(crate) p99: u64,
    pub(crate) max: u64,
}

/// The latency percentiles, in microseconds, from
/// [`Tracker::take_step_latency_us`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StepLatency {
    pub(crate) p50: u64,
    pub(crate) p95: u64,
    pub(crate) p99: u64,
}

/// The cumulative delivery counters. Take a snapshot with [`Tracker::counts`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Counts {
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

/// One still-pending entry, from [`Tracker::sample_pending`]: its
/// hash, whether ingress accepted the submit, and how long it has been
/// pending.
pub(crate) struct PendingSample {
    pub(crate) hash: B256,
    pub(crate) accepted: bool,
    pub(crate) age: Duration,
}

/// A shared, thread-safe delivery tracker.
pub(crate) struct Tracker {
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
    pub(crate) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            offered: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            receipted: AtomicU64::new(0),
            bad_status: AtomicU64::new(0),
            gas_used: AtomicU64::new(0),
            step_gas: AtomicU64::new(0),
            lat_us: Mutex::new(crate::config::new_latency_hist()?),
            step_lat_us: Mutex::new(crate::config::new_latency_hist()?),
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
    pub(crate) fn confirm_from_feed(&self, hash: B256, status: u64, gas: u64) {
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
    pub(crate) fn counts(&self) -> Counts {
        Counts {
            offered: self.offered.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            receipted: self.receipted.load(Ordering::Relaxed),
            bad_status: self.bad_status.load(Ordering::Relaxed),
        }
    }

    /// Sample up to `n` still-pending entries. These are the concrete
    /// identities behind `missing` and `unlanded`, for post-run
    /// forensics. Query each hash against each ingress replica
    /// directly, to tell apart per-replica stream loss, cache eviction,
    /// and harness accounting bugs.
    #[must_use]
    pub(crate) fn sample_pending(&self, n: usize) -> Vec<PendingSample> {
        lock(&self.pending)
            .iter()
            .take(n)
            .map(|(h, v)| PendingSample {
                hash: *h,
                accepted: v.accepted,
                age: v.submit_ts.elapsed(),
            })
            .collect()
    }

    /// The leftover pending transactions after the drain.
    #[must_use]
    pub(crate) fn remaining_pending(&self) -> PendingCounts {
        lock(&self.pending)
            .values()
            .fold(PendingCounts::default(), |acc, v| {
                if v.accepted {
                    PendingCounts {
                        missing: acc.missing + 1,
                        ..acc
                    }
                } else {
                    PendingCounts {
                        unlanded: acc.unlanded + 1,
                        ..acc
                    }
                }
            })
    }

    /// Drain the per-step gas counter, for per-step Mgas/s.
    #[must_use]
    pub(crate) fn take_step_gas(&self) -> u64 {
        self.step_gas.swap(0, Ordering::Relaxed)
    }

    /// The total gas used by receipted transactions.
    pub(crate) fn total_gas(&self) -> u64 {
        self.gas_used.load(Ordering::Relaxed)
    }

    /// The latency percentiles, in microseconds, over the confirmed set.
    pub(crate) fn latency_us(&self) -> Latency {
        let h = lock(&self.lat_us);
        let q = quantiles(&h);
        Latency {
            p50: q.p50,
            p95: q.p95,
            p99: q.p99,
            max: h.max(),
        }
    }

    /// Drain the per-step latency histogram: the percentiles, in
    /// microseconds, for everything confirmed since the previous call.
    /// Resets the histogram.
    #[must_use]
    pub(crate) fn take_step_latency_us(&self) -> StepLatency {
        let mut h = lock(&self.step_lat_us);
        let q = quantiles(&h);
        h.reset();
        q
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
        let _ = lock(&self.lat_us).record(us.clamp(HIST_LOW_US, HIST_HIGH_US));
        let _ = lock(&self.step_lat_us).record(us.clamp(HIST_LOW_US, HIST_HIGH_US));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every accessor reads through the poison-tolerant `lock()` helper,
    /// so a poisoned mutex still records the sample.
    #[test]
    fn confirm_with_gas_records_through_a_poisoned_mutex() {
        let tracker = Tracker::new().expect("tracker");
        // Poison the latency mutex, the way a panicking submit task would.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = tracker.lat_us.lock().unwrap();
            panic!("simulated submit-task panic while holding the lock");
        }));
        assert!(tracker.lat_us.is_poisoned());

        tracker.confirm_with_gas(1, Duration::from_micros(500), 21_000);

        let lat = tracker.latency_us();
        assert!(lat.p50 > 0, "sample was dropped: p50 is 0");
        assert!(lat.max > 0, "sample was dropped: max is 0");
    }
}
