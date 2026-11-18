//! This is a shared, thread-safe per-transaction delivery tracker. It
//! holds cumulative counters, latency histograms, gas accounting, and
//! the pending, un-receipted set that the drain and sweeper settle.
//! Every transaction is tracked, by its locally computed hash, to a
//! receipt. A leftover entry is classified as `missing`, meaning
//! accepted but never receipted, or `unlanded`, meaning the submit
//! failed and never landed.

mod classes;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use hdrhistogram::Histogram;

use crate::config::{HIST_HIGH_US, HIST_LOW_US};
use crate::load::config::ClassReport;
use crate::load::json_hex_u64;
use crate::load::plan::{PlannedTx, TxClass};
use crate::signers::DerivedSigner;

use classes::ClassTallies;

/// How many contradicting receipts get a full warning line. Past
/// this, only the counter moves: one bad block wrongs every sender at
/// once, and a warning per receipt would drown the report.
const CONTRADICTION_SAMPLES: u64 = 32;

/// The fields of a mined receipt the tracker reads: the status and
/// gas for the counters, and the sender and block for the check that
/// the receipt describes the transaction it was fetched for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceiptStatus {
    pub(crate) status: u64,
    pub(crate) gas: u64,
    pub(crate) from: Option<Address>,
    pub(crate) block: Option<u64>,
}

impl ReceiptStatus {
    /// Read a JSON receipt. `None` without a `status` field: that is a
    /// null or malformed answer, not a mined receipt.
    pub(crate) fn from_json(v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            status: json_hex_u64(&v["status"])?,
            gas: json_hex_u64(&v["gasUsed"]).unwrap_or(0),
            from: v["from"].as_str().and_then(|s| s.parse().ok()),
            block: json_hex_u64(&v["blockNumber"]),
        })
    }

    /// Whether this is the executor's skip receipt: status 0 and no gas.
    /// A skip does not spend the nonce, so the nonce can land again in a
    /// later block. A real execution always charges intrinsic gas.
    fn is_skip(&self) -> bool {
        self.status == 0 && self.gas == 0
    }

    /// A successful receipt with no sender or block, for a test that
    /// only counts.
    #[cfg(test)]
    pub(crate) fn ok(gas: u64) -> Self {
        Self {
            status: 1,
            gas,
            from: None,
            block: None,
        }
    }
}

/// What the receipt of a planned transaction must say: the signer,
/// and the nonce the signer spent, which orders its block against the
/// signer's other transactions. The class selects the tally of the
/// receipt.
#[derive(Debug, Clone, Copy)]
struct Expected {
    sender: Address,
    nonce: u64,
    class: TxClass,
}

/// The blocks every confirmed transaction landed in, per sender by
/// nonce. A sender's nonces execute in order, so its blocks must not
/// decrease along the nonce sequence.
#[derive(Default)]
struct Placement {
    by_sender: HashMap<Address, BTreeMap<u64, u64>>,
    /// The pacer registers each streamed batch before any submit can race its receipt.
    streamed: HashMap<B256, Expected>,
}

impl Placement {
    /// Record `block` for `(sender, nonce)`, and return the neighbour
    /// that contradicts it: a lower nonce in a later block, or a higher
    /// nonce in an earlier block.
    fn place(&mut self, e: Expected, block: u64) -> Option<(u64, u64)> {
        let blocks = self.by_sender.entry(e.sender).or_default();
        let before = blocks.range(..e.nonce).next_back();
        let after = blocks.range(e.nonce + 1..).next();
        let wrong = before
            .filter(|(_, b)| **b > block)
            .or(after.filter(|(_, b)| **b < block))
            .map(|(n, b)| (*n, *b));
        blocks.insert(e.nonce, block);
        wrong
    }
}

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
    /// The receipts that contradict their transaction: another sender,
    /// no block, or a block out of order with the sender's other nonces.
    pub bad_receipt: u64,
}

struct Pending {
    submit_ts: Instant,
    accepted: bool,
}

/// One still-pending entry, from [`Tracker::sample_pending`]: its
/// hash, whether ingress accepted the submit, and how long it has been
/// pending.
#[derive(Debug, serde::Serialize)]
pub(crate) struct PendingSample {
    pub(crate) hash: B256,
    pub(crate) accepted: bool,
    pub(crate) age: Duration,
    sender: Option<Address>,
    nonce: Option<u64>,
}

/// A shared, thread-safe delivery tracker.
pub(crate) struct Tracker {
    offered: AtomicU64,
    accepted: AtomicU64,
    receipted: AtomicU64,
    bad_status: AtomicU64,
    bad_receipt: AtomicU64,
    /// The submits the ingress refused, every attempt counted.
    refused: AtomicU64,
    /// What each planned transaction's receipt must say, by hash.
    /// Filled before the run, so the reads take no lock.
    expected: HashMap<B256, Expected>,
    placement: Mutex<Placement>,
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
    /// The receipts, failed receipts, and latency of each class. A
    /// receipt of a hash with no expectation counts in no class.
    classes: ClassTallies,
    pending: Mutex<HashMap<B256, Pending>>,
    /// In subscribe mode only: a receipt whose feed notification
    /// arrived before its submit task registered in `pending`. This
    /// handles the race between the feed and the ack.
    early: Mutex<HashMap<B256, ReceiptStatus>>,
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
            bad_receipt: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            expected: HashMap::new(),
            placement: Mutex::new(Placement::default()),
            gas_used: AtomicU64::new(0),
            step_gas: AtomicU64::new(0),
            lat_us: Mutex::new(crate::config::new_latency_hist()?),
            step_lat_us: Mutex::new(crate::config::new_latency_hist()?),
            classes: ClassTallies::new()?,
            pending: Mutex::new(HashMap::new()),
            early: Mutex::new(HashMap::new()),
        })
    }

    /// Record what the receipt of each planned transaction must say.
    /// `signers[tx.sender]` signed `tx`. A hash with no expectation is
    /// confirmed without the check.
    pub(crate) fn expect<'a>(
        &mut self,
        signers: &[DerivedSigner],
        planned: impl Iterator<Item = &'a PlannedTx>,
    ) {
        self.expected.extend(planned.filter_map(|tx| {
            let sender = signers.get(tx.sender)?.address;
            Some((
                tx.hash,
                Expected {
                    sender,
                    nonce: tx.nonce,
                    class: tx.class,
                },
            ))
        }));
    }

    pub(crate) fn expect_streamed<'a>(
        &self,
        signers: &[DerivedSigner],
        planned: impl Iterator<Item = &'a PlannedTx>,
    ) {
        lock(&self.placement).streamed.extend(planned.map(|tx| {
            (
                tx.hash,
                Expected {
                    sender: signers[tx.sender].address,
                    nonce: tx.nonce,
                    class: tx.class,
                },
            )
        }));
    }

    fn expectation(&self, hash: &B256) -> Option<Expected> {
        self.expected
            .get(hash)
            .copied()
            .or_else(|| lock(&self.placement).streamed.get(hash).copied())
    }

    /// Register an attempted submit.
    pub(crate) fn note_offered(&self) {
        self.offered.fetch_add(1, Ordering::Relaxed);
    }

    /// Register a refused submit attempt, and return the count of
    /// refusals with this one. The count only paces the refusal log, so
    /// a wrap is harmless.
    pub(crate) fn note_refused(&self) -> u64 {
        self.refused.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
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
    pub(crate) fn confirm_from_feed(&self, hash: B256, receipt: &ReceiptStatus) {
        let settled = lock(&self.pending).remove(&hash);
        match settled {
            Some(p) => self.confirm(hash, receipt, p.submit_ts.elapsed()),
            None => {
                lock(&self.early).insert(hash, *receipt);
            }
        }
    }

    /// The submit-side registration, for subscribe mode. Parks the
    /// accepted transaction until its feed notification arrives, and
    /// settles it right away if the notification won the race.
    pub(crate) fn await_feed(&self, hash: B256, submit_ts: Instant) {
        self.insert_pending(hash, submit_ts, true);
        let early = lock(&self.early).remove(&hash);
        let Some(receipt) = early else { return };
        if self.remove_pending(&hash) {
            self.confirm(hash, &receipt, submit_ts.elapsed());
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
            bad_receipt: self.bad_receipt.load(Ordering::Relaxed),
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
                sender: self.expectation(h).map(|expected| expected.sender),
                nonce: self.expectation(h).map(|expected| expected.nonce),
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

    /// The receipts, failed receipts, and latency of each class that
    /// got a receipt.
    pub(crate) fn class_report(&self) -> BTreeMap<TxClass, ClassReport> {
        self.classes.report()
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

    /// Confirm `hash` with its mined receipt: count the status and gas,
    /// record the latency, and check that the receipt describes the
    /// planned transaction.
    pub(crate) fn confirm(&self, hash: B256, receipt: &ReceiptStatus, latency: Duration) {
        self.gas_used.fetch_add(receipt.gas, Ordering::Relaxed);
        self.step_gas.fetch_add(receipt.gas, Ordering::Relaxed);
        self.receipted.fetch_add(1, Ordering::Relaxed);
        if receipt.status != 1 {
            self.bad_status.fetch_add(1, Ordering::Relaxed);
        }
        let expected = self.expectation(&hash);
        if let Some(why) = expected.and_then(|e| self.contradiction(e, receipt)) {
            let seen = self.bad_receipt.fetch_add(1, Ordering::Relaxed);
            if seen < CONTRADICTION_SAMPLES {
                tracing::warn!(%hash, "receipt contradicts its transaction: {why}");
            }
        }
        let us = u64::try_from(latency.as_micros())
            .unwrap_or(u64::MAX)
            .clamp(HIST_LOW_US, HIST_HIGH_US);
        let _ = lock(&self.lat_us).record(us);
        let _ = lock(&self.step_lat_us).record(us);
        if let Some(e) = expected {
            self.classes.record(e.class, receipt.status, us);
        }
    }

    /// Why `receipt` cannot be the receipt of the planned transaction
    /// `e`: it names another sender, it names no block, or its block is
    /// out of order with the sender's other nonces. `None` for a receipt
    /// that fits. A skip receipt does not take a place in the nonce
    /// order, because its nonce stays unspent.
    fn contradiction(&self, e: Expected, receipt: &ReceiptStatus) -> Option<String> {
        if receipt.from != Some(e.sender) {
            return Some(format!(
                "from {:?}, signed by {} nonce {}",
                receipt.from, e.sender, e.nonce
            ));
        }
        let Some(block) = receipt.block else {
            return Some(format!(
                "no block number, sender {} nonce {}",
                e.sender, e.nonce
            ));
        };
        if receipt.is_skip() {
            return None;
        }
        let (nonce, other) = lock(&self.placement).place(e, block)?;
        Some(format!(
            "sender {} nonce {} in block {block}, but nonce {nonce} in block {other}",
            e.sender, e.nonce
        ))
    }
}

#[cfg(test)]
mod tests;
