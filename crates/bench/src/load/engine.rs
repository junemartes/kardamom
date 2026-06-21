//! Open-loop, rate-paced send engine + per-tx delivery tracker.
//!
//! The ingress `eth_sendRawTransaction` parks the caller until the receipt
//! arrives (on-offer ack), so submit RTT ≈ end-to-end latency. To drive load
//! *open-loop* (rate set by a pacer, not by completions) we spawn each submit
//! as its own task — bounded by an in-flight semaphore — rather than awaiting
//! one before issuing the next. Every tx is tracked by its locally-computed
//! hash to a receipt; submits that error are retried, and a post-phase drain
//! confirms any tx whose receipt hadn't landed inline.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use hdrhistogram::Histogram;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::load::plan::PlannedTx;

const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 60_000_000;
const HIST_SIGFIGS: u8 = 3;

/// Per-sender FIFO queues consumed round-robin, preserving per-sender nonce
/// order (a sender's nonce k is popped before k+1).
pub struct Queues {
    per_sender: Vec<VecDeque<PlannedTx>>,
    rr: usize,
}

impl Queues {
    /// Build from per-sender pre-generated queues.
    #[must_use]
    pub fn new(queues: Vec<Vec<PlannedTx>>) -> Self {
        Self {
            per_sender: queues.into_iter().map(VecDeque::from).collect(),
            rr: 0,
        }
    }

    /// Pop the next tx round-robin across senders, or `None` when all drained.
    pub fn pop_next(&mut self) -> Option<PlannedTx> {
        let n = self.per_sender.len();
        if n == 0 {
            return None;
        }
        for _ in 0..n {
            let i = self.rr % n;
            self.rr = (self.rr + 1) % n;
            if let Some(tx) = self.per_sender[i].pop_front() {
                return Some(tx);
            }
        }
        None
    }

    /// Total txs still queued.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.per_sender.iter().map(VecDeque::len).sum()
    }
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
    lat_us: Mutex<Histogram<u64>>,
    pending: Mutex<HashMap<B256, Pending>>,
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
            lat_us: Mutex::new(Histogram::new_with_bounds(
                HIST_LOW_US,
                HIST_HIGH_US,
                HIST_SIGFIGS,
            )?),
            pending: Mutex::new(HashMap::new()),
        })
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

    /// `(missing_accepted, unlanded)` — leftover pending txs after the drain:
    /// accepted-but-never-receipted (a durability failure) vs offered whose
    /// submit failed and never landed.
    #[must_use]
    pub fn remaining_pending(&self) -> (u64, u64) {
        let p = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    /// Latency percentiles in microseconds over the confirmed set.
    #[must_use]
    pub fn latency_us(&self) -> (u64, u64, u64) {
        let h = self
            .lat_us
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.99),
            h.max(),
        )
    }

    fn confirm(&self, status: u64, latency: Duration) {
        self.receipted.fetch_add(1, Ordering::Relaxed);
        if status != 1 {
            self.bad_status.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut h) = self.lat_us.lock() {
            let us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
            let _ = h.record(us.clamp(HIST_LOW_US, HIST_HIGH_US));
        }
    }
}

/// Look up a receipt's status (`1`/`0`), or `None` if not mined yet.
async fn receipt_status(client: &HttpClient, hash: B256) -> Option<u64> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .ok()?;
    let v = v?;
    let s = v.get("status")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

async fn submit_task(
    client: Arc<HttpClient>,
    tracker: Arc<Tracker>,
    tx: PlannedTx,
    _permit: OwnedSemaphorePermit,
    retry: u32,
) {
    tracker.offered.fetch_add(1, Ordering::Relaxed);
    let t0 = Instant::now();
    let mut accepted = false;
    for attempt in 0..=retry {
        let res: Result<B256, _> = client
            .request("eth_sendRawTransaction", rpc_params![tx.raw.clone()])
            .await;
        if res.is_ok() {
            accepted = true;
            break;
        }
        if attempt < retry {
            tokio::time::sleep(Duration::from_millis(200 * (u64::from(attempt) + 1))).await;
        }
    }
    if accepted {
        // on-offer: a successful submit means the receipt already arrived (the
        // tx was executed + receipted), so count it delivered NOW. Do not gate
        // delivery on a follow-up eth_getTransactionReceipt — the ingress's
        // in-memory receipt cache is volatile across an ingress restart, which
        // would otherwise false-flag already-delivered txs as "missing" during
        // ingress chaos. Re-fetch only best-effort to check status / latency
        // (assume success if the cache no longer has it).
        tracker.accepted.fetch_add(1, Ordering::Relaxed);
        let status = receipt_status(&client, tx.hash).await.unwrap_or(1);
        tracker.confirm(status, t0.elapsed());
    } else {
        // Submit never returned a hash. It may still have landed (a prior
        // attempt reached the chain despite the RPC erroring) — let the drain
        // re-check; a never-confirmed leftover is counted `unlanded`, not
        // `missing` (it was never accepted).
        match receipt_status(&client, tx.hash).await {
            Some(status) => tracker.confirm(status, t0.elapsed()),
            None => {
                tracker
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(
                        tx.hash,
                        Pending {
                            submit_ts: t0,
                            accepted: false,
                        },
                    );
            }
        }
    }
}

/// Drive submissions at `rate` tx/s for `dur`, spawning each as a bounded
/// (`sem`) task. Returns early if the queues drain first. Submits keep being
/// confirmed asynchronously; call [`drain`] afterwards to settle the tail.
pub async fn pacer(
    client: Arc<HttpClient>,
    sem: Arc<Semaphore>,
    tracker: Arc<Tracker>,
    queues: &mut Queues,
    rate: u32,
    dur: Duration,
    retry: u32,
) {
    if rate == 0 {
        return;
    }
    let tick = Duration::from_millis(10);
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let cap = (f64::from(rate) * 0.5).max(1.0);
    let mut credit = 0.0_f64;
    let start = Instant::now();
    loop {
        ticker.tick().await;
        if start.elapsed() >= dur {
            break;
        }
        credit = (credit + f64::from(rate) * tick.as_secs_f64()).min(cap);
        while credit >= 1.0 {
            let Some(tx) = queues.pop_next() else {
                return;
            };
            credit -= 1.0;
            // acquire_owned awaits when at max in-flight → natural back-pressure
            // (offered rate drops to match the pipeline's drain rate).
            let Ok(permit) = Arc::clone(&sem).acquire_owned().await else {
                return;
            };
            tokio::spawn(submit_task(
                Arc::clone(&client),
                Arc::clone(&tracker),
                tx,
                permit,
                retry,
            ));
        }
    }
}

/// Poll outstanding (un-confirmed) txs until they receipt or `deadline`.
pub async fn drain(client: Arc<HttpClient>, tracker: Arc<Tracker>, deadline: Instant) {
    loop {
        let pending: Vec<(B256, Instant)> = {
            let p = tracker
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            p.iter().map(|(h, v)| (*h, v.submit_ts)).collect()
        };
        if pending.is_empty() {
            break;
        }
        for (hash, submit_ts) in pending {
            if let Some(status) = receipt_status(&client, hash).await {
                tracker
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&hash);
                tracker.confirm(status, submit_ts.elapsed());
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::plan::PlannedTx;
    use alloy_primitives::Bytes;

    fn tx(sender: usize, nonce: u64) -> PlannedTx {
        PlannedTx {
            raw: Bytes::new(),
            hash: B256::with_last_byte(u8::try_from(nonce).unwrap_or(0)),
            sender,
            nonce,
        }
    }

    #[test]
    fn queues_round_robin_preserve_per_sender_order() {
        let mut q = Queues::new(vec![vec![tx(0, 0), tx(0, 1)], vec![tx(1, 0), tx(1, 1)]]);
        assert_eq!(q.remaining(), 4);
        // Round-robin: s0n0, s1n0, s0n1, s1n1.
        let order: Vec<(usize, u64)> = std::iter::from_fn(|| q.pop_next())
            .map(|t| (t.sender, t.nonce))
            .collect();
        assert_eq!(order, vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
        assert_eq!(q.remaining(), 0);
    }

    #[test]
    fn queues_skip_drained_sender() {
        let mut q = Queues::new(vec![vec![tx(0, 0)], vec![tx(1, 0), tx(1, 1)]]);
        let mut got = vec![];
        while let Some(t) = q.pop_next() {
            got.push((t.sender, t.nonce));
        }
        assert_eq!(got, vec![(0, 0), (1, 0), (1, 1)]);
    }

    #[test]
    fn tracker_counts_start_zero() {
        let t = Tracker::new().unwrap();
        let c = t.counts();
        assert_eq!(
            (c.offered, c.accepted, c.receipted, c.bad_status),
            (0, 0, 0, 0)
        );
        assert_eq!(t.remaining_pending(), (0, 0));
    }
}
