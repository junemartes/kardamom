//! Open-loop, rate-paced send engine + per-tx delivery tracker.
//!
//! The ingress `eth_sendRawTransaction` parks the caller until the receipt
//! arrives (on-offer ack), so submit RTT ≈ end-to-end latency. To drive load
//! *open-loop* (rate set by a pacer, not by completions) we spawn each submit
//! as its own task — bounded by an in-flight semaphore and collected in a
//! [`tokio::task::JoinSet`] the caller joins before evaluating — rather than
//! awaiting one before issuing the next. Every tx is tracked by its
//! locally-computed hash to a receipt; submits that error are retried (after
//! checking the tx didn't already land, so a duplicate isn't resubmitted),
//! and a post-phase drain confirms any tx whose receipt hadn't landed inline.
//! With `verify_receipts` (non-chaos soak) an accepted submit whose receipt
//! can't be re-fetched stays pending and counts as `missing` if it never
//! confirms — the independent must-deliver check.

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

/// How a submit acks and how its receipt is observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitMode {
    /// `eth_sendRawTransaction`: the RPC parks until the receipt arrives, so
    /// a successful submit *is* the delivery confirmation (one held
    /// connection per in-flight tx).
    Blocking,
    /// `kardamom_sendRawTransactionAsync`: the RPC acks at publish; receipts
    /// arrive out-of-band on the `kardamom_subscribeReceipts` feed (see
    /// `receipt_feed_task`), with the drain's `eth_getTransactionReceipt`
    /// polling as the catch-all.
    Subscribe,
}

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
    /// Total gas consumed by receipted txs (the gas/s numerator — workload-
    /// independent throughput, unlike tx/s).
    gas_used: AtomicU64,
    /// Per-ramp-step gas, reset by `take_step_gas`.
    step_gas: AtomicU64,
    lat_us: Mutex<Histogram<u64>>,
    /// Per-ramp-step latency histogram, reset by [`Tracker::take_step_latencies`]
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

    /// Feed-side confirmation (subscribe mode): settle the pending entry for
    /// `hash`, or stash the status if the submit task hasn't registered yet.
    pub fn confirm_from_feed(&self, hash: B256, status: u64, gas: u64) {
        self.gas_used.fetch_add(gas, Ordering::Relaxed);
        self.step_gas.fetch_add(gas, Ordering::Relaxed);
        let settled = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&hash);
        match settled {
            Some(p) => self.confirm(status, p.submit_ts.elapsed()),
            None => {
                self.early
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(hash, status);
            }
        }
    }

    /// Submit-side registration (subscribe mode): park the accepted tx until
    /// its feed notification, settling immediately if the notification won
    /// the race.
    fn await_feed(&self, hash: B256, submit_ts: Instant) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                hash,
                Pending {
                    submit_ts,
                    accepted: true,
                },
            );
        let early = self
            .early
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&hash);
        let Some(status) = early else { return };
        if self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&hash)
            .is_some()
        {
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
        let p = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        p.iter()
            .take(n)
            .map(|(h, v)| (*h, v.accepted, v.submit_ts.elapsed()))
            .collect()
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
        let h = self
            .lat_us
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let mut h = self
            .step_lat_us
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let out = (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.95),
            h.value_at_quantile(0.99),
        );
        h.reset();
        out
    }

    fn confirm(&self, status: u64, latency: Duration) {
        self.confirm_with_gas(status, latency, 0);
    }

    /// Confirm with the receipt's gasUsed (the HTTP-refetch path).
    fn confirm_with_gas(&self, status: u64, latency: Duration, gas: u64) {
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

/// Look up a receipt's `(status, gasUsed)`, or `None` if not mined yet.
async fn receipt_status(client: &HttpClient, hash: B256) -> Option<(u64, u64)> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .ok()?;
    let v = v?;
    let status =
        u64::from_str_radix(v.get("status")?.as_str()?.trim_start_matches("0x"), 16).ok()?;
    let gas = v
        .get("gasUsed")
        .and_then(|g| g.as_str())
        .and_then(|g| u64::from_str_radix(g.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0);
    Some((status, gas))
}

#[allow(clippy::too_many_arguments)]
async fn submit_task(
    client: Arc<HttpClient>,
    tracker: Arc<Tracker>,
    tx: PlannedTx,
    _permit: OwnedSemaphorePermit,
    retry: u32,
    verify_receipts: bool,
    mode: SubmitMode,
    feed_confirm: bool,
) {
    let method = match mode {
        SubmitMode::Blocking => "eth_sendRawTransaction",
        SubmitMode::Subscribe => "kardamom_sendRawTransactionAsync",
    };
    tracker.offered.fetch_add(1, Ordering::Relaxed);
    let t0 = Instant::now();
    let mut accepted = false;
    for attempt in 0..=retry {
        let res: Result<B256, _> = client.request(method, rpc_params![tx.raw.clone()]).await;
        if res.is_ok() {
            accepted = true;
            break;
        }
        if attempt < retry {
            // The errored attempt may still have landed (e.g. the connection
            // died after ingress forwarded the tx). Resubmitting a landed tx
            // registers as a past-nonce drop at the sequencer, so check for a
            // receipt first and stop retrying if it's already there.
            if let Some((status, gas)) = receipt_status(&client, tx.hash).await {
                tracker.confirm_with_gas(status, t0.elapsed(), gas);
                return;
            }
            tokio::time::sleep(Duration::from_millis(200 * (u64::from(attempt) + 1))).await;
        }
    }
    if accepted {
        tracker.accepted.fetch_add(1, Ordering::Relaxed);
        if mode == SubmitMode::Subscribe {
            // The ack only means *published* — the receipt arrives on the
            // subscription feed (or the drain's polling settles it). No
            // chaos-mode ack-trust here: delivery is verified for real.
            tracker.await_feed(tx.hash, t0);
            return;
        }
        // Feed-confirm: skip the per-tx re-fetch — the WebSocket feed (or a
        // frame it already delivered: `await_feed` checks the early map)
        // confirms with real status, and the drain re-polls any straggler,
        // so every accepted tx still ends confirmed or counted `missing`.
        // One HTTP call per tx instead of two.
        if feed_confirm {
            tracker.await_feed(tx.hash, t0);
            return;
        }
        // on-offer: a successful submit means the receipt already arrived (the
        // tx was executed + receipted). Re-fetch it to check status/latency.
        match receipt_status(&client, tx.hash).await {
            Some((status, gas)) => tracker.confirm_with_gas(status, t0.elapsed(), gas),
            None if verify_receipts => {
                // Non-chaos soak: independently verify the on-offer contract.
                // No receipt for an accepted tx → keep it pending; the drain
                // re-polls it, and a never-confirmed leftover is counted
                // `missing` (must-deliver violated).
                tracker
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(
                        tx.hash,
                        Pending {
                            submit_ts: t0,
                            accepted: true,
                        },
                    );
            }
            None => {
                // Chaos mode: the ingress's in-memory receipt cache is
                // volatile across an ingress restart, so a failed re-fetch
                // would false-flag already-delivered txs as "missing". Trust
                // the on-offer ack and count it delivered with success status.
                tracker.confirm(1, t0.elapsed());
            }
        }
    } else {
        // Submit never returned a hash. It may still have landed (a prior
        // attempt reached the chain despite the RPC erroring) — let the drain
        // re-check; a never-confirmed leftover is counted `unlanded`, not
        // `missing` (it was never accepted).
        match receipt_status(&client, tx.hash).await {
            Some((status, gas)) => tracker.confirm_with_gas(status, t0.elapsed(), gas),
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
/// (`sem`) task collected in `tasks`. Returns early if the queues drain
/// first. Submits keep being confirmed asynchronously; call
/// [`join_submit_tasks`] then [`drain`] afterwards to settle the tail.
#[allow(clippy::too_many_arguments)]
pub async fn pacer(
    client: Arc<HttpClient>,
    sem: Arc<Semaphore>,
    tracker: Arc<Tracker>,
    tasks: &mut tokio::task::JoinSet<()>,
    queues: &mut Queues,
    rate: u32,
    dur: Duration,
    retry: u32,
    verify_receipts: bool,
    mode: SubmitMode,
    feed_confirm: bool,
) {
    if rate == 0 {
        return;
    }
    let tick = Duration::from_millis(10);
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Credit cap = 40ms of traffic. The old half-second cap released a
    // rate/2 burst at step start (5,000 txs at a 10k step) — enough to blow
    // straight through every sender's sequencer reorder window
    // (max_pending_per_sender) before pacing even began, tripping evictions
    // that read as a pipeline edge but are pure harness artifact. 4 ticks of
    // catch-up still absorbs scheduler hiccups without the flood.
    let cap = (f64::from(rate) * 0.04).max(1.0);
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
            tasks.spawn(submit_task(
                Arc::clone(&client),
                Arc::clone(&tracker),
                tx,
                permit,
                retry,
                verify_receipts,
                mode,
                feed_confirm,
            ));
        }
    }
}

/// Await all spawned submit tasks (each internally bounded by the client's
/// request timeout), giving up at `deadline` so a wedged task can't stall the
/// verdict forever. Must run before [`drain`]/the final counts read: an
/// in-flight task is counted `offered` but is not yet `accepted`, `missing`,
/// or `unlanded`, so evaluating early misclassifies the tail.
pub async fn join_submit_tasks(tasks: &mut tokio::task::JoinSet<()>, deadline: Instant) {
    while !tasks.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero()
            || tokio::time::timeout(remaining, tasks.join_next())
                .await
                .is_err()
        {
            tracing::warn!(
                outstanding = tasks.len(),
                "drain deadline hit with submit task(s) still in flight; \
                 their txs are counted offered but not accepted/missing/unlanded"
            );
            break;
        }
    }
}

/// One sweep over outstanding (un-confirmed) txs at least `min_age` old:
/// re-fetch each receipt and settle the entry if found. Returns how many
/// entries remained pending after the sweep. Confirm only if WE removed the
/// entry: the live feed keeps confirming concurrently, and a tx it settled
/// between our poll and this remove must not be double-counted.
pub async fn sweep_pending_once(
    client: &Arc<HttpClient>,
    tracker: &Arc<Tracker>,
    min_age: Duration,
) -> usize {
    let pending: Vec<(B256, Instant)> = {
        let p = tracker
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        p.iter()
            .filter(|(_, v)| v.submit_ts.elapsed() >= min_age)
            .map(|(h, v)| (*h, v.submit_ts))
            .collect()
    };
    for (hash, submit_ts) in pending {
        if let Some((status, gas)) = receipt_status(client, hash).await {
            let removed = tracker
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&hash)
                .is_some();
            if removed {
                tracker.confirm_with_gas(status, submit_ts.elapsed(), gas);
            }
        }
    }
    tracker
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len()
}

/// Poll outstanding (un-confirmed) txs until they receipt or `deadline`.
pub async fn drain(client: Arc<HttpClient>, tracker: Arc<Tracker>, deadline: Instant) {
    loop {
        if sweep_pending_once(&client, &tracker, Duration::ZERO).await == 0 {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Background sweeper for feed-confirmed runs: every `interval`, re-fetch
/// entries whose feed confirmation hasn't arrived within `min_age`. The
/// ~0.1% of confirmations the WebSocket feed misses would otherwise sit
/// pending until the END-OF-RUN drain — by which time a long soak has pushed
/// them past the ingress's bounded receipt cache and they read as `missing`
/// (observed: 584-1,620 phantom must-deliver violations on runs whose
/// honest drop counters were all ZERO). Sweeping while the run is live keeps
/// every entry inside the cache window at ~no request cost (the pending set
/// is tiny in steady state).
pub fn spawn_pending_sweeper(
    client: Arc<HttpClient>,
    tracker: Arc<Tracker>,
    interval: Duration,
    min_age: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let _ = sweep_pending_once(&client, &tracker, min_age).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::accounting::{EvalInput, evaluate};
    use crate::load::plan::PlannedTx;
    use crate::load::scrape::MetricsSnapshot;
    use alloy_primitives::Bytes;
    use jsonrpsee::RpcModule;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::server::{Server, ServerHandle};
    use jsonrpsee::types::ErrorObjectOwned;

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

    /// Mock ingress: counts `eth_sendRawTransaction` calls, either accepting
    /// (returns a hash) or erroring every submit, and serves a fixed value for
    /// `eth_getTransactionReceipt` (`Null` = receipt never found). The
    /// returned `ServerHandle` must stay alive for the server's lifetime.
    async fn mock_ingress(
        accept_submits: bool,
        receipt: serde_json::Value,
    ) -> (Arc<HttpClient>, Arc<AtomicU64>, ServerHandle) {
        let send_calls = Arc::new(AtomicU64::new(0));
        let mut module = RpcModule::new(Arc::clone(&send_calls));
        module
            .register_method("eth_sendRawTransaction", move |_, calls, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                if accept_submits {
                    Ok(B256::repeat_byte(0x11))
                } else {
                    Err(ErrorObjectOwned::owned(-32000, "boom", None::<()>))
                }
            })
            .unwrap();
        module
            .register_method("eth_getTransactionReceipt", move |_, _, _| receipt.clone())
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let handle = server.start(module);
        let client = Arc::new(
            HttpClientBuilder::default()
                .build(format!("http://{addr}"))
                .unwrap(),
        );
        (client, send_calls, handle)
    }

    async fn permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap()
    }

    /// F15.1 regression: an ingress that acks a submit whose receipt never
    /// materializes — the exact bug class the must-deliver gate exists to
    /// catch — must flow into `Pending { accepted: true }`, survive the
    /// drain, surface as `missing`, and fire the `assert_all_delivered` gate.
    #[tokio::test]
    async fn accepted_but_unreceipted_tx_counts_missing_and_fires_must_deliver() {
        let (client, _, _handle) = mock_ingress(true, serde_json::Value::Null).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            Arc::clone(&client),
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            0,
            true, // verify_receipts (non-chaos soak)
            SubmitMode::Blocking,
            false,
        )
        .await;

        let c = tracker.counts();
        assert_eq!((c.offered, c.accepted, c.receipted), (1, 1, 0));

        // The drain re-polls it (one pass — deadline already due) and still
        // finds no receipt, so it stays pending as accepted.
        drain(Arc::clone(&client), Arc::clone(&tracker), Instant::now()).await;
        let (missing, unlanded) = tracker.remaining_pending();
        assert_eq!(
            (missing, unlanded),
            (1, 0),
            "accepted-but-unreceipted must surface as missing"
        );

        // ...and the verdict gate actually fires on it.
        let base = MetricsSnapshot::default();
        let fin = MetricsSnapshot::default();
        let v = evaluate(&EvalInput {
            counts: tracker.counts(),
            missing,
            unlanded,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: false,
        });
        assert!(!v.pass);
        assert!(v.failures.iter().any(|f| f.contains("must-deliver")));
    }

    /// In chaos mode a failed post-accept re-fetch must NOT count as missing
    /// (the ingress receipt cache is volatile across the restarts chaos
    /// injects) — the on-offer ack is trusted and the tx counts delivered.
    #[tokio::test]
    async fn chaos_mode_trusts_on_offer_ack_when_receipt_refetch_fails() {
        let (client, _, _handle) = mock_ingress(true, serde_json::Value::Null).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            client,
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            0,
            false,
            SubmitMode::Blocking,
            false,
        )
        .await;
        let c = tracker.counts();
        assert_eq!((c.accepted, c.receipted, c.bad_status), (1, 1, 0));
        assert_eq!(tracker.remaining_pending(), (0, 0));
    }

    /// F15.2 regression: a submit whose RPC errored but whose tx actually
    /// landed (receipt exists) must not be resubmitted — a duplicate would be
    /// counted by the sequencer as a past-nonce drop and fail the run.
    #[tokio::test]
    async fn landed_tx_is_not_resubmitted_after_submit_error() {
        let (client, send_calls, _handle) =
            mock_ingress(false, serde_json::json!({"status": "0x1"})).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            client,
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            3,
            true,
            SubmitMode::Blocking,
            false,
        )
        .await;
        assert_eq!(
            send_calls.load(Ordering::Relaxed),
            1,
            "no duplicate resubmit once the receipt is found"
        );
        let c = tracker.counts();
        assert_eq!(
            (c.offered, c.accepted, c.receipted, c.bad_status),
            (1, 0, 1, 0)
        );
        assert_eq!(tracker.remaining_pending(), (0, 0));
    }

    /// F15.4 regression: the verdict must not be read while submit tasks are
    /// still in flight — `join_submit_tasks` waits for them (bounded).
    #[tokio::test]
    async fn join_submit_tasks_waits_for_in_flight_tasks() {
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async { tokio::time::sleep(Duration::from_millis(50)).await });
        join_submit_tasks(&mut tasks, Instant::now() + Duration::from_secs(5)).await;
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn join_submit_tasks_gives_up_at_deadline() {
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async { tokio::time::sleep(Duration::from_secs(60)).await });
        join_submit_tasks(&mut tasks, Instant::now() + Duration::from_millis(50)).await;
        assert_eq!(tasks.len(), 1, "wedged task left behind after the deadline");
    }
}
