//! This is an open-loop, rate-paced send engine.
//!
//! The ingress `eth_sendRawTransaction` call parks the caller until the
//! receipt arrives (an on-offer ack), so submit round-trip time is close
//! to end-to-end latency. To drive load open-loop, meaning the pacer
//! sets the rate, not completions, this module spawns each submit as
//! its own task. Each task is bounded by an in-flight semaphore and
//! collected in a [`tokio::task::JoinSet`] that the caller joins before
//! evaluating, instead of awaiting one submit before issuing the next.
//!
//! Every transaction is tracked, by its locally computed hash, to a
//! receipt; see [`Tracker`] in `load::tracker`. A submit that errors is
//! retried, after checking that the transaction did not already land,
//! so a duplicate is not resubmitted. A post-phase drain confirms any
//! transaction whose receipt did not land inline. With
//! `verify_receipts` set, for a non-chaos soak, an accepted submit
//! whose receipt cannot be re-fetched stays pending, and counts as
//! `missing` if it never confirms. This is the independent must-deliver
//! check.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::load::json_hex_u64;
use crate::load::plan::PlannedTx;

pub use crate::load::tracker::{Counts, Tracker};

/// How a submit acks, and how its receipt is observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitMode {
    /// `eth_sendRawTransaction`: the RPC call parks until the receipt
    /// arrives, so a successful submit is itself the delivery
    /// confirmation. Each in-flight transaction holds one connection.
    Blocking,
    /// `kardamom_sendRawTransactionAsync`: the RPC call acks at publish
    /// time. Receipts arrive out-of-band on the
    /// `kardamom_subscribeReceipts` feed, see `receipt_feed_task`, with
    /// the drain's `eth_getTransactionReceipt` polling as the catch-all.
    Subscribe,
}

/// Per-sender FIFO queues, consumed in rotation. This keeps each
/// sender's nonce order: a sender's nonce k is popped before k+1.
pub struct Queues {
    per_sender: Vec<VecDeque<PlannedTx>>,
    rr: usize,
}

impl Queues {
    /// Build from a set of per-sender pre-generated queues.
    #[must_use]
    pub fn new(queues: Vec<Vec<PlannedTx>>) -> Self {
        Self {
            per_sender: queues.into_iter().map(VecDeque::from).collect(),
            rr: 0,
        }
    }

    /// Pop the next transaction in rotation across senders. Returns
    /// `None` when all queues are drained.
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

    /// The total transactions still queued.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.per_sender.iter().map(VecDeque::len).sum()
    }
}

/// Look up a receipt's `(status, gasUsed)`. Returns `None` if not mined yet.
async fn receipt_status(client: &HttpClient, hash: B256) -> Option<(u64, u64)> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .ok()?;
    let v = v?;
    let status = json_hex_u64(&v["status"])?;
    let gas = json_hex_u64(&v["gasUsed"]).unwrap_or(0);
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
    tracker.note_offered();
    let t0 = Instant::now();
    let mut accepted = false;
    for attempt in 0..=retry {
        let res: Result<B256, _> = client.request(method, rpc_params![tx.raw.clone()]).await;
        if res.is_ok() {
            accepted = true;
            break;
        }
        if attempt < retry {
            // The errored attempt can still have landed, for example if the
            // connection died after ingress forwarded the transaction.
            // Resubmitting a landed transaction registers as a past-nonce
            // drop at the sequencer. So check for a receipt first, and stop
            // retrying if it is already there.
            if let Some((status, gas)) = receipt_status(&client, tx.hash).await {
                tracker.confirm_with_gas(status, t0.elapsed(), gas);
                return;
            }
            tokio::time::sleep(Duration::from_millis(200 * (u64::from(attempt) + 1))).await;
        }
    }
    if accepted {
        tracker.note_accepted();
        if mode == SubmitMode::Subscribe {
            // The ack only means published. The receipt arrives on the
            // subscription feed, or the drain's polling settles it. This
            // path does no chaos-mode ack-trust: delivery is verified for real.
            tracker.await_feed(tx.hash, t0);
            return;
        }
        // Feed-confirm skips the per-transaction re-fetch. The WebSocket
        // feed, or a frame it already delivered (`await_feed` checks the
        // early map), confirms with the real status, and the drain re-polls
        // any straggler. So every accepted transaction still ends confirmed
        // or counted `missing`, with one HTTP call per transaction instead
        // of two.
        if feed_confirm {
            tracker.await_feed(tx.hash, t0);
            return;
        }
        // Under on-offer acking, a successful submit means the receipt
        // already arrived: the transaction was executed and receipted.
        // Re-fetch it to check status and latency.
        match receipt_status(&client, tx.hash).await {
            Some((status, gas)) => tracker.confirm_with_gas(status, t0.elapsed(), gas),
            None if verify_receipts => {
                // In a non-chaos soak, independently verify the on-offer
                // contract. No receipt for an accepted transaction means
                // keep it pending. The drain re-polls it, and a
                // never-confirmed leftover counts as `missing`, a
                // must-deliver violation.
                tracker.insert_pending(tx.hash, t0, true);
            }
            None => {
                // In chaos mode, the ingress's in-memory receipt cache is
                // volatile across a restart, so a failed re-fetch would
                // wrongly flag an already-delivered transaction as
                // "missing". Trust the on-offer ack, and count it
                // delivered with success status.
                tracker.confirm(1, t0.elapsed());
            }
        }
    } else {
        // The submit never returned a hash. It can still have landed, if a
        // prior attempt reached the chain despite the RPC erroring. Let the
        // drain re-check it. A never-confirmed leftover counts as
        // `unlanded`, not `missing`, because it was never accepted.
        match receipt_status(&client, tx.hash).await {
            Some((status, gas)) => tracker.confirm_with_gas(status, t0.elapsed(), gas),
            None => {
                tracker.insert_pending(tx.hash, t0, false);
            }
        }
    }
}

/// Drive submissions at `rate` tx/s for `dur`. Each submit spawns as a
/// task bounded by `sem`, and is collected in `tasks`. Returns early if
/// the queues drain first. Submits keep confirming asynchronously.
/// Call [`join_submit_tasks`], then [`drain`] afterwards, to settle
/// the tail.
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
    // The credit cap is 40ms of traffic. The old half-second cap released a
    // rate/2 burst at the start of each step (5,000 transactions at a 10k
    // step). That was enough to blow straight through every sender's
    // sequencer reorder window (max_pending_per_sender) before pacing even
    // began, tripping evictions that looked like a pipeline edge but were a
    // pure harness artifact. 4 ticks of catch-up still absorb scheduler
    // hiccups, without the flood.
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
            // `acquire_owned` waits at the max in-flight limit. This gives
            // natural back pressure: the offered rate drops to match the
            // pipeline's drain rate.
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

/// Await every spawned submit task. Each task is bounded internally by
/// the client's request timeout. This function gives up at `deadline`,
/// so a wedged task cannot stall the verdict forever. Run this before
/// [`drain`] and the final counts read: an in-flight task is counted
/// `offered` but is not yet `accepted`, `missing`, or `unlanded`, so
/// evaluating early misclassifies the tail.
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

/// Sweep once over outstanding, un-confirmed transactions at least
/// `min_age` old. Re-fetch each receipt, and settle the entry if found.
/// Returns how many entries stayed pending after the sweep. This
/// function confirms an entry only if it removed the entry itself: the
/// live feed keeps confirming entries at the same time, and a
/// transaction it settled between this poll and this removal must not
/// count twice.
pub async fn sweep_pending_once(
    client: &Arc<HttpClient>,
    tracker: &Arc<Tracker>,
    min_age: Duration,
) -> usize {
    let pending = tracker.pending_older_than(min_age);
    for (hash, submit_ts) in pending {
        if let Some((status, gas)) = receipt_status(client, hash).await
            && tracker.remove_pending(&hash)
        {
            tracker.confirm_with_gas(status, submit_ts.elapsed(), gas);
        }
    }
    tracker.pending_len()
}

/// Poll outstanding, un-confirmed transactions until they receipt or
/// `deadline` passes.
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

/// A background sweeper for feed-confirmed runs. Every `interval`, it
/// re-fetches an entry whose feed confirmation has not arrived within
/// `min_age`. Without this, the small fraction of confirmations the
/// WebSocket feed misses would sit pending until the end-of-run drain.
/// By then, a long soak has pushed them past the ingress's bounded
/// receipt cache, and they read as `missing`, a phantom must-deliver
/// violation, even though the run's real drop counters are all zero.
/// Sweeping while the run is live keeps every entry inside the cache
/// window, at close to no request cost, since the pending set stays
/// small in steady state.
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
    use std::sync::atomic::{AtomicU64, Ordering};

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
        // In rotation, the order is s0n0, s1n0, s0n1, s1n1.
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

    /// A mock ingress. It counts `eth_sendRawTransaction` calls, and
    /// either accepts every submit with a hash or errors on every
    /// submit. It serves a fixed value for `eth_getTransactionReceipt`;
    /// `Null` means the receipt is never found. The returned
    /// `ServerHandle` must stay alive for the server's lifetime.
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

    /// Regression test: an ingress that acks a submit whose receipt never
    /// materializes is the exact bug class the must-deliver gate exists to
    /// catch. Such a transaction must flow into `Pending { accepted: true }`,
    /// survive the drain, surface as `missing`, and fire the
    /// `assert_all_delivered` gate.
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

        // The drain re-polls it once, since the deadline is already due, and
        // still finds no receipt, so it stays pending as accepted.
        drain(Arc::clone(&client), Arc::clone(&tracker), Instant::now()).await;
        let (missing, unlanded) = tracker.remaining_pending();
        assert_eq!(
            (missing, unlanded),
            (1, 0),
            "accepted-but-unreceipted must surface as missing"
        );

        // Check that the verdict gate actually fires on it.
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

    /// In chaos mode, a failed post-accept re-fetch must not count as
    /// missing, because the ingress receipt cache is volatile across the
    /// restarts chaos injects. The on-offer ack is trusted, and the
    /// transaction counts as delivered.
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

    /// Regression test: a submit whose RPC errored, but whose transaction
    /// actually landed, with a receipt that exists, must not be
    /// resubmitted. A duplicate would be counted by the sequencer as a
    /// past-nonce drop and fail the run.
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

    /// Regression test: the verdict must not be read while submit tasks
    /// are still in flight. `join_submit_tasks` waits for them, up to a bound.
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
