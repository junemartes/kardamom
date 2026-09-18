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
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::load::json_hex_u64;
use crate::load::plan::PlannedTx;

pub(crate) use crate::load::tracker::{Counts, Tracker};

/// How a submit acks, and how its receipt is observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitMode {
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
pub(crate) struct Queues {
    per_sender: Vec<VecDeque<PlannedTx>>,
    rr: usize,
}

impl Queues {
    /// Build from a set of per-sender pre-generated queues.
    #[must_use]
    pub(crate) fn new(queues: Vec<Vec<PlannedTx>>) -> Self {
        Self {
            per_sender: queues.into_iter().map(VecDeque::from).collect(),
            rr: 0,
        }
    }

    /// Pop the next transaction in rotation across senders. Returns
    /// `None` when all queues are drained.
    pub(crate) fn pop_next(&mut self) -> Option<PlannedTx> {
        let n = self.per_sender.len();
        (0..n).find_map(|_| {
            let i = self.rr % n;
            // `rr` is a rotating counter, immediately taken mod `n`: a
            // wrap back to 0 is the intended behavior, not an edge case,
            // so wrapping (not a panic on overflow, unreachable in
            // practice, but not free to rule out on a hot path) is the
            // correct arithmetic here.
            self.rr = self.rr.wrapping_add(1) % n;
            self.per_sender[i].pop_front()
        })
    }

    /// The total transactions still queued. Test-only: production code
    /// drains with `pop_next` until it returns `None`, and never needs
    /// the count.
    #[cfg(test)]
    fn remaining(&self) -> usize {
        self.per_sender.iter().map(VecDeque::len).sum()
    }
}

/// A mined receipt's status code and gas used.
struct ReceiptStatus {
    status: u64,
    gas: u64,
}

/// Look up a receipt's status and gas used. Returns `None` if not mined yet.
async fn receipt_status(client: &HttpClient, hash: B256) -> Option<ReceiptStatus> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .ok()?;
    let v = v?;
    let status = json_hex_u64(&v["status"])?;
    let gas = json_hex_u64(&v["gasUsed"]).unwrap_or(0);
    Some(ReceiptStatus { status, gas })
}

/// A submit's fixed knobs: retry policy, receipt-confirmation strategy,
/// and submit mode. These stay the same for every task in one pacer run.
#[derive(Clone, Copy)]
pub(crate) struct SubmitOpts {
    pub(crate) retry: u32,
    pub(crate) verify_receipts: bool,
    pub(crate) mode: SubmitMode,
    pub(crate) feed_confirm: bool,
}

/// One [`run_retries`] loop's final state.
enum RetryOutcome {
    /// A submit call returned a hash: the caller finalizes it as
    /// accepted.
    Accepted,
    /// A submit call errored, but a receipt already existed: this
    /// function already confirmed it, so the caller does nothing more.
    LandedEarly,
    /// Every attempt errored, with no receipt found along the way.
    GaveUp,
}

/// One submit's fixed retry context: the pieces that stay the same
/// across every attempt in [`run_retries`]'s loop.
struct Attempt<'a> {
    client: &'a HttpClient,
    tracker: &'a Tracker,
    tx: &'a PlannedTx,
    method: &'static str,
    retry: u32,
    t0: Instant,
}

impl Attempt<'_> {
    /// One retry attempt: submit once, and, unless this was the last
    /// attempt, check for a receipt that already landed before the
    /// caller retries.
    async fn try_once(&self, attempt: u32) -> ControlFlow<RetryOutcome> {
        let res: Result<B256, _> = self
            .client
            .request(self.method, rpc_params![self.tx.raw.clone()])
            .await;
        if res.is_ok() {
            return ControlFlow::Break(RetryOutcome::Accepted);
        }
        if attempt >= self.retry {
            return ControlFlow::Continue(());
        }
        // The errored attempt can still have landed, for example if the
        // connection died after ingress forwarded the transaction.
        // Resubmitting a landed transaction registers as a past-nonce
        // drop at the sequencer. So check for a receipt first, and stop
        // retrying if it is already there.
        if let Some(r) = receipt_status(self.client, self.tx.hash).await {
            self.tracker
                .confirm_with_gas(r.status, self.t0.elapsed(), r.gas);
            return ControlFlow::Break(RetryOutcome::LandedEarly);
        }
        tokio::time::sleep(Duration::from_millis(200 * (u64::from(attempt) + 1))).await;
        ControlFlow::Continue(())
    }
}

/// Submit `attempt.tx`, retrying on error up to `attempt.retry` times.
async fn run_retries(attempt: &Attempt<'_>) -> RetryOutcome {
    for n in 0..=attempt.retry {
        let ControlFlow::Break(outcome) = attempt.try_once(n).await else {
            continue;
        };
        return outcome;
    }
    RetryOutcome::GaveUp
}

/// Finalize an accepted submit: record it, then confirm or track it
/// pending, depending on `opts.mode` and `opts.feed_confirm`.
async fn finalize_accepted(
    client: &HttpClient,
    tracker: &Tracker,
    tx: &PlannedTx,
    t0: Instant,
    opts: SubmitOpts,
) {
    tracker.note_accepted();
    if opts.mode == SubmitMode::Subscribe {
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
    if opts.feed_confirm {
        tracker.await_feed(tx.hash, t0);
        return;
    }
    // Under on-offer acking, a successful submit means the receipt
    // already arrived: the transaction was executed and receipted.
    // Re-fetch it to check status and latency.
    match receipt_status(client, tx.hash).await {
        Some(r) => tracker.confirm_with_gas(r.status, t0.elapsed(), r.gas),
        None if opts.verify_receipts => {
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
}

/// Finalize a submit that never accepted: it can still have landed, if
/// a prior attempt reached the chain despite the RPC erroring. Let the
/// drain re-check it. A never-confirmed leftover counts as `unlanded`,
/// not `missing`, because it was never accepted.
async fn finalize_unaccepted(client: &HttpClient, tracker: &Tracker, tx: &PlannedTx, t0: Instant) {
    match receipt_status(client, tx.hash).await {
        Some(r) => tracker.confirm_with_gas(r.status, t0.elapsed(), r.gas),
        None => {
            tracker.insert_pending(tx.hash, t0, false);
        }
    }
}

async fn submit_task(
    client: Arc<HttpClient>,
    tracker: Arc<Tracker>,
    tx: PlannedTx,
    _permit: OwnedSemaphorePermit,
    opts: SubmitOpts,
) {
    let method = match opts.mode {
        SubmitMode::Blocking => "eth_sendRawTransaction",
        SubmitMode::Subscribe => "kardamom_sendRawTransactionAsync",
    };
    tracker.note_offered();
    let t0 = Instant::now();
    let attempt = Attempt {
        client: &client,
        tracker: &tracker,
        tx: &tx,
        method,
        retry: opts.retry,
        t0,
    };
    match run_retries(&attempt).await {
        RetryOutcome::LandedEarly => {}
        RetryOutcome::Accepted => finalize_accepted(&client, &tracker, &tx, t0, opts).await,
        RetryOutcome::GaveUp => finalize_unaccepted(&client, &tracker, &tx, t0).await,
    }
}

/// The shared client, in-flight semaphore, and tracker one pacer or
/// ramp step needs. These stay the same for the whole run.
#[derive(Clone)]
pub(crate) struct RunHandles {
    pub(crate) client: Arc<HttpClient>,
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) tracker: Arc<Tracker>,
}

/// A rate-paced credit spender: [`Pacer::tick`] accrues one tick's
/// spend credit and submits as many queued transactions as it allows.
struct Pacer<'a> {
    handles: &'a RunHandles,
    tasks: &'a mut tokio::task::JoinSet<()>,
    queues: &'a mut Queues,
    rate: u32,
    tick_len: Duration,
    cap: f64,
    credit: f64,
    start: Instant,
    dur: Duration,
    opts: SubmitOpts,
}

impl Pacer<'_> {
    /// Accrue this tick's credit and spend it. Returns
    /// [`ControlFlow::Break`] once the deadline passes, the queues
    /// drain, or the semaphore closes.
    async fn tick(&mut self) -> ControlFlow<()> {
        if self.start.elapsed() >= self.dur {
            return ControlFlow::Break(());
        }
        self.credit =
            (self.credit + f64::from(self.rate) * self.tick_len.as_secs_f64()).min(self.cap);
        if self.spend_credit().await {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// Spend the accumulated credit: submit as many queued transactions
    /// as `self.credit` allows, spawning one bounded task per submit.
    /// Returns `true` if the queues drained or the semaphore closed,
    /// meaning the caller should stop pacing.
    async fn spend_credit(&mut self) -> bool {
        while self.credit >= 1.0 {
            let ControlFlow::Continue(()) = self.spend_one_credit().await else {
                return true;
            };
        }
        false
    }

    /// Spend one credit: pop the next queued transaction and spawn its
    /// submit task under the in-flight semaphore. Returns
    /// [`ControlFlow::Break`] once the queues are empty, or the
    /// semaphore has closed (the run is shutting down) — either means
    /// the caller should stop pacing.
    async fn spend_one_credit(&mut self) -> ControlFlow<()> {
        let Some(tx) = self.queues.pop_next() else {
            return ControlFlow::Break(());
        };
        self.credit -= 1.0;
        // `acquire_owned` waits at the max in-flight limit. This gives
        // natural back pressure: the offered rate drops to match the
        // pipeline's drain rate.
        let Ok(permit) = Arc::clone(&self.handles.sem).acquire_owned().await else {
            return ControlFlow::Break(());
        };
        self.tasks.spawn(submit_task(
            Arc::clone(&self.handles.client),
            Arc::clone(&self.handles.tracker),
            tx,
            permit,
            self.opts,
        ));
        ControlFlow::Continue(())
    }
}

/// Drive submissions at `rate` tx/s for `dur`. Each submit spawns as a
/// task bounded by `handles.sem`, and is collected in `tasks`. Returns
/// early if the queues drain first. Submits keep confirming
/// asynchronously. Call [`join_submit_tasks`], then [`drain`]
/// afterwards, to settle the tail.
pub(crate) async fn pacer(
    handles: &RunHandles,
    tasks: &mut tokio::task::JoinSet<()>,
    queues: &mut Queues,
    rate: std::num::NonZeroU32,
    dur: Duration,
    opts: SubmitOpts,
) {
    let tick_len = Duration::from_millis(10);
    let mut ticker = tokio::time::interval(tick_len);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let rate = rate.get();
    // The credit cap is 40ms of traffic (4 ticks). This bounds the burst
    // at the start of each step, so pacing cannot blow through a
    // sender's sequencer reorder window (max_pending_per_sender) before
    // it begins, while still absorbing scheduler hiccups.
    let cap = (f64::from(rate) * 0.04).max(1.0);
    let mut pacer = Pacer {
        handles,
        tasks,
        queues,
        rate,
        tick_len,
        cap,
        credit: 0.0,
        start: Instant::now(),
        dur,
        opts,
    };
    loop {
        ticker.tick().await;
        let ControlFlow::Continue(()) = pacer.tick().await else {
            return;
        };
    }
}

mod drain;
pub(crate) use drain::{Drainer, join_submit_tasks};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::accounting::tests::base_input;
    use crate::load::accounting::{EvalInput, evaluate};
    use crate::load::plan::PlannedTx;
    use crate::load::scrape::MetricsSnapshot;
    use crate::load::tracker::PendingCounts;
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
        assert_eq!(t.remaining_pending(), PendingCounts::default());
    }

    /// A running [`mock_ingress`] server: the client to submit through,
    /// the count of `eth_sendRawTransaction` calls it has seen, and its
    /// handle, which must stay alive for the server's lifetime.
    struct MockIngress {
        client: Arc<HttpClient>,
        send_calls: Arc<AtomicU64>,
        _handle: ServerHandle,
    }

    /// A mock ingress. It counts `eth_sendRawTransaction` calls, and
    /// either accepts every submit with a hash or errors on every
    /// submit. It serves a fixed value for `eth_getTransactionReceipt`;
    /// `Null` means the receipt is never found.
    async fn mock_ingress(accept_submits: bool, receipt: serde_json::Value) -> MockIngress {
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
        MockIngress {
            client,
            send_calls,
            _handle: handle,
        }
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
        let ingress = mock_ingress(true, serde_json::Value::Null).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            Arc::clone(&ingress.client),
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            SubmitOpts {
                retry: 0,
                verify_receipts: true, // non-chaos soak
                mode: SubmitMode::Blocking,
                feed_confirm: false,
            },
        )
        .await;

        let c = tracker.counts();
        assert_eq!((c.offered, c.accepted, c.receipted), (1, 1, 0));

        // The drain re-polls it once, since the deadline is already due, and
        // still finds no receipt, so it stays pending as accepted.
        Drainer::new(Arc::clone(&ingress.client), Arc::clone(&tracker))
            .drain(Instant::now())
            .await;
        let pending = tracker.remaining_pending();
        assert_eq!(
            pending,
            PendingCounts {
                missing: 1,
                unlanded: 0
            },
            "accepted-but-unreceipted must surface as missing"
        );

        // Check that the verdict gate actually fires on it.
        let base = MetricsSnapshot::default();
        let fin = MetricsSnapshot::default();
        let v = evaluate(&EvalInput {
            counts: tracker.counts(),
            missing: pending.missing,
            unlanded: pending.unlanded,
            ..base_input(&base, &fin)
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
        let ingress = mock_ingress(true, serde_json::Value::Null).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            ingress.client,
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            SubmitOpts {
                retry: 0,
                verify_receipts: false,
                mode: SubmitMode::Blocking,
                feed_confirm: false,
            },
        )
        .await;
        let c = tracker.counts();
        assert_eq!((c.accepted, c.receipted, c.bad_status), (1, 1, 0));
        assert_eq!(tracker.remaining_pending(), PendingCounts::default());
    }

    /// Regression test: a submit whose RPC errored, but whose transaction
    /// actually landed, with a receipt that exists, must not be
    /// resubmitted. A duplicate would be counted by the sequencer as a
    /// past-nonce drop and fail the run.
    #[tokio::test]
    async fn landed_tx_is_not_resubmitted_after_submit_error() {
        let ingress = mock_ingress(false, serde_json::json!({"status": "0x1"})).await;
        let tracker = Arc::new(Tracker::new().unwrap());
        submit_task(
            ingress.client,
            Arc::clone(&tracker),
            tx(0, 0),
            permit().await,
            SubmitOpts {
                retry: 3,
                verify_receipts: true,
                mode: SubmitMode::Blocking,
                feed_confirm: false,
            },
        )
        .await;
        assert_eq!(
            ingress.send_calls.load(Ordering::Relaxed),
            1,
            "no duplicate resubmit once the receipt is found"
        );
        let c = tracker.counts();
        assert_eq!(
            (c.offered, c.accepted, c.receipted, c.bad_status),
            (1, 0, 1, 0)
        );
        assert_eq!(tracker.remaining_pending(), PendingCounts::default());
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
