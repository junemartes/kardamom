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
//! transaction whose receipt did not land inline. An accepted submit
//! whose receipt cannot be re-fetched stays pending, and counts as
//! `missing` if it never confirms. This is the independent must-deliver
//! check.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::load::plan::PlannedTx;
use crate::load::tracker::ReceiptStatus;

mod refusal;

use refusal::{PAUSE_RETRY_DELAY, Refusal, RetryState};

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

mod queues;
pub(crate) use queues::Queues;

/// Look up a receipt. Returns `None` if not mined yet.
async fn receipt_status(client: &HttpClient, hash: B256) -> Option<ReceiptStatus> {
    let v: Option<serde_json::Value> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .ok()?;
    ReceiptStatus::from_json(&v?)
}

/// A submit's fixed knobs: retry policy, receipt-confirmation strategy,
/// and submit mode. These stay the same for every task in one pacer run.
#[derive(Clone, Copy)]
pub(crate) struct SubmitOpts {
    pub(crate) retry: u32,
    /// How long a submit waits out the refusals of a paused ingress.
    pub(crate) pause_wait: Duration,
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
    pause_wait: Duration,
    t0: Instant,
}

impl Attempt<'_> {
    /// One retry attempt: submit once. A refusal of a paused ingress
    /// waits and goes again while `pause_wait` lasts, and does not count
    /// as an attempt. Any other error counts. Unless it was the last
    /// attempt, check for a receipt that already landed before the next
    /// one.
    async fn try_once(&self, state: &mut RetryState) -> ControlFlow<RetryOutcome> {
        let res: Result<B256, _> = self
            .client
            .request(self.method, rpc_params![self.tx.raw.clone()])
            .await;
        let Err(err) = res else {
            return ControlFlow::Break(RetryOutcome::Accepted);
        };
        let refusal = Refusal::of(&err);
        refusal.log(self.tx, self.tracker.note_refused());
        if refusal.is_paused() && state.waits_out_pause(self.pause_wait, Instant::now()) {
            tokio::time::sleep(PAUSE_RETRY_DELAY).await;
            return ControlFlow::Continue(());
        }
        if state.attempt >= self.retry {
            return ControlFlow::Break(RetryOutcome::GaveUp);
        }
        // The errored attempt can still have landed, for example if the
        // connection died after ingress forwarded the transaction.
        // Resubmitting a landed transaction registers as a past-nonce
        // drop at the sequencer. So check for a receipt first, and stop
        // retrying if it is already there.
        if let Some(r) = receipt_status(self.client, self.tx.hash).await {
            self.tracker.confirm(self.tx.hash, &r, self.t0.elapsed());
            return ControlFlow::Break(RetryOutcome::LandedEarly);
        }
        state.attempt = state.attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(200 * u64::from(state.attempt))).await;
        ControlFlow::Continue(())
    }
}

/// Submit `attempt.tx`, retrying on error up to `attempt.retry` times.
async fn run_retries(attempt: &Attempt<'_>) -> RetryOutcome {
    let mut state = RetryState::default();
    loop {
        if let ControlFlow::Break(outcome) = attempt.try_once(&mut state).await {
            return outcome;
        }
    }
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
        Some(r) => tracker.confirm(tx.hash, &r, t0.elapsed()),
        // A submit acknowledgement does not prove successful execution.
        // Missing receipts stay pending across faults and must be recovered.
        None => tracker.insert_pending(tx.hash, t0, true),
    }
}

/// Finalize a submit that never accepted: it can still have landed, if
/// a prior attempt reached the chain despite the RPC erroring. Let the
/// drain re-check it. A never-confirmed leftover counts as `unlanded`,
/// not `missing`, because it was never accepted.
async fn finalize_unaccepted(client: &HttpClient, tracker: &Tracker, tx: &PlannedTx, t0: Instant) {
    match receipt_status(client, tx.hash).await {
        Some(r) => tracker.confirm(tx.hash, &r, t0.elapsed()),
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
        pause_wait: opts.pause_wait,
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
    async fn tick(&mut self) -> anyhow::Result<ControlFlow<()>> {
        if self.start.elapsed() >= self.dur {
            return Ok(ControlFlow::Break(()));
        }
        self.credit =
            (self.credit + f64::from(self.rate) * self.tick_len.as_secs_f64()).min(self.cap);
        if self.spend_credit().await? {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Spend the accumulated credit: submit as many queued transactions
    /// as `self.credit` allows, spawning one bounded task per submit.
    /// Returns `true` if the queues drained or the semaphore closed,
    /// meaning the caller should stop pacing.
    async fn spend_credit(&mut self) -> anyhow::Result<bool> {
        while self.credit >= 1.0 {
            let ControlFlow::Continue(()) = self.spend_one_credit().await? else {
                return Ok(true);
            };
        }
        Ok(false)
    }

    /// Spend one credit: pop the next queued transaction and spawn its
    /// submit task under the in-flight semaphore. Returns
    /// [`ControlFlow::Break`] once the queues are empty, or the
    /// semaphore has closed (the run is shutting down) — either means
    /// the caller should stop pacing.
    async fn spend_one_credit(&mut self) -> anyhow::Result<ControlFlow<()>> {
        let Some(tx) = self.queues.next(&self.handles.tracker).await? else {
            return Ok(ControlFlow::Break(()));
        };
        self.credit -= 1.0;
        // `acquire_owned` waits at the max in-flight limit. This gives
        // natural back pressure: the offered rate drops to match the
        // pipeline's drain rate.
        let Ok(permit) = Arc::clone(&self.handles.sem).acquire_owned().await else {
            return Ok(ControlFlow::Break(()));
        };
        self.tasks.spawn(submit_task(
            Arc::clone(&self.handles.client),
            Arc::clone(&self.handles.tracker),
            tx,
            permit,
            self.opts,
        ));
        Ok(ControlFlow::Continue(()))
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
) -> anyhow::Result<()> {
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
        let ControlFlow::Continue(()) = pacer.tick().await? else {
            return Ok(());
        };
    }
}

mod drain;
pub(crate) use drain::{Drainer, join_submit_tasks};

#[cfg(test)]
mod tests;
