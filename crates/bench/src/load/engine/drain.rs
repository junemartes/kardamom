//! The post-phase drain and the live pending-sweeper: both settle
//! transactions this run's tracker still shows as un-confirmed, by
//! re-fetching each one's receipt.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use jsonrpsee::http_client::HttpClient;

use super::{Tracker, receipt_status};

/// Join one queued task, or time out at `deadline`. Returns
/// [`ControlFlow::Break`] once the deadline hits with tasks still
/// outstanding.
async fn join_one_or_timeout(
    tasks: &mut tokio::task::JoinSet<()>,
    deadline: Instant,
) -> ControlFlow<()> {
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
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
}

/// Await every spawned submit task. Each task is bounded internally by
/// the client's request timeout. This function gives up at `deadline`,
/// so a wedged task cannot stall the verdict forever. Run this before
/// [`drain`] and the final counts read: an in-flight task is counted
/// `offered` but is not yet `accepted`, `missing`, or `unlanded`, so
/// evaluating early misclassifies the tail.
pub(crate) async fn join_submit_tasks(tasks: &mut tokio::task::JoinSet<()>, deadline: Instant) {
    while !tasks.is_empty() {
        let ControlFlow::Continue(()) = join_one_or_timeout(tasks, deadline).await else {
            return;
        };
    }
}

/// Sweep once over outstanding, un-confirmed transactions at least
/// `min_age` old. Re-fetch each receipt, and settle the entry if found.
/// Returns how many entries stayed pending after the sweep. This
/// function confirms an entry only if it removed the entry itself: the
/// live feed keeps confirming entries at the same time, and a
/// transaction it settled between this poll and this removal must not
/// count twice.
async fn sweep_pending_once(
    client: &Arc<HttpClient>,
    tracker: &Arc<Tracker>,
    min_age: Duration,
) -> usize {
    let pending = tracker.pending_older_than(min_age);
    for (hash, submit_ts) in pending {
        settle_if_still_pending(client, tracker, hash, submit_ts).await;
    }
    tracker.pending_len()
}

/// Re-fetch `hash`'s receipt, and settle it if this call is the one
/// that removes it from `tracker`'s pending set. The live feed keeps
/// confirming entries at the same time, so only the caller that
/// actually removes the entry may count it.
async fn settle_if_still_pending(
    client: &Arc<HttpClient>,
    tracker: &Arc<Tracker>,
    hash: B256,
    submit_ts: Instant,
) {
    if let Some(r) = receipt_status(client, hash).await
        && tracker.remove_pending(&hash)
    {
        tracker.confirm_with_gas(r.status, submit_ts.elapsed(), r.gas);
    }
}

/// One [`drain`] poll: sweep pending transactions once, and report
/// whether the caller should keep polling.
async fn drain_tick(
    client: &Arc<HttpClient>,
    tracker: &Arc<Tracker>,
    deadline: Instant,
) -> ControlFlow<()> {
    if sweep_pending_once(client, tracker, Duration::ZERO).await == 0 {
        return ControlFlow::Break(());
    }
    if Instant::now() >= deadline {
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
}

/// Poll outstanding, un-confirmed transactions until they receipt or
/// `deadline` passes.
pub(crate) async fn drain(client: Arc<HttpClient>, tracker: Arc<Tracker>, deadline: Instant) {
    loop {
        let ControlFlow::Continue(()) = drain_tick(&client, &tracker, deadline).await else {
            return;
        };
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
pub(crate) fn spawn_pending_sweeper(
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
