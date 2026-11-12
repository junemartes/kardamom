//! The post-phase drain and the live pending-sweeper: both settle
//! transactions this run's tracker still shows as un-confirmed, by
//! re-fetching each one's receipt.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use anyhow::Context;
use futures::StreamExt;
use jsonrpsee::http_client::HttpClient;

use super::{Tracker, receipt_status};
use crate::load::tracker::ReceiptStatus;

/// The receipt reads of one sweep that run at the same time. A sweep
/// after a halt can hold hundreds of thousands of entries. The drain
/// reads the deadline only between sweeps, so one read at a time
/// overruns the deadline by the whole sweep.
const SWEEP_CONCURRENCY: usize = 64;

/// Join one queued task before the deadline. An unfinished or failed
/// task leaves its transaction outcome unknown, so the measurement fails.
async fn join_one_or_timeout(
    tasks: &mut tokio::task::JoinSet<()>,
    deadline: Instant,
) -> anyhow::Result<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let result = tokio::time::timeout(remaining, tasks.join_next())
        .await
        .with_context(|| format!("drain deadline with {} unfinished submit task(s); transaction outcomes are unknown", tasks.len()))?;
    if let Some(joined) = result {
        joined.context("load submit task fails; its transaction outcome is unknown")?;
    }
    Ok(())
}

/// Await every spawned submit task. Each task is bounded internally by
/// the client's request timeout. This function gives up at `deadline`,
/// so a wedged task cannot stall the verdict forever. An unfinished or
/// failed task fails the measurement. Run this before the final counts
/// read: an in-flight task is counted
/// `offered` but is not yet `accepted`, `missing`, or `unlanded`, so
/// evaluating early misclassifies the tail.
pub(crate) async fn join_submit_tasks(
    tasks: &mut tokio::task::JoinSet<()>,
    deadline: Instant,
) -> anyhow::Result<()> {
    while !tasks.is_empty() {
        join_one_or_timeout(tasks, deadline).await?;
    }
    Ok(())
}

/// The client and tracker every drain or sweep operation shares.
/// [`join_submit_tasks`] stays a standalone function, not a method
/// here: it touches neither `client` nor `tracker`, only a
/// caller-owned `JoinSet` with its own lifetime, so a `Drainer` would
/// carry it as dead weight.
pub(crate) struct Drainer {
    /// The submit client first, then one client per fallback ingress.
    clients: Vec<Arc<HttpClient>>,
    tracker: Arc<Tracker>,
}

impl Drainer {
    pub(crate) fn new(clients: Vec<Arc<HttpClient>>, tracker: Arc<Tracker>) -> Self {
        Self { clients, tracker }
    }

    /// The receipt of `hash` from the first client that has it. The
    /// submit client answers first; a fallback ingress answers for a
    /// receipt that only lived in a replica that restarted.
    async fn receipt_from_any(&self, hash: B256) -> Option<ReceiptStatus> {
        for client in &self.clients {
            if let Some(r) = receipt_status(client, hash).await {
                return Some(r);
            }
        }
        None
    }

    /// Sweep once over outstanding, un-confirmed transactions at least
    /// `min_age` old. Re-fetch each receipt, and settle the entry if
    /// found. Up to [`SWEEP_CONCURRENCY`] reads run at a time. Returns
    /// how many entries stayed pending after the sweep. This confirms
    /// an entry only if it removed the entry itself: the live feed
    /// keeps confirming entries at the same time, and a
    /// transaction it settled between this poll and this removal must
    /// not count twice.
    async fn sweep_pending_once(&self, min_age: Duration) -> usize {
        futures::stream::iter(self.tracker.pending_older_than(min_age))
            .for_each_concurrent(SWEEP_CONCURRENCY, |(hash, submit_ts)| {
                self.settle_if_still_pending(hash, submit_ts)
            })
            .await;
        self.tracker.pending_len()
    }

    /// Re-fetch `hash`'s receipt, and settle it if this call is the
    /// one that removes it from the tracker's pending set. The live
    /// feed keeps confirming entries at the same time, so only the
    /// caller that actually removes the entry may count it.
    async fn settle_if_still_pending(&self, hash: B256, submit_ts: Instant) {
        if let Some(r) = self.receipt_from_any(hash).await
            && self.tracker.remove_pending(&hash)
        {
            self.tracker.confirm(hash, &r, submit_ts.elapsed());
        }
    }

    /// One [`Self::drain`] poll: sweep pending transactions once, and
    /// report whether the caller should keep polling. Sleeps the poll
    /// interval before returning [`ControlFlow::Continue`], so the
    /// caller's loop is a single call per iteration.
    async fn drain_tick_or_wait(&self, deadline: Instant) -> ControlFlow<()> {
        if self.sweep_pending_once(Duration::ZERO).await == 0 {
            return ControlFlow::Break(());
        }
        if Instant::now() >= deadline {
            return ControlFlow::Break(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        ControlFlow::Continue(())
    }

    /// Poll outstanding, un-confirmed transactions until they receipt
    /// or `deadline` passes. Pending reads consume the same budget.
    pub(crate) async fn drain(&self, deadline: Instant) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _ = tokio::time::timeout(remaining, self.drain_pending(deadline)).await;
    }

    async fn drain_pending(&self, deadline: Instant) {
        loop {
            let ControlFlow::Continue(()) = self.drain_tick_or_wait(deadline).await else {
                return;
            };
        }
    }

    /// A background sweeper for feed-confirmed runs. Every `interval`,
    /// it re-fetches an entry whose feed confirmation has not arrived
    /// within `min_age`. Without this, the small fraction of
    /// confirmations the WebSocket feed misses would sit pending until
    /// the end-of-run drain. By then, a long soak has pushed them past
    /// the ingress's bounded receipt cache, and they read as
    /// `missing`, a phantom must-deliver violation, even though the
    /// run's real drop counters are all zero. Sweeping while the run
    /// is live keeps every entry inside the cache window, at close to
    /// no request cost, since the pending set stays small in steady
    /// state.
    pub(crate) fn spawn_pending_sweeper(
        self: Arc<Self>,
        interval: Duration,
        min_age: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                self.sweep_after_interval(interval, min_age).await;
            }
        })
    }

    /// Sleep `interval`, then sweep once. One tick of
    /// [`Self::spawn_pending_sweeper`]'s loop.
    async fn sweep_after_interval(&self, interval: Duration, min_age: Duration) {
        tokio::time::sleep(interval).await;
        let _ = self.sweep_pending_once(min_age).await;
    }
}
