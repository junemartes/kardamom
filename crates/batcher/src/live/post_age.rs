//! The age of the last post, as L1 tells it.
//!
//! The probe asks L1 for the latest `BatchPosted` log and the timestamp
//! of its block, and exports the seconds since then. The value never
//! comes from this process's memory of its own posts: a batcher that
//! posts into an endpoint whose logs vanish sees its own age grow, and
//! a batcher that stopped posting sees the same. Both are the alert.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::Address;
use alloy_provider::Provider;
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::SolEvent;
use anyhow::{Context, Result};
use metrics::gauge;
use tracing::warn;

use super::live_metric_names;
use crate::settlement::IKardamomL2Settlement;

/// The blocks one log query covers on the first, backward scan. A
/// public endpoint caps a query's range; 5,000 stays under every cap
/// seen, and an active chain's last post is in the first window.
const WINDOW: u64 = 5_000;

/// The probe's state: where the next forward scan starts, and the
/// timestamp of the last post seen.
pub(crate) struct PostAge<P> {
    provider: P,
    settlement: Address,
    /// The settlement's deployment block: the backward scan stops here.
    floor: u64,
    /// The first block the next scan reads.
    next_from: u64,
    /// The timestamp of the block of the last `BatchPosted` log seen.
    last_post: Option<u64>,
    every: Duration,
}

impl<P: Provider> PostAge<P> {
    pub(crate) fn new(
        provider: P,
        settlement: Address,
        deploy_block: u64,
        every: Duration,
    ) -> Self {
        Self {
            provider,
            settlement,
            floor: deploy_block,
            next_from: deploy_block,
            last_post: None,
            every,
        }
    }

    /// Probe forever at the cadence.
    pub(crate) async fn run(mut self) {
        let mut interval = tokio::time::interval(self.every);
        loop {
            interval.tick().await;
            self.tick().await;
        }
    }

    /// One probe: read L1, then export the age of what is known. A read
    /// that fails keeps the last post, so the age still grows.
    async fn tick(&mut self) {
        if let Err(error) = self.refresh().await {
            warn!(error = %format!("{error:#}"), "post age read failed");
        }
        self.publish();
    }

    fn publish(&self) {
        let Some(posted_at) = self.last_post else {
            return;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Metric value; f64 precision loss only above 2^52, never
        // reached by an age in seconds.
        #[allow(
            clippy::cast_precision_loss,
            reason = "metric value; never nears 2^52 for an age in seconds"
        )]
        gauge!(live_metric_names::LAST_POST_AGE).set(now.saturating_sub(posted_at) as f64);
    }

    /// Read the blocks since the last scan, backward from the head on the
    /// first one, and keep the timestamp of the latest post found.
    async fn refresh(&mut self) -> Result<()> {
        let head = self.provider.get_block_number().await?;
        if head < self.next_from {
            return Ok(());
        }
        let found = match self.last_post {
            None => self.scan_back(head).await?,
            Some(_) => self.latest_post_in(self.next_from, head).await?,
        };
        if let Some(block) = found {
            self.last_post = Some(self.timestamp_of(block).await?);
        }
        self.next_from = head.checked_add(1).context("L1 head overflowed u64")?;
        Ok(())
    }

    /// The block of the latest post at or below `head`, scanned one
    /// window at a time down to the floor.
    async fn scan_back(&self, head: u64) -> Result<Option<u64>> {
        let mut to = head;
        loop {
            match self.scan_window(to).await? {
                ControlFlow::Break(found) => return Ok(found),
                ControlFlow::Continue(next_to) => to = next_to,
            }
        }
    }

    /// One window ending at `to`: the latest post in it, or the end of
    /// the next window down. `Break(None)` at the floor.
    async fn scan_window(&self, to: u64) -> Result<ControlFlow<Option<u64>, u64>> {
        let from = to.saturating_sub(WINDOW).max(self.floor);
        if let Some(block) = self.latest_post_in(from, to).await? {
            return Ok(ControlFlow::Break(Some(block)));
        }
        Ok(from
            .checked_sub(1)
            .filter(|next| *next >= self.floor)
            .map_or(ControlFlow::Break(None), ControlFlow::Continue))
    }

    /// The block of the latest `BatchPosted` log in `[from, to]`.
    async fn latest_post_in(&self, from: u64, to: u64) -> Result<Option<u64>> {
        let filter = Filter::new()
            .address(self.settlement)
            .event_signature(IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH)
            .from_block(from)
            .to_block(to);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .with_context(|| format!("get_logs BatchPosted [{from}, {to}]"))?;
        Ok(logs.iter().filter_map(|log| log.block_number).max())
    }

    async fn timestamp_of(&self, block: u64) -> Result<u64> {
        let header = self
            .provider
            .get_block_by_number(block.into())
            .await
            .with_context(|| format!("read L1 block {block}"))?
            .with_context(|| format!("L1 block {block} is missing"))?
            .header;
        Ok(header.timestamp)
    }
}
