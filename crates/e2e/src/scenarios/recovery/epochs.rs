//! The L1 epoch checks of the combined cases: the da-watcher publishes
//! each L1 epoch to the sequencers, and the sealer orders it as the
//! chain's L1 origin. A loss of the sequencers or the sealer can lose a
//! published epoch; the da-watcher publishes an unconfirmed epoch again.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};

use super::Run;
use crate::harness::metrics;

/// The L1 origin of the last epoch that the da-watcher published.
const PUBLISHED_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";
/// The epochs that the da-watcher published.
const PUBLISHED: &str = "kardamom_da_watcher_epochs_published_total";
/// The epochs that the da-watcher published again, unconfirmed.
const REPUBLISHED: &str = "kardamom_da_watcher_epochs_republished_total";
/// The L1 origin that the sealer ordered, as each sequencer reads it.
const SEQUENCER_ORIGIN: &str = "kardamom_sequencer_l1_origin";
/// The re-publish of an unconfirmed epoch comes 30 s after its publish.
const REFILL_BOUND: Duration = Duration::from_secs(45);
/// The bound for the sealer's origin to reach the published one.
const ORIGIN_BOUND: Duration = Duration::from_secs(45);

/// The da-watcher at the return of the classes: what it published
/// during the outage, which nothing ordered yet.
#[derive(Debug, Clone, Copy)]
pub(super) struct EpochBaseline {
    origin: f64,
    published: f64,
    republished: f64,
}

impl Run<'_> {
    fn watcher(&self) -> Result<SocketAddr> {
        self.stack
            .da_watcher_metrics()
            .context("the epoch checks need the L1 stack and its da-watcher")
    }

    /// Read the da-watcher just before the classes return.
    pub(super) async fn epoch_baseline(&self) -> Result<EpochBaseline> {
        let scrape = metrics::scrape(self.watcher()?).await?;
        Ok(EpochBaseline {
            origin: scrape
                .value(PUBLISHED_ORIGIN)
                .context("no published epoch")?,
            published: scrape.value(PUBLISHED).unwrap_or(0.0),
            republished: scrape.value(REPUBLISHED).unwrap_or(0.0),
        })
    }

    /// The sealer's L1 origin reaches at least `floor`.
    async fn await_origin(&self, floor: f64) -> Result<f64> {
        let target = self.stack.target(Duration::from_secs(5))?;
        metrics::poll_until(
            &format!("the sealer's L1 origin reaches {floor}"),
            ORIGIN_BOUND,
            Duration::from_millis(500),
            async || {
                let origins = futures::future::join_all(
                    target.sequencer_metrics.iter().map(|a| Self::origin_of(*a)),
                )
                .await;
                Ok(origins.into_iter().flatten().find(|o| *o >= floor))
            },
        )
        .await
    }

    /// No origin gap: the sealer's L1 origin reaches the last epoch that
    /// the da-watcher published.
    pub(super) async fn assert_no_origin_gap(&self) -> Result<()> {
        let watcher = self.watcher()?;
        let published = metrics::poll_until(
            "the da-watcher publishes an epoch",
            ORIGIN_BOUND,
            Duration::from_millis(500),
            async || {
                Ok(metrics::scrape(watcher)
                    .await
                    .ok()
                    .and_then(|s| s.value(PUBLISHED_ORIGIN)))
            },
        )
        .await?;
        let origin = self.await_origin(published).await?;
        eprintln!(
            "{}: the sealer's origin {origin} reached the published {published}",
            self.at()
        );
        Ok(())
    }

    async fn origin_of(addr: SocketAddr) -> Option<f64> {
        metrics::scrape(addr).await.ok()?.value(SEQUENCER_ORIGIN)
    }

    /// The epochs that the da-watcher published while the sequencers or
    /// the sealer were down reach the chain: the da-watcher publishes
    /// again after the return, and the sealer's origin reaches the last
    /// epoch of the outage.
    pub(super) async fn assert_epoch_refill(&self, base: EpochBaseline) -> Result<()> {
        let watcher = self.watcher()?;
        metrics::poll_until(
            &format!("the da-watcher publishes after the return ({base:?})"),
            REFILL_BOUND,
            Duration::from_millis(500),
            async || {
                let s = metrics::scrape(watcher).await.ok();
                let read = |name| s.as_ref().and_then(|s| s.value(name)).unwrap_or(0.0);
                Ok(
                    (read(REPUBLISHED) > base.republished || read(PUBLISHED) > base.published)
                        .then_some(()),
                )
            },
        )
        .await?;
        let origin = self.await_origin(base.origin).await?;
        eprintln!(
            "{}: the epochs of the outage up to {} reached the chain (origin {origin})",
            self.at(),
            base.origin
        );
        Ok(())
    }
}
