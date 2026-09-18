//! The mirror actor: apply every receipt batch to Redis, publish the
//! head, and decide when a rebuild is due.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kardamom_cache::AccountCache;
use kardamom_log::aeron_live::TxReceiptsReceiver;
use kardamom_types::{BPosition, ReceiptBatch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::head::HeadFile;
use crate::rebuild::Rebuild;

/// How long to wait after a failed batch write before the retry.
const WRITE_RETRY: Duration = Duration::from_millis(100);
/// A write outage longer than this may have lost frames in the Aeron
/// buffers behind the mirror, so a rebuild follows the recovery.
const OUTAGE_REBUILD_AFTER: Duration = Duration::from_secs(10);
/// How often the local head file is refreshed.
const HEAD_PERSIST_EVERY: Duration = Duration::from_secs(1);

/// Why a rebuild runs. The reason is the metric label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuildReason {
    Forced,
    Cold,
    Regression,
    Outage,
    Audit,
}

impl RebuildReason {
    fn label(self) -> &'static str {
        match self {
            Self::Forced => "forced",
            Self::Cold => "cold",
            Self::Regression => "regression",
            Self::Outage => "outage",
            Self::Audit => "audit",
        }
    }
}

/// Everything [`Mirror::new`] needs.
pub(crate) struct MirrorInputs {
    pub(crate) cache: AccountCache,
    pub(crate) receipts: TxReceiptsReceiver,
    pub(crate) id: u32,
    pub(crate) head_count: u32,
    pub(crate) head: HeadFile,
    pub(crate) rebuild: Rebuild,
    pub(crate) force_rebuild: bool,
    pub(crate) audit_hours: u64,
    pub(crate) shutdown: CancellationToken,
}

/// The actor state. See the module doc.
pub(crate) struct Mirror {
    cache: AccountCache,
    receipts: TxReceiptsReceiver,
    id: u32,
    head_count: u32,
    head: HeadFile,
    rebuild: Rebuild,
    audit_every: Option<Duration>,
    shutdown: CancellationToken,
    /// The highest batch end applied in this process, as an index.
    applied: u64,
    /// The rebuild that runs on the next batch, if any.
    pending_rebuild: Option<RebuildReason>,
    last_rebuild: Instant,
    head_persisted: Instant,
}

impl Mirror {
    pub(crate) fn new(inputs: MirrorInputs) -> Self {
        let pending_rebuild = inputs.force_rebuild.then_some(RebuildReason::Forced);
        Self {
            cache: inputs.cache,
            receipts: inputs.receipts,
            id: inputs.id,
            head_count: inputs.head_count,
            head: inputs.head,
            rebuild: inputs.rebuild,
            audit_every: (inputs.audit_hours > 0)
                .then(|| Duration::from_secs(inputs.audit_hours.saturating_mul(3_600))),
            shutdown: inputs.shutdown,
            applied: 0,
            pending_rebuild,
            last_rebuild: Instant::now(),
            head_persisted: Instant::now(),
        }
    }

    /// Run until shutdown or the subscription closes.
    pub(crate) async fn run(mut self) -> Result<()> {
        if self.pending_rebuild.is_none() {
            self.pending_rebuild = self.resume_or_rebuild().await;
        }
        loop {
            let batch = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Ok(()),
                batch = self.receipts.recv_batch() => batch,
            };
            let Some((_, batch)) = batch else {
                info!("tx_receipts closed; the mirror stops");
                return Ok(());
            };
            self.apply(batch).await?;
        }
    }

    /// Decide at start: resume when Redis carries a live head at or beyond
    /// this mirror's persisted head, rebuild otherwise. A cold Redis (no
    /// live head from any mirror) rebuilds. A Redis head below the local
    /// head is a failover regression and rebuilds.
    async fn resume_or_rebuild(&self) -> Option<RebuildReason> {
        let ids: Vec<u32> = (0..self.head_count).collect();
        let heads = match self.cache.heads(&ids).await {
            Ok(heads) => heads,
            Err(e) => {
                warn!(error = %e, "start: heads unreadable; rebuilding");
                return Some(RebuildReason::Cold);
            }
        };
        let redis_head = heads.iter().flatten().max().copied();
        let local_head = self.head.read();
        match (redis_head, local_head) {
            (None, _) => Some(RebuildReason::Cold),
            (Some(redis), Some(local)) if redis < local => Some(RebuildReason::Regression),
            _ => None,
        }
    }

    /// Apply one batch: rows and receipts through the write rule, then
    /// the head. A due rebuild runs first, once the first live position is
    /// known.
    async fn apply(&mut self, batch: ReceiptBatch) -> Result<()> {
        let Some(end) = batch.end_tx_idx() else {
            return Ok(());
        };
        if let Some(reason) = self.pending_rebuild.take() {
            self.run_rebuild(reason, end).await?;
        }
        self.write_batch(&batch, end).await?;
        self.advance(end).await
    }

    /// Run the rebuild, with the first live position as its floor.
    async fn run_rebuild(&mut self, reason: RebuildReason, first_live: BPosition) -> Result<()> {
        info!(reason = reason.label(), "rebuild: starting");
        let started = Instant::now();
        self.rebuild
            .run(&self.cache, first_live, &self.shutdown)
            .await?;
        crate::metrics::record_rebuild(reason.label(), started.elapsed().as_secs_f64());
        self.last_rebuild = Instant::now();
        Ok(())
    }

    /// Write a batch's rows and receipts. A failure retries until the
    /// write lands or shutdown. An outage past the bound schedules a
    /// rebuild, because frames may have been lost behind the mirror.
    async fn write_batch(&mut self, batch: &ReceiptBatch, end: BPosition) -> Result<()> {
        let started = Instant::now();
        loop {
            match self.try_write(batch, end).await {
                Ok(()) => break,
                Err(e) => {
                    crate::metrics::record_write_retry();
                    warn!(error = %e, "batch write failed; retrying");
                }
            }
            tokio::select! {
                () = self.shutdown.cancelled() => anyhow::bail!("shutdown during a write retry"),
                () = tokio::time::sleep(WRITE_RETRY) => {}
            }
        }
        if started.elapsed() > OUTAGE_REBUILD_AFTER {
            self.pending_rebuild = Some(RebuildReason::Outage);
        }
        Ok(())
    }

    async fn try_write(&self, batch: &ReceiptBatch, end: BPosition) -> Result<()> {
        let written = self.cache.write_rows(end, &batch.accounts).await?;
        self.cache.write_receipts(&batch.receipts).await?;
        crate::metrics::record_batch(written.disagreed);
        Ok(())
    }

    /// Publish the head, wait for a replica, persist the local head, and
    /// schedule the audit rebuild when it is due.
    async fn advance(&mut self, end: BPosition) -> Result<()> {
        self.applied = self.applied.max(end.as_index());
        let head = BPosition::from_index(self.applied);
        if let Err(e) = self.cache.set_head(self.id, head).await {
            warn!(error = %e, "head publish failed; the next batch retries");
        }
        match self.cache.wait_replica().await {
            Ok(0) => crate::metrics::record_wait_replica_zero(),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "WAIT failed"),
        }
        crate::metrics::set_head(self.applied);
        if self.head_persisted.elapsed() >= HEAD_PERSIST_EVERY {
            self.head.write(self.applied).context("persist the head")?;
            self.head_persisted = Instant::now();
        }
        if self
            .audit_every
            .is_some_and(|every| self.last_rebuild.elapsed() >= every)
        {
            self.pending_rebuild = Some(RebuildReason::Audit);
        }
        Ok(())
    }
}
