//! The mirror actor: apply every receipt batch to Redis, publish the
//! head, and decide when a rebuild is due. A rollback frame of an
//! executor clears the projection, and a rebuild follows.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kardamom_cache::{AccountCache, AppliedRollbacks, BatchWrite};
use kardamom_log::aeron_live::{ReceiptFrame, TxReceiptsReceiver};
use kardamom_types::{BPosition, ReceiptBatch, ReceiptRollback};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::head::HeadFile;
use crate::metrics::MirrorSteps;
use crate::rebuild::Rebuild;

#[cfg(all(test, feature = "docker-e2e"))]
mod component_tests;

/// How long to wait after a failed batch write before the retry.
const WRITE_RETRY: Duration = Duration::from_millis(100);
/// A write outage longer than this may have lost frames in the Aeron
/// buffers behind the mirror, so a rebuild follows the recovery.
const OUTAGE_REBUILD_AFTER: Duration = Duration::from_secs(10);
/// How often the local head file is refreshed.
const HEAD_PERSIST_EVERY: Duration = Duration::from_secs(1);
/// The most batches in one write. Each executor replica publishes a batch
/// every few milliseconds, and the mirror gets a copy from each replica.
/// One Redis round trip for a group, instead of several for each batch,
/// lets the mirror keep up with the chain. The bound keeps one pipeline,
/// and its retry, small.
const MAX_GROUP: usize = 128;

/// Why a rebuild runs. The reason is the metric label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuildReason {
    Forced,
    Cold,
    Regression,
    Outage,
    Audit,
    Rollback,
    Interrupted,
}

impl RebuildReason {
    fn label(self) -> &'static str {
        match self {
            Self::Forced => "forced",
            Self::Cold => "cold",
            Self::Regression => "regression",
            Self::Outage => "outage",
            Self::Audit => "audit",
            Self::Rollback => "rollback",
            Self::Interrupted => "interrupted",
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
    progress: Progress,
    last_rebuild: Instant,
    head_persisted: Instant,
    steps: MirrorSteps,
}

impl Mirror {
    pub(crate) fn new(inputs: MirrorInputs) -> Self {
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
            progress: Progress::new(inputs.force_rebuild.then_some(RebuildReason::Forced)),
            last_rebuild: Instant::now(),
            head_persisted: Instant::now(),
            steps: MirrorSteps::new(),
        }
    }

    /// Run until shutdown or the subscription closes.
    pub(crate) async fn run(mut self) -> Result<()> {
        if self.progress.pending_rebuild.is_none() {
            self.progress.pending_rebuild = self.resume_or_rebuild().await?;
        }
        crate::metrics::set_serving(true);
        loop {
            let frame = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Ok(()),
                frame = self.receipts.recv_frame() => frame,
            };
            let Some((_, frame)) = frame else {
                info!("tx_receipts closed; the mirror stops");
                return Ok(());
            };
            self.on_frame(frame).await?;
        }
    }

    /// Apply one frame, in stream order. A batch takes the batches that
    /// already wait behind it into one group, up to [`MAX_GROUP`], and
    /// stops at a rollback, which runs after the group.
    async fn on_frame(&mut self, frame: ReceiptFrame) -> Result<()> {
        let first = match frame {
            ReceiptFrame::Batch(batch) => batch,
            ReceiptFrame::Rollback(rollback) => return self.roll_back(rollback).await,
        };
        let mut group = vec![first];
        let mut after = None;
        while group.len() < MAX_GROUP {
            match self.receipts.try_recv_frame() {
                Some((_, ReceiptFrame::Batch(batch))) => group.push(batch),
                Some((_, ReceiptFrame::Rollback(rollback))) => {
                    after = Some(rollback);
                    break;
                }
                None => break,
            }
        }
        self.apply(&group).await?;
        match after {
            Some(rollback) => self.roll_back(rollback).await,
            None => Ok(()),
        }
    }

    /// An executor rolled back: the receipts and rows above
    /// `to_block` can be gone. The receipt index has no block index, and
    /// the rows of the lost blocks carry positions above the new stream,
    /// so the monotone rule would keep them. Thus the mirror deletes the
    /// whole projection, and the next batch starts a rebuild from the
    /// newest checkpoint of the executor. A copy of a rollback that the
    /// mirror applied comes after rows of the new stream, so it changes
    /// nothing.
    async fn roll_back(&mut self, rollback: ReceiptRollback) -> Result<()> {
        if !self.progress.roll_back(&rollback) {
            info!(
                to_block = rollback.to_block,
                id = %rollback.id,
                "a copy of an applied rollback; the projection stays"
            );
            return Ok(());
        }
        warn!(
            to_block = rollback.to_block,
            id = %rollback.id,
            "an executor rolled back; clearing the projection"
        );
        self.head.begin_rebuild()?;
        self.write_until_landed(&Write::Clear).await
    }

    /// An unfinished recovery cannot resume from another writer's head.
    /// Otherwise resume when Redis carries a live head at or beyond
    /// this mirror's persisted head, rebuild otherwise. A cold Redis (no
    /// live head from any mirror) rebuilds. A Redis head below the local
    /// head is a failover regression and rebuilds.
    async fn resume_or_rebuild(&self) -> Result<Option<RebuildReason>> {
        if self.head.rebuild_pending()? {
            return Ok(Some(RebuildReason::Interrupted));
        }
        let ids: Vec<u32> = (0..self.head_count).collect();
        let heads = match self.cache.heads(&ids).await {
            Ok(heads) => heads,
            Err(e) => {
                warn!(error = %e, "start: heads unreadable; rebuilding");
                return Ok(Some(RebuildReason::Cold));
            }
        };
        let redis_head = heads.iter().flatten().max().copied();
        let local_head = self.head.read();
        Ok(match (redis_head, local_head) {
            (None, _) => Some(RebuildReason::Cold),
            (Some(redis), Some(local)) if redis < local => Some(RebuildReason::Regression),
            _ => None,
        })
    }

    /// Apply a group of batches: their rows and receipts through the write
    /// rule in one write, then the head once. A due rebuild runs first,
    /// with the end of the first batch as the first live position.
    async fn apply(&mut self, group: &[ReceiptBatch]) -> Result<()> {
        let ends: Vec<BPosition> = group.iter().filter_map(ReceiptBatch::end_tx_idx).collect();
        let (Some(&first), Some(&end)) = (ends.first(), ends.iter().max()) else {
            return Ok(());
        };
        if let Some(reason) = self.progress.pending_rebuild.take() {
            self.run_rebuild(reason, first).await?;
        }
        let started = Instant::now();
        self.write_until_landed(&Write::Batches(group)).await?;
        self.advance(end).await?;
        self.steps.group.since(started);
        self.steps.group_batches.record(group.len());
        self.steps
            .group_receipts
            .record(group.iter().map(|batch| batch.receipts.len()).sum());
        Ok(())
    }

    /// Run the rebuild, with the first live position as its floor.
    async fn run_rebuild(&mut self, reason: RebuildReason, first_live: BPosition) -> Result<()> {
        info!(reason = reason.label(), "rebuild: starting");
        crate::metrics::set_serving(false);
        self.head.begin_rebuild()?;
        let started = Instant::now();
        self.rebuild
            .run(&self.cache, first_live, &self.shutdown)
            .await?;
        self.head.finish_rebuild()?;
        crate::metrics::record_rebuild(reason.label(), started.elapsed().as_secs_f64());
        crate::metrics::set_serving(true);
        self.last_rebuild = Instant::now();
        Ok(())
    }

    /// Run one write. A failure retries until the write lands or
    /// shutdown. A failure writes the rebuild marker first, so a crash
    /// during the retries rebuilds at the next start. An outage past the
    /// bound schedules a rebuild, because frames may have been lost behind
    /// the mirror. A shorter outage loses no frame: the landed retry
    /// removes the marker, unless a scheduled rebuild still needs it.
    async fn write_until_landed(&mut self, write: &Write<'_>) -> Result<()> {
        let started = Instant::now();
        let mut retried = false;
        while self.write_or_wait(write).await?.is_continue() {
            retried = true;
        }
        if started.elapsed() > OUTAGE_REBUILD_AFTER {
            self.progress.pending_rebuild = Some(RebuildReason::Outage);
        } else if retried && self.progress.pending_rebuild.is_none() {
            self.head.finish_rebuild()?;
        }
        Ok(())
    }

    /// Try the write once. A failure writes the rebuild marker and waits
    /// for the retry period. `Break` means the write landed.
    async fn write_or_wait(&mut self, write: &Write<'_>) -> Result<ControlFlow<()>> {
        let Err(e) = self.try_write(write).await else {
            return Ok(ControlFlow::Break(()));
        };
        self.head.begin_rebuild()?;
        crate::metrics::record_write_retry();
        warn!(error = %e, write = write.label(), "write failed; retrying");
        tokio::select! {
            () = self.shutdown.cancelled() => anyhow::bail!("shutdown during a write retry"),
            () = tokio::time::sleep(WRITE_RETRY) => {}
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn try_write(&self, write: &Write<'_>) -> Result<()> {
        match write {
            Write::Batches(group) => {
                let writes: Vec<BatchWrite<'_>> = group
                    .iter()
                    .filter_map(|batch| {
                        Some(BatchWrite {
                            end: batch.end_tx_idx()?,
                            rows: &batch.accounts,
                            receipts: &batch.receipts,
                        })
                    })
                    .collect();
                let started = Instant::now();
                let written = self.cache.write_batches(&writes).await?;
                self.steps.write.since(started);
                crate::metrics::record_batches(writes.len(), written.disagreed);
            }
            Write::Clear => {
                let deleted = self.cache.clear_projection().await?;
                info!(deleted, "projection cleared");
            }
        }
        Ok(())
    }

    /// Publish the head, wait for a replica, persist the local head, and
    /// schedule the audit rebuild when it is due.
    async fn advance(&mut self, end: BPosition) -> Result<()> {
        let head = self.progress.advance(end);
        let started = Instant::now();
        let head_set = self.cache.set_head(self.id, head).await;
        self.steps.head_set.since(started);
        if let Err(e) = head_set {
            warn!(error = %e, "head publish failed; the next batch retries");
        }
        let started = Instant::now();
        let replicas = self.cache.wait_replica().await;
        self.steps.wait_replica.since(started);
        match replicas {
            Ok(0) => crate::metrics::record_wait_replica_zero(),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "WAIT failed"),
        }
        crate::metrics::set_head(head.as_index());
        if self.head_persisted.elapsed() >= HEAD_PERSIST_EVERY {
            self.head
                .write(head.as_index())
                .context("persist the head")?;
            self.head_persisted = Instant::now();
        }
        if self
            .audit_every
            .is_some_and(|every| self.last_rebuild.elapsed() >= every)
        {
            self.progress.pending_rebuild = Some(RebuildReason::Audit);
        }
        Ok(())
    }
}

/// One Redis write of the mirror, retried as a unit.
enum Write<'a> {
    /// The rows and receipts of a group of batches, each row at the end
    /// of its own batch.
    Batches(&'a [ReceiptBatch]),
    /// The delete of the whole projection after a rollback.
    Clear,
}

impl Write<'_> {
    /// The `write` field of the retry log line.
    fn label(&self) -> &'static str {
        match self {
            Self::Batches(_) => "batch",
            Self::Clear => "clear",
        }
    }
}

/// How far the mirror got in this process, and the rebuild it owes.
struct Progress {
    /// The highest batch end applied since the start or the last
    /// rollback, as an index.
    applied: u64,
    /// The rebuild that runs on the next batch, if any.
    pending_rebuild: Option<RebuildReason>,
    /// The rollback events applied in this process.
    rollbacks: AppliedRollbacks,
}

impl Progress {
    fn new(pending_rebuild: Option<RebuildReason>) -> Self {
        Self {
            applied: 0,
            pending_rebuild,
            rollbacks: AppliedRollbacks::default(),
        }
    }

    /// Take a batch end into the head, and return the head.
    fn advance(&mut self, end: BPosition) -> BPosition {
        self.applied = self.applied.max(end.as_index());
        BPosition::from_index(self.applied)
    }

    /// Start again after a rollback with a new id. The new stream has
    /// lower positions than the lost blocks, so the head follows it from
    /// zero. The projection goes, so the next batch rebuilds it. Returns
    /// whether the id is new: a copy of an applied rollback changes
    /// nothing.
    fn roll_back(&mut self, rollback: &ReceiptRollback) -> bool {
        if !self.rollbacks.first_time(rollback) {
            return false;
        }
        self.applied = 0;
        self.pending_rebuild = Some(RebuildReason::Rollback);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Progress, RebuildReason};
    use alloy_primitives::B256;
    use kardamom_types::{BPosition, ReceiptRollback};

    fn rollback(id: u8) -> ReceiptRollback {
        ReceiptRollback {
            to_block: 3,
            id: B256::repeat_byte(id),
        }
    }

    /// After a rollback, the head follows the lower positions of the new
    /// stream, and the next batch runs a rollback rebuild.
    #[test]
    fn a_rollback_lowers_the_head_and_schedules_a_rebuild() {
        let mut progress = Progress::new(None);
        assert_eq!(progress.advance(BPosition::from_index(900)).as_index(), 900);
        assert_eq!(progress.advance(BPosition::from_index(700)).as_index(), 900);
        assert!(progress.roll_back(&rollback(1)));
        assert_eq!(progress.pending_rebuild, Some(RebuildReason::Rollback));
        assert_eq!(progress.advance(BPosition::from_index(700)).as_index(), 700);
        assert_eq!(RebuildReason::Rollback.label(), "rollback");
    }

    /// A rollback replaces a rebuild that is already due: one rebuild
    /// after the clear covers both.
    #[test]
    fn a_rollback_replaces_a_due_rebuild() {
        let mut progress = Progress::new(Some(RebuildReason::Forced));
        assert!(progress.roll_back(&rollback(1)));
        assert_eq!(
            progress.pending_rebuild.take(),
            Some(RebuildReason::Rollback)
        );
        assert_eq!(progress.pending_rebuild, None);
    }

    /// A copy of an applied rollback leaves the head and the rebuild as
    /// they are: the rows after the first copy belong to the new stream.
    #[test]
    fn a_copy_of_an_applied_rollback_changes_nothing() {
        let mut progress = Progress::new(None);
        assert!(progress.roll_back(&rollback(7)));
        assert_eq!(
            progress.pending_rebuild.take(),
            Some(RebuildReason::Rollback)
        );
        progress.advance(BPosition::from_index(40));
        assert!(!progress.roll_back(&rollback(7)), "the copy is no news");
        assert_eq!(progress.pending_rebuild, None);
        assert_eq!(progress.advance(BPosition::from_index(10)).as_index(), 40);
        assert!(progress.roll_back(&rollback(8)), "a new event applies");
    }
}
