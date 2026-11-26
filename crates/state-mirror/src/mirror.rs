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

use crate::audit::AuditClock;
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
    Hole,
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
            Self::Hole => "hole",
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
    shutdown: CancellationToken,
    progress: Progress,
    audit: AuditClock,
    head_persisted: Instant,
    steps: MirrorSteps,
}

impl Mirror {
    pub(crate) fn new(inputs: MirrorInputs) -> Self {
        let audit_every = (inputs.audit_hours > 0)
            .then(|| Duration::from_secs(inputs.audit_hours.saturating_mul(3_600)));
        Self {
            audit: AuditClock::start(audit_every, inputs.head.last_rebuild()),
            cache: inputs.cache,
            receipts: inputs.receipts,
            id: inputs.id,
            head_count: inputs.head_count,
            head: inputs.head,
            rebuild: inputs.rebuild,
            shutdown: inputs.shutdown,
            progress: Progress::new(inputs.force_rebuild.then_some(RebuildReason::Forced)),
            head_persisted: Instant::now(),
            steps: MirrorSteps::new(),
        }
    }

    /// Run until shutdown or the subscription closes.
    pub(crate) async fn run(mut self) -> Result<()> {
        if self.progress.pending_rebuild.is_none() {
            let start = self.resume_or_rebuild().await?;
            self.progress.begin(start);
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
    /// Otherwise the start follows [`StartHeads::start`].
    async fn resume_or_rebuild(&self) -> Result<Start> {
        if self.head.rebuild_pending()? {
            return Ok(Start::Rebuild(RebuildReason::Interrupted));
        }
        let Ok(heads) = self
            .start_heads()
            .await
            .inspect_err(|e| warn!(error = %e, "start: heads unreadable; rebuilding"))
        else {
            return Ok(Start::Rebuild(RebuildReason::Cold));
        };
        Ok(heads.start())
    }

    /// The live heads of all mirrors, the durable head of this mirror,
    /// and the local head file.
    async fn start_heads(&self) -> Result<StartHeads, kardamom_cache::CacheError> {
        let ids: Vec<u32> = (0..self.head_count).collect();
        let live = self.cache.heads(&ids).await?.into_iter().flatten().max();
        Ok(StartHeads {
            live,
            durable: self.cache.durable_head(self.id).await?,
            local: self.head.read(),
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
        if let Some(start) = group.iter().find_map(|b| b.receipts.first()) {
            self.progress.check_follows(start.tx_idx);
        }
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
        self.head
            .record_rebuild(self.audit.rebuilt())
            .context("persist the rebuild time")?;
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
        let retried = self.write_or_wait(write).await?.is_continue();
        if retried {
            self.retry_until_landed(write).await?;
        }
        if started.elapsed() > OUTAGE_REBUILD_AFTER {
            self.progress.pending_rebuild = Some(RebuildReason::Outage);
        } else if retried && self.progress.pending_rebuild.is_none() {
            self.head.finish_rebuild()?;
        }
        Ok(())
    }

    /// Retry the write until it lands or shutdown.
    async fn retry_until_landed(&mut self, write: &Write<'_>) -> Result<()> {
        while self.write_or_wait(write).await?.is_continue() {}
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
        if self.audit.audit_due() {
            self.progress.pending_rebuild = Some(RebuildReason::Audit);
        }
        Ok(())
    }
}

/// What a start owes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    /// A live mirror applied the batches while this one was down.
    Resume,
    /// Redis holds the rows up to this head, and no live mirror covers
    /// the stop. The first live batch must follow the head.
    Unproven(u64),
    Rebuild(RebuildReason),
}

/// The heads that decide a start: the max of the live heads of all
/// mirrors, the durable head of this mirror, and the head of the local
/// head file.
struct StartHeads {
    live: Option<u64>,
    durable: Option<u64>,
    local: Option<u64>,
}

impl StartHeads {
    /// The Redis head is the max of the live heads and the durable head.
    /// A Redis with no head is cold. A Redis head below the local head is
    /// a failover regression. A live head at or beyond the local head
    /// resumes: that mirror applied the batches of the stop. In all other
    /// cases, the resume is valid only when the first live batch follows
    /// the Redis head.
    fn start(self) -> Start {
        let redis = self.live.max(self.durable);
        let live_covers = self
            .live
            .is_some_and(|live| self.local.is_none_or(|local| live >= local));
        match (redis, self.local) {
            (None, _) => Start::Rebuild(RebuildReason::Cold),
            (Some(redis), Some(local)) if redis < local => {
                Start::Rebuild(RebuildReason::Regression)
            }
            _ if live_covers => Start::Resume,
            (Some(redis), _) => Start::Unproven(redis),
        }
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
    /// The Redis head that the first live batch must follow, when no live
    /// mirror covered the stop of this one.
    unproven: Option<u64>,
    /// The rollback events applied in this process.
    rollbacks: AppliedRollbacks,
}

impl Progress {
    fn new(pending_rebuild: Option<RebuildReason>) -> Self {
        Self {
            applied: 0,
            pending_rebuild,
            unproven: None,
            rollbacks: AppliedRollbacks::default(),
        }
    }

    /// Take the decision of the start.
    fn begin(&mut self, start: Start) {
        match start {
            Start::Resume => {}
            Start::Unproven(head) => self.unproven = Some(head),
            Start::Rebuild(reason) => self.pending_rebuild = Some(reason),
        }
    }

    /// Check the first live receipt `first` against the unproven Redis
    /// head. Positions are not dense, because a marker takes a position
    /// with no receipt. So a first receipt at or below the position after
    /// the head proves that no batch is missing. A later first receipt
    /// can be a hole, and a hole rebuild runs.
    fn check_follows(&mut self, first: BPosition) {
        let Some(head) = self.unproven.take() else {
            return;
        };
        if head
            .checked_add(1)
            .is_some_and(|next| first.as_index() <= next)
        {
            return;
        }
        warn!(
            head,
            first = first.as_index(),
            "start: the first live batch does not follow the Redis head; rebuilding"
        );
        self.pending_rebuild.get_or_insert(RebuildReason::Hole);
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
        self.unproven = None;
        self.pending_rebuild = Some(RebuildReason::Rollback);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Progress, RebuildReason, Start, StartHeads};
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

    fn start(live: Option<u64>, durable: Option<u64>, local: Option<u64>) -> Start {
        StartHeads {
            live,
            durable,
            local,
        }
        .start()
    }

    /// A live head at or beyond the local head resumes. A durable head
    /// alone needs the first live batch to follow it. A Redis head below
    /// the local head is a failover regression. A Redis with no head is
    /// cold, whatever the local head is.
    #[test]
    fn the_start_rule_covers_each_case_of_the_heads() {
        let rebuild = Start::Rebuild;
        assert_eq!(start(Some(20), None, Some(20)), Start::Resume);
        assert_eq!(start(Some(30), Some(20), None), Start::Resume);
        assert_eq!(start(None, Some(20), Some(20)), Start::Unproven(20));
        assert_eq!(start(None, Some(30), None), Start::Unproven(30));
        assert_eq!(start(Some(10), Some(30), Some(20)), Start::Unproven(30));
        assert_eq!(
            start(None, Some(10), Some(20)),
            rebuild(RebuildReason::Regression)
        );
        assert_eq!(
            start(Some(10), None, Some(20)),
            rebuild(RebuildReason::Regression)
        );
        assert_eq!(start(None, None, Some(20)), rebuild(RebuildReason::Cold));
        assert_eq!(start(None, None, None), rebuild(RebuildReason::Cold));
    }

    /// A first live receipt right after the unproven head resumes. A later
    /// first receipt can be a hole, and rebuilds. Only the first batch
    /// counts.
    #[test]
    fn only_a_first_batch_that_follows_the_head_resumes() {
        let mut follows = Progress::new(None);
        follows.begin(Start::Unproven(20));
        follows.check_follows(BPosition::from_index(21));
        assert_eq!(follows.pending_rebuild, None);
        follows.check_follows(BPosition::from_index(90));
        assert_eq!(follows.pending_rebuild, None, "only the first batch counts");

        let mut overlaps = Progress::new(None);
        overlaps.begin(Start::Unproven(20));
        overlaps.check_follows(BPosition::from_index(15));
        assert_eq!(overlaps.pending_rebuild, None);

        let mut hole = Progress::new(None);
        hole.begin(Start::Unproven(20));
        hole.check_follows(BPosition::from_index(22));
        assert_eq!(hole.pending_rebuild, Some(RebuildReason::Hole));
        assert_eq!(RebuildReason::Hole.label(), "hole");
    }

    /// A rollback clears the projection, so the unproven head means
    /// nothing after it.
    #[test]
    fn a_rollback_drops_the_unproven_head() {
        let mut progress = Progress::new(None);
        progress.begin(Start::Unproven(20));
        assert!(progress.roll_back(&rollback(2)));
        assert_eq!(progress.unproven, None);
    }
}
