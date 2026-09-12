//! The live feed loop: `ReaderToExec` records to packed, posted batches.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_provider::Provider;
use anyhow::{Context, Result, bail};
use metrics::{counter, gauge};
use tokio::sync::mpsc::Receiver;

use kardamom_engine::reader::ReaderToExec;
use kardamom_types::BlockBoundaryStart;

use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::batcher::{
    BatcherConfig, MAX_BLOBS_PER_BATCH, PostedBatch, metric_names, pack_block_groups,
};
use crate::error::BatcherError;

use super::cursor::BatchCursor;
use super::live_metric_names;
use super::sender::LiveSender;

/// Feed-loop tunables.
#[derive(Clone, Debug)]
pub(crate) struct FeedConfig {
    pub blocks_per_batch: NonZeroUsize,
    pub compress: bool,
    /// The L2 chain id. See [`BatcherConfig::chain_id`].
    pub chain_id: u64,
    /// Post a partial group if the oldest pending block has waited this long.
    pub flush: Duration,
    /// Drop closed blocks at or below this number without posting. L1
    /// already covers them, from the startup reconcile.
    pub skip_through_block: u64,
}

/// A pending close-policy group: the blocks buffered so far, when the
/// oldest one arrived (for the flush timeout), and the [`BatchCursor`] a
/// post of this group right now would confirm. Existing only while
/// non-empty makes an empty post unrepresentable, instead of checked with
/// a `blocks.last()` at post time.
struct PendingGroup {
    since: Instant,
    blocks: Vec<ClosedBlock>,
    cursor: BatchCursor,
}

impl PendingGroup {
    /// The cursor a post that ends at `block_number` confirms: the group's
    /// cursor when that is the group's last block, else the cursor just
    /// past the named block.
    fn cursor_at(&self, block_number: u64) -> Result<BatchCursor> {
        if self.cursor.next_block == block_number.saturating_add(1) {
            return Ok(self.cursor);
        }
        let end = self
            .blocks
            .iter()
            .find(|b| b.block_number == block_number)
            .with_context(|| format!("batch end block {block_number} is not in the group"))?;
        Ok(BatchCursor {
            next_index: end.end_tx_idx.as_index(),
            next_block: block_number
                .checked_add(1)
                .context("block_number overflowed u64")?,
            // `LiveSender::post_confirmed` stamps `last_batch_index`.
            last_batch_index: 0,
        })
    }
}

/// The live feed loop's state: the accumulator, the close policy's pending
/// group, and the sender it posts confirmed batches to. `ReaderToExec`
/// records flow through the accumulator, then the close policy, then to
/// [`LiveSender`]. The reader thread feeds it over a bounded tokio channel,
/// until that channel closes or a post fails and stops the loop. This is
/// crash-only: there is no graceful drain. The cursor is at-least-once, and
/// a restart re-observes records.
pub(crate) struct FeedLoop<P> {
    rx: Receiver<ReaderToExec>,
    sender: LiveSender<P>,
    cfg: FeedConfig,
    pack_cfg: BatcherConfig,
    acc: BatchAccumulator,
    pending: Option<PendingGroup>,
}

impl<P: Provider> FeedLoop<P> {
    pub(crate) fn new(rx: Receiver<ReaderToExec>, sender: LiveSender<P>, cfg: FeedConfig) -> Self {
        let pack_cfg = BatcherConfig {
            blocks_per_batch: cfg.blocks_per_batch,
            compress: cfg.compress,
            chain_id: cfg.chain_id,
            ..Default::default()
        };
        Self {
            rx,
            sender,
            cfg,
            pack_cfg,
            acc: BatchAccumulator::new(),
            pending: None,
        }
    }

    /// Run until the channel closes or a post fails after its retry budget.
    ///
    /// # Errors
    /// Returns an error when the ordering channel closes, or when
    /// [`LiveSender::post_confirmed`] fails after its retry budget.
    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            let event = tokio::time::timeout(self.cfg.flush, self.rx.recv()).await;
            self.handle_event(event).await?;
        }
    }

    /// One [`Self::run`] tick: a channel event (a new record to buffer, or
    /// the channel closing), or the flush timeout, whichever comes first.
    async fn handle_event(
        &mut self,
        event: Result<Option<ReaderToExec>, tokio::time::error::Elapsed>,
    ) -> Result<()> {
        match event {
            Ok(Some(ReaderToExec::Tx {
                envelope, position, ..
            })) => self.acc.observe_tx(envelope, position),
            // Deposits are absent from DA by design. A reconstructor
            // re-derives them from L1 (this mirrors MultiArchiveReader
            // skipping DepositRefs offline). Skip the epoch marker for
            // the same reason: a reconstructor reads the origin from the
            // block boundary and re-derives that L1 block's deposits
            // itself. XChain is the per-message expansion of a
            // RemoteEpoch record (below): the messages already travel by
            // value inside the buffered record, so these expanded
            // dispatches add nothing new, the same way the exec side
            // expands them again from the record.
            Ok(Some(
                ReaderToExec::Deposit { .. }
                | ReaderToExec::Epoch { .. }
                | ReaderToExec::XChain { .. },
            )) => {}
            // Remote-epoch records travel in DA. Unlike deposits, they
            // are not derivable again from this chain's L1 origin. So the
            // record, with its messages and calldata by value, is
            // buffered into the block it leads. It travels in the KAR1 v2
            // payload for the reconstruction replay to run again.
            Ok(Some(ReaderToExec::RemoteEpoch { record, .. })) => {
                self.acc.observe_remote_epoch(*record);
            }
            Ok(Some(ReaderToExec::Boundary(b))) => {
                self.observe_boundary(&b)?;
                let full = self.cfg.blocks_per_batch.get();
                self.flush_if(|g| g.blocks.len() >= full).await?;
            }
            // Flush timeout.
            Err(_) => {
                let flush = self.cfg.flush;
                self.flush_if(|g| g.since.elapsed() >= flush).await?;
            }
            Ok(None) => bail!("tx_ordering reader channel closed; see reader thread error"),
        }
        Ok(())
    }

    /// Close the current block. Buffer it for posting, unless L1 already
    /// covers it (a stale-cursor replay after a crash between post and
    /// cursor write). Creates the pending group on the first buffered
    /// block; every push refreshes the group's cursor to the block just
    /// closed, so a post right after this call always confirms through it.
    ///
    /// # Errors
    /// Returns an error when `closed.block_number` overflows `u64`
    /// computing the cursor's `next_block`.
    fn observe_boundary(&mut self, b: &BlockBoundaryStart) -> Result<()> {
        let closed = self.acc.observe_boundary(b);
        counter!(metric_names::BLOCKS_OBSERVED).increment(1);
        if closed.block_number <= self.cfg.skip_through_block {
            counter!(live_metric_names::SKIPPED_POSTED_BLOCKS).increment(1);
            return Ok(());
        }
        let next_block = closed
            .block_number
            .checked_add(1)
            .context("block_number overflowed u64")?;
        let cursor = BatchCursor {
            next_index: closed.end_tx_idx.as_index(),
            next_block,
            // `LiveSender::post_confirmed` stamps `last_batch_index`.
            last_batch_index: 0,
        };
        let group = self.pending.get_or_insert_with(|| PendingGroup {
            since: Instant::now(),
            blocks: Vec::new(),
            cursor,
        });
        group.blocks.push(closed);
        group.cursor = cursor;
        // Metric value; f64 precision loss only above 2^52, never reached
        // by a pending-block count.
        #[allow(
            clippy::cast_precision_loss,
            reason = "pending-block count never nears 2^52"
        )]
        gauge!(live_metric_names::PENDING_BLOCKS).set(group.blocks.len() as f64);
        Ok(())
    }

    /// Post the pending group if `ready` says it's due — the shape both
    /// the `Boundary` arm (the group fills) and the flush-timeout arm (the
    /// flush deadline elapses) share.
    async fn flush_if(&mut self, ready: impl FnOnce(&PendingGroup) -> bool) -> Result<()> {
        if let Some(group) = self.pending.take_if(|g| ready(g)) {
            self.post_group(group).await?;
        }
        Ok(())
    }

    /// Pack and post `group`. A group that overflows the blob ceiling
    /// splits into several posts, one cursor each. A single block that
    /// overflows on its own is fatal: the loop stops with a log line that
    /// names the block.
    async fn post_group(&mut self, group: PendingGroup) -> Result<()> {
        gauge!(live_metric_names::PENDING_BLOCKS).set(0.0);
        let batches = pack_block_groups(&self.pack_cfg, &group.blocks).map_err(log_pack_error)?;
        if batches.len() > 1 {
            tracing::warn!(
                blocks = group.blocks.len(),
                batches = batches.len(),
                "group split to stay under the blob ceiling"
            );
        }
        for batch in &batches {
            self.post_one(batch, &group).await?;
        }
        Ok(())
    }

    /// Post one packed batch of `group`, confirming through the cursor of
    /// the block the batch ends at. The last batch confirms the group's
    /// own cursor.
    async fn post_one(&mut self, batch: &PostedBatch, group: &PendingGroup) -> Result<()> {
        let cursor = group.cursor_at(batch.l2_block_end)?;
        self.sender.post_confirmed(batch, cursor).await
    }
}

/// Log a fatal single-block overflow by name before the error stops the
/// loop; every other pack error passes through.
fn log_pack_error(e: BatcherError) -> anyhow::Error {
    if let BatcherError::BlockTooLarge {
        block_number,
        blobs,
    } = &e
    {
        tracing::error!(
            block = block_number,
            blobs,
            ceiling = MAX_BLOBS_PER_BATCH,
            "FATAL: one block alone exceeds the blob ceiling; the batcher cannot post it"
        );
    }
    e.into()
}
