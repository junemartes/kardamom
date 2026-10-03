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
use crate::batcher::{BatcherConfig, PostedBatch, metric_names, pack_block_groups};
use crate::error::BatcherError;

use kardamom_types::xchain::remote_epoch_wire_bytes;

use super::cursor::BatchCursor;
use super::spool::{Restored, Spool};

/// How often the group's timers are checked when no record arrives.
const TICK: Duration = Duration::from_secs(1);
use super::live_metric_names;
use super::sender::LiveSender;

/// Feed-loop tunables.
#[derive(Clone, Debug)]
pub(crate) struct FeedConfig {
    pub blocks_per_batch: NonZeroUsize,
    pub compress: bool,
    /// The L2 chain id. See [`BatcherConfig::chain_id`].
    pub chain_id: u64,
    /// Post the group when its oldest block has waited this long and the
    /// group holds a transaction or a remote-epoch record.
    pub flush: Duration,
    /// The same, for a group of empty blocks. A real L1 takes a long one:
    /// an idle chain closes a block a second, and each post costs gas.
    pub idle_flush: Duration,
    /// Post the group once its blocks' raw bytes reach this: one full
    /// post per fee, under the payload ceiling so it stays one post.
    pub target_payload_bytes: NonZeroUsize,
    /// Drop closed blocks at or below this number without posting. L1
    /// already covers them, from the startup reconcile.
    pub skip_through_block: u64,
}

/// A pending close-policy group: the blocks buffered so far, when the
/// oldest one arrived (for the flush timers), whether any block carries
/// something to post beyond its boundary, and the [`BatchCursor`] a post
/// of this group right now would confirm. Existing only while non-empty
/// makes an empty post unrepresentable, instead of checked with a
/// `blocks.last()` at post time.
struct PendingGroup {
    since: Instant,
    blocks: Vec<ClosedBlock>,
    /// A transaction or a remote-epoch record is in the group: the
    /// shorter flush timer applies.
    has_traffic: bool,
    /// The raw bytes of the group's transactions and records, an upper
    /// bound of the compressed payload.
    raw_bytes: usize,
    cursor: BatchCursor,
}

/// The raw bytes a closed block adds to a payload: its transactions and
/// its remote-epoch records as framed, before compression.
fn raw_bytes_of(block: &ClosedBlock) -> usize {
    let txs: usize = block.txs.iter().map(|t| t.envelope.raw_tx.len()).sum();
    let records: usize = block
        .remote_epochs
        .iter()
        .map(|r| remote_epoch_wire_bytes(r.messages.iter().map(|m| m.input.len())))
        .sum();
    txs.saturating_add(records)
}

impl PendingGroup {
    /// The group is due: it is full by count or by bytes, or its oldest
    /// block has waited past the timer its contents select.
    fn due(&self, cfg: &FeedConfig) -> bool {
        let timer = if self.has_traffic {
            cfg.flush
        } else {
            cfg.idle_flush
        };
        self.blocks.len() >= cfg.blocks_per_batch.get()
            || self.raw_bytes >= cfg.target_payload_bytes.get()
            || self.since.elapsed() >= timer
    }

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
/// a restart re-observes records. The loop outlives one reader stack: a
/// refused replay ends the stack, the store fills the gap into the
/// group, and the loop runs on the next stack's channel.
pub(crate) struct FeedLoop<P> {
    sender: LiveSender<P>,
    cfg: FeedConfig,
    pack_cfg: BatcherConfig,
    acc: BatchAccumulator,
    pending: Option<PendingGroup>,
    /// The consumed, unposted blocks on disk (`super::spool`).
    spool: Spool,
}

impl PendingGroup {
    /// The group a restarted batcher continues: the spooled blocks, aged
    /// from the oldest one's write.
    fn restored(restored: Restored) -> Option<Self> {
        let last = restored.blocks.last()?;
        let age = restored
            .oldest_written
            .and_then(|t| t.elapsed().ok())
            .unwrap_or_default();
        let has_traffic = restored
            .blocks
            .iter()
            .any(|b| !b.txs.is_empty() || !b.remote_epochs.is_empty());
        let raw_bytes = restored.blocks.iter().map(raw_bytes_of).sum();
        let cursor = BatchCursor {
            next_index: last.end_tx_idx.as_index(),
            next_block: last.block_number.saturating_add(1),
            last_batch_index: 0,
        };
        Some(Self {
            since: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
            blocks: restored.blocks,
            has_traffic,
            raw_bytes,
            cursor,
        })
    }
}

impl<P: Provider> FeedLoop<P> {
    /// A loop whose pending group starts as `restored`, the spool's
    /// content; the reader resumes just past it (see `run`).
    pub(crate) fn new(
        sender: LiveSender<P>,
        cfg: FeedConfig,
        spool: Spool,
        restored: Restored,
    ) -> Self {
        let pack_cfg = BatcherConfig {
            blocks_per_batch: cfg.blocks_per_batch,
            compress: cfg.compress,
            chain_id: cfg.chain_id,
            ..Default::default()
        };
        let pending = PendingGroup::restored(restored);
        if let Some(group) = &pending {
            tracing::info!(
                blocks = group.blocks.len(),
                through = group.cursor.next_block.saturating_sub(1),
                "pending group restored from the spool"
            );
        }
        Self {
            sender,
            cfg,
            pack_cfg,
            acc: BatchAccumulator::new(),
            pending,
            spool,
        }
    }

    /// Run on `rx` until the channel closes or a post fails after its
    /// retry budget, and return why: the ordering channel closed, or
    /// [`LiveSender::post_confirmed`] failed after its retry budget.
    pub(crate) async fn run(&mut self, mut rx: Receiver<ReaderToExec>) -> anyhow::Error {
        loop {
            let event = tokio::time::timeout(TICK, rx.recv()).await;
            if let Err(why) = self.handle_event(event).await {
                return why;
            }
        }
    }

    /// The block the reader resumes after: re-observed blocks up to it
    /// drop.
    #[cfg(test)]
    pub(crate) fn skip_through_block(&self) -> u64 {
        self.cfg.skip_through_block
    }

    /// The blocks in the pending group.
    #[cfg(test)]
    pub(crate) fn pending_blocks(&self) -> usize {
        self.pending.as_ref().map_or(0, |g| g.blocks.len())
    }

    /// Add rebuilt blocks, in order, as if the sealer had served them:
    /// into the spool and the pending group. The reader
    /// then resumes just past them, and re-observed blocks up to the last
    /// one drop. Returns the cursor the reader resumes at.
    ///
    /// # Errors
    /// Returns an error when the spool write fails, or a block number
    /// overflows.
    pub(crate) fn absorb(&mut self, blocks: Vec<ClosedBlock>) -> Result<BatchCursor> {
        let count = blocks.len();
        let mut resume = self
            .pending
            .as_ref()
            .map_or(BatchCursor::genesis(), |g| g.cursor);
        for closed in blocks {
            resume = self.push_closed(closed)?;
        }
        self.cfg.skip_through_block = resume.next_block.saturating_sub(1);
        counter!(live_metric_names::REBUILT_BLOCKS).increment(count as u64);
        Ok(resume)
    }

    /// One [`Self::run`] tick: a channel event (a new record to buffer, or
    /// the channel closing), or the tick, whichever comes first. The
    /// group's timers are checked on every boundary and on every tick: a
    /// live chain closes a block a second, so a check only when the
    /// channel is quiet would never run.
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
                ReaderToExec::Deposit(_)
                | ReaderToExec::Epoch(_)
                | ReaderToExec::XChain { .. }
                | ReaderToExec::Vacant { .. },
            )) => {}
            // Remote-epoch records travel in DA. Unlike deposits, they
            // are not derivable again from this chain's L1 origin. So the
            // record, with its messages and calldata by value, is
            // buffered into the block it leads. It travels in the KAR1 v2
            // payload for the reconstruction replay to run again.
            Ok(Some(ReaderToExec::RemoteEpoch(record))) => {
                self.acc.observe_remote_epoch(*record);
            }
            Ok(Some(ReaderToExec::Boundary(b))) => {
                self.observe_boundary(&b)?;
                self.flush_if_due().await?;
            }
            Err(_) => self.flush_if_due().await?,
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
        self.push_closed(closed).map(|_| ())
    }

    /// Spool `closed` and add it to the pending group. Returns the cursor
    /// a post right after this call confirms.
    fn push_closed(&mut self, closed: ClosedBlock) -> Result<BatchCursor> {
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
        self.spool.append(&closed)?;
        let group = self.pending.get_or_insert_with(|| PendingGroup {
            since: Instant::now(),
            blocks: Vec::new(),
            has_traffic: false,
            raw_bytes: 0,
            cursor,
        });
        group.has_traffic |= !closed.txs.is_empty() || !closed.remote_epochs.is_empty();
        group.raw_bytes = group.raw_bytes.saturating_add(raw_bytes_of(&closed));
        group.blocks.push(closed);
        group.cursor = cursor;
        // Metric value; f64 precision loss only above 2^52, never reached
        // by a pending-block count.
        #[allow(
            clippy::cast_precision_loss,
            reason = "pending-block count never nears 2^52"
        )]
        gauge!(live_metric_names::PENDING_BLOCKS).set(group.blocks.len() as f64);
        Ok(cursor)
    }

    /// Post the pending group if it is due.
    async fn flush_if_due(&mut self) -> Result<()> {
        let cfg = &self.cfg;
        if let Some(group) = self.pending.take_if(|g| g.due(cfg)) {
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
        self.sender.post_confirmed(batch, cursor).await?;
        self.spool.clear_through(batch.l2_block_end)
    }
}

/// Log a fatal single-block overflow by name before the error stops the
/// loop; every other pack error passes through.
fn log_pack_error(e: BatcherError) -> anyhow::Error {
    if let BatcherError::BlockTooLarge {
        block_number,
        bytes,
        ceiling,
    } = &e
    {
        tracing::error!(
            block = block_number,
            bytes,
            ceiling,
            "FATAL: one block alone exceeds the payload ceiling; the batcher cannot post it"
        );
    }
    e.into()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn cfg() -> FeedConfig {
        FeedConfig {
            blocks_per_batch: NonZeroUsize::new(3).unwrap(),
            compress: false,
            chain_id: 1,
            flush: Duration::from_secs(60),
            idle_flush: Duration::from_secs(3600),
            target_payload_bytes: NonZeroUsize::new(1000).unwrap(),
            skip_through_block: 0,
        }
    }

    fn empty_block(block_number: u64) -> ClosedBlock {
        ClosedBlock {
            block_number,
            l2_timestamp: 0,
            end_tx_idx: kardamom_types::BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l1_origin: 0,
            remote_epochs: Vec::new(),
            txs: Vec::new(),
        }
    }

    fn group(age: Duration, blocks: u64, has_traffic: bool) -> PendingGroup {
        PendingGroup {
            since: Instant::now().checked_sub(age).unwrap(),
            blocks: (1..=blocks).map(empty_block).collect(),
            has_traffic,
            raw_bytes: 0,
            cursor: BatchCursor::genesis(),
        }
    }

    #[test]
    fn a_group_with_traffic_waits_the_short_timer() {
        assert!(!group(Duration::from_secs(59), 1, true).due(&cfg()));
        assert!(group(Duration::from_secs(60), 1, true).due(&cfg()));
    }

    #[test]
    fn an_idle_group_waits_the_long_timer() {
        assert!(!group(Duration::from_secs(60), 1, false).due(&cfg()));
        assert!(!group(Duration::from_secs(3599), 1, false).due(&cfg()));
        assert!(group(Duration::from_secs(3600), 1, false).due(&cfg()));
    }

    #[test]
    fn a_full_group_is_due_at_once() {
        assert!(group(Duration::ZERO, 3, false).due(&cfg()));
        assert!(!group(Duration::ZERO, 2, true).due(&cfg()));
    }

    #[test]
    fn a_group_at_the_byte_target_is_due_at_once() {
        let mut g = group(Duration::ZERO, 1, true);
        g.raw_bytes = 999;
        assert!(!g.due(&cfg()));
        g.raw_bytes = 1000;
        assert!(g.due(&cfg()));
    }
}
