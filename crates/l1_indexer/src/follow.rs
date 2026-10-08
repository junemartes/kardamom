//! The follower: each tick reads the finalized L1 blocks after the
//! cursor, a bounded range at a time, archives them, and publishes one
//! record per block on the `l1_blocks` stream.
//!
//! For a range `[from, to]` the order is fixed:
//!
//! 1. the finalized tip (one read);
//! 2. the header of every block of the range (one batch request);
//! 3. the chain check: each header names the one before it as its
//!    parent, and the first names the cursor's block;
//! 4. the light client anchor, where a light client runs: the last header
//!    of a range that ends at the finalized tip is the light client's
//!    finalized header for that number;
//! 5. the logs of the settlement and the lockbox (one query with both
//!    addresses per chunk of `max_log_range` blocks);
//! 6. the archive: each batch's payload from the DA proxy, the batches,
//!    the epochs and the records;
//! 7. the records on the stream, in block order;
//! 8. the cursor.
//!
//! A crash or a failure before the cursor write reads the range again;
//! every write is idempotent, and the consumers drop the copies of a
//! record, so nothing is lost or doubled.
//!
//! Every read goes through the [`L1Source`] set, so two sources must
//! agree on it. A break of the chain means the L1 view changed under a
//! finalized block, which finality forbids: the follower halts
//! (`l1_chain_break`) and reads the range again every slot.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_batcher::da::DaProxy;
use kardamom_da_watcher::{L1Source, L1SourceError};
use metrics::{counter, gauge};

use crate::metrics::{
    BATCHES_TOTAL, INDEXED_BLOCK, L1_FINALIZED, L1_READS_TOTAL, PAYLOAD_BYTES_TOTAL,
    PUBLISHED_BLOCK, gauge_value,
};
use crate::schedule::FinalitySchedule;
use crate::sink::BlockSink;
use crate::store::Store;
use crate::{BatchEntry, BlockId, Cursor, IndexerError, L1Block};

mod range;
mod run;

/// What to follow, and how fast.
#[derive(Clone, Debug)]
pub struct FollowConfig {
    pub settlement: Address,
    pub lockbox: Address,
    /// The first L1 block to index on an empty archive: the block of the
    /// contract deploy, or later. `None` starts at the finalized block of
    /// the first tick.
    pub start_block: Option<u64>,
    /// One slot: the read cadence while the tip does not move, and the
    /// whole cadence on a chain without a schedule.
    pub poll_interval: Duration,
    /// The most blocks one tick indexes; bounds the header batch and the
    /// time between cursor writes while catching up.
    pub blocks_per_tick: NonZeroU64,
    /// The most blocks one log query spans. A provider caps the span of
    /// one query (Alchemy's free plan: 10 blocks).
    pub max_log_range: NonZeroU64,
    /// The beacon chain's finality schedule. `None` reads at the fixed
    /// poll interval.
    pub schedule: Option<FinalitySchedule>,
}

/// The pieces a [`Follower`] is built from.
pub struct FollowerParts<S, K> {
    pub source: S,
    pub sink: K,
    pub da: DaProxy,
    pub store: Store,
    pub cfg: FollowConfig,
}

/// The follower.
pub struct Follower<S, K> {
    source: S,
    sink: K,
    da: DaProxy,
    store: Store,
    cfg: FollowConfig,
    cursor: Cursor,
    /// The epoch boundary the follower waits for, in Unix seconds, once a
    /// range reached the finalized tip.
    next_step: Option<u64>,
}

/// What one tick did.
#[derive(Debug, PartialEq, Eq)]
pub enum Tick {
    /// Nothing new is finalized.
    Idle,
    /// The range up to `to` is indexed and published. `caught_up` says
    /// the range ended at the finalized tip.
    Advanced {
        to: u64,
        batches: usize,
        caught_up: bool,
    },
}

/// The kind of one L1 read, as the reads counter labels it.
#[derive(Debug, Clone, Copy)]
enum Read {
    Tip,
    Headers,
    Logs,
    LightClient,
}

impl Read {
    fn count(self) {
        let read = match self {
            Self::Tip => "tip",
            Self::Headers => "headers",
            Self::Logs => "logs",
            Self::LightClient => "light_client",
        };
        counter!(L1_READS_TOTAL, "read" => read).increment(1);
    }
}

impl<S: L1Source, K: BlockSink> Follower<S, K> {
    /// Build a follower on an opened store; resume from its cursor.
    ///
    /// # Errors
    /// Returns an error when the cursor does not parse.
    pub fn open(parts: FollowerParts<S, K>) -> Result<Self, IndexerError> {
        let cursor = parts.store.cursor()?;
        Ok(Self {
            source: parts.source,
            sink: parts.sink,
            da: parts.da,
            store: parts.store,
            cfg: parts.cfg,
            cursor,
            next_step: None,
        })
    }

    /// The cursor as of the last tick.
    #[must_use]
    pub const fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// Index and publish the next range of finalized blocks, if any.
    ///
    /// # Errors
    /// Returns an error when L1, the DA proxy, the store, or the stream
    /// fails, when a block does not descend from the indexed one, or when
    /// the range's last header is not the light client's.
    pub async fn tick(&mut self) -> Result<Tick, IndexerError> {
        Read::Tip.count();
        let finalized = match self.source.finalized_block_number().await {
            Err(L1SourceError::NotFinalized) => return Ok(Tick::Idle),
            other => other?,
        };
        gauge!(L1_FINALIZED).set(gauge_value(finalized));
        let from = self.next_block(finalized)?;
        if from > finalized {
            return Ok(Tick::Idle);
        }
        let span = self.cfg.blocks_per_tick.get() - 1;
        let to = finalized.min(
            from.checked_add(span)
                .ok_or(IndexerError::Overflow("range end"))?,
        );
        let blocks = self.read_range(from, to, to == finalized).await?;
        let batches = self.archive(&blocks).await?;
        blocks.iter().try_for_each(|block| self.publish(block))?;
        self.advance(&blocks)?;
        Ok(Tick::Advanced {
            to,
            batches,
            caught_up: to == finalized,
        })
    }

    fn next_block(&self, finalized: u64) -> Result<u64, IndexerError> {
        self.cursor
            .l1_block
            .map_or(Ok(self.cfg.start_block.unwrap_or(finalized)), |b| {
                b.number
                    .checked_add(1)
                    .ok_or(IndexerError::Overflow("next block"))
            })
    }

    /// Archive a range: each batch with its payload, then each block's
    /// epoch and record. Returns how many batches the range holds.
    async fn archive(&self, blocks: &[L1Block]) -> Result<usize, IndexerError> {
        let batches: Vec<&BatchEntry> = blocks.iter().flat_map(|b| &b.batches).collect();
        for entry in &batches {
            self.archive_batch(entry).await?;
        }
        blocks.iter().try_for_each(|block| {
            self.store.put_epoch(&block.epoch)?;
            self.store.put_block(block)
        })?;
        Ok(batches.len())
    }

    async fn archive_batch(&self, entry: &BatchEntry) -> Result<(), IndexerError> {
        let payload = self.da.get(&entry.da_cert).await?;
        self.store.put_payload(&entry.da_cert, &payload)?;
        self.store.put_batch(entry)?;
        counter!(BATCHES_TOTAL).increment(1);
        counter!(PAYLOAD_BYTES_TOTAL).increment(payload.len() as u64);
        tracing::info!(
            index = entry.index,
            l1_block = entry.l1_block,
            payload_bytes = payload.len(),
            "batch archived"
        );
        Ok(())
    }

    fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
        self.sink.publish(block)?;
        gauge!(PUBLISHED_BLOCK).set(gauge_value(block.number));
        Ok(())
    }

    /// Write the cursor after the last block of a range.
    fn advance(&mut self, blocks: &[L1Block]) -> Result<(), IndexerError> {
        let last_batch = blocks
            .iter()
            .flat_map(|b| &b.batches)
            .map(|b| b.index)
            .next_back();
        let mut cursor = self.cursor;
        cursor.last_batch = last_batch.or(cursor.last_batch);
        cursor.l1_block = blocks
            .last()
            .map(|b| BlockId {
                number: b.number,
                hash: b.hash,
            })
            .or(self.cursor.l1_block);
        self.store.set_cursor(&cursor)?;
        self.cursor = cursor;
        if let Some(block) = self.cursor.l1_block {
            gauge!(INDEXED_BLOCK).set(gauge_value(block.number));
        }
        if let Some(index) = self.cursor.last_batch {
            gauge!(crate::metrics::LAST_BATCH).set(gauge_value(index));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "follow_tests.rs"]
mod tests;
