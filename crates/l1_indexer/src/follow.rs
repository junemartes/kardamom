//! The follower: each tick indexes the finalized L1 blocks after the
//! cursor, a bounded range at a time.
//!
//! For a range `[from, to]` the order is fixed: the batches of the range
//! (event logs, then each batch's payload from the DA proxy), then the
//! epoch of each block, then the cursor. A crash before the cursor write
//! re-indexes the range; every write is idempotent, so nothing is lost or
//! doubled.
//!
//! Each block's parent hash is checked against the indexed block's hash
//! (through the cursor, so the check survives a restart). A break means
//! the L1 view changed under a finalized block, which finality forbids.
//! The follower halts on it (`l1_chain_break`), retries the same range
//! on every tick, and resumes by itself when the source serves a block
//! that descends from the indexed one. An L1 source that does not
//! answer halts it the same way (`l1_unreachable`).

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, Log};
use alloy_sol_types::SolEvent;
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_da_watcher::{L1Source, L1SourceError};
use kardamom_obs::halt;
use kardamom_types::epoch::derive_epoch;
use metrics::{counter, gauge};

use crate::metrics::{
    BATCHES_TOTAL, INDEXED_BLOCK, L1_FINALIZED, PAYLOAD_BYTES_TOTAL, TICK_TOTAL, gauge_value,
};
use crate::store::Store;
use crate::{BatchEntry, BlockId, Cursor, IndexerError};

/// What to follow, and how fast.
#[derive(Clone, Debug)]
pub struct FollowConfig {
    pub settlement: Address,
    pub lockbox: Address,
    /// The first L1 block to index on an empty archive: the block of the
    /// contract deploy, or later. `None` starts at the finalized block of
    /// the first tick.
    pub start_block: Option<u64>,
    pub poll_interval: Duration,
    /// The most blocks one tick indexes; bounds the log query and the
    /// time between cursor writes while catching up.
    pub blocks_per_tick: NonZeroU64,
}

/// The pieces a [`Follower`] is built from.
pub struct FollowerParts<S, P> {
    pub source: S,
    pub provider: P,
    pub da: DaProxy,
    pub store: Store,
    pub cfg: FollowConfig,
}

/// The follower.
pub struct Follower<S, P> {
    source: S,
    provider: P,
    da: DaProxy,
    store: Store,
    cfg: FollowConfig,
    cursor: Cursor,
}

/// What one tick did.
#[derive(Debug, PartialEq, Eq)]
pub enum Tick {
    /// Nothing new is finalized.
    Idle,
    /// The range up to `to` is indexed.
    Advanced { to: u64, batches: usize },
}

impl BatchEntry {
    fn from_log(log: &Log) -> Result<Self, IndexerError> {
        let ev = IKardamomL2Settlement::BatchPosted::decode_log(&log.inner)
            .map_err(|e| IndexerError::Provider(format!("decode BatchPosted: {e}")))?;
        let l1_block = log.block_number.ok_or_else(|| {
            IndexerError::Provider("BatchPosted log without a block number".into())
        })?;
        let l1_tx = log
            .transaction_hash
            .ok_or_else(|| IndexerError::Provider("BatchPosted log without a tx hash".into()))?;
        Ok(Self {
            index: ev.data.batchIndex,
            da_cert: ev.data.daCert.clone(),
            l2_block_start: ev.data.l2BlockStart,
            l2_block_end: ev.data.l2BlockEnd,
            records_commitment: ev.data.recordsCommitment,
            l1_block,
            l1_tx,
        })
    }
}

impl<S: L1Source, P: Provider> Follower<S, P> {
    /// Build a follower on an opened store; resume from its cursor.
    ///
    /// # Errors
    /// Returns an error when the cursor does not parse.
    pub fn open(parts: FollowerParts<S, P>) -> Result<Self, IndexerError> {
        let cursor = parts.store.cursor()?;
        Ok(Self {
            source: parts.source,
            provider: parts.provider,
            da: parts.da,
            store: parts.store,
            cfg: parts.cfg,
            cursor,
        })
    }

    /// The cursor as of the last tick.
    #[must_use]
    pub const fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// Tick forever at the poll interval. An error is logged and counted,
    /// and a chain break or an unreachable L1 raises the follower's halt;
    /// the next tick retries, and a good tick clears the halt.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.cfg.poll_interval);
        loop {
            self.step(&mut interval).await;
        }
    }

    async fn step(&mut self, interval: &mut tokio::time::Interval) {
        interval.tick().await;
        let outcome = self.tick().await;
        kardamom_obs::ready::mark_now(crate::metrics::LAST_TICK_UNIX_SECONDS);
        match outcome {
            Ok(Tick::Idle) => {
                counter!(TICK_TOTAL, "outcome" => "idle").increment(1);
                halt::clear();
            }
            Ok(Tick::Advanced { to, batches }) => {
                tracing::info!(to, batches, "indexed");
                counter!(TICK_TOTAL, "outcome" => "advanced").increment(1);
                halt::clear();
            }
            Err(error) => {
                tracing::error!(%error, "tick failed");
                counter!(TICK_TOTAL, "outcome" => "error").increment(1);
                if let Some(halt) = error.halt() {
                    halt::raise(halt);
                }
            }
        }
    }

    /// Index the next range of finalized blocks, if any.
    ///
    /// # Errors
    /// Returns an error when L1, the DA proxy, or the store fails, or
    /// when a block does not descend from the indexed one.
    pub async fn tick(&mut self) -> Result<Tick, IndexerError> {
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
        let batches = self.batches_in(from, to).await?;
        for entry in &batches {
            self.archive_batch(entry).await?;
        }
        for number in from..=to {
            self.archive_epoch(number).await?;
        }
        self.advance(batches.last().map(|b| b.index))?;
        Ok(Tick::Advanced {
            to,
            batches: batches.len(),
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

    async fn batches_in(&self, from: u64, to: u64) -> Result<Vec<BatchEntry>, IndexerError> {
        let filter = Filter::new()
            .address(self.cfg.settlement)
            .event_signature(IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH)
            .from_block(from)
            .to_block(to);
        let logs = self.provider.get_logs(&filter).await.map_err(|e| {
            IndexerError::Provider(format!("get_logs BatchPosted [{from}, {to}]: {e}"))
        })?;
        logs.iter().map(BatchEntry::from_log).collect()
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

    async fn archive_epoch(&mut self, number: u64) -> Result<(), IndexerError> {
        let (hash, parent) = self.source.block_ids(number).await?;
        self.check_chain(number, parent)?;
        let logs = self
            .source
            .lockbox_logs(self.cfg.lockbox, number, number)
            .await?;
        let epoch = derive_epoch(number, hash, &logs).map_err(|e| IndexerError::Derive {
            number,
            error: e.to_string(),
        })?;
        self.store.put_epoch(&epoch)?;
        self.cursor.l1_block = Some(BlockId { number, hash });
        Ok(())
    }

    fn check_chain(&self, number: u64, parent: B256) -> Result<(), IndexerError> {
        match self.cursor.l1_block {
            Some(BlockId { hash: expected, .. }) if parent != expected => {
                Err(IndexerError::ChainBreak {
                    number,
                    expected,
                    parent,
                })
            }
            _ => Ok(()),
        }
    }

    fn advance(&mut self, last_batch: Option<u64>) -> Result<(), IndexerError> {
        self.cursor.last_batch = last_batch.or(self.cursor.last_batch);
        self.store.set_cursor(&self.cursor)?;
        if let Some(block) = self.cursor.l1_block {
            gauge!(INDEXED_BLOCK).set(gauge_value(block.number));
        }
        if let Some(index) = self.cursor.last_batch {
            gauge!(crate::metrics::LAST_BATCH).set(gauge_value(index));
        }
        Ok(())
    }
}
