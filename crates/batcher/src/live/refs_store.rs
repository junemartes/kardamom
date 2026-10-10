//! The block references as a resume source: the executors and the
//! validator keep, with every receipt, where the transaction's bytes are
//! on a `tx_data` archive, and serve a block's list of them as
//! `kardamom_getBlockRefs(number)`. An executor can also name where its
//! own archive holds the block's records (`exec_locator`).
//!
//! The sealer retains a bounded window of the canonical stream. A
//! batcher that was down for longer asks for a replay the sealer refuses
//! (`REPLAY_UNAVAILABLE`, with the oldest block it still holds). The
//! blocks between the cursor and that floor are not on L1 and not in the
//! spool, and the sealer cannot serve them. Their ordering is in every
//! state database and their bytes are on the archives; this client reads
//! the ordering, block by block, from the first endpoint that serves it,
//! and checks every block's canonical end against its predecessor. The
//! rebuild from the archives is [`super::rebuild`].

use std::ops::ControlFlow;

use alloy_primitives::B256;
use anyhow::{Context, Result, bail};
use kardamom_engine::reader::ArchiveLocator;
use serde::Deserialize;
use tracing::{info, warn};

use super::cursor::BatchCursor;

/// The references of one block, as the query endpoint serves them: the
/// block's boundary, and one entry per transaction in canonical order.
/// Deposits are not listed; a payload never carries them.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct BlockRefs {
    pub(crate) block_number: u64,
    /// The block's canonical end, as an index.
    pub(crate) end_tx_idx: u64,
    pub(crate) l1_origin: u64,
    pub(crate) l2_timestamp: u64,
    pub(crate) refs: Vec<BlockTxRef>,
    /// Where the archive of the answering executor holds the records of
    /// the block: a position at or before the first one. A validator
    /// answers without it. The rebuild from the executor archives replays
    /// from it first, and asks the executors for a record it still lacks.
    #[serde(default)]
    pub(crate) exec_locator: Option<BlockLocator>,
}

/// The `exec_locator` of a block, as an executor serves it with the
/// references: the archive, the session of the recording, and a raw
/// position at or before the block's first record.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct BlockLocator {
    pub(crate) archive_id: String,
    pub(crate) session_id: i32,
    pub(crate) position: i64,
}

impl From<&BlockLocator> for ArchiveLocator {
    fn from(at: &BlockLocator) -> Self {
        Self {
            archive_id: at.archive_id.clone(),
            session_id: at.session_id,
            position: at.position,
        }
    }
}

/// One transaction of a block: its hash, its canonical position, and
/// where its bytes are on the `tx_data` archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct BlockTxRef {
    pub(crate) tx_hash: B256,
    /// The canonical position, as an index.
    pub(crate) tx_idx: u64,
    pub(crate) shard_id: u8,
    pub(crate) session_id: i32,
    /// The archive position, as `BPosition::as_index` packs it.
    pub(crate) position: u64,
}

/// A client of the query endpoints of the executors and the validator
/// (`http://host:port` each).
#[derive(Clone, Debug)]
pub(crate) struct RefsStore {
    endpoints: Vec<String>,
    client: reqwest::Client,
}

/// The JSON-RPC result of one query, or its error.
#[derive(Debug, Deserialize)]
struct Response {
    #[serde(default)]
    result: Option<BlockRefs>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// The range read so far: the blocks, and the end index the next block
/// must reach.
struct Range {
    blocks: Vec<BlockRefs>,
    prev_end: u64,
}

impl RefsStore {
    pub(crate) fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            client: reqwest::Client::new(),
        }
    }

    /// No endpoint is configured: the store cannot help a refused replay.
    pub(crate) fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// The references of the blocks from the cursor's next block through
    /// `through`, each checked against its predecessor: the numbers are
    /// contiguous, and each block's end index covers its transactions.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint serves a block of the range, or
    /// when a block does not continue its predecessor.
    pub(crate) async fn blocks(&self, cursor: BatchCursor, through: u64) -> Result<Vec<BlockRefs>> {
        if through < cursor.next_block {
            bail!(
                "the sealer retains block {through} but the cursor is already at block {}",
                cursor.next_block
            );
        }
        let mut range = Range {
            blocks: Vec::new(),
            prev_end: cursor.next_index,
        };
        for number in cursor.next_block..=through {
            self.next_block(&mut range, number).await?;
        }
        info!(
            from = cursor.next_block,
            through,
            blocks = range.blocks.len(),
            "block references read from the state databases"
        );
        Ok(range.blocks)
    }

    /// Fetch block `number`, check it against the range so far, and add
    /// it.
    async fn next_block(&self, range: &mut Range, number: u64) -> Result<()> {
        let block = self.block(number).await?;
        let txs = u64::try_from(block.refs.len()).context("reference count overflows u64")?;
        if block.end_tx_idx < range.prev_end.saturating_add(txs) {
            bail!(
                "block {number} ends at index {}, but its predecessor ends at {} and the block \
                 holds {txs} transactions",
                block.end_tx_idx,
                range.prev_end
            );
        }
        range.prev_end = block.end_tx_idx;
        range.blocks.push(block);
        Ok(())
    }

    /// Block `number` from the first endpoint that serves it. An endpoint
    /// that fails is skipped; an endpoint that holds no such block answers
    /// `null`. With no block served, the error names the last failure.
    async fn block(&self, number: u64) -> Result<BlockRefs> {
        let mut last_failure: Option<anyhow::Error> = None;
        for endpoint in &self.endpoints {
            if let ControlFlow::Break(block) =
                self.try_endpoint(endpoint, number, &mut last_failure).await
            {
                return Ok(block);
            }
        }
        match last_failure {
            Some(e) => Err(e).with_context(|| format!("no query endpoint served block {number}")),
            None => bail!("no query endpoint holds block {number}"),
        }
    }

    /// One endpoint of the walk: `Break` carries the block; a failure
    /// lands in `last_failure`.
    async fn try_endpoint(
        &self,
        endpoint: &str,
        number: u64,
        last_failure: &mut Option<anyhow::Error>,
    ) -> ControlFlow<BlockRefs> {
        match self.fetch_one(endpoint, number).await {
            Ok(Some(block)) => ControlFlow::Break(block),
            Ok(None) => ControlFlow::Continue(()),
            Err(e) => {
                warn!(endpoint, number, error = %format!("{e:#}"), "query endpoint failed");
                *last_failure = Some(e);
                ControlFlow::Continue(())
            }
        }
    }

    /// One `kardamom_getBlockRefs(number)` call to `endpoint`. The block
    /// served must be the one asked for.
    async fn fetch_one(&self, endpoint: &str, number: u64) -> Result<Option<BlockRefs>> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "kardamom_getBlockRefs",
            "params": [number],
        });
        let response: Response = self
            .client
            .post(endpoint)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("query {endpoint}"))?
            .error_for_status()
            .with_context(|| format!("query {endpoint}"))?
            .json()
            .await
            .with_context(|| format!("parse the answer of {endpoint}"))?;
        if let Some(error) = response.error {
            bail!("{endpoint} answered an error for block {number}: {error}");
        }
        let Some(block) = response.result else {
            return Ok(None);
        };
        if block.block_number != number {
            bail!(
                "{endpoint} served block {} for block {number}",
                block.block_number
            );
        }
        Ok(Some(block))
    }
}

#[cfg(test)]
#[path = "refs_store_tests.rs"]
pub(crate) mod tests;
