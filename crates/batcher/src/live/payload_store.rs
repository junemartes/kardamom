//! The block payload store as a resume source: the executors and the
//! validator keep every block they execute in the form the batcher
//! posts, and serve it as `kardamom_getBlockPayload(number)`.
//!
//! The sealer retains a bounded window of the canonical stream. A
//! batcher that was down for longer asks for a replay the sealer refuses
//! (`REPLAY_UNAVAILABLE`, with the oldest block it still holds). The
//! blocks between the cursor and that floor are not on L1 and not in the
//! spool, and the sealer cannot serve them. They are in the store: one
//! row per block on every executor and on the validator, pruned only
//! past the store's retention. This client reads the missing range from
//! the first endpoint that serves each block, checks every block's
//! canonical end index against its predecessor, and turns the frames
//! back into closed blocks the feed loop packs as if the sealer had
//! served them.

use std::ops::ControlFlow;

use alloy_primitives::Bytes;
use anyhow::{Context, Result, bail};
use kardamom_types::BPosition;
use kardamom_types::kar1::{BlockFrame, decode};
use serde::Deserialize;
use tracing::{info, warn};

use crate::batch::{ClosedBlock, RecordedTx};
use kardamom_types::TxEnvelope;

use super::cursor::BatchCursor;

/// A client of the query endpoints of the executors and the validator
/// (`http://host:port` each).
#[derive(Clone, Debug)]
pub(crate) struct PayloadStore {
    endpoints: Vec<String>,
    client: reqwest::Client,
}

/// The JSON-RPC result of one payload query, or its error.
#[derive(Debug, Deserialize)]
struct Response {
    #[serde(default)]
    result: Option<Bytes>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// The range recovered so far: the blocks, and the end index the next
/// block must reach.
struct Recovered {
    blocks: Vec<ClosedBlock>,
    prev_end: u64,
}

impl ClosedBlock {
    /// The closed block of a stored frame. The frame carries what the
    /// payload carries: the four envelope fields the batcher packs, and
    /// no inclusion deadline, which the sealer has already applied. The
    /// record position of a transaction is not in the payload either;
    /// the batcher never reads it after the block closes.
    fn from_frame(frame: BlockFrame) -> Result<Self> {
        let cursor = frame.cursor.with_context(|| {
            format!(
                "stored block {} carries no cursor (a version 4 payload)",
                frame.block_number
            )
        })?;
        let txs = frame
            .txs
            .into_iter()
            .map(|tx| RecordedTx {
                position: BPosition::ZERO,
                envelope: TxEnvelope {
                    correlation_id: tx.correlation_id,
                    raw_tx: tx.raw_tx,
                    sender: tx.sender,
                    tx_hash: tx.tx_hash,
                    max_inclusion_block: 0,
                },
            })
            .collect();
        Ok(Self {
            block_number: frame.block_number,
            l2_timestamp: frame.l2_timestamp,
            end_tx_idx: BPosition::from_index(cursor.end_tx_idx),
            l1_origin: cursor.l1_origin,
            remote_epochs: frame.remote_epochs,
            txs,
        })
    }
}

impl PayloadStore {
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

    /// The blocks from the cursor's next block through `through`, each
    /// checked against its predecessor: the numbers are contiguous, and
    /// each block's end index covers its records.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint serves a block of the range, or
    /// when a block does not continue its predecessor.
    pub(crate) async fn blocks(
        &self,
        cursor: BatchCursor,
        through: u64,
    ) -> Result<Vec<ClosedBlock>> {
        if through < cursor.next_block {
            bail!(
                "the sealer retains block {through} but the cursor is already at block {}",
                cursor.next_block
            );
        }
        let mut recovered = Recovered {
            blocks: Vec::new(),
            prev_end: cursor.next_index,
        };
        for number in cursor.next_block..=through {
            self.next_block(&mut recovered, number).await?;
        }
        info!(
            from = cursor.next_block,
            through,
            blocks = recovered.blocks.len(),
            "blocks recovered from the payload store"
        );
        Ok(recovered.blocks)
    }

    /// Fetch block `number`, check it against the range so far, and add
    /// it.
    async fn next_block(&self, recovered: &mut Recovered, number: u64) -> Result<()> {
        let block = self.block(number).await?;
        let records = u64::try_from(block.txs.len().saturating_add(block.remote_epochs.len()))
            .context("record count overflows u64")?;
        let end = block.end_tx_idx.as_index();
        if end < recovered.prev_end.saturating_add(records) {
            bail!(
                "stored block {number} ends at index {end}, but its predecessor ends at {} and \
                 the block holds {records} records",
                recovered.prev_end
            );
        }
        recovered.prev_end = end;
        recovered.blocks.push(block);
        Ok(())
    }

    /// Block `number` from the first endpoint that serves it. An endpoint
    /// that fails is skipped; an endpoint that holds no such block answers
    /// `null`. With no block served, the error names the last failure.
    async fn block(&self, number: u64) -> Result<ClosedBlock> {
        let mut last_failure: Option<anyhow::Error> = None;
        for endpoint in &self.endpoints {
            if let ControlFlow::Break(block) =
                self.try_endpoint(endpoint, number, &mut last_failure).await
            {
                return Ok(block);
            }
        }
        match last_failure {
            Some(e) => Err(e).with_context(|| format!("no payload source served block {number}")),
            None => bail!("no payload source holds block {number}"),
        }
    }

    /// One endpoint of the walk: `Break` carries the block; a failure
    /// lands in `last_failure`.
    async fn try_endpoint(
        &self,
        endpoint: &str,
        number: u64,
        last_failure: &mut Option<anyhow::Error>,
    ) -> ControlFlow<ClosedBlock> {
        match self.fetch_one(endpoint, number).await {
            Ok(Some(block)) => ControlFlow::Break(block),
            Ok(None) => ControlFlow::Continue(()),
            Err(e) => {
                warn!(endpoint, number, error = %format!("{e:#}"), "payload source failed");
                *last_failure = Some(e);
                ControlFlow::Continue(())
            }
        }
    }

    /// One `kardamom_getBlockPayload(number)` call to `endpoint`. The
    /// stored bytes are a one-block payload; the block must be the one
    /// asked for.
    async fn fetch_one(&self, endpoint: &str, number: u64) -> Result<Option<ClosedBlock>> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "kardamom_getBlockPayload",
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
            bail!("{endpoint} answered an error: {error}");
        }
        let Some(bytes) = response.result else {
            return Ok(None);
        };
        let payload =
            decode(&bytes).with_context(|| format!("decode block {number} from {endpoint}"))?;
        let [frame] = <[BlockFrame; 1]>::try_from(payload.blocks).map_err(|blocks| {
            anyhow::anyhow!(
                "{endpoint} served {} blocks for block {number}",
                blocks.len()
            )
        })?;
        if frame.block_number != number {
            bail!(
                "{endpoint} served block {} for block {number}",
                frame.block_number
            );
        }
        ClosedBlock::from_frame(frame).map(Some)
    }
}

#[cfg(test)]
#[path = "payload_store_tests.rs"]
pub(crate) mod tests;
