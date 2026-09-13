//! The streaming L1 sender.

use std::ops::ControlFlow;
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::Provider;
use anyhow::{Context, Result, bail};
use metrics::{counter, gauge};
use tracing::{info, warn};

use crate::batcher::{PostedBatch, metric_names};
use crate::da_store::FsBlobStore;
use crate::l1::post_batch;

use super::cursor::{BatchCursor, L1Truth, read_l1_truth};
use super::live_metric_names;

/// A streaming L1 sender. It posts one packed group at a time, strictly
/// serialized by the contract's CAS check, and persists the cursor only
/// after a confirmed post.
pub struct LiveSender<P> {
    provider: P,
    settlement: Address,
    da_store: FsBlobStore,
    prev_index: u64,
    max_retries: u32,
    cursor_path: PathBuf,
}

impl<P: Provider> LiveSender<P> {
    pub fn new(
        provider: P,
        settlement: Address,
        da_store: FsBlobStore,
        prev_index: u64,
        max_retries: u32,
        cursor_path: PathBuf,
    ) -> Self {
        Self {
            provider,
            settlement,
            da_store,
            prev_index,
            max_retries,
            cursor_path,
        }
    }

    /// Post `batch` and persist `cursor` once the post is confirmed. Retry
    /// transient failures with bounded backoff. A failure that reconciles
    /// on-chain as this batch having landed (for example, a duplicate send
    /// after a receipt timeout, or a CAS revert of the duplicate) counts as
    /// success. Any foreign advance of `lastBatchIndex` is fatal. The
    /// batcher is single-instance, and the CAS check exists to make that
    /// race loud.
    ///
    /// # Errors
    /// Returns an error when the post fails after `max_retries` attempts,
    /// when a foreign writer advances `lastBatchIndex` past this batch, or
    /// when persisting the cursor after a confirmed post fails.
    pub async fn post_confirmed(
        &mut self,
        batch: &PostedBatch,
        mut cursor: BatchCursor,
    ) -> Result<()> {
        cursor.last_batch_index = self.next_index()?;
        let mut attempt: u32 = 0;
        loop {
            match self.attempt_post(batch, attempt).await? {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(a) => attempt = a,
            }
        }
        cursor
            .store(&self.cursor_path)
            .context("persist cursor after confirmed post")?;
        self.record_post_metrics(batch);
        Ok(())
    }

    /// One `post_confirmed` retry-loop iteration: try the post, and
    /// either confirm it (the send landed, or reconciliation found it
    /// already landed — either way `self.prev_index` is current when
    /// this returns `Break`) or back off and report the next attempt
    /// count to retry with.
    ///
    /// # Errors
    /// Returns an error when reconciliation itself fails, or when
    /// `attempt` (after this failure) exceeds `self.max_retries`.
    async fn attempt_post(
        &mut self,
        batch: &PostedBatch,
        attempt: u32,
    ) -> Result<ControlFlow<(), u32>> {
        let e = match post_batch(
            &self.provider,
            self.settlement,
            self.prev_index,
            batch,
            &self.da_store,
        )
        .await
        {
            Ok(next) => {
                self.prev_index = next;
                return Ok(ControlFlow::Break(()));
            }
            Err(e) => e,
        };
        if self.reconcile_after_error(batch, &e).await? {
            return Ok(ControlFlow::Break(()));
        }
        // Bounded by `max_retries` below; saturate rather than wrap so
        // an implausibly long retry run still compares as "too many",
        // not silently back to zero.
        let attempt = attempt.saturating_add(1);
        if attempt > self.max_retries {
            return Err(e).with_context(|| {
                format!(
                    "post batch (prev_index {}) after {attempt} attempts",
                    self.prev_index
                )
            });
        }
        counter!(live_metric_names::L1_POST_RETRIES).increment(1);
        let backoff = Duration::from_secs(1 << attempt.min(4));
        warn!(
            attempt,
            backoff_s = backoff.as_secs(),
            error = %format!("{e:#}"),
            "L1 post failed; retrying"
        );
        tokio::time::sleep(backoff).await;
        Ok(ControlFlow::Continue(attempt))
    }

    /// Ask the chain whether a send failure's transaction actually landed.
    /// A receipt timeout and a `StaleBatchIndex` revert of a duplicate send
    /// both look like local errors. Returns `Ok(true)` when it landed as
    /// ours (the caller should stop retrying), `Ok(false)` when it did not
    /// land (a genuine transient failure, so the caller retries).
    async fn reconcile_after_error(
        &mut self,
        batch: &PostedBatch,
        e: &crate::error::BatcherError,
    ) -> Result<bool> {
        let truth = read_l1_truth(&self.provider, self.settlement).await;
        let next_index = self.next_index()?;
        match truth {
            Ok(t) if t.last_batch_index == next_index => {
                return self.reconcile_matched_index(t, next_index, batch, e);
            }
            Ok(t) if t.last_batch_index > next_index => bail!(
                "lastBatchIndex jumped from {} to {} — a second batcher is writing; \
                 refusing to continue (send error was: {e})",
                self.prev_index,
                t.last_batch_index,
            ),
            Ok(_) => {} // not landed: a genuine transient failure
            Err(re) => warn!(error = %format!("{re:#}"), "reconcile read failed"),
        }
        Ok(false)
    }

    /// Handle the reconcile case where L1's `lastBatchIndex` now equals
    /// what this sender expected to post next: the failed send may have
    /// landed as ours. Returns `Ok(true)` when it did.
    fn reconcile_matched_index(
        &mut self,
        t: L1Truth,
        next_index: u64,
        batch: &PostedBatch,
        e: &crate::error::BatcherError,
    ) -> Result<bool> {
        if t.covered_through_block != batch.l2_block_end {
            bail!(
                "lastBatchIndex advanced to {} covering block {} but our batch \
                 ends at {} — a second batcher is writing; refusing to continue \
                 (send error was: {e})",
                t.last_batch_index,
                t.covered_through_block,
                batch.l2_block_end,
            );
        }
        info!(
            batch_index = t.last_batch_index,
            "post reconciled on-chain as ours after send error"
        );
        self.prev_index = next_index;
        Ok(true)
    }

    /// `self.prev_index + 1`, the batch index this sender's next post must
    /// land at. `prev_index` comes from the L1 CAS counter; erroring
    /// instead of wrapping keeps a would-be-impossible overflow from
    /// corrupting the cursor or matching a stale on-chain read by
    /// coincidence.
    fn next_index(&self) -> Result<u64> {
        self.prev_index
            .checked_add(1)
            .context("prev_index overflowed u64")
    }

    /// Record the metrics and log line for a confirmed post.
    fn record_post_metrics(&self, batch: &PostedBatch) {
        counter!(metric_names::BATCHES_POSTED).increment(1);
        counter!(metric_names::BLOBS_POSTED).increment(batch.blobs.len() as u64);
        // Metric value; f64 precision loss only above 2^52, never
        // reached by an L2 block number.
        #[allow(
            clippy::cast_precision_loss,
            reason = "metric value; never nears 2^52 for an L2 block number"
        )]
        gauge!(live_metric_names::LAST_POSTED_BLOCK).set(batch.l2_block_end as f64);
        // Metric value; f64 precision loss only above 2^52, never
        // reached by a batch index.
        #[allow(
            clippy::cast_precision_loss,
            reason = "metric value; never nears 2^52 for a batch index"
        )]
        gauge!(live_metric_names::LAST_BATCH_INDEX).set(self.prev_index as f64);
        info!(
            batch_index = self.prev_index,
            l2_block_start = batch.l2_block_start,
            l2_block_end = batch.l2_block_end,
            blobs = batch.blobs.len(),
            "batch confirmed on L1"
        );
    }
}
