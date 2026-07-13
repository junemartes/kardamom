//! Executor-side BAL publication.
//!
//! Wraps the executor's [`StateWriterQueue`] so that, at each block close, the
//! per-block [`BlockDelta`] (the Block Access List the validator cross-checks
//! against) is published on `tx_bal` before being forwarded to the real state
//! writer. The publish is fire-and-forget on the **isolated publication
//! runtime** (`PubHandle::publish_best_effort` just hands the encoded frame to
//! that runtime's Aeron thread), so the executor's exec/commit hot path is never
//! blocked on BAL delivery. A dropped BAL is harmless: the validator simply
//! leaves that block unverified (it never causes a false divergence).

use kardamom_engine::{ExecutorError, StateWriterQueue};
use kardamom_log::aeron_live::PubHandle;
use kardamom_types::{BlockBoundary, BlockDelta};

/// Tee each block's `BlockDelta` to `tx_bal`, then forward to the inner writer
/// queue (which commits the delta to libmdbx).
pub struct BalPublishingWriterQueue<Q: StateWriterQueue> {
    inner: Q,
    bal_pub: PubHandle,
}

impl<Q: StateWriterQueue> BalPublishingWriterQueue<Q> {
    pub fn new(inner: Q, bal_pub: PubHandle) -> Self {
        Self { inner, bal_pub }
    }
}

impl<Q: StateWriterQueue> StateWriterQueue for BalPublishingWriterQueue<Q> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        // Encode on the exec thread (one small encode per block), then fire-and-
        // forget to the publication runtime's Aeron thread — non-blocking.
        match kardamom_log::codec::encode(&delta) {
            Ok(bytes) => self.bal_pub.publish_best_effort(bytes),
            Err(e) => {
                tracing::warn!(block = block.block_number, error = %e, "BAL encode failed")
            }
        }
        self.inner.submit(block, delta)
    }
}
