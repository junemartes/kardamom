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
    fn submit(&mut self, block: BlockBoundary, mut delta: BlockDelta) -> Result<(), ExecutorError> {
        // The BAL frame carries STATE MUTATIONS ONLY — never the block's
        // receipts (#109 put them inside the BlockDelta for the writer):
        // receipts (with their logs) dominate the delta's byte size, and the
        // tx_bal term buffer is BYTE-bounded, so fat frames collapse the
        // validator's lapse window — observed live as bal_missing growing
        // ~+60 per validator-lapse window, 3/3 on PR #113 CI, while the
        // pre-#109 frames (empty receipts vec) held the <=5 tolerance. The
        // receipts are taken out for the encode and restored for the writer
        // — zero copies of the state maps. This also keeps the frame
        // byte-identical to the validator's own (receipts-free) delta.
        let receipts = std::mem::take(&mut delta.receipts);
        // Encode on the exec thread (one small encode per block), then fire-and-
        // forget to the publication runtime's Aeron thread — non-blocking.
        match kardamom_log::codec::encode(&delta) {
            Ok(bytes) => self.bal_pub.publish_best_effort(bytes),
            Err(e) => {
                tracing::warn!(block = block.block_number, error = %e, "BAL encode failed")
            }
        }
        delta.receipts = receipts;
        self.inner.submit(block, delta)
    }
}
