//! Outbound ports of the executor actor: the tx_receipts publication and the
//! two state-writer seams (durability signal + hand-off queue). Trait objects
//! get forwarding impls so a binary can box role-specific wrappers (e.g. the
//! validator's attester tee) without monomorphising `Executor::run` twice.

use kardamom_types::{BlockBoundary, BlockDelta, Receipt};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

/// Publication handle for tx_receipts.
pub trait TxReceiptsPublication: Send {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError>;

    /// Publish a RUN of receipts, amortizing per-publish overhead where the
    /// transport supports it. Returns `(published, error)`: the first
    /// `published` receipts are handed off; an error applies to the rest, so
    /// the caller's must-deliver retry resumes at the failed suffix. The
    /// default loops singles — the validator's verifying sink keeps its
    /// exact per-receipt divergence semantics — while the live transport
    /// packs the whole slice into ONE `Vec<Receipt>` wire frame: one encode
    /// and one blocking ack instead of one per receipt (the per-receipt ack
    /// round trip was the executor commit thread's dominant cost).
    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        for (i, r) in receipts.iter().enumerate() {
            if let Err(e) = self.publish(CMessage::Receipt(r.clone())) {
                return (i, Some(e));
            }
        }
        (receipts.len(), None)
    }
}

/// Signal from the state writer (S6): "block N is durable in mdbx; you may
/// swap to a snapshot >= N."
pub trait StateWriterSignal: Send {
    /// Block until the state writer reports a block number >= `await_at_least`
    /// has been committed. Returns the committed block number.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;

    /// Non-blocking probe: the highest durably-committed block right now
    /// (0 if nothing has committed yet). Drives the pipelined commit's
    /// settle sweep — completed commits settle opportunistically at each
    /// boundary without ever parking the exec thread.
    fn committed(&mut self) -> Result<u64, ExecutorError>;
}

/// Hand-off queue from executor → state writer. The state writer (S6)
/// consumes these to apply the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

// Lets a binary pick between role-specific queue wrappers at runtime (e.g. the
// validator's optional attester tee) without monomorphising `Executor::run`
// twice.
impl StateWriterQueue for Box<dyn StateWriterQueue> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        (**self).submit(block, delta)
    }
}

// Same reason on the receipts seam: the validator boxes its sink so the
// optional attester tee can wrap it at runtime.
impl TxReceiptsPublication for Box<dyn TxReceiptsPublication> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        (**self).publish(msg)
    }

    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        (**self).publish_receipts(receipts)
    }
}
