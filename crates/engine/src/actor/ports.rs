//! Outbound ports of the executor actor: the tx_receipts publication and the
//! two state-writer seams (a durability signal and a hand-off queue).
//!
//! Trait objects have forwarding impls. A binary that picks a role-specific
//! wrapper at runtime (for example, the validator's attester tee) can name
//! `Box<dyn ...>` as that associated type in its
//! [`EngineWiring`](super::EngineWiring). The API does not force boxing;
//! the caller chooses it.

use kardamom_types::{BlockBoundary, BlockDelta, Receipt};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

/// Publication handle for tx_receipts.
pub trait TxReceiptsPublication: Send {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError>;

    /// Publish a batch of receipts. Where the transport supports it, this
    /// cuts the per-publish overhead.
    ///
    /// Returns `(published, error)`. The first `published` receipts are
    /// handed off. An error applies to the rest, so the caller's
    /// must-deliver retry resumes at the failed suffix.
    ///
    /// The default implementation publishes one receipt at a time. The
    /// validator's verifying sink relies on this to keep its exact
    /// per-receipt divergence behavior. The live transport instead packs
    /// the whole slice into one `Vec<Receipt>` wire frame: one encode and
    /// one blocking ack, instead of one ack per receipt. The per-receipt
    /// ack round trip was the commit thread's biggest cost.
    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        for (i, r) in receipts.iter().enumerate() {
            if let Err(e) = self.publish(CMessage::Receipt(r.clone())) {
                return (i, Some(e));
            }
        }
        (receipts.len(), None)
    }
}

/// Signal from the state writer: block N is durable in mdbx, and the
/// caller may swap to a snapshot at or after N.
pub trait StateWriterSignal: Send {
    /// Block until the state writer commits a block number at or after
    /// `await_at_least`. Returns the committed block number.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;

    /// Non-blocking probe for the highest durably-committed block right now.
    /// Returns 0 if nothing has committed yet.
    ///
    /// The pipelined commit's settle sweep uses this value. Completed
    /// commits settle at each boundary without blocking the exec thread.
    fn committed(&mut self) -> Result<u64, ExecutorError>;
}

/// Hand-off queue from the executor to the state writer. The state writer
/// reads these entries and applies the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

// Lets a binary pick a role-specific queue wrapper at runtime (for example,
// the validator's optional attester tee) by naming the boxed type in its
// wiring.
impl StateWriterQueue for Box<dyn StateWriterQueue> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        (**self).submit(block, delta)
    }
}

// Same reason for the receipts seam: the validator boxes its sink so the
// optional attester tee can wrap it at runtime.
impl TxReceiptsPublication for Box<dyn TxReceiptsPublication> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        (**self).publish(msg)
    }

    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        (**self).publish_receipts(receipts)
    }
}
