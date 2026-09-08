//! Outbound ports of the executor actor: the `tx_receipts` publication and the
//! two state-writer seams (a durability signal and a hand-off queue).
//!
//! [`TxReceiptsPublication`] has a forwarding impl for its boxed form. A
//! binary that picks a role-specific wrapper at runtime (for example, the
//! validator's attester tee) can name `Box<dyn TxReceiptsPublication>` as
//! that associated type in its [`EngineWiring`](super::EngineWiring). The
//! API does not force boxing; the caller chooses it.

use kardamom_types::{BlockBoundary, BlockDelta, Receipt};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

/// Publication handle for `tx_receipts`.
pub trait TxReceiptsPublication: Send {
    /// Publish one message.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the transport rejects or cannot deliver the
    /// message (for example, the sink is not connected), or, for a
    /// verifying sink, when the message diverges from the expected value.
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
    /// the whole slice into one `Vec<Receipt>` wire frame, so a batch pays
    /// one encode and one blocking ack instead of one ack per receipt.
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
    ///
    /// # Errors
    ///
    /// Returns `Err` when the state writer stops before committing a
    /// qualifying block.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;

    /// Non-blocking probe for the highest durably-committed block right now.
    /// Returns 0 if nothing has committed yet.
    ///
    /// The pipelined commit's settle sweep uses this value. Completed
    /// commits settle at each boundary without blocking the exec thread.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the state writer is gone.
    fn committed(&mut self) -> Result<u64, ExecutorError>;
}

/// Hand-off queue from the executor to the state writer. The state writer
/// reads these entries and applies the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    /// Submit `block`'s delta to the writer.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the writer's channel is closed (the writer
    /// thread is gone).
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

// The validator boxes its receipts sink so the optional attester tee can
// wrap it at runtime.
impl TxReceiptsPublication for Box<dyn TxReceiptsPublication> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        (**self).publish(msg)
    }

    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        (**self).publish_receipts(receipts)
    }
}
