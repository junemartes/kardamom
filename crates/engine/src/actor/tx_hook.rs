//! Per-transaction hooks: [`TxHook`] runs on the exec thread when each tx
//! record arrives, and again when its result exists. A role names its hook
//! type as [`ExecPorts::TxHook`](super::wiring::ExecPorts::TxHook). A pair
//! `(A, B)` runs `A`, then `B`, so hooks stack with no boxing.

use kardamom_types::{BPosition, Receipt, TxEnvelope};

use crate::delta::WriteSet;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

/// One tx record, as a [`TxHook`] sees it.
pub struct TxContext<'a> {
    /// The block the record belongs to.
    pub block: u64,
    /// The record's absolute canonical index.
    pub tx_idx: TxIndex,
    pub position: BPosition,
    pub envelope: &'a TxEnvelope,
}

/// The result of one tx. The receipt has not streamed yet.
#[derive(Clone, Copy)]
pub enum TxOutcome<'a> {
    Applied {
        receipt: &'a Receipt,
        /// The tx's own writes. `Some` on the streaming path, before they
        /// fold into the block's delta. `None` on the whole-block path: a
        /// strategy returns only the block's merged delta.
        write_set: Option<&'a WriteSet>,
    },
    /// The streaming path failed to execute the tx. A whole-block
    /// strategy fails the whole block, so no tx gets this outcome there.
    Failed(&'a ExecutorError),
}

/// A role-specific hook around each tx record, in canonical order.
///
/// [`Self::before`] runs at arrival, before the streaming or whole-block
/// branch. So a record that `before` rejects cannot execute now, and
/// cannot hide in the block buffer. [`Self::after`] runs when the tx's
/// result exists, before its receipt streams: at once on the streaming
/// path, at the boundary on the whole-block path.
///
/// Both run on the exec thread, the executor's hottest path. They must be
/// cheap. Anything with network latency belongs on a background task.
pub trait TxHook: Send {
    /// # Errors
    ///
    /// Returns `Err` to reject the record. This is fail-stop: the engine
    /// halts before the record executes.
    fn before(&mut self, _tx: &TxContext<'_>) -> Result<(), ExecutorError> {
        Ok(())
    }

    /// # Errors
    ///
    /// Returns `Err` to reject the result. This is fail-stop: the engine
    /// halts before the receipt streams and before the block commits. For
    /// [`TxOutcome::Failed`], the engine halts with the execution error and
    /// ignores this return value.
    fn after(&mut self, _tx: &TxContext<'_>, _outcome: TxOutcome<'_>) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// `None` wires no hook: every call is a no-op. `Some` forwards.
impl<H: TxHook> TxHook for Option<H> {
    fn before(&mut self, tx: &TxContext<'_>) -> Result<(), ExecutorError> {
        self.as_mut().map_or(Ok(()), |hook| hook.before(tx))
    }

    fn after(&mut self, tx: &TxContext<'_>, outcome: TxOutcome<'_>) -> Result<(), ExecutorError> {
        self.as_mut().map_or(Ok(()), |hook| hook.after(tx, outcome))
    }
}

/// `A`, then `B`. `B` does not run when `A` rejects.
impl<A: TxHook, B: TxHook> TxHook for (A, B) {
    fn before(&mut self, tx: &TxContext<'_>) -> Result<(), ExecutorError> {
        self.0.before(tx)?;
        self.1.before(tx)
    }

    fn after(&mut self, tx: &TxContext<'_>, outcome: TxOutcome<'_>) -> Result<(), ExecutorError> {
        self.0.after(tx, outcome)?;
        self.1.after(tx, outcome)
    }
}

/// The no-op [`TxHook`], for roles that wire no tx hook. This exists so
/// such a wiring still has a concrete type to name.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTxHook;

impl TxHook for NoTxHook {}

/// Re-derive every tx record's identity on arrival: check that
/// `tx_hash == keccak256(raw_tx)`, and that `sender` matches the
/// signature's recovered signer. This is the same check as
/// [`crate::stateless::verify_record_identity`], which the zk guest runs.
/// On a mismatch, abort the pipeline with [`ExecutorError::RecordIdentity`].
///
/// The stream carries both fields as proxy claims. A role that does not
/// wire this hook executes whatever identity the proxy asserted.
///
/// The validator wires this hook unconditionally and treats a mismatch as
/// an integrity halt. The executor does not: with the validator checking,
/// a forged envelope cannot commit unnoticed. So sequencer-side rejection
/// is defense-in-depth, priced at one ecrecover per transaction on the hot
/// path.
///
/// Deposit records are out of scope. Their identity (`source_hash`) stays
/// a trusted input until the witness is anchored on L1.
#[derive(Debug, Clone, Copy, Default)]
pub struct VerifyRecordIdentity;

impl TxHook for VerifyRecordIdentity {
    fn before(&mut self, tx: &TxContext<'_>) -> Result<(), ExecutorError> {
        crate::stateless::verify_record_identity(tx.envelope).inspect_err(|e| {
            tracing::error!(block = tx.block, position = ?tx.position, tx_idx = ?tx.tx_idx, error = ?e, "exec ERROR: record identity forged");
        })
    }
}
