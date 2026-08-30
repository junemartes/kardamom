//! Errors raised by the engine actor and its helpers.

use alloc::string::String;

use alloy_primitives::B256;

use crate::exec_types::TxIndex;
use kardamom_types::BPosition;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("state backend error: {0}")]
    State(String),

    /// A proven divergence between a validator's re-execution and the
    /// sequencer's published output (write-set does not match BAL, or a
    /// receipt mismatch). This error is fatal and not retryable. The commit
    /// thread's must-deliver retry must not retry it. The error must
    /// propagate so the pipeline halts and the process stops.
    #[error("proven divergence: {0}")]
    Divergence(String),

    #[error("revm execution failure at tx {idx:?}: {detail}")]
    Execution { idx: TxIndex, detail: String },

    /// A stateless (guest-shaped) execution re-derived a tx record's
    /// identity from its raw bytes, and it contradicts the envelope. This
    /// means a forged or corrupt `sender` or `tx_hash`. The live pipeline
    /// never produces this error, because it trusts the proxy's fields.
    #[error("record identity mismatch: {0}")]
    RecordIdentity(String),

    /// The witness could not be tied to `pre_state_root`. Causes include a
    /// missing or undecodable proof node, a witness entry the trie refutes,
    /// or a post-root recompute that the node set cannot complete. This
    /// error aborts a stateless execution, either before the first EVM step
    /// (verification) or after it (recompute). Both cases fail closed, like
    /// the witness's own incompleteness errors.
    #[error("witness unanchored: {0}")]
    WitnessUnanchored(String),

    #[error("out-of-order tx_idx: got {got:?}, expected {expected:?}")]
    OutOfOrderTx { got: TxIndex, expected: TxIndex },

    #[error(
        "block boundary closes before observed end_tx_idx: end={end:?} last_seen={last_seen:?}"
    )]
    BoundaryMisaligned {
        end: BPosition,
        last_seen: BPosition,
    },

    #[error("tx_ordering subscription closed")]
    TxOrderingClosed,

    #[error(
        "cluster replay unavailable: need from index {from_index}, oldest retained index {oldest_index} (block {oldest_block}) — full resync required"
    )]
    ClusterReplayUnavailable {
        from_index: u64,
        oldest_index: u64,
        oldest_block: u64,
    },

    #[error("tx_data[{sequencer_id}] subscription closed")]
    TxDataClosed { sequencer_id: u8 },

    #[error("tx_deposits subscription closed")]
    DepositsClosed,

    #[error("tx_receipts publication closed")]
    TxReceiptsClosed,

    /// The tx_ordering reader pulled a [`kardamom_types::TxRef`], but the
    /// referenced envelope never appeared on tx_data within the join
    /// timeout. Either the tx_data publisher failed, or the sequencer
    /// published a ref to a position it never wrote. Both are upstream bugs.
    #[error(
        "join timeout: TxRef(sequencer_id={sequencer_id}, tx_data_position={tx_data_position:?}) not found within {timeout_ms} ms"
    )]
    JoinTimeout {
        sequencer_id: u8,
        tx_data_position: BPosition,
        timeout_ms: u64,
    },

    /// Mirror of [`Self::JoinTimeout`] for the deposit path. The
    /// tx_ordering reader pulled a [`kardamom_types::DepositRef`], but the
    /// referenced [`kardamom_types::Deposit`] never landed on `tx_deposits`
    /// within the join timeout. Either the DA watcher failed, or the
    /// sequencer republished a ref to a position the watcher never wrote.
    #[error(
        "deposit join timeout: source_hash={source_hash:?} deposit_position={deposit_position:?} not found within {timeout_ms} ms"
    )]
    DepositJoinTimeout {
        source_hash: B256,
        deposit_position: BPosition,
        timeout_ms: u64,
    },
}

/// Role-agnostic alias for the engine error. New engine and validator code
/// should use `EngineError`. `ExecutorError` stays for existing executor call
/// sites; both names refer to the same type.
pub type EngineError = ExecutorError;
