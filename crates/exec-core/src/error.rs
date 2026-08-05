//! Errors raised by the engine actor and its helpers.

use alloc::string::String;

use alloy_primitives::B256;

use crate::exec_types::TxIndex;
use kardamom_types::BPosition;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("state backend error: {0}")]
    State(String),

    /// A PROVEN divergence between a validator's re-execution and the
    /// sequencer's published output (write-set != BAL, receipt mismatch).
    /// FATAL and non-retryable: unlike a transient transport error, the
    /// commit thread's must-deliver retry must NOT spin on this — it has to
    /// propagate so the pipeline halts and the process fail-stops.
    #[error("proven divergence: {0}")]
    Divergence(String),

    #[error("revm execution failure at tx {idx:?}: {detail}")]
    Execution { idx: TxIndex, detail: String },

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

    /// The tx_ordering reader pulled a [`kardamom_types::TxRef`] but the
    /// referenced envelope never appeared on its tx_data within the
    /// configured join timeout. Either the tx_data publisher fell over,
    /// or the sequencer published a ref pointing at a position it never
    /// actually wrote. Both are upstream bugs.
    #[error(
        "join timeout: TxRef(sequencer_id={sequencer_id}, tx_data_position={tx_data_position:?}) not found within {timeout_ms} ms"
    )]
    JoinTimeout {
        sequencer_id: u8,
        tx_data_position: BPosition,
        timeout_ms: u64,
    },

    /// Mirror of [`Self::JoinTimeout`] for the deposit path: the
    /// tx_ordering reader pulled a [`kardamom_types::DepositRef`] but the
    /// referenced [`kardamom_types::Deposit`] never landed on `tx_deposits` within
    /// the configured join timeout. Either the DA watcher fell over or a
    /// sequencer republished a ref pointing at a position the watcher
    /// never wrote.
    #[error(
        "deposit join timeout: source_hash={source_hash:?} deposit_position={deposit_position:?} not found within {timeout_ms} ms"
    )]
    DepositJoinTimeout {
        source_hash: B256,
        deposit_position: BPosition,
        timeout_ms: u64,
    },
}

/// Role-agnostic alias for the engine error. New engine/validator code should
/// prefer `EngineError`; `ExecutorError` is retained for the existing executor
/// call sites and is the same type.
pub type EngineError = ExecutorError;
