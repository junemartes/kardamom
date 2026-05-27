//! Errors raised by the executor actor and its helpers.

use alloy_primitives::B256;

use crate::exec_types::TxIndex;
use kardamom_types::BPosition;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("state backend error: {0}")]
    State(String),

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

    #[error("tx_data[{sequencer_id}] subscription closed")]
    TxDataClosed { sequencer_id: u8 },

    #[error("tx_deposits subscription closed")]
    DepositsClosed,

    #[error("tx_receipts publication closed")]
    TxReceiptsClosed,

    #[error("state-writer signal channel closed")]
    StateWriterClosed,

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
