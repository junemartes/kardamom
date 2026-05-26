//! Errors raised by the executor actor and its helpers.

use crate::types::TxIndex;
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

    #[error("channel-B subscription closed")]
    ChannelBClosed,

    #[error("channel-A[{sequencer_id}] subscription closed")]
    ChannelAClosed { sequencer_id: u8 },

    #[error("channel-C publication closed")]
    ChannelCClosed,

    #[error("state-writer signal channel closed")]
    StateWriterClosed,

    /// The channel-B reader pulled a [`kardamom_types::TxRef`] but the
    /// referenced envelope never appeared on its channel A within the
    /// configured join timeout. Either the channel-A publisher fell over,
    /// or the sequencer published a ref pointing at a position it never
    /// actually wrote. Both are upstream bugs.
    #[error(
        "join timeout: TxRef(sequencer_id={sequencer_id}, position_a={position_a:?}) not found within {timeout_ms} ms"
    )]
    JoinTimeout {
        sequencer_id: u8,
        position_a: BPosition,
        timeout_ms: u64,
    },
}
