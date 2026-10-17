//! `TxStatus`: the record of the `tx_status` stream (id 1018), one step
//! of a transaction's path through the pipeline.
//!
//! No service of this release publishes or reads the stream. The format
//! registry (`formats.toml`, `aeron-stream-records`) keeps the layout
//! while a supported release still writes the stream, and the layout test
//! of this crate ties this type to the registry. The layout is frozen.

use alloy_primitives::{Address, B256};
use rkyv::{Archive, Deserialize, Serialize};

use crate::tx_error::TxErrorReason;
use crate::wire;

/// One status record: the transaction's hash and the stage it reached.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxStatus {
    /// keccak256 of the canonical RLP-encoded transaction.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    /// The stage, with the identity the publisher holds at that stage.
    pub stage: TxStage,
}

/// The stage a transaction reached. The order per transaction is fixed:
/// `Offered`, then `Sealed`, then `Executed`; or `Rejected` in place of
/// the later stages. A reader orders by stage, not by arrival.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxStage {
    /// The sequencer accepted the transaction into its order and offered
    /// it to the sealer.
    Offered {
        #[rkyv(with = wire::AddressBytes)]
        sender: Address,
        nonce: u64,
    },
    /// The sealer gave the transaction a canonical index.
    Sealed,
    /// An executor ran the transaction. `status` is the receipt status.
    Executed {
        #[rkyv(with = wire::AddressBytes)]
        sender: Address,
        nonce: u64,
        status: bool,
    },
    /// The sequencer refused the transaction. It is never ordered.
    Rejected {
        #[rkyv(with = wire::AddressBytes)]
        sender: Address,
        nonce: u64,
        reason: TxErrorReason,
    },
}
