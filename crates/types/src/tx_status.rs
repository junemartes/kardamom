//! `TxStatus`: one step of a transaction's path through the pipeline.
//!
//! The status stream (`tx_status`) carries one small record per step. A
//! record names the transaction by hash and states the stage it reached.
//! The sequencer publishes `Offered` and `Rejected`. The ingress's egress
//! tap publishes `Sealed`. The notifier derives `Executed` from
//! `tx_receipts` and never puts it on the stream; the variant exists so
//! one type describes the whole lifecycle in the notifier's ring and in
//! its webhook outboxes.
//!
//! The record carries the identity the publisher holds. A relayed egress
//! frame names a transaction by hash only, so `Sealed` has no sender and
//! no nonce. A reader joins the stages of one transaction by hash.
//!
//! The stream is RAM only: a side channel, best effort, never on the hot
//! path. The receipt stream stays the truth.

use alloy_primitives::{Address, B256};
use rkyv::{Archive, Deserialize, Serialize};

use crate::receipt::Receipt;
use crate::tx_error::{TxError, TxErrorReason};
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

/// The stage without its payload: the key a reader deduplicates on, and
/// the word a client sees.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum TxStageKind {
    Offered,
    Sealed,
    Executed,
    Rejected,
}

impl TxStageKind {
    /// Every kind, in stage order.
    pub const ALL: [Self; 4] = [Self::Offered, Self::Sealed, Self::Executed, Self::Rejected];

    /// The lower-case word a client sees in the `stage` field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Offered => "offered",
            Self::Sealed => "sealed",
            Self::Executed => "executed",
            Self::Rejected => "rejected",
        }
    }

    /// One bit per kind, for a per-transaction "stages seen" set.
    #[must_use]
    pub const fn bit(self) -> u8 {
        match self {
            Self::Offered => 1,
            Self::Sealed => 2,
            Self::Executed => 4,
            Self::Rejected => 8,
        }
    }
}

impl TxStage {
    /// The stage without its payload.
    #[must_use]
    pub const fn kind(&self) -> TxStageKind {
        match self {
            Self::Offered { .. } => TxStageKind::Offered,
            Self::Sealed => TxStageKind::Sealed,
            Self::Executed { .. } => TxStageKind::Executed,
            Self::Rejected { .. } => TxStageKind::Rejected,
        }
    }

    /// The sender and nonce, when the publisher held them.
    #[must_use]
    pub const fn identity(&self) -> Option<(Address, u64)> {
        match self {
            Self::Offered { sender, nonce }
            | Self::Executed { sender, nonce, .. }
            | Self::Rejected { sender, nonce, .. } => Some((*sender, *nonce)),
            Self::Sealed => None,
        }
    }
}

impl TxStatus {
    /// The sequencer offered the transaction to the sealer.
    #[must_use]
    pub const fn offered(tx_hash: B256, sender: Address, nonce: u64) -> Self {
        Self {
            tx_hash,
            stage: TxStage::Offered { sender, nonce },
        }
    }

    /// The sealer relayed the transaction with a canonical index.
    #[must_use]
    pub const fn sealed(tx_hash: B256) -> Self {
        Self {
            tx_hash,
            stage: TxStage::Sealed,
        }
    }

    /// An executor ran the transaction: its receipt arrived.
    #[must_use]
    pub const fn executed(receipt: &Receipt) -> Self {
        Self {
            tx_hash: receipt.tx_hash,
            stage: TxStage::Executed {
                sender: receipt.from,
                nonce: receipt.nonce,
                status: receipt.status,
            },
        }
    }

    /// The sequencer rejected the transaction `tx_hash` with `error`.
    #[must_use]
    pub fn rejected(tx_hash: B256, error: &TxError) -> Self {
        Self {
            tx_hash,
            stage: TxStage::Rejected {
                sender: error.sender,
                nonce: error.nonce,
                reason: error.reason.clone(),
            },
        }
    }

    /// The sender and nonce, when the publisher held them.
    #[must_use]
    pub const fn identity(&self) -> Option<(Address, u64)> {
        self.stage.identity()
    }

    /// The stage without its payload.
    #[must_use]
    pub const fn kind(&self) -> TxStageKind {
        self.stage.kind()
    }
}
