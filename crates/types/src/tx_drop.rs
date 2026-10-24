//! `TxDrop`: the record of one transaction that the sequencer dropped.
//!
//! Each sequencer twin publishes its drops on the `tx_drops` stream of its
//! lane. The egress reads the stream of every lane and keeps the drop as
//! the status of the transaction hash. The record is small: the hash, the
//! sender, the nonce and one reason byte. The stream is RAM only and best
//! effort. A receipt of the hash always wins over a drop.

use alloy_primitives::{Address, B256};
use rkyv::{Archive, Deserialize, Serialize};

use crate::tx_error::TxErrorReason;
use crate::wire;

/// One dropped transaction.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxDrop {
    /// keccak256 of the canonical RLP-encoded transaction.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    /// The nonce of the dropped transaction.
    pub nonce: u64,
    pub reason: DropReason,
}

/// Why the sequencer dropped a transaction. Each reason has the error text
/// of geth, so Ethereum tools understand it. A variant goes only at the
/// end: the layout is frozen.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum DropReason {
    /// The nonce is below the next nonce of the sender.
    NonceTooLow,
    /// The balance does not cover the worst-case cost.
    InsufficientFunds,
    /// Another transaction holds the same sender and nonce, and this one
    /// cannot replace it.
    ReplacementUnderpriced,
    /// The parked list of the sender is full, or the chain refuses new
    /// transactions for a time.
    TxpoolFull,
    /// The fee cap is below the base fee.
    FeeCapBelowBaseFee,
    /// A parked transaction expired, or its inclusion deadline passed.
    Expired,
    /// The tip is above the fee cap.
    TipAboveFeeCap,
    /// The sealer refused the transaction reference that the sequencer
    /// offered without the bytes of its transaction. The status is
    /// provisional: the twin can offer the same transaction with its bytes,
    /// and then its receipt wins over the drop.
    SealerRefusedReference,
}

impl DropReason {
    /// The error text of geth for the reason.
    #[must_use]
    pub fn text(self) -> &'static str {
        match self {
            Self::NonceTooLow => "nonce too low",
            Self::InsufficientFunds => "insufficient funds for gas * price + value",
            Self::ReplacementUnderpriced => "replacement transaction underpriced",
            Self::TxpoolFull => "txpool is full",
            Self::FeeCapBelowBaseFee => "max fee per gas less than block base fee",
            Self::Expired => "transaction expired",
            Self::TipAboveFeeCap => "max priority fee per gas higher than max fee per gas",
            Self::SealerRefusedReference => "transaction dropped by the sealer, send it again",
        }
    }

    /// The name of the reason as a metric label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::NonceTooLow => "nonce_too_low",
            Self::InsufficientFunds => "insufficient_funds",
            Self::ReplacementUnderpriced => "replacement_underpriced",
            Self::TxpoolFull => "txpool_full",
            Self::FeeCapBelowBaseFee => "fee_cap_below_base_fee",
            Self::Expired => "expired",
            Self::TipAboveFeeCap => "tip_above_fee_cap",
            Self::SealerRefusedReference => "sealer_refused_reference",
        }
    }
}

impl TxErrorReason {
    /// The drop reason of a rejection. A rejection that names a nonce the
    /// chain or a held transaction already took is `NonceTooLow` or
    /// `ReplacementUnderpriced`. A refusal of the sealer for a halt or a lag
    /// is `TxpoolFull`, because the client can send again later.
    #[must_use]
    pub fn drop_reason(&self) -> DropReason {
        match self {
            Self::DuplicatedTx { .. } | Self::SealerNonceAhead { .. } => DropReason::NonceTooLow,
            Self::AlreadyKnown { .. } | Self::NonceInFlight { .. } => {
                DropReason::ReplacementUnderpriced
            }
            Self::Evicted { .. } | Self::DaLag { .. } | Self::RecordLag { .. } => {
                DropReason::TxpoolFull
            }
            Self::Expired { .. } | Self::PastDeadline { .. } => DropReason::Expired,
            Self::FeeInvalid { .. } => DropReason::TipAboveFeeCap,
            Self::FeeTooLow { .. } => DropReason::FeeCapBelowBaseFee,
            Self::InsufficientFunds { .. } => DropReason::InsufficientFunds,
        }
    }
}

#[cfg(test)]
#[path = "tx_drop_tests.rs"]
mod tests;
