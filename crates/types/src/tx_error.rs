//! `TxError`: a rejection signal the sequencer emits when it cannot
//! canonicalize an inbound transaction.
//!
//! This flows on the dedicated `tx_errors` Aeron channel (RAM-only, not
//! recorded). The ingress subscribes and releases parked `(sender, nonce)`
//! clients with a JSON-RPC error. This way they do not wait for a receipt
//! that will never arrive.

use alloy_primitives::Address;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wire;

/// A sequencer-emitted rejection for one submitted transaction.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxError {
    /// Sender that submitted the rejected transaction.
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    /// Nonce the client submitted. This is the rejected value, not the
    /// expected next nonce.
    pub nonce: u64,
    /// Why the sequencer rejected the transaction.
    pub reason: TxErrorReason,
}

/// Reasons the sequencer rejects an inbound transaction. Add a new variant
/// for each new rejection class.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxErrorReason {
    /// The sender's nonce is below the next expected nonce. The transaction
    /// is already canonical, or it replays an earlier one.
    DuplicatedTx { expected_nonce: u64 },
    /// The sequencer's overload protection shed this transaction. Either it
    /// was the furthest-future buffered nonce, evicted to make room, or it
    /// arrived too far past the sender's next expected nonce while the
    /// reorder buffer was full. The sequencer never sequences this
    /// transaction; the client must resubmit once its nonce is back within
    /// the window.
    Evicted { expected_nonce: u64 },
    /// The transaction waited on a nonce gap for longer than the
    /// sequencer's `tx_ttl`. The sequencer dropped it from its pending
    /// buffer. This is the explicit end of a transaction's lifetime. The
    /// client must resubmit it after the gap fills. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.3.
    Expired { expected_nonce: u64 },
    /// The sealer refused the offer because its own block number had passed
    /// the transaction's `max_inclusion_block`. The transaction is not
    /// ordered, and no copy of it can be ordered later: the deadline is a
    /// property of the signed submission, not of the replica that offered
    /// it. The client resubmits.
    ///
    /// This is not [`Self::Expired`], which is the sequencer's own
    /// nonce-gap `tx_ttl`. See `docs/agents/offer-inclusion-deadline-spec.md`.
    PastDeadline {
        max_inclusion_block: u64,
        at_block: u64,
    },
    /// The tip rate is above the fee cap, so the bid can never be paid.
    /// The transaction is invalid, not merely unpayable. The client signs
    /// again with a tip rate within the cap.
    FeeInvalid {
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    },
    /// The fee cap is under the base fee of the latest block the sequencer
    /// has seen, so the transaction cannot be included at that price. The
    /// client signs again with a higher cap.
    FeeTooLow { max_fee_per_gas: u128, base_fee: u128 },
    /// The sender's balance, as the sequencer sees it, cannot cover the
    /// worst case the transaction can be charged: the fee cap on every
    /// gas unit plus the value.
    InsufficientFunds { have: u128, want: u128 },
}
