//! TxError: a rejection signal the sequencer emits when it cannot
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
/// for each new rejection class. v0 ships only `DuplicatedTx`. Future
/// variants may cover failed signature reverification, or malformed
/// envelopes that slip past the proxy.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxErrorReason {
    /// The sender's nonce is below the next expected nonce. The transaction
    /// is already canonical, or it replays an earlier one.
    DuplicatedTx { expected_nonce: u64 },
    /// The sequencer's overload protection shed this transaction. Either it
    /// was the furthest-future buffered nonce, evicted to make room, or it
    /// arrived too far past the sender's next expected nonce while the
    /// reorder buffer was full. The sequencer will never sequence it. The
    /// client must resubmit once its nonce is back within the window. This
    /// drop used to be silent: the parked submit waited for a receipt that
    /// could never arrive. That was the cause of the permanent-nonce-gap
    /// wedge.
    Evicted { expected_nonce: u64 },
}
