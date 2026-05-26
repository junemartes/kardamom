//! TxError: rejection signal emitted by the sequencer when an inbound tx
//! cannot be canonicalised.
//!
//! Flows on the dedicated `tx_errors` Aeron channel (RAM-only, not recorded).
//! Ingress subscribes and releases parked `(sender, nonce)` clients with a
//! JSON-RPC error so they don't wait for a receipt that will never arrive.

use alloy_primitives::Address;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wire;

/// Sequencer-emitted rejection for a single submitted tx.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxError {
    /// Sender that originally submitted the rejected tx.
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    /// Nonce the client submitted (which is the value the sequencer rejected
    /// — not the expected-next nonce).
    pub nonce: u64,
    /// Why the tx was rejected.
    pub reason: TxErrorReason,
}

/// Reasons the sequencer rejects an inbound tx. Extensible: add a new
/// variant per rejection class. v0 ships `DuplicatedTx`; future entries may
/// cover signature reverification failures, malformed envelopes that slipped
/// past the proxy, etc.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxErrorReason {
    /// The sender's nonce is below the next expected nonce — the tx has
    /// already been canonicalised (or is a replay of a prior one).
    DuplicatedTx { expected_nonce: u64 },
}
