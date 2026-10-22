//! Transaction envelope. The proxy always sets `sender` and `tx_hash`.
//! Downstream code trusts both fields without a check.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wire;

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxEnvelope {
    pub correlation_id: u64,
    #[rkyv(with = wire::BytesVec)]
    pub raw_tx: Bytes,
    /// The proxy recovers this from the secp256k1 signature at decode time.
    /// This is the CFT trust boundary: every consumer treats this field as authoritative.
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    /// The proxy computes this as `keccak256(raw_tx)` during signature verification.
    /// Downstream code never recomputes it. It propagates unchanged into `Receipt.tx_hash`.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    /// The last block the sealer may order this transaction into. The proxy
    /// stamps it as `latest_block_number + inclusion_horizon_blocks`; the
    /// sealer rejects an offer once its own block number passes it.
    ///
    /// The deadline rides on the envelope, not on the `TxRef`, so every
    /// racing sequencer replica reads the same value from the one shared
    /// `tx_data` record and relays it unchanged. The racing offers stay
    /// byte-identical, which the sealer's first-wins dedup relies on.
    ///
    /// See `docs/agents/offer-inclusion-deadline-spec.md`.
    pub max_inclusion_block: u64,
}

impl TxEnvelope {
    /// The bits of `correlation_id` that carry the per-ingress sequence.
    const SEQ_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    /// A globally unique, opaque `correlation_id`: the top 16 bits are
    /// `ingress_id`, and the low 48 bits are `seq`.
    #[must_use]
    pub const fn pack_correlation_id(ingress_id: u16, seq: u64) -> u64 {
        ((ingress_id as u64) << 48) | (seq & Self::SEQ_MASK)
    }

    /// The id of the ingress that published an envelope with
    /// `correlation_id`.
    #[must_use]
    pub const fn ingress_id_of(correlation_id: u64) -> u16 {
        (correlation_id >> 48) as u16
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
