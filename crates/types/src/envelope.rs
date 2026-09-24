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
