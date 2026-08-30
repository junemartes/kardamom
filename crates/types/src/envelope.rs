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
}
