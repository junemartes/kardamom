//! Tx envelope. `sender` and `tx_hash` are *always* populated by the proxy
//!. Downstream code trusts both fields unconditionally.

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
    /// Recovered by the proxy from the secp256k1 signature at decode time.
    /// CFT trust boundary: every downstream consumer treats this as authoritative.
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    /// `keccak256(raw_tx)` computed by the proxy alongside sig verification.
    /// Never recomputed downstream; propagates unchanged into `Receipt.tx_hash`.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
}
