//! Per-tx execution receipt and the receipt-cache message.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire;

/// Lightweight log entry that mirrors `alloy_primitives::Log` but uses our
/// rkyv-friendly wire types. Topic count is variable per the EVM spec.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct WireLog {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    #[rkyv(with = wire::VecB256)]
    pub topics: Vec<B256>,
    #[rkyv(with = wire::BytesVec)]
    pub data: Bytes,
}

/// Per-tx execution receipt. Published on channel C by executor replicas.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Receipt {
    pub tx_idx: BPosition,
    /// Copied from `TxEnvelope.tx_hash` — never recomputed by the executor (D-Sh4).
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<WireLog>,
    #[rkyv(with = wire::B256Bytes)]
    pub write_set_hash: B256,
}

/// Receipt-cache message: pushed by the executor onto the receipt-cache
/// channel so consumers (proxy nonce cache, RPC frontends) can invalidate and
/// repopulate without round-tripping through libmdbx.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct CachedReceipt {
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    pub nonce: u64,
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    pub receipt: Receipt,
}
