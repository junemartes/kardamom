//! Per-tx execution receipt.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize, with::Map};

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

/// Per-tx execution receipt. Published on tx_receipts by executor replicas.
///
/// Carries every field the ingress needs to answer
/// `eth_getTransactionReceipt` without joining against the state DB. Ingress
/// maintains an in-memory `tx_hash → Receipt` (and `(sender, nonce) → Receipt`)
/// index off the tx_receipts subscription, so the JSON-RPC handler reads
/// straight from RAM.
///
/// `block_hash` is intentionally not carried in v0: the slim
/// `BlockBoundary` has no state commitment, so a meaningful hash is not
/// available. Ingress returns `null` for `block_hash` to RPC clients (JSON-RPC
/// permits this).
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Receipt {
    pub tx_idx: BPosition,
    /// Copied from `TxEnvelope.tx_hash` — never recomputed by the executor.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<WireLog>,
    #[rkyv(with = wire::B256Bytes)]
    pub write_set_hash: B256,

    // ---- RPC enrichment ---------------------------------------------------
    /// Sender-recovered nonce (the tx's nonce, not the post-state nonce).
    pub nonce: u64,
    /// Sender address — copied straight from the inbound envelope.
    #[rkyv(with = wire::AddressBytes)]
    pub from: Address,
    /// Recipient address; `None` for contract-creation txs.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub to: Option<Address>,
    /// Address of a newly deployed contract; `Some` iff `to` is `None`.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub contract_address: Option<Address>,
    /// Effective gas price the tx paid (v0: from `TxEnv.gas_price`, the legacy
    /// or 1559-derived value picked when building the env).
    pub effective_gas_price: u128,
    /// Block this tx was included in.
    pub block_number: u64,
    /// Zero-based index within the block (distinct from `tx_idx`'s BPosition).
    pub transaction_index: u64,
    /// Running sum of `gas_used` over all txs in the block up to and
    /// including this one.
    pub cumulative_gas_used: u64,
}

impl Receipt {
    /// Whether this receipt marks a DETERMINISTICALLY-INVALID canonical tx
    /// that execution SKIPPED rather than applied (#92: derivation is total —
    /// an invalid record must never halt the chain, and a halt would also
    /// wedge recovery replay on the same record forever).
    ///
    /// The marker is `status == false && gas_used == 0`: unreachable by real
    /// execution, because any executed tx — success, revert, or halt —
    /// charges at least intrinsic gas. INVARIANT: consumers of the receipts
    /// stream must treat a skip as "this tx did NOT happen" — in particular
    /// it consumed NO nonce (the sequencer's receipt floors must ignore it)
    /// and wrote NO state (its `write_set_hash` is the empty-set hash).
    pub fn is_invalid_skip(&self) -> bool {
        !self.status && self.gas_used == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_skip_marker_boundaries() {
        let mut r = Receipt {
            status: false,
            gas_used: 0,
            ..Receipt::default()
        };
        assert!(r.is_invalid_skip());
        // A revert (or any executed tx) charges at least intrinsic gas.
        r.gas_used = 21_000;
        assert!(!r.is_invalid_skip());
        // A zero-gas SUCCESS does not exist either, but the marker requires
        // status=false regardless.
        r.gas_used = 0;
        r.status = true;
        assert!(!r.is_invalid_skip());
    }
}
