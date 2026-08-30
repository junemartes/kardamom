//! Per-tx execution receipt.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize, with::Map};

use crate::position::BPosition;
use crate::wire;

/// EIP-2718 type byte for a legacy (untyped, RLP-list) transaction.
pub const TX_TYPE_LEGACY: u8 = 0x00;
/// EIP-2718 type byte for an L1-originated deposit. This is the OP-stack value.
pub const TX_TYPE_DEPOSIT: u8 = 0x7E;

/// The EIP-2718 type byte of a raw encoded transaction. For a typed
/// envelope, the leading byte is the type (`0x00..=0x7f`). Any byte above
/// that range is the first byte of a legacy RLP list.
pub fn tx_type_of(raw_tx: &[u8]) -> u8 {
    match raw_tx.first() {
        Some(&b) if b <= 0x7f => b,
        _ => TX_TYPE_LEGACY,
    }
}

/// A light log entry. It mirrors `alloy_primitives::Log`, but uses this
/// crate's rkyv-friendly wire types. Topic count varies, per the EVM spec.
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

/// Per-transaction execution receipt. Executor replicas publish this on
/// tx_receipts.
///
/// Carries every field the ingress needs to answer
/// `eth_getTransactionReceipt` without a join against the state DB. The
/// ingress keeps an in-memory index, `tx_hash → Receipt` and
/// `(sender, nonce) → Receipt`, from the tx_receipts subscription. So the
/// JSON-RPC handler reads straight from RAM.
///
/// This struct does not carry `block_hash` in v0. The slim `BlockBoundary`
/// has no state commitment, so no meaningful hash exists yet. The ingress
/// returns `null` for `block_hash` to RPC clients; JSON-RPC allows this.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Receipt {
    pub tx_idx: BPosition,
    /// EIP-2718 transaction type: [`TX_TYPE_LEGACY`] for an ordinary signed
    /// L2 transaction (or the envelope's type byte for a typed one), and
    /// [`TX_TYPE_DEPOSIT`] for an L1-originated deposit.
    ///
    /// This field matters beyond RPC fidelity. A deposit carries no L2
    /// nonce; the `nonce` field below is a filler `0`. So a consumer that
    /// reasons about nonces must branch on this field, not on `nonce == 0`.
    /// Before this field existed, a deposit and a genuine nonce-0
    /// transaction looked the same on the wire. This forced the sequencer's
    /// publish-confirmation ledger to ignore all nonce-0 receipts. That left
    /// a one-transaction sender's nonce-0 reference unconfirmable. It was
    /// re-offered on every confirm timeout, forever.
    pub tx_type: u8,
    /// Copied from `TxEnvelope.tx_hash`. The executor never recomputes it.
    #[rkyv(with = wire::B256Bytes)]
    pub tx_hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<WireLog>,
    #[rkyv(with = wire::B256Bytes)]
    pub write_set_hash: B256,

    // ---- RPC enrichment ---------------------------------------------------
    /// Sender-recovered nonce. This is the transaction's nonce, not the
    /// post-state nonce.
    pub nonce: u64,
    /// Sender address. Copied straight from the inbound envelope.
    #[rkyv(with = wire::AddressBytes)]
    pub from: Address,
    /// Recipient address. `None` for contract-creation transactions.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub to: Option<Address>,
    /// Address of a newly deployed contract. This is `Some` only when `to`
    /// is `None`.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub contract_address: Option<Address>,
    /// Effective gas price the transaction paid. In v0 this comes from
    /// `TxEnv.gas_price`: the legacy or 1559-derived value chosen when the
    /// env is built.
    pub effective_gas_price: u128,
    /// Block that included this transaction.
    pub block_number: u64,
    /// Zero-based index within the block. This differs from `tx_idx`'s BPosition.
    pub transaction_index: u64,
    /// Running sum of `gas_used` for all transactions in the block, up to
    /// and including this one.
    pub cumulative_gas_used: u64,
}

impl Receipt {
    /// Returns true if this receipt marks a canonical transaction that
    /// execution skipped as deterministically invalid, instead of applying
    /// it. Derivation is total: an invalid record must never halt the
    /// chain. A halt would also block recovery replay on the same record,
    /// forever.
    ///
    /// The marker is `status == false && gas_used == 0`. Real execution
    /// cannot produce this: every executed transaction, whether it
    /// succeeds, reverts, or halts, charges at least the intrinsic gas.
    ///
    /// Invariant: a consumer of the receipts stream must treat a skip as
    /// "this transaction did not happen". A skip consumes no nonce (the
    /// sequencer's receipt floors must ignore it), and it writes no state
    /// (its `write_set_hash` is the empty-set hash).
    pub fn is_invalid_skip(&self) -> bool {
        !self.status && self.gas_used == 0
    }

    /// Returns true if this receipt is for an L1-originated deposit. A
    /// deposit consumes no L2 nonce; see [`Receipt::tx_type`].
    pub fn is_deposit(&self) -> bool {
        self.tx_type == TX_TYPE_DEPOSIT
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
        // A revert, or any executed transaction, charges at least intrinsic gas.
        r.gas_used = 21_000;
        assert!(!r.is_invalid_skip());
        // A zero-gas success does not exist either. The marker still
        // requires status=false.
        r.gas_used = 0;
        r.status = true;
        assert!(!r.is_invalid_skip());
    }
}
