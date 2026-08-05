//! Block-write payload from executor to state writer.
//!
//! Carries the full account / storage / code mutations + receipts produced by
//! a sealed block, so the S6 state writer can commit them atomically.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::receipt::Receipt;
use crate::wire;

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct AccountChange {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    pub nonce: u64,
    #[rkyv(with = wire::U256Bytes)]
    pub balance: U256,
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct StorageChange {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    #[rkyv(with = wire::B256Bytes)]
    pub key: B256,
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
}

/// A single code-hash → bytecode mapping in a block delta. Stored as its own
/// struct (rather than the `(B256, Bytes)` tuple in the original plan) so we
/// can apply the rkyv `with` adapters cleanly.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct CodeEntry {
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
    #[rkyv(with = wire::BytesVec)]
    pub code: Bytes,
}

/// `tx_bal` wire frame (spec:
/// docs/agents/bal-attribution-parallel-validation-spec.md): the merged
/// final-value write set plus the EIP-7928 Block Access List (canonical
/// alloy RLP) carrying per-slot `(tx_index, value)` write lists and
/// per-account storage reads.
///
/// UNVERSIONED by choice: this was a V1/V2 enum, but V1 (the receipts-free
/// delta alone) had exactly one producer — a legacy writer-queue tee that
/// the publisher thread superseded and that nothing wired anymore — so the
/// discriminant only bought a permanent match arm on every consumer and an
/// injection-path footgun (the S7 corrupt-BAL drill silently stopped
/// drilling when its hand-rolled frames kept the pre-wrapper shape). While
/// the chain is v0 the wire is free to change; add versioning back when
/// there is a second live shape to carry.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BalFrame {
    /// The merged final-value write set for the block (receipts stripped:
    /// they dominate frame size and fat frames collapsed the validator's
    /// lapse window, #113).
    pub delta: BlockDelta,
    /// RLP-encoded EIP-7928 block access list (attribution), quantized at
    /// `granularity`. Empty when capture is disabled.
    pub bal_rlp: Vec<u8>,
    /// Attribution granularity: 1 = per-tx; K > 1 collapses K-tx chunks.
    pub granularity: u16,
}

impl BalFrame {
    /// The merged final-value section.
    pub fn delta(&self) -> &BlockDelta {
        &self.delta
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockDelta {
    pub block_number: u64,
    pub accounts: Vec<AccountChange>,
    pub storage: Vec<StorageChange>,
    pub code: Vec<CodeEntry>,
    pub receipts: Vec<Receipt>,
}
