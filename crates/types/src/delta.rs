//! Block-write payload from executor to state writer.
//!
//! Carries all account, storage, and code mutations, plus receipts, from a
//! sealed block. The state writer commits them atomically.

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

/// A single code-hash-to-bytecode mapping in a block delta. This is its own
/// struct, not a `(B256, Bytes)` tuple as in the original plan. This lets the
/// rkyv `with` adapters apply cleanly.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct CodeEntry {
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
    #[rkyv(with = wire::BytesVec)]
    pub code: Bytes,
}

/// `tx_bal` wire frame. See
/// `docs/agents/bal-attribution-parallel-validation-spec.md`. It carries the
/// merged final-value write set, plus the EIP-7928 Block Access List
/// (canonical alloy RLP). The list carries per-slot `(tx_index, value)`
/// write lists and per-account storage reads.
///
/// This type has no version, by choice. It was once a V1/V2 enum. V1 (the
/// delta alone, without receipts) had only one producer: a legacy
/// writer-queue tee. The publisher thread replaced that producer, and
/// nothing else used V1. So the version tag added only a permanent match arm
/// for every consumer, and a risk for injection paths: the corrupt-BAL
/// drill silently stopped working when its hand-built frames kept the old
/// shape. The wire format can still change while the chain is at v0. Add
/// versioning back when there is a second live shape to carry.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BalFrame {
    /// The merged final-value write set for the block. Receipts are
    /// stripped: they dominate frame size, and large frames collapsed the
    /// validator's lapse window.
    pub delta: BlockDelta,
    /// RLP-encoded EIP-7928 block access list (attribution), quantized at
    /// `granularity`. This is empty when capture is disabled.
    pub bal_rlp: Vec<u8>,
    /// Attribution granularity. `1` means per-transaction. `K > 1` collapses
    /// chunks of `K` transactions.
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
