//! Block-write payload from executor to state writer.
//!
//! Carries the full account / storage / code mutations + receipts produced by
//! a sealed block, so the S6 state writer can commit them atomically.

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

/// Versioned `tx_bal` wire frame (spec:
/// docs/agents/bal-attribution-parallel-validation-spec.md).
///
/// `V1` is exactly the pre-attribution payload (receipts-stripped
/// [`BlockDelta`]); `V2` adds the EIP-7928 Block Access List — canonical
/// alloy RLP bytes — carrying per-slot `(tx_index, value)` write lists and
/// per-account storage reads. The merged `delta` section is unchanged so
/// V1-only consumers (prefetch, the write-set-hash cross-check) read it
/// identically. `granularity` = 1 for per-tx attribution; K > 1 means
/// BalIndex was quantized to ceil(idx/K) chunks (the size-degradation
/// ladder).
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum BalFrame {
    V1(BlockDelta),
    V2 {
        delta: BlockDelta,
        bal_rlp: Vec<u8>,
        granularity: u16,
    },
}

impl BalFrame {
    /// The merged final-value section, whichever version.
    pub fn delta(&self) -> &BlockDelta {
        match self {
            BalFrame::V1(d) => d,
            BalFrame::V2 { delta, .. } => delta,
        }
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
