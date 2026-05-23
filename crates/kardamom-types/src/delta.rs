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

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockDelta {
    pub block_number: u64,
    pub accounts: Vec<AccountChange>,
    pub storage: Vec<StorageChange>,
    pub code: Vec<CodeEntry>,
    pub receipts: Vec<Receipt>,
}
