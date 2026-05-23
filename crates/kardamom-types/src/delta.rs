// stub: real impl in Task 2.
use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;

use crate::receipt::Receipt;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AccountChange {
    pub address: Address,
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageChange {
    pub address: Address,
    pub key: B256,
    pub value: U256,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockDelta {
    pub block_number: u64,
    pub accounts: Vec<AccountChange>,
    pub storage: Vec<StorageChange>,
    pub code: Vec<(B256, Bytes)>,
    pub receipts: Vec<Receipt>,
}
