// stub: real impl in Task 2.
use alloy_primitives::{Address, B256};
use bytes::Bytes;

use crate::position::BPosition;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WireLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Receipt {
    pub tx_idx: BPosition,
    pub tx_hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<WireLog>,
    pub write_set_hash: B256,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CachedReceipt {
    pub sender: Address,
    pub nonce: u64,
    pub tx_hash: B256,
    pub receipt: Receipt,
}
