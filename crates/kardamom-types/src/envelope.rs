// stub: real impl in Task 2.
use alloy_primitives::{Address, B256};
use bytes::Bytes;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TxEnvelope {
    pub correlation_id: u64,
    pub raw_tx: Bytes,
    pub sender: Address,
    pub tx_hash: B256,
}
