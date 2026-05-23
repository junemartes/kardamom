// stub: real impl in Task 2.
use crate::position::BPosition;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}
