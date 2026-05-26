//! Block boundary markers. State root is **not** carried.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Block-boundary marker emitted by the sealer onto tx_ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}

/// Block-boundary closeout emitted by executors onto tx_receipts once they have
/// finished executing through `end_tx_idx`. No `state_root_commitment` field
///.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}
