//! Block boundary markers. These do not carry the state root.
//!
//! Both markers carry the block's L1 origin. This is the L1 block number for
//! the epoch of this L2 block. This makes deposit derivation a pure function
//! of L1: a reconstructor that reads only L1 and the DA payload can find
//! which epoch's deposits go at the front of each block. See
//! `docs/agents/l1-origin-deposit-derivation-spec.md`.
//!
//! The sealer assigns the origin. The sealer never reads L1 itself. It copies
//! the value from an ordered origin-advancing record (see
//! `kardamom_cluster_adapter::wire::KIND_ORIGIN_RECORD`). Keeping L1 out of
//! the Raft state machine keeps the state machine deterministic across replicas.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Block-boundary marker emitted by the sealer onto tx_ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// L1 block number for this block's epoch. This is `0` until the first
    /// origin-advancing record is ordered, and on older chains from before such records.
    pub l1_origin: u64,
}

/// Block-boundary closeout. Executors emit this onto tx_receipts after they
/// finish executing through `end_tx_idx`. This struct has no `state_root_commitment` field.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// This value comes from [`BlockBoundaryStart::l1_origin`]. Downstream
    /// consumers and the DA payload see the same origin the sealer set.
    pub l1_origin: u64,
}
