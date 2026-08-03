//! Block boundary markers. State root is **not** carried.
//!
//! Both markers carry the block's **L1 origin** — the L1 block number whose
//! epoch this L2 block belongs to. It is what makes deposit derivation a pure
//! function of L1: a reconstructor reading only L1 + the DA payload can tell
//! which epoch's deposits belong at the front of which block. See
//! `docs/agents/l1-origin-deposit-derivation-spec.md`.
//!
//! The origin is assigned by the SEALER, which never reads L1 — it echoes the
//! value carried by an ordered origin-advancing record (see
//! `kardamom_cluster_adapter::wire::KIND_ORIGIN_RECORD`). Keeping L1 out of
//! the Raft state machine is what keeps it deterministic across replicas.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Block-boundary marker emitted by the sealer onto tx_ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// L1 block number this block's epoch corresponds to. `0` until the first
    /// origin-advancing record is ordered (and on chains that predate them).
    pub l1_origin: u64,
}

/// Block-boundary closeout emitted by executors onto tx_receipts once they have
/// finished executing through `end_tx_idx`. No `state_root_commitment` field.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// Carried through from [`BlockBoundaryStart::l1_origin`] so downstream
    /// consumers (and the DA payload) see the same origin the sealer stamped.
    pub l1_origin: u64,
}
