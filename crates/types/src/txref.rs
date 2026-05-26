//! `TxRef` — the tiny canonical-orderer record carried on tx_ordering in the
//! split (tx_data + tx_ordering) architecture.
//!
//! TxOrdering carries `{ tx_hash, shard_id, tx_data_position }` records (~41 B
//! per record) plus the occasional [`BlockBoundaryStart`] from the sealer.
//! The full transaction bytes live on the per-shard **tx_data** archive
//! at `tx_data_position`.
//!
//! ## Why `tx_hash` is on the ref
//!
//! Under the MDS (multi-destination shared-publisher) model, the **P
//! sequencers per shard** all race to publish the same ref onto tx_ordering
//! when they observe a tx on tx_data. TxOrdering therefore carries P
//! duplicate refs per tx. Downstream consumers (executor, batcher) dedup
//! on `tx_hash` to drop duplicates O(1). `(shard_id, tx_data_position)` is what
//! they use to resolve the ref back to the envelope; `tx_hash` is what
//! they use to dedup.
//!
//! ## Why `shard_id` (not `sequencer_id`)
//!
//! TxData is sharded by sender address (`hash(sender) % K`), not per
//! sequencer. The same sender's traffic always lands on the same channel
//! A regardless of which proxy validated it or which sequencer republished
//! its ref to B. Executors maintain K parallel A-readers keyed by
//! `shard_id`.

use alloy_primitives::B256;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire::B256Bytes;

/// Reference to a transaction stored on a sender-shard tx_data archive.
///
/// On the wire (tx_ordering): ~41 B (rkyv-packed). The canonical L2 ordering
/// is determined by the B-Archive position of the containing record; this
/// type carries `(tx_hash, shard, A-position)`. Downstream consumers:
/// dedup on `tx_hash`, look up the envelope at `(shard_id, tx_data_position)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct TxRef {
    /// keccak256 of the canonical RLP-encoded transaction. Used for O(1)
    /// dedup of duplicate refs from racing sequencers.
    #[rkyv(with = B256Bytes)]
    pub tx_hash: B256,
    /// Sender-shard index identifying which tx_data archive holds the
    /// envelope (`0..K`).
    pub shard_id: u8,
    /// Aeron position on channel_A[shard_id] where the envelope starts.
    pub tx_data_position: BPosition,
}

impl TxRef {
    pub const fn new(tx_hash: B256, shard_id: u8, tx_data_position: BPosition) -> Self {
        Self {
            tx_hash,
            shard_id,
            tx_data_position,
        }
    }
}
