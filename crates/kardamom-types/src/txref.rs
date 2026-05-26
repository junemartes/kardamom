//! `TxRef` — the tiny canonical-orderer record carried on channel B in the
//! split (channel A + channel B) architecture (D-Sh12, spec D11 / §2.3).
//!
//! Channel B carries `{ tx_hash, shard_id, position_a }` records (~41 B
//! per record) plus the occasional [`BlockBoundaryStart`] from the sealer.
//! The full transaction bytes live on the per-shard **channel A** archive
//! at `position_a`.
//!
//! ## Why `tx_hash` is on the ref
//!
//! Under the MDS (multi-destination shared-publisher) model, the **P
//! sequencers per shard** all race to publish the same ref onto channel B
//! when they observe a tx on channel A. Channel B therefore carries P
//! duplicate refs per tx. Downstream consumers (executor, batcher) dedup
//! on `tx_hash` to drop duplicates O(1). `(shard_id, position_a)` is what
//! they use to resolve the ref back to the envelope; `tx_hash` is what
//! they use to dedup.
//!
//! ## Why `shard_id` (not `sequencer_id`)
//!
//! Channel A is sharded by sender address (`hash(sender) % K`), not per
//! sequencer. The same sender's traffic always lands on the same channel
//! A regardless of which proxy validated it or which sequencer republished
//! its ref to B. Executors maintain K parallel A-readers keyed by
//! `shard_id`.

use alloy_primitives::B256;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire::B256Bytes;

/// Reference to a transaction stored on a sender-shard channel A archive.
///
/// On the wire (channel B): ~41 B (rkyv-packed). The canonical L2 ordering
/// is determined by the B-Archive position of the containing record; this
/// type carries `(tx_hash, shard, A-position)`. Downstream consumers:
/// dedup on `tx_hash`, look up the envelope at `(shard_id, position_a)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct TxRef {
    /// keccak256 of the canonical RLP-encoded transaction. Used for O(1)
    /// dedup of duplicate refs from racing sequencers.
    #[rkyv(with = B256Bytes)]
    pub tx_hash: B256,
    /// Sender-shard index identifying which channel A archive holds the
    /// envelope (`0..K`).
    pub shard_id: u8,
    /// Aeron position on channel_A[shard_id] where the envelope starts.
    pub position_a: BPosition,
}

impl TxRef {
    pub const fn new(tx_hash: B256, shard_id: u8, position_a: BPosition) -> Self {
        Self {
            tx_hash,
            shard_id,
            position_a,
        }
    }
}
