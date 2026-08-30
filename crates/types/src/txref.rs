//! `TxRef` is the small canonical-orderer record carried on tx_ordering in
//! the split (tx_data + tx_ordering) architecture.
//!
//! TxOrdering carries `{ tx_hash, shard_id, tx_data_position }` records,
//! about 41 bytes each, plus the occasional [`BlockBoundaryStart`] from the
//! sealer. The full transaction bytes live on the per-shard tx_data
//! archive, at `tx_data_position`.
//!
//! ## Why `tx_hash` is on the ref
//!
//! Under the MDS (multi-destination shared-publisher) model, all P
//! sequencers per shard race to publish the same reference onto
//! tx_ordering when they see a transaction on tx_data. So tx_ordering
//! carries P duplicate references per transaction. Downstream consumers,
//! such as the executor and batcher, dedup on `tx_hash` to drop duplicates
//! in O(1). They use `(shard_id, tx_data_position)` to resolve a reference
//! back to its envelope, and `tx_hash` to dedup.
//!
//! ## Why `shard_id` (not `sequencer_id`)
//!
//! TxData is sharded by sender address (`hash(sender) % K`), not by
//! sequencer. The same sender's traffic always lands on the same channel A,
//! no matter which proxy validated it or which sequencer republished its
//! reference to B. Executors keep K parallel A-readers, keyed by
//! `shard_id`.

use alloy_primitives::B256;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire::B256Bytes;

/// Reference to a transaction stored on a sender-shard tx_data archive.
///
/// On the wire (tx_ordering) this is about 41 bytes, rkyv-packed. The
/// B-Archive position of the containing record sets the canonical L2
/// order. This type carries `(tx_hash, shard, A-position)`. A downstream
/// consumer dedups on `tx_hash`, then looks up the envelope at
/// `(shard_id, tx_data_position)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct TxRef {
    /// keccak256 of the canonical RLP-encoded transaction. Used for O(1)
    /// dedup of duplicate references from racing sequencers.
    #[rkyv(with = B256Bytes)]
    pub tx_hash: B256,
    /// Sender-shard index that identifies which tx_data archive holds the
    /// envelope (`0..K`).
    pub shard_id: u8,
    /// Aeron position on channel_A[shard_id] where the envelope starts.
    pub tx_data_position: BPosition,
    /// Aeron publisher `session_id` of the tx_data fragment. This tells
    /// apart concurrent ingress publishers on one shard. With
    /// active-active ingress, two publishers have independent term spaces,
    /// so `tx_data_position` `(term_id, term_offset)` can collide across
    /// sessions. The executor's join key,
    /// `(shard_id, tx_data_session_id, tx_data_position)`, stays unique. A
    /// single-publisher deployment just carries one session id throughout.
    pub tx_data_session_id: i32,
}

impl TxRef {
    pub const fn new(
        tx_hash: B256,
        shard_id: u8,
        tx_data_position: BPosition,
        tx_data_session_id: i32,
    ) -> Self {
        Self {
            tx_hash,
            shard_id,
            tx_data_position,
            tx_data_session_id,
        }
    }
}
