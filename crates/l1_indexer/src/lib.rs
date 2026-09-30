//! The inbox indexer: an archive of what the chain needs from its L1 inbox.
//!
//! The indexer follows the finalized L1 through an [`L1Source`] (the light
//! client on a production deployment) and keeps, for as long as the
//! operator says:
//!
//! - every batch the settlement contract posted (`BatchPosted`): its
//!   descriptor, and its blob bytes, fetched from the beacon API while the
//!   sidecar is retained and checked against the versioned hashes L1
//!   committed to before they are stored;
//! - every finalized L1 block's epoch record, derived through the same
//!   [`derive_epoch`] the da-watcher and the validator use.
//!
//! It serves them over JSON-RPC on the private network. The consumers: the
//! rebuild (`kardamom-reconstruct`, through [`client::HttpBlobSource`],
//! which implements the batcher's [`BlobSource`] trait), the validator's
//! epoch check, and the batcher's resume. Nothing here is a source of
//! truth: everything is re-derivable from L1, and every consumer checks a
//! commitment. Losing the archive costs a re-index from the start block,
//! not the chain.
//!
//! [`L1Source`]: kardamom_da_watcher::L1Source
//! [`derive_epoch`]: kardamom_types::epoch::derive_epoch
//! [`BlobSource`]: kardamom_batcher::da_store::BlobSource

pub mod api;
pub mod beacon;
pub mod client;
pub mod follow;
pub mod metrics;
pub mod store;

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// One posted batch, as the indexer keeps it: the `BatchPosted` event's
/// fields plus where on L1 it was posted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchEntry {
    pub index: u64,
    /// KZG versioned hashes of the batch's blobs, in blob order.
    pub versioned_hashes: Vec<B256>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
    pub records_commitment: B256,
    /// The L1 block that carried the `postBatch` transaction.
    pub l1_block: u64,
    pub l1_tx: B256,
}

/// A finalized L1 block: its number and its hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockId {
    pub number: u64,
    pub hash: B256,
}

/// The indexer's progress, as [`store::Store`] persists it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// The last finalized L1 block whose batches and epoch are indexed.
    /// Its hash is what the next block's parent hash must equal.
    pub l1_block: Option<BlockId>,
    /// The highest batch index indexed.
    pub last_batch: Option<u64>,
}

/// Errors of the indexer.
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("L1 source: {0}")]
    Source(#[from] kardamom_da_watcher::L1SourceError),
    #[error("L1 provider: {0}")]
    Provider(String),
    #[error("beacon API: {0}")]
    Beacon(String),
    #[error("slot {slot} carries no blob with versioned hash {versioned_hash}")]
    BlobMissing { slot: u64, versioned_hash: B256 },
    #[error(
        "L1 block {number} does not descend from the indexed block: parent {parent}, expected {expected}"
    )]
    ChainBreak {
        number: u64,
        expected: B256,
        parent: B256,
    },
    #[error("store: {0}")]
    Store(String),
    #[error("epoch derivation at L1 block {number}: {error}")]
    Derive { number: u64, error: String },
    #[error("API: {0}")]
    Api(String),
    #[error("arithmetic overflow: {0}")]
    Overflow(&'static str),
    #[error("batcher: {0}")]
    Batcher(#[from] kardamom_batcher::error::BatcherError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
