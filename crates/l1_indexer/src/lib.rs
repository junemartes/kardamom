//! The L1 follower: the one service that reads L1 data, and the archive of
//! what the chain needs from its L1 inbox.
//!
//! The follower reads the finalized L1 through an [`L1Source`] set, once
//! per finality step, and publishes one [`L1Block`] record for each
//! finalized block on the `l1_blocks` stream. The da-watcher and the
//! batcher read the stream instead of L1. It keeps, for as long as the
//! operator says:
//!
//! - every batch the settlement contract posted (`BatchPosted`): its
//!   descriptor, and its payload, fetched from the EigenDA proxy by the
//!   certificate L1 committed to while EigenDA retains it (14 days); the
//!   proxy checks the certificate and the bytes against it;
//! - every finalized L1 block's epoch record, derived through the same
//!   [`derive_epoch`] the da-watcher and the validator use.
//!
//! It serves them over JSON-RPC on the private network. The consumers: the
//! rebuild (`kardamom-reconstruct`) and the batcher's resume, through the
//! batcher's `IndexerClient`, and the validator's epoch check. Nothing
//! here is a source of truth: everything is re-derivable from L1 and the
//! DA layer within its retention. Losing the archive costs a re-index
//! from the start block, not the chain.
//!
//! [`L1Source`]: kardamom_da_watcher::L1Source
//! [`derive_epoch`]: kardamom_types::epoch::derive_epoch

pub mod api;
pub mod follow;
pub mod metrics;
pub mod schedule;
pub mod sink;
pub mod store;

use alloy_primitives::B256;
use kardamom_obs::halt::{Halt, HaltCause};
use serde::{Deserialize, Serialize};

pub use kardamom_types::l1_block::{BatchEntry, L1Block};

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

/// Errors of the indexer. A chain break, an L1 source that does not
/// answer, and a light client that disagrees put the follower in a halt
/// (see [`IndexerError::halt`]); every other error is a failed tick that
/// the next tick retries.
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("L1 source: {0}")]
    Source(#[from] kardamom_da_watcher::L1SourceError),
    #[error("L1 provider: {0}")]
    Provider(String),
    #[error(
        "L1 block {number} does not descend from the indexed block: parent {parent}, expected {expected}"
    )]
    ChainBreak {
        number: u64,
        expected: B256,
        parent: B256,
    },
    #[error(
        "L1 block {number} ends the finality step with hash {follower}, but the light client's finalized header has {light_client}"
    )]
    LightClientMismatch {
        number: u64,
        follower: B256,
        light_client: B256,
    },
    #[error("l1_blocks stream: {0}")]
    Publish(String),
    #[error("beacon API: {0}")]
    Beacon(String),
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

impl IndexerError {
    /// The halt this error puts the follower in: `l1_chain_break` for a
    /// block that does not descend from the indexed one, `l1_unreachable`
    /// for a provider or a beacon API that does not answer,
    /// `l1_light_client_mismatch` for a step that does not end at the
    /// light client's header, and the halt of an L1 read that failed
    /// ([`kardamom_da_watcher::L1SourceError::halt_cause`]). `None` for
    /// every other error. The light client halt waits for an operator;
    /// every other one clears by itself: the follower retries the same
    /// range every slot.
    #[must_use]
    pub fn halt(&self) -> Option<Halt> {
        let cause = match self {
            Self::ChainBreak { .. } => Some(HaltCause::L1ChainBreak),
            Self::LightClientMismatch { .. } => Some(HaltCause::L1LightClientMismatch),
            Self::Source(e) => e.halt_cause(),
            Self::Provider(_) | Self::Beacon(_) => Some(HaltCause::L1Unreachable),
            _ => None,
        };
        cause.map(|cause| Halt::new(cause, self.to_string()))
    }
}
