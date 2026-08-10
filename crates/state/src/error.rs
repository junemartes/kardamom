//! Error type for all `kardamom-state` operations.

use thiserror::Error;

/// Errors a state-database operation may surface.
///
/// `kardamom_types::StateError` is the marker trait the trait surface returns;
/// we implement it for this type so a `StateDatabase` impl over `StateSnapshot`
/// can advertise this as its associated `Error`.
#[derive(Debug, Error)]
pub enum StateError {
    #[error("mdbx error: {0}")]
    Mdbx(#[from] signet_libmdbx::MdbxError),
    #[error("mdbx read error: {0}")]
    MdbxRead(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("rlp decode error: {0}")]
    Rlp(#[from] alloy_rlp::Error),
    #[error("decode error in table {table}: expected {expected} bytes, got {got}")]
    BadEncoding {
        table: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("rkyv decode error in table {table}: {detail}")]
    RkyvDecode { table: &'static str, detail: String },
    #[error("recovery failed: {0}")]
    Recovery(String),
    /// A checkpoint image whose bytes do not hash to what its source (the
    /// MANIFEST, or the serving peer's headers) claims. The Display string is
    /// pinned: tests and operators grep for the CORRUPT marker.
    #[error("checkpoint {image} is CORRUPT: image hashes to {got:#x}, {claimant} says {want:#x}")]
    CorruptCheckpointImage {
        image: String,
        claimant: String,
        got: alloy_primitives::B256,
        want: alloy_primitives::B256,
    },
    /// A checkpoint image bound to a different chain's genesis than this
    /// node's. The Display string is pinned: tests and operators grep for the
    /// DIFFERENT CHAIN marker.
    #[error(
        "checkpoint {image} belongs to a DIFFERENT CHAIN: its genesis digest is {image_genesis:#x}, this node's is {expected:#x}"
    )]
    ForeignChainCheckpoint {
        image: String,
        image_genesis: alloy_primitives::B256,
        expected: alloy_primitives::B256,
    },
    #[error(
        "genesis mismatch: this env was seeded from a different genesis (stored digest {stored}, supplied {supplied}); refusing to run on divergent genesis state"
    )]
    GenesisMismatch {
        stored: alloy_primitives::B256,
        supplied: alloy_primitives::B256,
    },
    #[error(
        "trie shadow-check mismatch at block {block}: incremental {incremental} != rebuilt {rebuilt}"
    )]
    ShadowMismatch {
        block: u64,
        incremental: alloy_primitives::B256,
        rebuilt: alloy_primitives::B256,
    },
}

impl From<signet_libmdbx::ReadError> for StateError {
    fn from(value: signet_libmdbx::ReadError) -> Self {
        // `ReadError` is the read-only variant that can additionally carry
        // decode errors. We flatten to a string — the BadEncoding variant is
        // already covered by the explicit decoders.
        StateError::MdbxRead(value.to_string())
    }
}

// Implement the marker trait required by `kardamom_types::StateDatabase`.
impl kardamom_types::StateError for StateError {}
