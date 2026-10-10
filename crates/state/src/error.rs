//! Error type for all `kardamom-state` operations.

use thiserror::Error;

/// Errors from a state-database operation.
///
/// `kardamom_types::StateError` is the marker trait for the trait surface's
/// return type. This type implements it, so a `StateDatabase` implementation
/// over `StateSnapshot` can use it as its associated `Error` type.
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
    /// A checkpoint image whose bytes do not match the hash from its source
    /// (the MANIFEST, or the serving peer's headers).
    ///
    /// Do not change the Display string. Tests and operators search for the
    /// CORRUPT marker.
    #[error("checkpoint {image} is CORRUPT: image hashes to {got:#x}, {claimant} says {want:#x}")]
    CorruptCheckpointImage {
        image: String,
        claimant: String,
        got: alloy_primitives::B256,
        want: alloy_primitives::B256,
    },
    /// A checkpoint image that is bound to a different chain's genesis than
    /// this node's.
    ///
    /// Do not change the Display string. Tests and operators search for the
    /// DIFFERENT CHAIN marker.
    #[error(
        "checkpoint {image} belongs to a DIFFERENT CHAIN: its genesis digest is {image_genesis:#x}, this node's is {expected:#x}"
    )]
    ForeignChainCheckpoint {
        image: String,
        image_genesis: alloy_primitives::B256,
        expected: alloy_primitives::B256,
    },
    /// A checkpoint image of a state schema that this release does not
    /// read. The adopter skips it and takes the next checkpoint.
    #[error(
        "checkpoint {image} holds state schema {schema}; this release reads schema {supported} only"
    )]
    UnreadableCheckpointSchema {
        image: String,
        schema: u32,
        supported: u32,
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
    /// The references of a committed block cannot rebuild its payload.
    /// The query endpoint answers this as a JSON-RPC error with the cause.
    #[error("block {block} cannot be rebuilt from references: {cause}")]
    NoBlockRefs { block: u64, cause: NoRefsCause },
}

/// Why a committed block has no full list of references.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NoRefsCause {
    /// The node rebuilt the block from its L1 payload, which carries no
    /// archive reference. The block is on L1 already.
    #[error("this node rebuilt the block from L1, so it keeps no archive reference for it")]
    RebuiltFromL1,
    /// A transaction other than a deposit has no reference: a cross-chain
    /// message, whose remote-epoch record is in no archive, or a row
    /// written before the reference existed.
    #[error("transaction {tx_hash} (type {tx_type:#x}) has no archive reference")]
    Unreferenced {
        tx_hash: alloy_primitives::B256,
        tx_type: u8,
    },
}

impl From<signet_libmdbx::ReadError> for StateError {
    fn from(value: signet_libmdbx::ReadError) -> Self {
        // `ReadError` is the read-only variant. It can also carry decode
        // errors. We flatten it to a string. The explicit decoders already
        // cover the BadEncoding variant.
        StateError::MdbxRead(value.to_string())
    }
}

impl kardamom_types::StateError for StateError {}
