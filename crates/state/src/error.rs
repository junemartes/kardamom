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
    #[error("writer channel closed before block {block} was committed")]
    WriterChannelClosed { block: u64 },
    #[error("recovery failed: {0}")]
    Recovery(String),
    #[error("snapshot exhausted: writer outran the {horizon}-block version horizon")]
    SnapshotExhausted { horizon: u32 },
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
