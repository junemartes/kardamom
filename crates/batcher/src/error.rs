//! Batcher error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BatcherError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(String),
    #[error("compress: {0}")]
    Compress(String),
    #[error("frame: {0}")]
    Frame(String),
    #[error("blob: {0}")]
    Blob(String),
    #[error("l1: {0}")]
    L1(String),
    #[error("config: {0}")]
    Config(String),
    #[error("reconstruct: {0}")]
    Reconstruct(String),
    /// Archive data that is present but wrong. Examples: a segment whose
    /// bytes differ from the redundant copy, or a malformed frame in the
    /// middle of the file (not a live tail). This differs from
    /// `Reconstruct`, so callers can route to the heal path instead of
    /// treating it as an operational error.
    #[error("archive corruption: {0}")]
    Corruption(String),
    /// One block on its own packs to more than the 6-blob ceiling. The
    /// batcher cannot split a block, so it cannot post this one.
    #[error(
        "block {block_number} alone packs to {blobs} blobs; the ceiling is 6 blobs; \
         the batcher cannot post this block"
    )]
    BlockTooLarge { block_number: u64, blobs: usize },
}
