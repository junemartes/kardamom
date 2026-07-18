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
}
