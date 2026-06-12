//! zstd compression wrappers used by the batcher.

use crate::error::BatcherError;

/// Default zstd compression level — balances throughput vs ratio for tx-stream-like data.
pub const DEFAULT_LEVEL: i32 = 9;

pub fn encode_zstd(input: &[u8], level: i32) -> Result<Vec<u8>, BatcherError> {
    zstd::stream::encode_all(input, level).map_err(|e| BatcherError::Compress(e.to_string()))
}

pub fn decode_zstd(input: &[u8]) -> Result<Vec<u8>, BatcherError> {
    zstd::stream::decode_all(input).map_err(|e| BatcherError::Compress(e.to_string()))
}
