//! zstd compression wrappers used by the batcher.

use crate::error::BatcherError;

/// The default zstd compression level. It balances throughput against ratio
/// for tx-stream data.
pub const DEFAULT_LEVEL: i32 = 9;

pub fn encode_zstd(input: &[u8], level: i32) -> Result<Vec<u8>, BatcherError> {
    zstd::stream::encode_all(input, level).map_err(|e| BatcherError::Compress(e.to_string()))
}

pub fn decode_zstd(input: &[u8]) -> Result<Vec<u8>, BatcherError> {
    zstd::stream::decode_all(input).map_err(|e| BatcherError::Compress(e.to_string()))
}
