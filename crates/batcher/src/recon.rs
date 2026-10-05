//! Reconstruction: the payload the batcher posted, back to block frames.
//!
//! The L1 calldata holds only the certificate and the block range; the
//! payload comes from EigenDA by the certificate. This rebuilds the
//! `Vec<BlockFrame>` the batcher fed in, the whole pipeline in one round
//! trip.

use crate::compress::decode_zstd;
use crate::error::BatcherError;
use crate::frame::{BlockFrame, Kar1Payload, decode as frame_decode};

/// Reconstruct the per-block tx stream from one posted batch's payload.
///
/// The payload is a zstd stream or a bare KAR1 frame; the zstd magic
/// tells them apart.
///
/// # Errors
/// Returns an error when decompression or KAR1 decoding fails.
pub fn reconstruct(payload: &[u8]) -> Result<Vec<BlockFrame>, BatcherError> {
    let framed = if is_zstd(payload) {
        decode_zstd(payload)?
    } else {
        payload.to_vec()
    };
    let payload: Kar1Payload = frame_decode(&framed)?;
    Ok(payload.blocks)
}

/// True if the buffer begins with the zstd magic (0xFD2FB528 little-endian).
fn is_zstd(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] == 0x28 && bytes[1] == 0xB5 && bytes[2] == 0x2F && bytes[3] == 0xFD
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::MAGIC;

    /// The KAR1 magic must not look like a zstd stream. Its first byte is
    /// 'K' (0x4B). zstd streams start with 0x28.
    #[test]
    fn kar1_magic_is_not_zstd() {
        assert!(!is_zstd(&MAGIC));
    }
}
