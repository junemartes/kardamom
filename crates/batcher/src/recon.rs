//! Reconstruction support for the section 6 conformance test.
//!
//! Takes the blobs the batcher posted. The L1 calldata holds only the
//! versioned hashes and block range; the blob bytes themselves come from the
//! beacon-chain blob sidecar. Rebuilds the `Vec<BlockFrame>` the batcher fed
//! in. This validates the whole batcher pipeline with one round trip.

use alloy_eips::eip4844::Blob;

use crate::blob::unpack_from_blobs;
use crate::compress::decode_zstd;
use crate::error::BatcherError;
use crate::frame::{BlockFrame, Kar1Payload, decode as frame_decode};

/// Reconstruct the per-block tx stream from a sequence of blobs.
///
/// `blobs` must be the in-order set of blobs for one posted batch. This
/// function unpacks the 31-byte field encoding. It checks whether the
/// unpacked bytes are a zstd stream, and decompresses them if so. Then it
/// decodes the KAR1 framing.
pub fn reconstruct(blobs: &[Blob]) -> Result<Vec<BlockFrame>, BatcherError> {
    let raw = unpack_from_blobs(blobs)?;
    let framed = if is_zstd(&raw) {
        decode_zstd(&raw)?
    } else {
        raw
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
