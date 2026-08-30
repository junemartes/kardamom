//! Packs 31 bytes per field element into EIP-4844 blobs.
//!
//! The BLS12-381 field constraint requires each 32-byte field element to
//! encode a value less than `BLS_MODULUS`. The safe encoding keeps the high
//! byte of every field element zero, and packs 31 payload bytes into the low
//! 31 bytes. A single blob (`BYTES_PER_BLOB` = 131072 bytes, 4096 field
//! elements) carries `4096 * 31 = 126976` payload bytes
//! (`USABLE_BYTES_PER_BLOB`).
//!
//! A 4-byte little-endian length header goes before the payload, before
//! chunking. This lets `unpack_from_blobs` find the end of the original
//! payload and drop the trailing zero padding.

use alloy_eips::eip4844::{BYTES_PER_BLOB, Blob, FIELD_ELEMENT_BYTES_USIZE};

use crate::error::BatcherError;

/// The number of payload bytes packed into the low end of each 32-byte field
/// element. The high byte stays zero, so the field element is always less
/// than `BLS_MODULUS`. Alloy's `USABLE_BYTES_PER_BLOB` (130048) comes from a
/// tighter 254-bit packing scheme. This crate uses the simpler 31-byte
/// scheme instead. It composes cleanly with `zstd` and avoids trusted-setup
/// edge cases.
const USABLE_BYTES_PER_FIELD: usize = FIELD_ELEMENT_BYTES_USIZE - 1; // 31

/// Total bytes one blob carries under the 31-byte-per-field scheme: 4096
/// field elements times 31 bytes each equals 126_976.
pub const USABLE_BYTES_PER_BLOB: usize =
    (BYTES_PER_BLOB / FIELD_ELEMENT_BYTES_USIZE) * USABLE_BYTES_PER_FIELD;

const LENGTH_HEADER_BYTES: usize = 4;

/// Pack `payload` into one or more `Blob`s using the 31-byte safe encoding.
pub fn pack_to_blobs(payload: &[u8]) -> Result<Vec<Blob>, BatcherError> {
    let len_u32: u32 = payload
        .len()
        .try_into()
        .map_err(|_| BatcherError::Blob("payload length overflows u32".into()))?;

    let mut prefixed = Vec::with_capacity(LENGTH_HEADER_BYTES + payload.len());
    prefixed.extend_from_slice(&len_u32.to_le_bytes());
    prefixed.extend_from_slice(payload);

    if prefixed.len() <= LENGTH_HEADER_BYTES && payload.is_empty() {
        // For an empty payload, encode one blob with a length prefix of 0.
        // The rest is padding. This keeps the round trip well defined.
        let blob = encode_one_blob(&prefixed);
        return Ok(vec![blob]);
    }

    let mut blobs = Vec::new();
    let mut offset = 0;
    while offset < prefixed.len() {
        let end = (offset + USABLE_BYTES_PER_BLOB).min(prefixed.len());
        let chunk = &prefixed[offset..end];
        blobs.push(encode_one_blob(chunk));
        offset = end;
    }
    Ok(blobs)
}

/// Inverse of [`pack_to_blobs`]. Returns the original payload bytes, stripped
/// of the length prefix and trailing zero padding.
pub fn unpack_from_blobs(blobs: &[Blob]) -> Result<Vec<u8>, BatcherError> {
    if blobs.is_empty() {
        return Err(BatcherError::Blob("no blobs to unpack".into()));
    }
    let mut raw = Vec::with_capacity(blobs.len() * USABLE_BYTES_PER_BLOB);
    for blob in blobs {
        raw.extend(decode_one_blob(blob));
    }
    if raw.len() < LENGTH_HEADER_BYTES {
        return Err(BatcherError::Blob(
            "blob payload too short for header".into(),
        ));
    }
    let len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    if LENGTH_HEADER_BYTES + len > raw.len() {
        return Err(BatcherError::Blob(format!(
            "declared length {len} exceeds blob capacity {}",
            raw.len() - LENGTH_HEADER_BYTES
        )));
    }
    Ok(raw[LENGTH_HEADER_BYTES..LENGTH_HEADER_BYTES + len].to_vec())
}

/// Encode up to `USABLE_BYTES_PER_BLOB` bytes into one blob (zero-padded).
fn encode_one_blob(chunk: &[u8]) -> Blob {
    debug_assert!(chunk.len() <= USABLE_BYTES_PER_BLOB);
    let mut blob_bytes = [0u8; BYTES_PER_BLOB];
    let mut field_idx = 0;
    let mut src = 0;
    while src < chunk.len() {
        let take = (chunk.len() - src).min(USABLE_BYTES_PER_FIELD);
        let dst = field_idx * FIELD_ELEMENT_BYTES_USIZE;
        // dst[0] stays zero (the high byte). The loop writes into dst[1..1+take].
        blob_bytes[dst + 1..dst + 1 + take].copy_from_slice(&chunk[src..src + take]);
        src += take;
        field_idx += 1;
    }
    Blob::new(blob_bytes)
}

/// Decode one blob to its 126976-byte payload area. This still includes any
/// trailing zero padding. The caller strips it using the length header.
fn decode_one_blob(blob: &Blob) -> Vec<u8> {
    let raw: &[u8] = blob.as_slice();
    debug_assert_eq!(raw.len(), BYTES_PER_BLOB);
    let mut out = Vec::with_capacity(USABLE_BYTES_PER_BLOB);
    for field_idx in 0..(BYTES_PER_BLOB / FIELD_ELEMENT_BYTES_USIZE) {
        let dst = field_idx * FIELD_ELEMENT_BYTES_USIZE;
        out.extend_from_slice(&raw[dst + 1..dst + FIELD_ELEMENT_BYTES_USIZE]);
    }
    out
}
