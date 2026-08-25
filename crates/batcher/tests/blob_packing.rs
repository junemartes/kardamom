//! 31-byte-per-field-element blob packing round-trip tests.

use alloy_eips::eip4844::BYTES_PER_BLOB;
use kardamom_batcher::blob::{USABLE_BYTES_PER_BLOB, pack_to_blobs, unpack_from_blobs};

#[test]
fn empty_payload_packs_to_one_blob() {
    let blobs = pack_to_blobs(&[]).unwrap();
    assert_eq!(blobs.len(), 1);
    let back = unpack_from_blobs(&blobs).unwrap();
    assert_eq!(back, Vec::<u8>::new());
}

#[test]
fn small_payload_roundtrip() {
    let payload = b"hello, kardamom!".to_vec();
    let blobs = pack_to_blobs(&payload).unwrap();
    assert_eq!(blobs.len(), 1);
    let back = unpack_from_blobs(&blobs).unwrap();
    assert_eq!(back, payload);
}

#[test]
fn high_byte_of_every_field_element_is_zero() {
    let payload: Vec<u8> = (0..1000u32).map(|i| (i & 0xFF) as u8).collect();
    let blobs = pack_to_blobs(&payload).unwrap();
    let raw = blobs[0].as_slice();
    for chunk in raw.as_chunks::<32>().0 {
        assert_eq!(chunk[0], 0, "high byte of every field element must be 0");
    }
}

#[test]
fn each_blob_is_canonical_size() {
    let payload = vec![0xCDu8; USABLE_BYTES_PER_BLOB - 4]; // leaves room for 4-byte length prefix
    let blobs = pack_to_blobs(&payload).unwrap();
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].as_slice().len(), BYTES_PER_BLOB);
}

#[test]
fn multi_blob_payload_roundtrip() {
    // Force two blobs: payload + 4-byte header > USABLE_BYTES_PER_BLOB.
    let payload = vec![0xAB; USABLE_BYTES_PER_BLOB + 1000];
    let blobs = pack_to_blobs(&payload).unwrap();
    assert!(blobs.len() >= 2);
    let back = unpack_from_blobs(&blobs).unwrap();
    assert_eq!(back, payload);
}

#[test]
fn varied_sizes_roundtrip() {
    for &size in &[0usize, 1, 30, 31, 32, 33, 1023, 1024, 1025] {
        let payload: Vec<u8> = (0..size as u32).map(|i| (i & 0xFF) as u8).collect();
        let blobs = pack_to_blobs(&payload).unwrap();
        let back = unpack_from_blobs(&blobs).unwrap();
        assert_eq!(back, payload, "round-trip failed at size {size}");
    }
}
