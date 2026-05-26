//! zstd round-trip + a ratio sanity test.

use kardamom_batcher::compress::{DEFAULT_LEVEL, decode_zstd, encode_zstd};

#[test]
fn roundtrip_arbitrary_bytes() {
    let input: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
    let z = encode_zstd(&input, DEFAULT_LEVEL).unwrap();
    let back = decode_zstd(&z).unwrap();
    assert_eq!(back, input);
}

#[test]
fn compressible_input_shrinks() {
    let input = vec![0xAAu8; 64 * 1024];
    let z = encode_zstd(&input, DEFAULT_LEVEL).unwrap();
    assert!(
        z.len() < input.len() / 8,
        "expected highly compressible input to shrink dramatically (got {} -> {})",
        input.len(),
        z.len()
    );
    assert_eq!(decode_zstd(&z).unwrap(), input);
}

#[test]
fn empty_input_roundtrips() {
    let z = encode_zstd(&[], DEFAULT_LEVEL).unwrap();
    assert_eq!(decode_zstd(&z).unwrap(), Vec::<u8>::new());
}
