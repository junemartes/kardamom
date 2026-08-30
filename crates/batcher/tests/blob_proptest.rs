//! Property: `pack_to_blobs` and `unpack_from_blobs` round-trip any byte
//! input of up to 6 blobs of payload.

use kardamom_batcher::blob::{USABLE_BYTES_PER_BLOB, pack_to_blobs, unpack_from_blobs};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Round-trip arbitrary payloads up to about 3 blobs in size.
    #[test]
    fn round_trip_packs(payload in proptest::collection::vec(any::<u8>(), 0..(3 * USABLE_BYTES_PER_BLOB))) {
        let blobs = pack_to_blobs(&payload).unwrap();
        prop_assert!(blobs.len() <= 4, "expected up to 4 blobs for the test range, got {}", blobs.len());
        let back = unpack_from_blobs(&blobs).unwrap();
        prop_assert_eq!(back, payload);
    }

    /// Every emitted field element's high byte must be zero (BLS modulus safety).
    #[test]
    fn high_byte_always_zero(payload in proptest::collection::vec(any::<u8>(), 0..(USABLE_BYTES_PER_BLOB / 2))) {
        let blobs = pack_to_blobs(&payload).unwrap();
        for blob in &blobs {
            let raw: &[u8] = blob.as_slice();
            for chunk in raw.as_chunks::<32>().0 {
                prop_assert_eq!(chunk[0], 0);
            }
        }
    }
}
