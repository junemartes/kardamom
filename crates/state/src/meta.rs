//! `meta` table: well-known keys for durable cursors.
//!
//! All writes go through the same RW txn as the block delta they correspond
//! to. The atomic boundary is the mdbx commit; cold-start reads the cursors
//! to find the post-recovery snapshot point.
//!
//! | Key                                  | Value                          |
//! |--------------------------------------|--------------------------------|
//! | `last_committed_block`               | `u64 BE`                       |
//! | `last_committed_end_tx_position`     | `BPosition` (8 B, i32 BE + i32 BE) |
//! | `last_fsynced_b_position`            | `BPosition` (8 B)              |
//! | `schema_version`                     | `u32 BE` (currently 1)         |

use kardamom_types::BPosition;

use crate::error::StateError;

pub const KEY_LAST_COMMITTED_BLOCK: &[u8] = b"last_committed_block";
pub const KEY_LAST_COMMITTED_END_TX_POSITION: &[u8] = b"last_committed_end_tx_position";
pub const KEY_LAST_FSYNCED_B_POSITION: &[u8] = b"last_fsynced_b_position";
pub const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";
/// Presence-only flag written once when genesis allocations are seeded into a
/// fresh env (see `crate::genesis::seed_genesis`). Makes genesis seeding
/// idempotent across restarts independently of the block cursor (genesis is
/// "block 0", so `last_committed_block` stays 0 until the first real block).
pub const KEY_GENESIS_APPLIED: &[u8] = b"genesis_applied";

pub const SCHEMA_VERSION: u32 = 1;

pub fn encode_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

pub fn encode_u32(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

pub fn decode_u32(bytes: &[u8]) -> Result<u32, StateError> {
    if bytes.len() != 4 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 4,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(arr))
}

pub fn encode_b_position(p: BPosition) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&p.term_id.to_be_bytes());
    out[4..].copy_from_slice(&p.term_offset.to_be_bytes());
    out
}

pub fn decode_b_position(bytes: &[u8]) -> Result<BPosition, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut term_id_bytes = [0u8; 4];
    term_id_bytes.copy_from_slice(&bytes[..4]);
    let mut term_offset_bytes = [0u8; 4];
    term_offset_bytes.copy_from_slice(&bytes[4..]);
    Ok(BPosition {
        term_id: i32::from_be_bytes(term_id_bytes),
        term_offset: i32::from_be_bytes(term_offset_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_roundtrip() {
        for v in [0u64, 1, 250, u64::MAX] {
            assert_eq!(decode_u64(&encode_u64(v)).unwrap(), v);
        }
    }

    #[test]
    fn b_position_roundtrip() {
        let p = BPosition {
            term_id: 7,
            term_offset: 12345,
        };
        let bytes = encode_b_position(p);
        assert_eq!(bytes.len(), 8);
        assert_eq!(decode_b_position(&bytes).unwrap(), p);
    }

    #[test]
    fn schema_version_codec() {
        assert_eq!(
            decode_u32(&encode_u32(SCHEMA_VERSION)).unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn bad_length_errors() {
        assert!(matches!(
            decode_u64(&[0u8; 7]),
            Err(StateError::BadEncoding {
                expected: 8,
                got: 7,
                ..
            })
        ));
        assert!(matches!(
            decode_b_position(&[0u8; 4]),
            Err(StateError::BadEncoding {
                expected: 8,
                got: 4,
                ..
            })
        ));
    }
}
