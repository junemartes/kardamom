//! The `meta` table holds well-known keys for durable cursors.
//!
//! Every write goes through the same read-write transaction as the block
//! delta it belongs to. The mdbx commit is the atomic boundary. On cold
//! start, the node reads these cursors to find the post-recovery snapshot
//! point.
//!
//! | Key                                  | Value                          |
//! |--------------------------------------|--------------------------------|
//! | `last_committed_block`               | `u64 BE`                       |
//! | `last_committed_end_tx_position`     | `BPosition` (8 B, i32 BE + i32 BE) |
//! | `last_fsynced_b_position`            | `BPosition` (8 B)              |
//! | `schema_version`                     | `u32 BE`                       |

use kardamom_types::BPosition;

use signet_libmdbx::Database;
use signet_libmdbx::tx::{TransactionKind, Tx};

use crate::error::StateError;

pub(crate) const KEY_LAST_COMMITTED_BLOCK: &[u8] = b"last_committed_block";
pub(crate) const KEY_LAST_COMMITTED_END_TX_POSITION: &[u8] = b"last_committed_end_tx_position";
pub(crate) const KEY_LAST_FSYNCED_B_POSITION: &[u8] = b"last_fsynced_b_position";
pub(crate) const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";
/// A presence-only flag. It is written once, when genesis allocations are
/// seeded into a fresh env (see `crate::genesis::seed_genesis`).
///
/// This flag makes genesis seeding idempotent across restarts, independent
/// of the block cursor. Genesis is "block 0", so `last_committed_block`
/// stays 0 until the first real block commits.
pub(crate) const KEY_GENESIS_APPLIED: &[u8] = b"genesis_applied";
/// The 32-byte keccak digest of the genesis allocations seeded into this
/// env (see `crate::genesis`).
///
/// The node compares this digest on every restart. This makes startup fail
/// on a changed `--chain` file, or a node pointed at the wrong state
/// directory, instead of running silently on divergent genesis state.
///
/// This key may be absent on an env seeded before the digest existed. The
/// node backfills it on the next start.
pub(crate) const KEY_GENESIS_DIGEST: &[u8] = b"genesis_digest";
/// The latest computed Ethereum MPT world-state root (32 bytes).
///
/// The trie-aware writer (`StateWriter::spawn_with_trie`) writes this in
/// the same read-write transaction as the block it commits. It is absent
/// on databases written by the plain, non-trie executor writer. See
/// `crate::trie`.
pub(crate) const KEY_STATE_ROOT: &[u8] = b"state_root";

// The database refuses a version other than `SCHEMA_VERSION`; only
// fresh-from-genesis is supported.
pub const SCHEMA_VERSION: u32 = 2;

// ---------- typed meta readers ----------
//
// Each function does one `get` and decode for a well-known key. Every
// startup and verify path that reads a cursor from the `meta` table shares
// these functions. `Ok(None)` means the key is absent. A present but
// undecodable value returns the decoder's `BadEncoding` error.
//
// These functions are generic over the transaction kind. This lets
// read-only callers (snapshot, recovery) and read-write callers (writer,
// genesis) share one implementation.

/// Read `key` from `db` and decode it, or `Ok(None)` if the key is
/// absent. Every typed meta reader, and every `snapshot.rs` field read,
/// is a `decode` fn plugged into this one `get`-then-decode shape.
///
/// # Errors
///
/// Propagates whatever `decode` returns for a present-but-malformed value.
pub(crate) fn get_decoded<K: TransactionKind, T>(
    txn: &Tx<K>,
    db: Database,
    key: &[u8],
    decode: fn(&[u8]) -> Result<T, StateError>,
) -> Result<Option<T>, StateError> {
    match txn.get::<Vec<u8>>(db.dbi(), key)? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if the stored value is not 8 bytes.
pub(crate) fn read_meta_u64<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<u64>, StateError> {
    get_decoded(txn, meta, key, decode_u64)
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if the stored value is not 4 bytes.
pub(crate) fn read_meta_u32<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<u32>, StateError> {
    get_decoded(txn, meta, key, decode_u32)
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if the stored value is not 8 bytes.
pub(crate) fn read_meta_b_position<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<BPosition>, StateError> {
    get_decoded(txn, meta, key, decode_b_position)
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if the stored value is not 32 bytes.
pub(crate) fn read_meta_b256<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<alloy_primitives::B256>, StateError> {
    get_decoded(txn, meta, key, decode_b256)
}

/// Parse `bytes` as a fixed-width row, or a [`StateError::BadEncoding`]
/// naming `table` and the length mismatch. Every fixed-width decoder in
/// this crate goes through this, so a length check and its `BadEncoding`
/// never drift from each other, and no decoder needs its own
/// `try_into().expect(..)` after the check.
pub(crate) fn fixed<'b, const N: usize>(
    table: &'static str,
    bytes: &'b [u8],
) -> Result<&'b [u8; N], StateError> {
    bytes.try_into().map_err(|_| StateError::BadEncoding {
        table,
        expected: N,
        got: bytes.len(),
    })
}

#[must_use]
pub(crate) fn encode_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 8 bytes.
pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StateError> {
    Ok(u64::from_be_bytes(*fixed::<8>("meta", bytes)?))
}

#[must_use]
pub(crate) fn encode_u32(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 4 bytes.
pub(crate) fn decode_u32(bytes: &[u8]) -> Result<u32, StateError> {
    Ok(u32::from_be_bytes(*fixed::<4>("meta", bytes)?))
}

#[must_use]
pub(crate) fn encode_b_position(p: BPosition) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&p.term_id.to_be_bytes());
    out[4..].copy_from_slice(&p.term_offset.to_be_bytes());
    out
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 8 bytes.
pub(crate) fn decode_b_position(bytes: &[u8]) -> Result<BPosition, StateError> {
    let &[b0, b1, b2, b3, b4, b5, b6, b7] = fixed::<8>("meta", bytes)?;
    Ok(BPosition {
        term_id: i32::from_be_bytes([b0, b1, b2, b3]),
        term_offset: i32::from_be_bytes([b4, b5, b6, b7]),
    })
}

#[must_use]
pub(crate) fn encode_b256(v: alloy_primitives::B256) -> [u8; 32] {
    v.into()
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 32 bytes.
pub(crate) fn decode_b256(bytes: &[u8]) -> Result<alloy_primitives::B256, StateError> {
    Ok(alloy_primitives::B256::from(*fixed::<32>("meta", bytes)?))
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
