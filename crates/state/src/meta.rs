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
//! | `schema_version`                     | `u32 BE` (currently 1)         |

use kardamom_types::BPosition;

use signet_libmdbx::Database;
use signet_libmdbx::tx::{TransactionKind, Tx};

use crate::error::StateError;

pub const KEY_LAST_COMMITTED_BLOCK: &[u8] = b"last_committed_block";
pub const KEY_LAST_COMMITTED_END_TX_POSITION: &[u8] = b"last_committed_end_tx_position";
pub const KEY_LAST_FSYNCED_B_POSITION: &[u8] = b"last_fsynced_b_position";
pub const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";
/// A presence-only flag. It is written once, when genesis allocations are
/// seeded into a fresh env (see `crate::genesis::seed_genesis`).
///
/// This flag makes genesis seeding idempotent across restarts, independent
/// of the block cursor. Genesis is "block 0", so `last_committed_block`
/// stays 0 until the first real block commits.
pub const KEY_GENESIS_APPLIED: &[u8] = b"genesis_applied";
/// The 32-byte keccak digest of the genesis allocations seeded into this
/// env (see `crate::genesis`).
///
/// The node compares this digest on every restart. This makes startup fail
/// on a changed `--chain` file, or a node pointed at the wrong state
/// directory, instead of running silently on divergent genesis state.
///
/// This key may be absent on an env seeded before the digest existed. The
/// node backfills it on the next start.
pub const KEY_GENESIS_DIGEST: &[u8] = b"genesis_digest";
/// The latest computed Ethereum MPT world-state root (32 bytes).
///
/// The trie-aware writer (`StateWriter::spawn_with_trie`) writes this in
/// the same read-write transaction as the block it commits. It is absent
/// on databases written by the plain, non-trie executor writer. See
/// `crate::trie`.
pub const KEY_STATE_ROOT: &[u8] = b"state_root";

// v2 adds the incremental state-trie tables: account_trie, storage_trie,
// hashed_accounts, and hashed_storage. The database refuses a v1 DB; only
// fresh-from-genesis is supported. See
// docs/specs/2026-06-23-incremental-trie-design.md, section 8.
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

pub fn read_meta_u64<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<u64>, StateError> {
    match txn.get::<Vec<u8>>(meta.dbi(), key)? {
        Some(b) => Ok(Some(decode_u64(&b)?)),
        None => Ok(None),
    }
}

pub fn read_meta_u32<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<u32>, StateError> {
    match txn.get::<Vec<u8>>(meta.dbi(), key)? {
        Some(b) => Ok(Some(decode_u32(&b)?)),
        None => Ok(None),
    }
}

pub fn read_meta_b_position<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<BPosition>, StateError> {
    match txn.get::<Vec<u8>>(meta.dbi(), key)? {
        Some(b) => Ok(Some(decode_b_position(&b)?)),
        None => Ok(None),
    }
}

pub fn read_meta_b256<K: TransactionKind>(
    txn: &Tx<K>,
    meta: Database,
    key: &[u8],
) -> Result<Option<alloy_primitives::B256>, StateError> {
    match txn.get::<Vec<u8>>(meta.dbi(), key)? {
        Some(b) => Ok(Some(decode_b256(&b)?)),
        None => Ok(None),
    }
}

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

pub fn encode_b256(v: alloy_primitives::B256) -> [u8; 32] {
    v.into()
}

pub fn decode_b256(bytes: &[u8]) -> Result<alloy_primitives::B256, StateError> {
    if bytes.len() != 32 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 32,
            got: bytes.len(),
        });
    }
    Ok(alloy_primitives::B256::from_slice(bytes))
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
