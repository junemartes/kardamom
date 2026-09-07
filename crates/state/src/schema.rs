//! libmdbx schema: the tables listed in [`ALL_TABLES`], each with a fixed
//! key/value encoding.
//!
//! | Table            | Key                              | Value                                            |
//! |------------------|-----------------------------------|--------------------------------------------------|
//! | `accounts`       | `Address` (20 B)                 | RLP `(u64 nonce, U256 balance, B256 code_hash, B256 storage_root)` |
//! | `storage`        | `Address ++ B256 key` (52 B)     | `U256 value` (32 B, big-endian)                  |
//! | `code`           | `B256 code_hash` (32 B)          | raw bytecode                                     |
//! | `headers`        | `u64 block_number` (8 B BE)      | encoded `(BPosition end_tx_idx, u64 l2_timestamp, u64 l1_origin)`, no state root |
//! | `receipts`       | `BPosition tx_idx` (8 B)         | encoded `Receipt` (rkyv archive, owned at rest)  |
//! | `tx_hash_index`  | `B256 tx_hash` (32 B)            | `BPosition` (8 B, i32 BE `term_id` ++ i32 BE `term_offset`), for `eth_getTransactionReceipt(hash)` |
//! | `meta`           | `&[u8]` (well-known keys, below) | varies, see `meta.rs`                            |
//! | `account_trie`   | trie path (raw nibbles)          | branch node, see `trie/node.rs`                  |
//! | `storage_trie`   | account hash ++ trie path        | branch node, see `trie/node.rs`                  |
//! | `hashed_accounts`| `keccak256(Address)` (32 B)      | hashed-state mirror leaf                         |
//! | `hashed_storage` | account hash ++ `keccak256(key)` | hashed-state mirror leaf                         |
//!
//! Big-endian encoding on the `headers` key keeps `block_number` ordered
//! under mdbx's lexicographic cursor. The cold-start scan depends on this
//! order. `BPosition` encoding (`term_id` i32 BE, then `term_offset` i32 BE, 8
//! bytes total) is lexicographically ordered by `(term_id, term_offset)`.
//! The `receipts` table has the same property.

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use kardamom_types::{AccountChange, BPosition, CodeEntry, Receipt};

use crate::error::StateError;

pub(crate) const TABLE_ACCOUNTS: &str = "accounts";
pub(crate) const TABLE_STORAGE: &str = "storage";
pub(crate) const TABLE_CODE: &str = "code";
pub(crate) const TABLE_HEADERS: &str = "headers";
pub(crate) const TABLE_RECEIPTS: &str = "receipts";
pub(crate) const TABLE_TX_HASH_INDEX: &str = "tx_hash_index";
pub(crate) const TABLE_META: &str = "meta";

// --- Incremental state-trie tables (schema v2; see crate::trie) ---
//
// These tables store intermediate branch nodes, keyed by the trie path.
// The path uses raw, unpacked nibbles, one nibble per byte, so lexicographic
// mdbx order matches trie order. See `trie/cursor.rs::node_key`. Storage-trie
// keys prepend the 32-byte account hash.
//
// The hashed-state mirror holds the leaves, keyed by keccak.
pub(crate) const TABLE_ACCOUNT_TRIE: &str = "account_trie";
pub(crate) const TABLE_STORAGE_TRIE: &str = "storage_trie";
pub(crate) const TABLE_HASHED_ACCOUNTS: &str = "hashed_accounts";
pub(crate) const TABLE_HASHED_STORAGE: &str = "hashed_storage";

pub const ALL_TABLES: &[&str] = &[
    TABLE_ACCOUNTS,
    TABLE_STORAGE,
    TABLE_CODE,
    TABLE_HEADERS,
    TABLE_RECEIPTS,
    TABLE_TX_HASH_INDEX,
    TABLE_META,
    TABLE_ACCOUNT_TRIE,
    TABLE_STORAGE_TRIE,
    TABLE_HASHED_ACCOUNTS,
    TABLE_HASHED_STORAGE,
];

// ---------- accounts ----------

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub(crate) struct AccountValue {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

#[must_use]
pub(crate) fn encode_account_key(addr: Address) -> [u8; 20] {
    addr.into_array()
}

#[must_use]
pub(crate) fn encode_account_value(v: &AccountValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(96);
    v.encode(&mut buf);
    buf
}

/// # Errors
///
/// Returns [`StateError`] if `bytes` does not RLP-decode as an
/// [`AccountValue`].
pub(crate) fn decode_account_value(bytes: &[u8]) -> Result<AccountValue, StateError> {
    AccountValue::decode(&mut &bytes[..]).map_err(StateError::from)
}

// ---------- storage ----------

/// Storage key is `Address (20 B) ++ B256 slot (32 B) = 52 B`.
#[must_use]
pub(crate) fn encode_storage_key(addr: Address, slot: B256) -> [u8; 52] {
    let mut out = [0u8; 52];
    out[..20].copy_from_slice(addr.as_slice());
    out[20..].copy_from_slice(slot.as_slice());
    out
}

#[must_use]
pub(crate) fn encode_storage_value(v: U256) -> [u8; 32] {
    v.to_be_bytes::<32>()
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 32 bytes.
pub(crate) fn decode_storage_value(bytes: &[u8]) -> Result<U256, StateError> {
    Ok(U256::from_be_bytes(*crate::meta::fixed::<32>(
        TABLE_STORAGE,
        bytes,
    )?))
}

// ---------- code ----------

#[must_use]
pub(crate) fn encode_code_key(hash: B256) -> [u8; 32] {
    hash.into()
}

// ---------- headers ----------
//
// Headers do not carry a state-root commitment. The encoded value is
// `(end_tx_idx: BPosition, l2_timestamp: u64, l1_origin: u64)`. This uses a
// hand-rolled, fixed-width encoding (8 + 8 + 8 = 24 bytes) instead of RLP.
// The row has a fixed size, and `BPosition` is not an RLP-native type.
//
// A 24-byte row carries `l1_origin`; a 20-byte row decodes as
// `l1_origin = 0`, so an existing state DB keeps reading without a
// migration.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderValue {
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// The L1 block number for the epoch this block belongs to.
    pub l1_origin: u64,
}

#[must_use]
pub(crate) fn encode_block_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

#[must_use]
pub(crate) fn encode_header_value(v: &HeaderValue) -> [u8; 24] {
    let mut out = [0u8; 24];
    out[..4].copy_from_slice(&v.end_tx_idx.term_id.to_be_bytes());
    out[4..8].copy_from_slice(&v.end_tx_idx.term_offset.to_be_bytes());
    out[8..16].copy_from_slice(&v.l2_timestamp.to_be_bytes());
    out[16..24].copy_from_slice(&v.l1_origin.to_be_bytes());
    out
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is neither the
/// current 24-byte row nor the pre-origin 20-byte row.
pub(crate) fn decode_header_value(bytes: &[u8]) -> Result<HeaderValue, StateError> {
    // A 20-byte value is the pre-origin row. Anything else is corruption.
    // Matching each fixed-size array by value, rather than slicing and
    // `try_into`-ing sub-ranges, makes every field width a compile-time
    // fact instead of a runtime check.
    if let Ok(row) = <&[u8; 24]>::try_from(bytes) {
        let &[
            t0,
            t1,
            t2,
            t3,
            o0,
            o1,
            o2,
            o3,
            l0,
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
            l7,
            r0,
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
        ] = row;
        return Ok(HeaderValue {
            end_tx_idx: BPosition {
                term_id: i32::from_be_bytes([t0, t1, t2, t3]),
                term_offset: i32::from_be_bytes([o0, o1, o2, o3]),
            },
            l2_timestamp: u64::from_be_bytes([l0, l1, l2, l3, l4, l5, l6, l7]),
            l1_origin: u64::from_be_bytes([r0, r1, r2, r3, r4, r5, r6, r7]),
        });
    }
    if let Ok(row) = <&[u8; 20]>::try_from(bytes) {
        // The last 4 bytes of the pre-origin row are a reserved field
        // the old format never used; ignore them, same as the original
        // decoder did.
        let &[
            t0,
            t1,
            t2,
            t3,
            o0,
            o1,
            o2,
            o3,
            l0,
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
            l7,
            _reserved @ ..,
        ] = row;
        return Ok(HeaderValue {
            end_tx_idx: BPosition {
                term_id: i32::from_be_bytes([t0, t1, t2, t3]),
                term_offset: i32::from_be_bytes([o0, o1, o2, o3]),
            },
            l2_timestamp: u64::from_be_bytes([l0, l1, l2, l3, l4, l5, l6, l7]),
            l1_origin: 0,
        });
    }
    Err(StateError::BadEncoding {
        table: TABLE_HEADERS,
        expected: 24,
        got: bytes.len(),
    })
}

// ---------- receipts ----------
//
// Key: `BPosition` (8 bytes: i32 BE term_id, then i32 BE term_offset). The
// codec lives in [`crate::meta`] (`encode_b_position` and
// `decode_b_position`), because it is also used for several meta-cursor
// values.
//
// Value: an rkyv-archived `Receipt` (from `kardamom_types`).

/// # Panics
///
/// Never in practice: rkyv serialization of an owned, non-shared `Receipt`
/// has no failure path.
#[must_use]
pub(crate) fn encode_receipt_value(r: &Receipt) -> Vec<u8> {
    // The upstream `Receipt` type derives `rkyv::Archive`, `rkyv::Serialize`,
    // and `rkyv::Deserialize`.
    rkyv::to_bytes::<rkyv::rancor::Error>(r)
        .expect("Receipt rkyv serialize is infallible for owned data")
        .to_vec()
}

/// # Errors
///
/// Returns [`StateError::RkyvDecode`] if `bytes` does not decode as an
/// archived [`Receipt`].
pub(crate) fn decode_receipt_value(bytes: &[u8]) -> Result<Receipt, StateError> {
    rkyv::from_bytes::<Receipt, rkyv::rancor::Error>(bytes).map_err(|e| StateError::RkyvDecode {
        table: TABLE_RECEIPTS,
        detail: e.to_string(),
    })
}

// ---------- tx_hash_index ----------
//
// Key: `B256 tx_hash` (32 bytes). Value: `BPosition` (8 bytes, the same
// layout as the receipts-table key; see [`crate::meta::encode_b_position`]).
//
// Block commit populates one entry per receipt. On the read path,
// `eth_getTransactionReceipt(hash)` calls
// `StateDatabase::get_tx_position(hash)`, then `StateDatabase::get_receipt(pos)`.

#[must_use]
pub(crate) fn encode_tx_hash_key(hash: B256) -> [u8; 32] {
    hash.into()
}

#[must_use]
pub(crate) fn encode_tx_hash_value(pos: BPosition) -> [u8; 8] {
    crate::meta::encode_b_position(pos)
}

/// # Errors
///
/// Returns [`StateError::BadEncoding`] if `bytes` is not 8 bytes.
pub(crate) fn decode_tx_hash_value(bytes: &[u8]) -> Result<BPosition, StateError> {
    crate::meta::decode_b_position(bytes).map_err(|e| match e {
        StateError::BadEncoding { expected, got, .. } => StateError::BadEncoding {
            table: TABLE_TX_HASH_INDEX,
            expected,
            got,
        },
        other => other,
    })
}

// ---------- table iteration ----------

/// Walk every row of `db` in key order, and call `f(key, value)` for each
/// row.
///
/// Integrity checks, recovery scans, and the trie rebuild oracle share
/// this full-table cursor walk. It starts at `first`, steps with `next`,
/// and stops at the end, or earlier if `f` returns [`ControlFlow::Break`].
/// Errors from the cursor or from `f` propagate unchanged.
///
/// This function is generic over the transaction kind, so read-only and
/// read-write callers share one implementation.
pub(crate) fn for_each_row<K: signet_libmdbx::TransactionKind>(
    txn: &signet_libmdbx::tx::Tx<K>,
    db: signet_libmdbx::Database,
    mut f: impl FnMut(Vec<u8>, Vec<u8>) -> Result<std::ops::ControlFlow<()>, StateError>,
) -> Result<(), StateError> {
    let mut cur = txn.cursor(db)?;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
        if f(k, v)?.is_break() {
            return Ok(());
        }
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    Ok(())
}

/// Delete `key` from `db`. An absent key is fine — every trie-table and
/// hashed-mirror delete in this crate is idempotent by construction — so
/// this folds `MdbxError::NotFound` into success. Any other mdbx failure
/// still surfaces.
pub(crate) fn del_if_present(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    key: impl AsRef<[u8]>,
) -> Result<(), StateError> {
    match txn.del(db, key, None) {
        Ok(_) | Err(signet_libmdbx::MdbxError::NotFound) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Put `key` into `db`, unless it already holds a value. `value`'s bytes
/// are the same regardless of which caller's write lost the race, so a
/// redundant write of content-addressed data (code, by hash) is expected,
/// not an error.
pub(crate) fn put_if_absent(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    key: impl AsRef<[u8]>,
    value: impl AsRef<[u8]>,
) -> Result<(), StateError> {
    match txn.put(db, key, value, signet_libmdbx::WriteFlags::NO_OVERWRITE) {
        Ok(()) | Err(signet_libmdbx::MdbxError::KeyExist) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Write every account change. The block-commit writer and genesis
/// seeding both build this same [`AccountValue`], always with
/// `storage_root: B256::ZERO` — the accounts table never carries the
/// trie's per-account root, only `hashed_accounts` does (see
/// `crate::trie`) — so both share this one write.
///
/// This uses a cursor with sorted input: `BlockDelta`'s accounts vector
/// comes from `BTreeMap` iteration, so keys ascend, and a cursor upsert
/// walks down from its previous position instead of from the root. This
/// is the writer's per-block hot path, so the cursor here is
/// load-bearing, not incidental.
pub(crate) fn write_accounts(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    changes: &[AccountChange],
) -> Result<(), StateError> {
    let mut cur = txn.cursor(db)?;
    for change in changes {
        let key = encode_account_key(change.address);
        let v = AccountValue {
            nonce: change.nonce,
            balance: change.balance,
            code_hash: change.code_hash,
            storage_root: B256::ZERO,
        };
        cur.put(
            &key,
            &encode_account_value(&v),
            signet_libmdbx::WriteFlags::UPSERT,
        )?;
    }
    Ok(())
}

/// Write every new code entry. Code is content-addressed, so a key that
/// already exists carries the same bytes; [`put_if_absent`] skips the
/// redundant write.
pub(crate) fn write_code(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    entries: &[CodeEntry],
) -> Result<(), StateError> {
    for entry in entries {
        let key = encode_code_key(entry.code_hash);
        put_if_absent(txn, db, key, &entry.code)?;
    }
    Ok(())
}

/// Walk every row of `db` whose key starts with `prefix`, ascending, and
/// stops at the first key outside the prefix, or earlier if `f` returns
/// [`ControlFlow::Break`]. Errors from the cursor or from `f` propagate
/// unchanged.
///
/// This is [`for_each_row`]'s prefix-scoped counterpart: a `set_range` to
/// the prefix, then `next` while the key still starts with it. Every
/// cursor prefix scan in this crate — trie node deletion, trie leaf
/// collection — shares this loop.
pub(crate) fn for_each_prefix<K: signet_libmdbx::TransactionKind>(
    txn: &signet_libmdbx::tx::Tx<K>,
    db: signet_libmdbx::Database,
    prefix: &[u8],
    mut f: impl FnMut(Vec<u8>, Vec<u8>) -> Result<std::ops::ControlFlow<()>, StateError>,
) -> Result<(), StateError> {
    let mut cur = txn.cursor(db)?;
    let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(prefix)?;
    while let Some((k, v)) = item {
        if !k.starts_with(prefix) {
            break;
        }
        if f(k, v)?.is_break() {
            return Ok(());
        }
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    Ok(())
}

/// Delete every row in `db` whose key starts with `prefix`. Used to drop
/// a deleted account's whole hashed-storage or storage-trie subtree, and
/// to clear a rebuilt subtrie's stale nodes before its upserts land.
pub(crate) fn del_prefix(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    prefix: &[u8],
) -> Result<(), StateError> {
    // Collect first, then delete: a cursor cannot mutate the table it is
    // walking.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for_each_prefix(txn, db, prefix, |k, _v| {
        keys.push(k);
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    for k in keys {
        del_if_present(txn, db, k)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    #[test]
    fn account_value_roundtrip() {
        let v = AccountValue {
            nonce: 42,
            balance: U256::from(1_234_567_890u64),
            code_hash: b256!("0x0000000000000000000000000000000000000000000000000000000000000011"),
            storage_root: b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000022"
            ),
        };
        let bytes = encode_account_value(&v);
        let got = decode_account_value(&bytes).unwrap();
        assert_eq!(v, got);
    }

    #[test]
    fn storage_key_layout() {
        let addr = address!("0x00000000000000000000000000000000000000aa");
        let slot = B256::from(U256::from(7u64));
        let key = encode_storage_key(addr, slot);
        assert_eq!(&key[..20], addr.as_slice());
        assert_eq!(key[51], 7);
    }

    #[test]
    fn storage_value_roundtrip() {
        let v = U256::from(u128::MAX);
        let bytes = encode_storage_value(v);
        assert_eq!(decode_storage_value(&bytes).unwrap(), v);
    }

    #[test]
    fn storage_value_wrong_length_errors() {
        let err = decode_storage_value(&[0u8; 31]).unwrap_err();
        assert!(matches!(
            err,
            StateError::BadEncoding { table, expected: 32, got: 31 } if table == TABLE_STORAGE
        ));
    }

    #[test]
    fn block_key_layout_is_pinned() {
        // `headers` is an at-rest format. The key is the block number as 8
        // big-endian bytes. This test pins the exact byte layout; there is
        // no decoder for the key.
        assert_eq!(
            encode_block_key(0x0102_0304_0506_0708),
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(encode_block_key(0), [0u8; 8]);
    }

    #[test]
    fn header_value_layout_is_pinned() {
        // `headers` value is an at-rest format: term_id (i32 BE), then
        // term_offset (i32 BE), then l2_timestamp (u64 BE), then l1_origin
        // (u64 BE). Total: 24 bytes.
        let v = HeaderValue {
            end_tx_idx: BPosition {
                term_id: 0x0102_0304,
                term_offset: 0x0506_0708,
            },
            l2_timestamp: 0x1112_1314_1516_1718,
            l1_origin: 0x2122_2324_2526_2728,
        };
        assert_eq!(
            encode_header_value(&v),
            [
                0x01, 0x02, 0x03, 0x04, // term_id BE
                0x05, 0x06, 0x07, 0x08, // term_offset BE
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // l2_timestamp BE
                0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // l1_origin BE
            ]
        );
        assert_eq!(decode_header_value(&encode_header_value(&v)).unwrap(), v);
    }

    /// A state DB written before the origin field existed must keep
    /// working. Its 20-byte rows mean origin 0, which is what those chains
    /// actually had.
    #[test]
    fn pre_origin_header_rows_still_decode() {
        let legacy = [
            0x01, 0x02, 0x03, 0x04, // term_id BE
            0x05, 0x06, 0x07, 0x08, // term_offset BE
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // l2_timestamp BE
            0x00, 0x00, 0x00, 0x00, // the old reserved tail
        ];
        let v = decode_header_value(&legacy).unwrap();
        assert_eq!(v.l2_timestamp, 0x1112_1314_1516_1718);
        assert_eq!(v.l1_origin, 0);
        // A value that is neither width is corruption, not a third format version.
        assert!(decode_header_value(&legacy[..19]).is_err());
        assert!(decode_header_value(&[0u8; 32]).is_err());
    }

    #[test]
    fn block_key_is_big_endian_ordered() {
        let a = encode_block_key(1);
        let b = encode_block_key(2);
        let c = encode_block_key(256);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn tx_hash_index_roundtrip() {
        let hash = b256!("0x000000000000000000000000000000000000000000000000000000000000dead");
        let pos = BPosition {
            term_id: 7,
            term_offset: 12345,
        };
        let k = encode_tx_hash_key(hash);
        let v = encode_tx_hash_value(pos);
        assert_eq!(k.len(), 32);
        assert_eq!(v.len(), 8);
        assert_eq!(decode_tx_hash_value(&v).unwrap(), pos);
    }

    #[test]
    fn b_position_key_lexicographically_ordered() {
        use crate::meta::encode_b_position;
        let a = encode_b_position(BPosition {
            term_id: 0,
            term_offset: 1,
        });
        let b = encode_b_position(BPosition {
            term_id: 0,
            term_offset: 2,
        });
        let c = encode_b_position(BPosition {
            term_id: 1,
            term_offset: 0,
        });
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn receipt_value_roundtrip() {
        // Check that the rkyv codec round-trips a non-trivial Receipt.
        use kardamom_types::WireLog;
        let r = Receipt {
            tx_idx: BPosition {
                term_id: 1,
                term_offset: 1024,
            },
            tx_hash: b256!("0x000000000000000000000000000000000000000000000000000000000000beef"),
            status: true,
            gas_used: 42_000,
            logs: vec![WireLog {
                address: address!("0x00000000000000000000000000000000000000bb"),
                topics: vec![B256::ZERO],
                data: bytes::Bytes::from_static(b"hi"),
            }],
            write_set_hash: b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000099"
            ),
            ..Default::default()
        };
        let bytes = encode_receipt_value(&r);
        let got = decode_receipt_value(&bytes).unwrap();
        assert_eq!(r, got);
    }
}
