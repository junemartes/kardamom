//! libmdbx schema. Seven named tables, each with a fixed key/value encoding.
//!
//! | Table           | Key                              | Value                                            |
//! |-----------------|----------------------------------|--------------------------------------------------|
//! | `accounts`      | `Address` (20 B)                 | RLP `(u64 nonce, U256 balance, B256 code_hash, B256 storage_root)` |
//! | `storage`       | `Address ++ B256 key` (52 B)     | `U256 value` (32 B, big-endian)                  |
//! | `code`          | `B256 code_hash` (32 B)          | raw bytecode                                     |
//! | `headers`       | `u64 block_number` (8 B BE)      | encoded `(BPosition end_tx_idx, u64 l2_timestamp)`, no state root |
//! | `receipts`      | `BPosition tx_idx` (8 B)         | encoded `Receipt` (rkyv archive, owned at rest)  |
//! | `tx_hash_index` | `B256 tx_hash` (32 B)            | `BPosition` (8 B, i32 BE term_id ++ i32 BE term_offset), for `eth_getTransactionReceipt(hash)` |
//! | `meta`          | `&[u8]` (well-known keys, below) | varies, see `meta.rs`                            |
//!
//! Big-endian encoding on the `headers` key keeps `block_number` ordered
//! under mdbx's lexicographic cursor. The cold-start scan depends on this
//! order. `BPosition` encoding (term_id i32 BE, then term_offset i32 BE, 8
//! bytes total) is lexicographically ordered by `(term_id, term_offset)`.
//! The `receipts` table has the same property.

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use kardamom_types::{BPosition, Receipt};

use crate::error::StateError;

pub const TABLE_ACCOUNTS: &str = "accounts";
pub const TABLE_STORAGE: &str = "storage";
pub const TABLE_CODE: &str = "code";
pub const TABLE_HEADERS: &str = "headers";
pub const TABLE_RECEIPTS: &str = "receipts";
pub const TABLE_TX_HASH_INDEX: &str = "tx_hash_index";
pub const TABLE_META: &str = "meta";

// --- Incremental state-trie tables (schema v2; see crate::trie) ---
//
// These tables store intermediate branch nodes, keyed by the trie path.
// The path uses raw, unpacked nibbles, one nibble per byte, so lexicographic
// mdbx order matches trie order. See `trie/cursor.rs::node_key`. Storage-trie
// keys prepend the 32-byte account hash.
//
// The hashed-state mirror holds the leaves, keyed by keccak. See
// docs/specs/2026-06-23-incremental-trie-design.md.
pub const TABLE_ACCOUNT_TRIE: &str = "account_trie";
pub const TABLE_STORAGE_TRIE: &str = "storage_trie";
pub const TABLE_HASHED_ACCOUNTS: &str = "hashed_accounts";
pub const TABLE_HASHED_STORAGE: &str = "hashed_storage";

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
pub struct AccountValue {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

pub fn encode_account_key(addr: Address) -> [u8; 20] {
    addr.into_array()
}

pub fn encode_account_value(v: &AccountValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(96);
    v.encode(&mut buf);
    buf
}

pub fn decode_account_value(bytes: &[u8]) -> Result<AccountValue, StateError> {
    AccountValue::decode(&mut &bytes[..]).map_err(StateError::from)
}

// ---------- storage ----------

/// Storage key is `Address (20 B) ++ B256 slot (32 B) = 52 B`.
pub fn encode_storage_key(addr: Address, slot: B256) -> [u8; 52] {
    let mut out = [0u8; 52];
    out[..20].copy_from_slice(addr.as_slice());
    out[20..].copy_from_slice(slot.as_slice());
    out
}

pub fn encode_storage_value(v: U256) -> [u8; 32] {
    v.to_be_bytes::<32>()
}

pub fn decode_storage_value(bytes: &[u8]) -> Result<U256, StateError> {
    if bytes.len() != 32 {
        return Err(StateError::BadEncoding {
            table: TABLE_STORAGE,
            expected: 32,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(U256::from_be_bytes(arr))
}

// ---------- code ----------

pub fn encode_code_key(hash: B256) -> [u8; 32] {
    hash.into()
}

// code value = raw bytes; no codec needed

// ---------- headers ----------
//
// Headers do not carry a state-root commitment. The encoded value is
// `(end_tx_idx: BPosition, l2_timestamp: u64, l1_origin: u64)`. This uses a
// hand-rolled, fixed-width encoding (8 + 8 + 8 = 24 bytes) instead of RLP.
// The row has a fixed size, and `BPosition` is not an RLP-native type.
//
// The origin field added 4 bytes to the 4 reserved bytes, so rows grew
// from 20 to 24 bytes. Decoding still accepts the 20-byte form and reports
// `l1_origin: 0`. This is what a pre-origin chain meant, so an existing
// state DB keeps reading without a migration.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderValue {
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    /// The L1 block number for the epoch this block belongs to. See
    /// `docs/agents/l1-origin-deposit-derivation-spec.md`.
    pub l1_origin: u64,
}

pub fn encode_block_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub fn encode_header_value(v: &HeaderValue) -> [u8; 24] {
    let mut out = [0u8; 24];
    out[..4].copy_from_slice(&v.end_tx_idx.term_id.to_be_bytes());
    out[4..8].copy_from_slice(&v.end_tx_idx.term_offset.to_be_bytes());
    out[8..16].copy_from_slice(&v.l2_timestamp.to_be_bytes());
    out[16..24].copy_from_slice(&v.l1_origin.to_be_bytes());
    out
}

pub fn decode_header_value(bytes: &[u8]) -> Result<HeaderValue, StateError> {
    // A 20-byte value is the pre-origin row. Anything else is corruption.
    if bytes.len() != 24 && bytes.len() != 20 {
        return Err(StateError::BadEncoding {
            table: TABLE_HEADERS,
            expected: 24,
            got: bytes.len(),
        });
    }
    Ok(HeaderValue {
        end_tx_idx: BPosition {
            term_id: i32::from_be_bytes(bytes[..4].try_into().expect("4 bytes")),
            term_offset: i32::from_be_bytes(bytes[4..8].try_into().expect("4 bytes")),
        },
        l2_timestamp: u64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes")),
        l1_origin: if bytes.len() == 24 {
            u64::from_be_bytes(bytes[16..24].try_into().expect("8 bytes"))
        } else {
            0
        },
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

pub fn encode_receipt_value(r: &Receipt) -> Vec<u8> {
    // The upstream `Receipt` type derives `rkyv::Archive`, `rkyv::Serialize`,
    // and `rkyv::Deserialize`.
    rkyv::to_bytes::<rkyv::rancor::Error>(r)
        .expect("Receipt rkyv serialize is infallible for owned data")
        .to_vec()
}

pub fn decode_receipt_value(bytes: &[u8]) -> Result<Receipt, StateError> {
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

pub fn encode_tx_hash_key(hash: B256) -> [u8; 32] {
    hash.into()
}

pub fn encode_tx_hash_value(pos: BPosition) -> [u8; 8] {
    crate::meta::encode_b_position(pos)
}

pub fn decode_tx_hash_value(bytes: &[u8]) -> Result<BPosition, StateError> {
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
