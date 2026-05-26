//! libmdbx schema. Seven named tables, each with a fixed key/value encoding.
//!
//! | Table           | Key                              | Value                                            |
//! |-----------------|----------------------------------|--------------------------------------------------|
//! | `accounts`      | `Address` (20 B)                 | RLP `(u64 nonce, U256 balance, B256 code_hash, B256 storage_root)` |
//! | `storage`       | `Address ++ B256 key` (52 B)     | `U256 value` (32 B, big-endian)                  |
//! | `code`          | `B256 code_hash` (32 B)          | raw bytecode                                     |
//! | `headers`       | `u64 block_number` (8 B BE)      | encoded `(BPosition end_tx_idx, u64 l2_timestamp)` — **no state root** |
//! | `receipts`      | `BPosition tx_idx` (8 B)         | encoded `Receipt` (rkyv archive, owned at rest)  |
//! | `tx_hash_index` | `B256 tx_hash` (32 B)            | `BPosition` (8 B, i32 BE term_id ++ i32 BE term_offset) — feeds S1 `eth_getTransactionReceipt(hash)` |
//! | `meta`          | `&[u8]` (well-known keys, below) | varies — see `meta.rs`                           |
//!
//! BE encoding on the `headers` key keeps `block_number` ordered under mdbx's
//! lexicographic cursor; we depend on that for the cold-start scan. `BPosition`
//! encoding (term_id i32 BE ++ term_offset i32 BE, 8 bytes) is lexicographically
//! ordered by `(term_id, term_offset)` — same property holds for `receipts`.

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use types::{BPosition, Receipt};

use crate::error::StateError;

pub const TABLE_ACCOUNTS: &str = "accounts";
pub const TABLE_STORAGE: &str = "storage";
pub const TABLE_CODE: &str = "code";
pub const TABLE_HEADERS: &str = "headers";
pub const TABLE_RECEIPTS: &str = "receipts";
pub const TABLE_TX_HASH_INDEX: &str = "tx_hash_index";
pub const TABLE_META: &str = "meta";

pub const ALL_TABLES: &[&str] = &[
    TABLE_ACCOUNTS,
    TABLE_STORAGE,
    TABLE_CODE,
    TABLE_HEADERS,
    TABLE_RECEIPTS,
    TABLE_TX_HASH_INDEX,
    TABLE_META,
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
// Per, headers do NOT carry a state-root commitment. The encoded value
// is `(end_tx_idx: BPosition, l2_timestamp: u64)`. We use a hand-rolled fixed-
// width encoding (8 + 8 + 4 reserved = 20 bytes) instead of RLP — the row is
// fixed-size and BPosition is not an RLP-native type.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderValue {
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}

pub fn encode_block_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub fn decode_block_key(bytes: &[u8]) -> Result<u64, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: TABLE_HEADERS,
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

pub fn encode_header_value(v: &HeaderValue) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[..4].copy_from_slice(&v.end_tx_idx.term_id.to_be_bytes());
    out[4..8].copy_from_slice(&v.end_tx_idx.term_offset.to_be_bytes());
    out[8..16].copy_from_slice(&v.l2_timestamp.to_be_bytes());
    // bytes 16..20 reserved (zero-filled) for forward-compat
    out
}

pub fn decode_header_value(bytes: &[u8]) -> Result<HeaderValue, StateError> {
    if bytes.len() < 16 {
        return Err(StateError::BadEncoding {
            table: TABLE_HEADERS,
            expected: 16,
            got: bytes.len(),
        });
    }
    let mut t_id = [0u8; 4];
    t_id.copy_from_slice(&bytes[..4]);
    let mut t_off = [0u8; 4];
    t_off.copy_from_slice(&bytes[4..8]);
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&bytes[8..16]);
    Ok(HeaderValue {
        end_tx_idx: BPosition {
            term_id: i32::from_be_bytes(t_id),
            term_offset: i32::from_be_bytes(t_off),
        },
        l2_timestamp: u64::from_be_bytes(ts),
    })
}

// ---------- receipts ----------
//
// Key: BPosition (8 bytes — i32 BE term_id ++ i32 BE term_offset). The codec
// itself lives in [`crate::meta`] (`encode_b_position` / `decode_b_position`)
// since it's used for both receipt keys and several meta-cursor values.
// Value: rkyv-archived `Receipt` (kardamom-types).

pub fn encode_receipt_value(r: &Receipt) -> Vec<u8> {
    // `Receipt` upstream derives `rkyv::Archive/Serialize/Deserialize`.
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
// Key: `B256 tx_hash` (32 B). Value: `BPosition` (8 B, same layout as
// the receipts-table key — see [`crate::meta::encode_b_position`]). Populated
// during block commit (one entry per receipt). Read path: S1 proxy's
// `eth_getTransactionReceipt(hash)` calls `StateDatabase::get_tx_position(hash)`
// → `StateDatabase::get_receipt(pos)`.

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
    fn block_key_is_big_endian_ordered() {
        let a = encode_block_key(1);
        let b = encode_block_key(2);
        let c = encode_block_key(256);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn block_key_roundtrip() {
        for n in [0u64, 1, 250, u64::MAX] {
            assert_eq!(decode_block_key(&encode_block_key(n)).unwrap(), n);
        }
    }

    #[test]
    fn header_value_roundtrip_no_state_root() {
        //: headers carry NO state_root_commitment.
        let v = HeaderValue {
            end_tx_idx: BPosition {
                term_id: 3,
                term_offset: 4096,
            },
            l2_timestamp: 1_700_000_000,
        };
        let bytes = encode_header_value(&v);
        assert_eq!(decode_header_value(&bytes).unwrap(), v);
    }

    #[test]
    fn tx_hash_index_roundtrip() {
        //: tx_hash → BPosition lookup table.
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
        // Sanity-check that the rkyv codec round-trips a non-trivial Receipt.
        use types::WireLog;
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
        };
        let bytes = encode_receipt_value(&r);
        let got = decode_receipt_value(&bytes).unwrap();
        assert_eq!(r, got);
    }
}
