//! Fixed-layout codecs for on-disk values.
//!
//! Keys are written by the caller (always fixed-layout for our tables).
//! Values use these helpers so we don't sprinkle ad-hoc encoding through
//! the store.

use alloy_consensus::Header;
use alloy_primitives::{B256, U256};
use alloy_rlp::{Decodable, Encodable};

use crate::error::StateError;

/// One row of the `accounts` table.
///
/// Fixed 72-byte layout: 32 B balance ‖ 8 B nonce (BE) ‖ 32 B code_hash.
/// No RLP overhead on the hottest table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountRow {
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
}

pub const ACCOUNT_ROW_LEN: usize = 32 + 8 + 32;

impl AccountRow {
    pub fn encode(&self) -> [u8; ACCOUNT_ROW_LEN] {
        let mut buf = [0u8; ACCOUNT_ROW_LEN];
        buf[..32].copy_from_slice(&self.balance.to_be_bytes::<32>());
        buf[32..40].copy_from_slice(&self.nonce.to_be_bytes());
        buf[40..72].copy_from_slice(self.code_hash.as_slice());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateError> {
        if bytes.len() != ACCOUNT_ROW_LEN {
            return Err(StateError::codec(format!(
                "AccountRow expects {ACCOUNT_ROW_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        let balance = U256::from_be_slice(&bytes[..32]);
        let mut nonce_buf = [0u8; 8];
        nonce_buf.copy_from_slice(&bytes[32..40]);
        let nonce = u64::from_be_bytes(nonce_buf);
        let code_hash = B256::from_slice(&bytes[40..72]);
        Ok(Self {
            balance,
            nonce,
            code_hash,
        })
    }
}

/// Storage value: a `U256` stored as 32 big-endian bytes.
pub fn encode_storage_value(v: U256) -> [u8; 32] {
    v.to_be_bytes::<32>()
}

pub fn decode_storage_value(bytes: &[u8]) -> Result<U256, StateError> {
    if bytes.len() != 32 {
        return Err(StateError::codec(format!(
            "storage value expects 32 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(U256::from_be_slice(bytes))
}

/// Header RLP via alloy. This is the same encoding L1 uses, so a future
/// validator reads it without translation.
pub fn encode_header(header: &Header) -> Vec<u8> {
    let mut buf = Vec::with_capacity(header.length());
    header.encode(&mut buf);
    buf
}

pub fn decode_header(bytes: &[u8]) -> Result<Header, StateError> {
    let mut slice = bytes;
    Header::decode(&mut slice).map_err(|e| StateError::codec(format!("header rlp: {e}")))
}

/// Composite key for the `storage` table: 20 B address ‖ 32 B slot = 52 B.
pub const STORAGE_KEY_LEN: usize = 20 + 32;

pub fn encode_storage_key(addr: &alloy_primitives::Address, slot: &B256) -> [u8; STORAGE_KEY_LEN] {
    let mut buf = [0u8; STORAGE_KEY_LEN];
    buf[..20].copy_from_slice(addr.as_slice());
    buf[20..].copy_from_slice(slot.as_slice());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, b256};

    fn sample_row(seed: u8) -> AccountRow {
        AccountRow {
            balance: U256::from(seed as u64) * U256::from(1_000_000_000u64),
            nonce: seed as u64,
            code_hash: B256::repeat_byte(seed),
        }
    }

    #[test]
    fn account_row_roundtrip_boundaries() {
        let cases = [
            AccountRow {
                balance: U256::ZERO,
                nonce: 0,
                code_hash: B256::ZERO,
            },
            AccountRow {
                balance: U256::from(1u64),
                nonce: 1,
                code_hash: B256::repeat_byte(0x01),
            },
            AccountRow {
                balance: U256::MAX,
                nonce: u64::MAX,
                code_hash: B256::repeat_byte(0xff),
            },
            sample_row(42),
        ];
        for row in cases {
            let bytes = row.encode();
            assert_eq!(bytes.len(), ACCOUNT_ROW_LEN);
            let decoded = AccountRow::decode(&bytes).expect("decode");
            assert_eq!(row, decoded);
        }
    }

    #[test]
    fn account_row_decode_rejects_wrong_length() {
        assert!(AccountRow::decode(&[]).is_err());
        assert!(AccountRow::decode(&[0u8; 71]).is_err());
        assert!(AccountRow::decode(&[0u8; 73]).is_err());
    }

    #[test]
    fn storage_value_roundtrip_boundaries() {
        let cases = [
            U256::ZERO,
            U256::from(1u64),
            U256::from(u64::MAX),
            U256::MAX,
            U256::from(0xdead_beefu64),
        ];
        for v in cases {
            let bytes = encode_storage_value(v);
            let back = decode_storage_value(&bytes).expect("decode");
            assert_eq!(v, back);
        }
    }

    #[test]
    fn storage_value_decode_rejects_wrong_length() {
        assert!(decode_storage_value(&[]).is_err());
        assert!(decode_storage_value(&[0u8; 31]).is_err());
        assert!(decode_storage_value(&[0u8; 33]).is_err());
    }

    #[test]
    fn header_roundtrip_preserves_fields() {
        // alloy's Header derives RLP itself; this test mostly guards against us
        // doing something silly with the bytes (truncation, slicing).
        let header = Header {
            parent_hash: b256!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            ),
            number: 7,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            timestamp: 1_700_000_000,
            ..Default::default()
        };
        let bytes = encode_header(&header);
        let decoded = decode_header(&bytes).expect("decode");
        assert_eq!(header.parent_hash, decoded.parent_hash);
        assert_eq!(header.number, decoded.number);
        assert_eq!(header.gas_limit, decoded.gas_limit);
        assert_eq!(header.gas_used, decoded.gas_used);
        assert_eq!(header.timestamp, decoded.timestamp);
    }

    #[test]
    fn storage_key_encoding_is_address_then_slot() {
        let addr = Address::from([0x11u8; 20]);
        let slot = B256::repeat_byte(0x22);
        let key = encode_storage_key(&addr, &slot);
        assert_eq!(key.len(), STORAGE_KEY_LEN);
        assert_eq!(&key[..20], addr.as_slice());
        assert_eq!(&key[20..], slot.as_slice());
    }
}
