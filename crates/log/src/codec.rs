//! rkyv zero-copy access helpers.
//!
//! The hot path reads `Archived<T>` views straight from Aeron fragment
//! buffers, with no allocation and no decode pass. Convert to an owned `T`
//! only when the caller asks for it (`materialize`), for example when the
//! value must outlive the fragment buffer.
//!
//! This crate uses rkyv v0.8, not the earlier bincode choice. Wire types
//! live in `kardamom-types`; this crate is transport only.

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::LogError;

/// Encode a wire value to a fresh `AlignedVec` suitable for handing to
/// `rusteron`'s `offer()`.
pub fn encode<T>(value: &T) -> Result<AlignedVec, LogError>
where
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    rkyv::to_bytes::<rancor::Error>(value).map_err(|e| LogError::Codec(e.to_string()))
}

/// Zero-copy access: borrow an `&Archived<T>` view of `bytes` without
/// allocating. Returns an error if the bytes are not a valid rkyv archive
/// for `T`.
pub fn access<T>(bytes: &[u8]) -> Result<&T::Archived, LogError>
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    rkyv::access::<T::Archived, rancor::Error>(bytes).map_err(|e| LogError::Codec(e.to_string()))
}

/// Owning decode: copy an `Archived<T>` into an owned `T`. Use when the value
/// must outlive the fragment buffer or when downstream code needs `T`
/// directly. Hot-path consumers prefer [`access`] instead.
pub fn materialize<T>(bytes: &[u8]) -> Result<T, LogError>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    rkyv::from_bytes::<T, rancor::Error>(bytes).map_err(|e| LogError::Codec(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_types::{AccountChange, BlockDelta, CodeEntry, StorageChange};

    /// The BAL payload (`BlockDelta`) must survive an encode-materialize
    /// round trip unchanged. The executor publishes this on tx_bal, and the
    /// validator decodes it to cross-check its re-execution.
    #[test]
    fn block_delta_bal_roundtrips() {
        let delta = BlockDelta {
            block_number: 42,
            accounts: vec![
                AccountChange {
                    address: Address::from([0x11; 20]),
                    nonce: 7,
                    balance: U256::from(1_000_000u64),
                    code_hash: B256::from([0xaa; 32]),
                },
                AccountChange {
                    address: Address::from([0x22; 20]),
                    nonce: 0,
                    balance: U256::ZERO,
                    code_hash: B256::ZERO,
                },
            ],
            storage: vec![StorageChange {
                address: Address::from([0x11; 20]),
                key: B256::from([0x01; 32]),
                value: U256::from(99u64),
            }],
            code: vec![CodeEntry {
                code_hash: B256::from([0xaa; 32]),
                code: vec![0x60, 0x00, 0x60, 0x00].into(),
            }],
            receipts: Vec::new(),
        };

        let bytes = encode(&delta).expect("encode BlockDelta");
        let decoded: BlockDelta = materialize(&bytes).expect("materialize BlockDelta");
        assert_eq!(decoded, delta);
    }
}
