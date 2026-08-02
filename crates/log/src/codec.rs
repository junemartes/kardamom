//! rkyv zero-copy access helpers.
//!
//! The hot path reads `Archived<T>` views straight out of Aeron fragment
//! buffers — no allocation, no decode pass. Convert to an owned `T` only when
//! the caller explicitly asks (`materialize`), e.g. when they need to outlive
//! the fragment buffer.
//!
//! Per: rkyv v0.8 replaces the earlier bincode choice. Wire types live
//! in `kardamom-types`; this crate is transport only.

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

/// An owned, validated rkyv frame: ONE aligned copy of the wire bytes,
/// checked at construction, then read IN PLACE forever after — the
/// zero-copy alternative to [`materialize`] for values that must outlive
/// the Aeron fragment buffer.
///
/// `Arc`-backed so clones are refcount bumps (the validator's parallel
/// batches clone envelopes per batch; with owned `TxEnvelope` each clone
/// copied the raw tx).
pub struct ArchivedFrame<T> {
    bytes: std::sync::Arc<AlignedVec>,
    _t: std::marker::PhantomData<T>,
}

impl<T> Clone for ArchivedFrame<T> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            _t: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for ArchivedFrame<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArchivedFrame<{}>({}B)",
            std::any::type_name::<T>(),
            self.bytes.len()
        )
    }
}

impl<T> ArchivedFrame<T>
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    /// Copy `bytes` into an aligned buffer and VALIDATE once. All later
    /// reads use the unchecked accessor — safe because the buffer is
    /// immutable from here on.
    pub fn new(bytes: &[u8]) -> Result<Self, LogError> {
        let mut av = AlignedVec::with_capacity(bytes.len());
        av.extend_from_slice(bytes);
        rkyv::access::<T::Archived, rancor::Error>(&av)
            .map_err(|e| LogError::Codec(e.to_string()))?;
        Ok(Self {
            bytes: std::sync::Arc::new(av),
            _t: std::marker::PhantomData,
        })
    }

    /// Encode an owned value into a frame (rare paths: archive refetch,
    /// tests). One encode + one validation.
    pub fn from_owned(value: &T) -> Result<Self, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        let av = encode(value)?;
        rkyv::access::<T::Archived, rancor::Error>(&av)
            .map_err(|e| LogError::Codec(e.to_string()))?;
        Ok(Self {
            bytes: std::sync::Arc::new(av),
            _t: std::marker::PhantomData,
        })
    }

    /// The underlying wire bytes (verbatim republish, tests).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The archived view. Zero-copy, zero-cost after construction.
    #[must_use]
    pub fn get(&self) -> &T::Archived {
        // SAFETY: validated in `new`/`from_owned`; the buffer is behind an
        // Arc and never mutated.
        unsafe { rkyv::access_unchecked::<T::Archived>(&self.bytes) }
    }

    /// Materialize an owned `T` (off the hot path: forensics, dumps).
    pub fn to_owned_value(&self) -> Result<T, LogError>
    where
        T::Archived: Deserialize<T, HighDeserializer<rancor::Error>>,
    {
        materialize(&self.bytes)
    }
}

/// Zero-copy field accessors for the tx_data hot path: every tx crosses
/// this frame once per subscriber, so reads must not allocate.
impl ArchivedFrame<kardamom_types::TxEnvelope> {
    #[must_use]
    pub fn correlation_id(&self) -> u64 {
        self.get().correlation_id.to_native()
    }
    #[must_use]
    pub fn sender(&self) -> kardamom_types::Address {
        kardamom_types::wire::address_from_archived(&self.get().sender)
    }
    #[must_use]
    pub fn tx_hash(&self) -> kardamom_types::B256 {
        kardamom_types::wire::b256_from_archived(&self.get().tx_hash)
    }
    /// The raw signed transaction bytes, borrowed from the frame.
    #[must_use]
    pub fn raw_tx(&self) -> &[u8] {
        self.get().raw_tx.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_types::{AccountChange, BlockDelta, CodeEntry, StorageChange};

    /// The BAL payload (`BlockDelta`) must survive an encode → materialize
    /// round-trip unchanged — this is what the executor publishes on tx_bal and
    /// the validator decodes to cross-check its re-execution.
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
