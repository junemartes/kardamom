//! rkyv zero-copy access helpers.
//!
//! The hot path reads `Archived<T>` views straight out of Aeron fragment
//! buffers — no allocation, no decode pass. Convert to an owned `T` only when
//! the caller explicitly asks (`materialize`), e.g. when they need to outlive
//! the fragment buffer.
//!
//! Per D-Sh2: rkyv v0.8 replaces the earlier bincode choice. Wire types live
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
