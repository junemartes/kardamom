//! rkyv `with` adapters for alloy-primitives types.
//!
//! alloy's `Address`, `B256`, `U256`, and `bytes::Bytes` do not derive
//! `rkyv::Archive` upstream. We provide thin `with` wrappers that archive each
//! as a fixed-size byte array (or, for `Bytes`, a length-prefixed byte slice).
//! Annotate a field with `#[rkyv(with = wire::AddressBytes)]` (etc.) to opt in.
//!
//! This keeps wire types ergonomic (`pub sender: Address`) while keeping rkyv
//! happy and the on-wire format stable.

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rancor::Fallible;
use rkyv::{
    Archive, Archived, Deserialize, Place, Resolver, Serialize,
    rancor,
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};

// ===========================================================================
// Address ↔ [u8; 20]
// ===========================================================================

/// rkyv `with` adapter that archives `alloy_primitives::Address` as `[u8; 20]`.
pub struct AddressBytes;

impl ArchiveWith<Address> for AddressBytes {
    type Archived = Archived<[u8; 20]>;
    type Resolver = Resolver<[u8; 20]>;

    fn resolve_with(field: &Address, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let bytes: [u8; 20] = field.into_array();
        bytes.resolve(resolver, out);
    }
}

impl<S> SerializeWith<Address, S> for AddressBytes
where
    S: Fallible + ?Sized,
    [u8; 20]: Serialize<S>,
{
    fn serialize_with(field: &Address, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let bytes: [u8; 20] = field.into_array();
        bytes.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<[u8; 20]>, Address, D> for AddressBytes
where
    D: Fallible + ?Sized,
    Archived<[u8; 20]>: Deserialize<[u8; 20], D>,
{
    fn deserialize_with(
        field: &Archived<[u8; 20]>,
        deserializer: &mut D,
    ) -> Result<Address, D::Error> {
        let bytes: [u8; 20] = field.deserialize(deserializer)?;
        Ok(Address::from(bytes))
    }
}

// ===========================================================================
// B256 ↔ [u8; 32]
// ===========================================================================

/// rkyv `with` adapter that archives `alloy_primitives::B256` as `[u8; 32]`.
pub struct B256Bytes;

impl ArchiveWith<B256> for B256Bytes {
    type Archived = Archived<[u8; 32]>;
    type Resolver = Resolver<[u8; 32]>;

    fn resolve_with(field: &B256, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let bytes: [u8; 32] = field.0;
        bytes.resolve(resolver, out);
    }
}

impl<S> SerializeWith<B256, S> for B256Bytes
where
    S: Fallible + ?Sized,
    [u8; 32]: Serialize<S>,
{
    fn serialize_with(field: &B256, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        field.0.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<[u8; 32]>, B256, D> for B256Bytes
where
    D: Fallible + ?Sized,
    Archived<[u8; 32]>: Deserialize<[u8; 32], D>,
{
    fn deserialize_with(field: &Archived<[u8; 32]>, deserializer: &mut D) -> Result<B256, D::Error> {
        let bytes: [u8; 32] = field.deserialize(deserializer)?;
        Ok(B256::from(bytes))
    }
}

// ===========================================================================
// U256 ↔ [u8; 32] (big-endian)
// ===========================================================================

/// rkyv `with` adapter that archives `alloy_primitives::U256` as a
/// big-endian `[u8; 32]`.
pub struct U256Bytes;

impl ArchiveWith<U256> for U256Bytes {
    type Archived = Archived<[u8; 32]>;
    type Resolver = Resolver<[u8; 32]>;

    fn resolve_with(field: &U256, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let bytes: [u8; 32] = field.to_be_bytes();
        bytes.resolve(resolver, out);
    }
}

impl<S> SerializeWith<U256, S> for U256Bytes
where
    S: Fallible + ?Sized,
    [u8; 32]: Serialize<S>,
{
    fn serialize_with(field: &U256, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let bytes: [u8; 32] = field.to_be_bytes();
        bytes.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<[u8; 32]>, U256, D> for U256Bytes
where
    D: Fallible + ?Sized,
    Archived<[u8; 32]>: Deserialize<[u8; 32], D>,
{
    fn deserialize_with(field: &Archived<[u8; 32]>, deserializer: &mut D) -> Result<U256, D::Error> {
        let bytes: [u8; 32] = field.deserialize(deserializer)?;
        Ok(U256::from_be_bytes(bytes))
    }
}

// ===========================================================================
// Bytes ↔ Vec<u8>
// ===========================================================================

/// rkyv `with` adapter that archives `bytes::Bytes` as a `Vec<u8>`.
///
/// rkyv 0.8 ships a `bytes-1` feature that derives `Archive` for `Bytes`
/// directly, but that produces an `ArchivedBytes` newtype which is awkward to
/// expose. Adapting through `Vec<u8>` keeps the archived view as a plain byte
/// slice that downstream tooling can read trivially.
pub struct BytesVec;

impl ArchiveWith<Bytes> for BytesVec {
    type Archived = Archived<Vec<u8>>;
    type Resolver = Resolver<Vec<u8>>;

    fn resolve_with(field: &Bytes, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let v: Vec<u8> = field.to_vec();
        v.resolve(resolver, out);
    }
}

impl<S> SerializeWith<Bytes, S> for BytesVec
where
    S: Fallible + ?Sized,
    Vec<u8>: Serialize<S>,
{
    fn serialize_with(field: &Bytes, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let v: Vec<u8> = field.to_vec();
        v.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<Vec<u8>>, Bytes, D> for BytesVec
where
    D: Fallible + ?Sized,
    Archived<Vec<u8>>: Deserialize<Vec<u8>, D>,
{
    fn deserialize_with(field: &Archived<Vec<u8>>, deserializer: &mut D) -> Result<Bytes, D::Error> {
        let v: Vec<u8> = field.deserialize(deserializer)?;
        Ok(Bytes::from(v))
    }
}

// ===========================================================================
// Vec<B256> ↔ Vec<[u8; 32]>
// ===========================================================================

/// rkyv `with` adapter that archives `Vec<B256>` as `Vec<[u8; 32]>`.
pub struct VecB256;

impl ArchiveWith<Vec<B256>> for VecB256 {
    type Archived = Archived<Vec<[u8; 32]>>;
    type Resolver = Resolver<Vec<[u8; 32]>>;

    fn resolve_with(field: &Vec<B256>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let v: Vec<[u8; 32]> = field.iter().map(|b| b.0).collect();
        v.resolve(resolver, out);
    }
}

impl<S> SerializeWith<Vec<B256>, S> for VecB256
where
    S: Fallible + ?Sized,
    Vec<[u8; 32]>: Serialize<S>,
{
    fn serialize_with(field: &Vec<B256>, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let v: Vec<[u8; 32]> = field.iter().map(|b| b.0).collect();
        v.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<Vec<[u8; 32]>>, Vec<B256>, D> for VecB256
where
    D: Fallible + ?Sized,
    Archived<Vec<[u8; 32]>>: Deserialize<Vec<[u8; 32]>, D>,
{
    fn deserialize_with(
        field: &Archived<Vec<[u8; 32]>>,
        deserializer: &mut D,
    ) -> Result<Vec<B256>, D::Error> {
        let v: Vec<[u8; 32]> = field.deserialize(deserializer)?;
        Ok(v.into_iter().map(B256::from).collect())
    }
}
