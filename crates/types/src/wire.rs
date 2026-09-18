//! rkyv `with` adapters for alloy-primitives types.
//!
//! alloy's `Address`, `B256`, `U256`, and `bytes::Bytes` types do not
//! derive `rkyv::Archive` upstream. This module provides thin `with`
//! wrappers that archive each as a fixed-size byte array, or for `Bytes`,
//! as a length-prefixed byte slice. Annotate a field with
//! `#[rkyv(with = wire::AddressBytes)]`, or the matching wrapper, to opt in.
//!
//! This keeps wire types easy to use (`pub sender: Address`), while keeping
//! rkyv working and the on-wire format stable.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rancor::Fallible;
use rkyv::{
    Archive, Archived, Deserialize, Place, Resolver, Serialize, rancor,
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};

/// Defines one rkyv `with` adapter `$name`, converting `$ty` to and from
/// a plain-old-data archive shape `$pod`. `$to` converts a `&$ty` field
/// (bound to `$to_in`) into `$pod`, for both `resolve_with` and
/// `serialize_with`. `$from` converts a deserialized `$pod` (bound to
/// `$from_in`) back into `$ty`, for `deserialize_with`.
///
/// Every adapter in this module is this exact `ArchiveWith` +
/// `SerializeWith` + `DeserializeWith` trio around one convert-to-POD,
/// convert-back pair; this macro is their one shared body.
macro_rules! wire_adapter {
    (
        $name:ident, $ty:ty, $pod:ty, $doc:literal,
        |$to_in:ident| $to:expr,
        |$from_in:ident| $from:expr $(,)?
    ) => {
        #[doc = $doc]
        pub struct $name;

        impl ArchiveWith<$ty> for $name {
            type Archived = Archived<$pod>;
            type Resolver = Resolver<$pod>;

            fn resolve_with(field: &$ty, resolver: Self::Resolver, out: Place<Self::Archived>) {
                let $to_in = field;
                let pod: $pod = $to;
                pod.resolve(resolver, out);
            }
        }

        impl<S> SerializeWith<$ty, S> for $name
        where
            S: Fallible + ?Sized,
            $pod: Serialize<S>,
        {
            fn serialize_with(field: &$ty, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                let $to_in = field;
                let pod: $pod = $to;
                pod.serialize(serializer)
            }
        }

        impl<D> DeserializeWith<Archived<$pod>, $ty, D> for $name
        where
            D: Fallible + ?Sized,
            Archived<$pod>: Deserialize<$pod, D>,
        {
            fn deserialize_with(
                field: &Archived<$pod>,
                deserializer: &mut D,
            ) -> Result<$ty, D::Error> {
                let $from_in: $pod = field.deserialize(deserializer)?;
                Ok($from)
            }
        }
    };
}

wire_adapter!(
    AddressBytes,
    Address,
    [u8; 20],
    "rkyv `with` adapter that archives `alloy_primitives::Address` as `[u8; 20]`.",
    |f| f.into_array(),
    |b| Address::from(b),
);

wire_adapter!(
    B256Bytes,
    B256,
    [u8; 32],
    "rkyv `with` adapter that archives `alloy_primitives::B256` as `[u8; 32]`.",
    |f| f.0,
    |b| B256::from(b),
);

wire_adapter!(
    U256Bytes,
    U256,
    [u8; 32],
    "rkyv `with` adapter that archives `alloy_primitives::U256` as a big-endian `[u8; 32]`.",
    |f| f.to_be_bytes(),
    |b| U256::from_be_bytes(b),
);

wire_adapter!(
    BytesVec,
    Bytes,
    Vec<u8>,
    "rkyv `with` adapter that archives `bytes::Bytes` as a `Vec<u8>`.\n\n\
     rkyv 0.8 ships a `bytes-1` feature that derives `Archive` for `Bytes` directly. But that \
     produces an `ArchivedBytes` newtype, which is awkward to expose. Adapting through `Vec<u8>` \
     keeps the archived view as a plain byte slice, so downstream tooling can read it directly.",
    |f| f.to_vec(),
    |v| Bytes::from(v),
);

wire_adapter!(
    VecB256,
    Vec<B256>,
    Vec<[u8; 32]>,
    "rkyv `with` adapter that archives `Vec<B256>` as `Vec<[u8; 32]>`.",
    |f| f.iter().map(|b| b.0).collect(),
    |v| v.into_iter().map(B256::from).collect(),
);
