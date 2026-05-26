//! Channel-B wire message: the canonical-orderer payload in the split
//! architecture (D-Sh12).
//!
//! TxOrdering carries only two things, both small:
//!   1. [`TxRef`] — a pointer into the per-sequencer channel A archive,
//!      written by sequencers via Aeron *concurrent* multi-publisher.
//!   2. [`BlockBoundaryStart`] — block-boundary marker written by the sealer
//!      (also concurrent multi-publisher on the same stream so the boundary
//!      is canonically ordered with the surrounding refs).
//!
//! Both variants are tiny (~16-32 B), so the channel-B CAS cursor sees only
//! reference traffic; the bulk-data path runs on M parallel exclusive channel
//! A archives. See spec §2.3.
//!
//! Encoded as a 1-byte tag prefix followed by the rkyv archive of the variant.
//! We keep the tag *outside* the rkyv archive so that a reader can branch
//! cheaply on the first byte before paying the validation cost.

use rkyv::{Archive, Deserialize, Serialize};

use crate::boundary::BlockBoundaryStart;
use crate::txref::TxRef;

/// One channel-B wire record. Variants are kept narrow to preserve the
/// "tiny payload" property that makes channel-B's concurrent publication
/// affordable (D-Sh12: ~16-32 B per record).
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxOrderingMessage {
    /// A reference to a transaction on a channel-A archive.
    TxRef(TxRef),
    /// A block-boundary marker emitted by the sealer.
    BoundaryStart(BlockBoundaryStart),
}

impl TxOrderingMessage {
    /// Whether this record is a tx ref (vs. a sealer-emitted boundary).
    pub const fn is_tx_ref(&self) -> bool {
        matches!(self, Self::TxRef(_))
    }

    /// Whether this record is a block-boundary marker.
    pub const fn is_boundary(&self) -> bool {
        matches!(self, Self::BoundaryStart(_))
    }

    /// If this record is a tx ref, return the contained `TxRef`.
    pub const fn as_tx_ref(&self) -> Option<&TxRef> {
        match self {
            Self::TxRef(r) => Some(r),
            Self::BoundaryStart(_) => None,
        }
    }

    /// If this record is a boundary marker, return it.
    pub const fn as_boundary(&self) -> Option<&BlockBoundaryStart> {
        match self {
            Self::BoundaryStart(b) => Some(b),
            Self::TxRef(_) => None,
        }
    }
}

impl From<TxRef> for TxOrderingMessage {
    fn from(r: TxRef) -> Self {
        Self::TxRef(r)
    }
}

impl From<BlockBoundaryStart> for TxOrderingMessage {
    fn from(b: BlockBoundaryStart) -> Self {
        Self::BoundaryStart(b)
    }
}
