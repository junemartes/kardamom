//! `TxOrdering` wire message: the canonical-orderer payload in the split
//! architecture.
//!
//! `TxOrdering` carries these reference records, all small:
//!   1. [`TxRef`] — a pointer into the per-sequencer `tx_data` archive.
//!      Sequencers write it using Aeron's concurrent multi-publisher.
//!   2. [`DepositRef`] — a pointer into the `tx_deposits` archive. Sequencers
//!      also write it, in response to deposit messages seen on
//!      `tx_deposits`. It carries the canonical order of L1 deposits,
//!      interleaved with regular L2 transactions.
//!   3. [`BlockBoundaryStart`] — a block-boundary marker written by the
//!      sealer. The sealer also uses concurrent multi-publisher on the same
//!      stream, so the boundary is canonically ordered with the surrounding
//!      references.
//!
//! A fourth variant, [`TxOrderingMessage::Epoch`], also travels on this
//! stream. Unlike the three above it is not a small reference: it carries
//! an epoch's deposits by value. See its own doc for why.
//!
//! The three reference variants above are tiny, about 16 to 36 bytes. So
//! the `tx_ordering` CAS cursor sees mostly reference traffic. The bulk-data
//! path runs on M parallel exclusive channel A archives, plus the deposits
//! archive.
//!
//! This is encoded as a 1-byte tag prefix, followed by the rkyv archive of
//! the variant. The tag stays outside the rkyv archive, so a reader can
//! branch cheaply on the first byte before it pays the validation cost.

use rkyv::{Archive, Deserialize, Serialize};

use crate::boundary::BlockBoundaryStart;
use crate::deposit::DepositRef;
use crate::epoch::EpochRecord;
use crate::txref::TxRef;
use crate::xchain::RemoteEpochRecord;

/// One `tx_ordering` wire record. Variants stay narrow. This keeps the "tiny
/// payload" property that makes `tx_ordering`'s concurrent publication
/// affordable.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxOrderingMessage {
    /// A reference to a transaction on a `tx_data` archive.
    TxRef(TxRef),
    /// A reference to a deposit on the `tx_deposits` archive.
    DepositRef(DepositRef),
    /// A block-boundary marker emitted by the sealer.
    BoundaryStart(BlockBoundaryStart),
    /// An L1 epoch: its origin, plus that L1 block's deposits, in log order.
    ///
    /// This record is ordered like any other. But the sealer treats its
    /// frame as origin-advancing: it closes the current block first, so an
    /// epoch's deposits always lead a block. The sealer then stamps the
    /// epoch's `l1_number` into every following boundary.
    Epoch(EpochRecord),
    /// A remote epoch: one peer Kardamom chain's contiguous outbox-message
    /// batch, in seq order. Origin-advancing like [`Self::Epoch`] (the sealer
    /// closes the open block first, per-peer), but its origin marker is NOT
    /// stamped into boundaries — remote origin positions are recoverable from
    /// these records themselves, which keeps boundary size independent of the
    /// peer count.
    RemoteEpoch(RemoteEpochRecord),
}

/// Generates one variant's `is_*`/`as_*` pair. `$other` lists every
/// other `TxOrderingMessage` variant, spelled out rather than a wildcard
/// arm: adding a new variant without updating every invocation here then
/// fails to compile (a non-exhaustive match), instead of the new variant
/// silently falling through to `None`.
macro_rules! variant_accessor {
    ($is_name:ident, $as_name:ident, $doc:literal, $variant:ident, $ty:ty, [$($other:ident),+ $(,)?]) => {
        #[doc = concat!("Returns true if this record is a ", $doc, ".")]
        #[must_use]
        pub const fn $is_name(&self) -> bool {
            matches!(self, Self::$variant(_))
        }

        #[doc = concat!("Returns the contained value if this record is a ", $doc, ".")]
        #[must_use]
        pub const fn $as_name(&self) -> Option<&$ty> {
            match self {
                Self::$variant(v) => Some(v),
                $(Self::$other(_))|+ => None,
            }
        }
    };
}

impl TxOrderingMessage {
    variant_accessor!(
        is_tx_ref,
        as_tx_ref,
        "transaction reference",
        TxRef,
        TxRef,
        [DepositRef, BoundaryStart, Epoch, RemoteEpoch]
    );
    variant_accessor!(
        is_deposit_ref,
        as_deposit_ref,
        "deposit reference",
        DepositRef,
        DepositRef,
        [TxRef, BoundaryStart, Epoch, RemoteEpoch]
    );
    variant_accessor!(
        is_boundary,
        as_boundary,
        "block-boundary marker",
        BoundaryStart,
        BlockBoundaryStart,
        [TxRef, DepositRef, Epoch, RemoteEpoch]
    );
}

impl From<RemoteEpochRecord> for TxOrderingMessage {
    fn from(r: RemoteEpochRecord) -> Self {
        Self::RemoteEpoch(r)
    }
}

impl From<TxRef> for TxOrderingMessage {
    fn from(r: TxRef) -> Self {
        Self::TxRef(r)
    }
}

impl From<DepositRef> for TxOrderingMessage {
    fn from(d: DepositRef) -> Self {
        Self::DepositRef(d)
    }
}

impl From<BlockBoundaryStart> for TxOrderingMessage {
    fn from(b: BlockBoundaryStart) -> Self {
        Self::BoundaryStart(b)
    }
}
