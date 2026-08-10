//! TxOrdering wire message: the canonical-orderer payload in the split
//! architecture.
//!
//! TxOrdering carries only three things, all small:
//!   1. [`TxRef`] — a pointer into the per-sequencer tx_data archive,
//!      written by sequencers via Aeron *concurrent* multi-publisher.
//!   2. [`DepositRef`] — a pointer into the `tx_deposits` archive, also
//!      written by sequencers in response to deposit messages observed on
//!      `tx_deposits`. Carries the canonical ordering of L1 deposits
//!      interleaved with regular L2 txs.
//!   3. [`BlockBoundaryStart`] — block-boundary marker written by the sealer
//!      (also concurrent multi-publisher on the same stream so the boundary
//!      is canonically ordered with the surrounding refs).
//!
//! All three variants are tiny (~16-36 B), so the tx_ordering CAS cursor sees
//! only reference traffic; the bulk-data path runs on M parallel exclusive
//! channel A archives plus the deposits archive.
//!
//! Encoded as a 1-byte tag prefix followed by the rkyv archive of the variant.
//! We keep the tag *outside* the rkyv archive so that a reader can branch
//! cheaply on the first byte before paying the validation cost.

use rkyv::{Archive, Deserialize, Serialize};

use crate::boundary::BlockBoundaryStart;
use crate::deposit::DepositRef;
use crate::epoch::EpochRecord;
use crate::txref::TxRef;
use crate::xchain::RemoteEpochRecord;

/// One tx_ordering wire record. Variants are kept narrow to preserve the
/// "tiny payload" property that makes tx_ordering's concurrent publication
/// affordable.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum TxOrderingMessage {
    /// A reference to a transaction on a tx_data archive.
    TxRef(TxRef),
    /// A reference to a deposit on the `tx_deposits` archive.
    DepositRef(DepositRef),
    /// A block-boundary marker emitted by the sealer.
    BoundaryStart(BlockBoundaryStart),
    /// An L1 epoch: its origin plus that L1 block's deposits, in log order.
    ///
    /// Ordered like any other record, but the sealer treats the carrying
    /// frame as origin-advancing — it closes the current block first, so an
    /// epoch's deposits always LEAD a block, and stamps its `l1_number` into
    /// every subsequent boundary. See
    /// `docs/agents/l1-origin-deposit-derivation-spec.md`.
    Epoch(EpochRecord),
    /// A remote epoch: one peer Kardamom chain's contiguous outbox-message
    /// batch, in seq order. Origin-advancing like [`Self::Epoch`] (the sealer
    /// closes the open block first, per-peer), but its origin marker is NOT
    /// stamped into boundaries — remote origin positions are recoverable from
    /// these records themselves, which keeps boundary size independent of the
    /// peer count. See `docs/specs/interop-outbox-messaging-spec.md`.
    RemoteEpoch(RemoteEpochRecord),
}

impl TxOrderingMessage {
    /// Whether this record is a tx ref (vs. a sealer-emitted boundary or a
    /// deposit ref).
    pub const fn is_tx_ref(&self) -> bool {
        matches!(self, Self::TxRef(_))
    }

    /// Whether this record is a deposit ref.
    pub const fn is_deposit_ref(&self) -> bool {
        matches!(self, Self::DepositRef(_))
    }

    /// Whether this record is a block-boundary marker.
    pub const fn is_boundary(&self) -> bool {
        matches!(self, Self::BoundaryStart(_))
    }

    /// Whether this record is an L1 epoch.
    pub const fn is_epoch(&self) -> bool {
        matches!(self, Self::Epoch(_))
    }

    /// Whether this record is a remote (peer-chain) epoch.
    pub const fn is_remote_epoch(&self) -> bool {
        matches!(self, Self::RemoteEpoch(_))
    }

    /// If this record is a tx ref, return the contained `TxRef`.
    pub const fn as_tx_ref(&self) -> Option<&TxRef> {
        match self {
            Self::TxRef(r) => Some(r),
            Self::DepositRef(_)
            | Self::BoundaryStart(_)
            | Self::Epoch(_)
            | Self::RemoteEpoch(_) => None,
        }
    }

    /// If this record is a deposit ref, return the contained `DepositRef`.
    pub const fn as_deposit_ref(&self) -> Option<&DepositRef> {
        match self {
            Self::DepositRef(d) => Some(d),
            Self::TxRef(_) | Self::BoundaryStart(_) | Self::Epoch(_) | Self::RemoteEpoch(_) => None,
        }
    }

    /// If this record is an epoch, return it.
    pub const fn as_epoch(&self) -> Option<&EpochRecord> {
        match self {
            Self::Epoch(e) => Some(e),
            Self::TxRef(_)
            | Self::DepositRef(_)
            | Self::BoundaryStart(_)
            | Self::RemoteEpoch(_) => None,
        }
    }

    /// If this record is a remote epoch, return it.
    pub const fn as_remote_epoch(&self) -> Option<&RemoteEpochRecord> {
        match self {
            Self::RemoteEpoch(r) => Some(r),
            Self::TxRef(_) | Self::DepositRef(_) | Self::BoundaryStart(_) | Self::Epoch(_) => None,
        }
    }

    /// If this record is a boundary marker, return it.
    pub const fn as_boundary(&self) -> Option<&BlockBoundaryStart> {
        match self {
            Self::BoundaryStart(b) => Some(b),
            Self::TxRef(_) | Self::DepositRef(_) | Self::Epoch(_) | Self::RemoteEpoch(_) => None,
        }
    }
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
