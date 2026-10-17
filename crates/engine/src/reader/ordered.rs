//! [`Ordered`]: one item of the canonical order, as a subscription yields it.

use kardamom_cluster_adapter::wire::{self, CanonicalTx};
use kardamom_types::{TxOrderingMessage, VoidRecord};

/// One item of the canonical order.
///
/// A reference stream yields only [`Ordered::Message`]: the reader joins
/// each `TxRef` with its envelope from `tx_data`. A stream that carries the
/// bytes yields [`Ordered::Tx`] for a canonical entry. The reader executes
/// that entry with no join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ordered {
    /// A record of the reference stream, or a boundary.
    Message(TxOrderingMessage),
    /// A canonical entry with its bytes. The subscription checked that the
    /// hash of its raw transaction is its canonical id.
    Tx(CanonicalTx),
}

impl From<TxOrderingMessage> for Ordered {
    fn from(msg: TxOrderingMessage) -> Self {
        Self::Message(msg)
    }
}

impl Ordered {
    /// The canonical slots of this item. A canonical entry takes one slot.
    /// An epoch takes the marker plus one slot for each deposit, so the
    /// cursor must skip the whole range, or the next record reads as a gap.
    pub(crate) fn slots(&self) -> u64 {
        match self {
            Self::Message(TxOrderingMessage::Epoch(e)) => wire::epoch_slots(e),
            Self::Message(TxOrderingMessage::RemoteEpoch(r)) => wire::remote_epoch_slots(r),
            Self::Message(
                TxOrderingMessage::TxRef(_)
                | TxOrderingMessage::DepositRef(_)
                | TxOrderingMessage::BoundaryStart(_)
                | TxOrderingMessage::Void(_),
            )
            | Self::Tx(_) => 1,
        }
    }

    /// Whether this item is a boundary. A boundary takes no slot of its own.
    pub(crate) fn is_boundary(&self) -> bool {
        matches!(self, Self::Message(TxOrderingMessage::BoundaryStart(_)))
    }

    /// The void record in this item, if any.
    pub(crate) fn as_void(&self) -> Option<&VoidRecord> {
        match self {
            Self::Message(msg) => msg.as_void(),
            Self::Tx(_) => None,
        }
    }
}
