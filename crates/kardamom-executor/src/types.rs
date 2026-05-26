//! Channel-C executor-side demux wrapper + `TxIndex` newtype.
//!
//! Shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`,
//! `BlockBoundaryStart`, `BlockDelta`, `AccountChange`, `ChannelBMessage`,
//! `TxRef`) are imported from `kardamom-types` per S0 D-Sh1; we never
//! redefine them here.
//!
//! Pre-S4-arch-update this module also held a `BMessage` enum that was the
//! executor's internal inbound demux (`Tx | BoundaryStart` with full
//! envelopes). Post-D-Sh12 the inbound type is `kardamom_types::
//! ChannelBMessage` (tiny refs + boundaries) plus the per-A `TxEnvelope`
//! streams; the executor reads them through the `reader.rs` module rather
//! than a single demux enum.
//!
//! The executor's `ReceiptStatus` enum is a local presentation of revm's
//! execution outcome — `kardamom_types::Receipt.status` is a single `bool`
//! (success/failure); the executor converts before publishing.

use kardamom_types::{BlockBoundary, Receipt};
use revm::context::result::HaltReason;

/// Monotonically increasing global index of a tx within the canonical
/// channel-B stream. Derived by the executor's channel-B reader from the
/// input order, starting at 0 for the first tx after genesis. The
/// downstream `Receipt.tx_idx` is the `BPosition` (the canonical wire id);
/// this `TxIndex` is an executor-local sanity counter only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxIndex(pub u64);

impl TxIndex {
    pub const ZERO: TxIndex = TxIndex(0);
    pub fn next(self) -> TxIndex {
        TxIndex(self.0 + 1)
    }
}

/// One published record on channel C — receipts and sealed boundaries.
#[derive(Debug, Clone)]
pub enum CMessage {
    Receipt(Receipt),
    BlockBoundary(BlockBoundary),
}

/// Executor-local presentation of revm's execution outcome. Folded into a
/// `bool` when materializing `kardamom_types::Receipt.status` (success vs.
/// failure); the richer Halt reason stays internal for diagnostics/logs only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptStatus {
    Success,
    Revert,
    Halt(HaltReason),
}

impl ReceiptStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, ReceiptStatus::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    #[test]
    fn tx_index_next_increments() {
        assert_eq!(TxIndex(5).next(), TxIndex(6));
    }

    #[test]
    fn bposition_orders_by_term_then_offset() {
        // BPosition comes from kardamom-types; sanity-check the import works.
        let a = BPosition {
            term_id: 0,
            term_offset: 100,
        };
        let b = BPosition {
            term_id: 1,
            term_offset: 0,
        };
        let c = BPosition {
            term_id: 0,
            term_offset: 200,
        };
        assert!(a < b);
        assert!(a < c);
        assert!(c < b);
    }

    #[test]
    fn receipt_status_is_success_helper() {
        assert!(ReceiptStatus::Success.is_success());
        assert!(!ReceiptStatus::Revert.is_success());
    }
}
