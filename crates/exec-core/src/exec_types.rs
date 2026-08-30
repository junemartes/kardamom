//! `TxReceipts` executor-side demultiplex wrapper, and the `TxIndex` newtype.
//!
//! Shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`,
//! `BlockBoundaryStart`, `BlockDelta`, `AccountChange`, `TxOrderingMessage`,
//! `TxRef`) come from `kardamom-types`. This module never redefines them.
//!
//! The executor's `ReceiptStatus` enum is a local view of revm's execution
//! outcome. `kardamom_types::Receipt.status` is a single `bool` (success or
//! failure). The executor converts the outcome before it publishes.

use kardamom_types::{BlockBoundary, Receipt};
use revm::context::result::HaltReason;

/// A global index of a tx in the canonical tx_ordering stream. The value
/// increases with each tx. The executor's tx_ordering reader derives it
/// from the input order, starting at 0 for the first tx after genesis. The
/// downstream `Receipt.tx_idx` uses `BPosition`, the canonical wire id. This
/// `TxIndex` is only a local sanity counter for the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxIndex(pub u64);

impl TxIndex {
    pub const ZERO: TxIndex = TxIndex(0);
    pub fn next(self) -> TxIndex {
        TxIndex(self.0 + 1)
    }
}

/// One published record on tx_receipts: a receipt or a sealed boundary.
#[derive(Debug, Clone)]
pub enum CMessage {
    Receipt(Receipt),
    BlockBoundary(BlockBoundary),
}

/// A local view of revm's execution outcome. The executor folds this into
/// a `bool` for `kardamom_types::Receipt.status` (success or failure). The
/// richer halt reason stays internal, for diagnostics and logs only.
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
        // BPosition comes from kardamom-types. This checks that the import works.
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
