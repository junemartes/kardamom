//! `TxReceipts` executor-side demultiplex wrapper, and the `TxIndex` newtype.
//!
//! Shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`,
//! `BlockBoundaryStart`, `BlockDelta`, `AccountChange`, `TxOrderingMessage`,
//! `TxRef`) come from `kardamom-types`. This module never redefines them.
//!
//! The executor's `ReceiptStatus` enum is a local view of revm's execution
//! outcome. `kardamom_types::Receipt.status` is a single `bool` (success or
//! failure). The executor converts the outcome before it publishes.

use kardamom_types::{BPosition, BlockBoundary, Receipt};
use revm::context::result::HaltReason;

/// A global index of a tx in the canonical `tx_ordering` stream. The value
/// increases with each tx. The executor's `tx_ordering` reader derives it
/// from the input order, starting at 0 for the first tx after genesis. The
/// downstream `Receipt.tx_idx` uses `BPosition`, the canonical wire id. This
/// `TxIndex` is only a local sanity counter for the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxIndex(pub u64);

impl TxIndex {
    pub const ZERO: TxIndex = TxIndex(0);

    /// # Panics
    ///
    /// Panics if the counter would overflow `u64`. A saturating add would
    /// be wrong here: it would repeat an id instead of ending the chain.
    #[must_use]
    pub fn next(self) -> Self {
        TxIndex(
            self.0
                .checked_add(1)
                .expect("TxIndex counter overflowed u64"),
        )
    }
}

/// One tx's position and running gas total, threaded through every
/// canonical-tx and derived-tx entry point (`Executor::execute_tx`,
/// `execute_deposit_tx`, `execute_xchain_tx`, `skip_receipt`, and their
/// callers). `tx_idx` is the local sanity counter; `tx_position` is the
/// canonical wire id; `tx_index_in_block` and `cumulative_gas_used_before`
/// feed the receipt's RPC-enrichment fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxSlot {
    pub tx_idx: TxIndex,
    pub tx_position: BPosition,
    pub tx_index_in_block: u64,
    pub cumulative_gas_used_before: u64,
}

/// One published record on `tx_receipts`: a receipt or a sealed boundary.
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
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, ReceiptStatus::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
