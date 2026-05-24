//! Channel B / Channel C executor-side demux wrappers.
//!
//! Shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`,
//! `BlockBoundaryStart`, `BlockDelta`, `AccountChange`) are imported from
//! `kardamom-types` per S0 D-Sh1; we never redefine them here. The executor's
//! `ReceiptStatus` enum is a local presentation of revm's execution outcome —
//! `kardamom_types::Receipt.status` is a single `bool` (success/failure); the
//! executor converts before publishing.

use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, Receipt, TxEnvelope};
use revm::context::result::HaltReason;

/// Monotonically increasing global index of a tx within the canonical channel-B
/// stream. Derived by the executor's reader from the input order, starting at 0
/// for the first tx after genesis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxIndex(pub u64);

impl TxIndex {
    pub const ZERO: TxIndex = TxIndex(0);
    pub fn next(self) -> TxIndex {
        TxIndex(self.0 + 1)
    }
}

/// One canonical-ordered record off channel B. The sealer emits
/// `BlockBoundaryStart` records inline; the sequencer emits `Tx` records.
///
/// `envelope` is `kardamom_types::TxEnvelope`, which already carries the
/// proxy-populated `sender` and `tx_hash` (S0 D-Sh3, D-Sh4). The executor
/// trusts those fields unconditionally — no re-recovery, no re-hash.
#[derive(Debug, Clone)]
pub enum BMessage {
    Tx {
        position: BPosition,
        tx_idx: TxIndex,
        envelope: TxEnvelope,
    },
    BlockBoundaryStart(BlockBoundaryStart),
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
