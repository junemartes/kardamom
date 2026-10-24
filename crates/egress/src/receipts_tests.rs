use std::num::NonZeroUsize;

use alloy_primitives::B256;
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::{BPosition, Receipt, ReceiptBatch, ReceiptRollback};

use super::Receipts;

/// The receipt at canonical index `index`, in block `block`.
fn receipt(index: u64, block: u64) -> Receipt {
    Receipt {
        tx_idx: BPosition::from_index(index),
        block_number: block,
        tx_hash: B256::with_last_byte(u8::try_from(index).unwrap()),
        ..Receipt::default()
    }
}

fn batch(receipts: &[(u64, u64)]) -> ReceiptFrame {
    ReceiptFrame::Batch(ReceiptBatch {
        receipts: receipts.iter().map(|&(i, b)| receipt(i, b)).collect(),
        accounts: Vec::new(),
    })
}

/// The indexes of the receipts of a frame passed on.
fn indexes(frame: Option<ReceiptFrame>) -> Vec<u64> {
    match frame {
        Some(ReceiptFrame::Batch(b)) => b.receipts.iter().map(|r| r.tx_idx.as_index()).collect(),
        Some(ReceiptFrame::Rollback(_)) | None => Vec::new(),
    }
}

fn receipts() -> Receipts {
    Receipts::new(NonZeroUsize::new(16).unwrap())
}

#[test]
fn batches_cut_differently_pass_on_each_receipt_once() {
    let mut r = receipts();
    assert_eq!(indexes(r.take(batch(&[(0, 1), (1, 1)]))), [0, 1]);
    // The other executor cut its batch at 2: only 2 is new.
    assert_eq!(indexes(r.take(batch(&[(0, 1), (1, 1), (2, 1)]))), [2]);
    assert!(r.take(batch(&[(1, 1), (2, 1)])).is_none(), "nothing new");
    assert_eq!(
        r.receipt(&B256::with_last_byte(2))
            .unwrap()
            .tx_idx
            .as_index(),
        2
    );
}

#[test]
fn a_rollback_passes_on_once_and_the_new_history_comes_in() {
    let mut r = receipts();
    r.take(batch(&[(0, 1), (1, 2), (2, 3)]));
    let rollback = ReceiptRollback {
        to_block: 1,
        id: B256::repeat_byte(9),
    };
    assert!(matches!(
        r.take(ReceiptFrame::Rollback(rollback)),
        Some(ReceiptFrame::Rollback(_))
    ));
    assert!(
        r.take(ReceiptFrame::Rollback(rollback)).is_none(),
        "the copy of another executor"
    );
    assert!(r.receipt(&B256::with_last_byte(2)).is_none(), "void");
    assert_eq!(r.block(), 1);
    assert_eq!(
        indexes(r.take(batch(&[(1, 2), (2, 2)]))),
        [1, 2],
        "the new history"
    );
}

#[test]
fn the_index_keeps_the_newest_receipts() {
    let mut r = Receipts::new(NonZeroUsize::new(2).unwrap());
    r.take(batch(&[(0, 1), (1, 1), (2, 1)]));
    assert!(r.receipt(&B256::with_last_byte(0)).is_none());
    assert!(r.receipt(&B256::with_last_byte(2)).is_some());
}
