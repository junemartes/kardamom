//! Tests for the exec-side counters with logic: the must-deliver retries
//! and the dropped BAL handoffs.

use kardamom_types::{BlockBoundary, Receipt, ReceiptRollback, ReceiptRows};

use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::exec_types::CMessage;
use crate::meter::tests::{render, value};

use super::exec_block::BalCapture;
use super::test_support::feed_commits;
use super::{CommitLoop, ExecToCommit, TxReceiptsPublication};

/// Refuses the first `fails_left` publishes with a transient error.
struct RefusesFirst {
    fails_left: u32,
}

impl TxReceiptsPublication for RefusesFirst {
    fn publish_rollback(&mut self, _rollback: ReceiptRollback) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        let Some(left) = self.fails_left.checked_sub(1) else {
            return Ok(());
        };
        self.fails_left = left;
        Err(ExecutorError::TxReceiptsClosed)
    }
}

#[test]
fn each_failed_receipt_publish_counts_one_retry_and_the_batch_records_once() {
    let text = render(|| {
        let rx = feed_commits(vec![ExecToCommit::Receipt(Box::new(ReceiptRows::bare(
            Receipt::default(),
        )))]);
        CommitLoop::new(RefusesFirst { fails_left: 2 }, rx)
            .spawn()
            .join()
            .unwrap()
            .unwrap();
    });
    assert_eq!(
        value(&text, "kardamom_executor_receipt_publish_retries_total"),
        2.0,
        "{text}"
    );
    assert_eq!(
        value(
            &text,
            r#"kardamom_step_seconds_count{step="executor.receipt_publish"}"#
        ),
        1.0,
        "{text}"
    );
    assert_eq!(
        value(
            &text,
            r#"kardamom_batch_items_sum{batch="executor.receipt_batch"}"#
        ),
        1.0,
        "{text}"
    );
}

#[test]
fn a_bal_handoff_to_a_full_queue_counts_one_drop() {
    let (tx, _rx) = crossbeam_channel::bounded(0);
    let text = render(|| {
        let mut capture = BalCapture::new(tx);
        capture.handoff(&BlockBoundary::default(), &PendingDelta::default());
    });
    assert_eq!(
        value(&text, "kardamom_executor_bal_handoff_dropped_total"),
        1.0,
        "{text}"
    );
}
