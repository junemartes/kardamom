//! The receipt check after a rollback of an executor.

use std::time::Duration;

use kardamom_types::Receipt;

use super::tests::{R1, publish, receipt};
use super::*;

/// An executor publishes a receipt of a lost stream, then rolls back and
/// publishes the receipt of the new stream at the same position. After
/// the rollback, the old receipt does not meet the local receipt: the
/// check passes and the validator does not halt.
#[test]
fn a_receipt_republished_after_a_rollback_checks_equal() {
    let buf = ReceiptBuffer::new();
    let div = Divergence::new();
    let mut sink =
        ValidatorReceiptSink::new(buf.clone(), div.clone()).with_wait(Duration::from_millis(50));
    let in_block = |wsh| Receipt {
        block_number: 7,
        ..receipt(3, true, 21_000, wsh)
    };
    publish(&buf, R1, in_block(0xee));
    buf.roll_back(R1, 6);
    publish(&buf, R1, in_block(0xab));
    sink.publish(CMessage::Receipt(in_block(0xab))).unwrap();
    assert!(!div.is_halted());
}
