//! The receipt check after a rollback of an executor.

use std::time::Duration;

use kardamom_types::Receipt;

use super::tests::{R1, publish, receipt};
use super::*;
use crate::replica::ReplicaId;

/// The session of the run that published the stale receipts.
const OLD: ReplicaId = R1;
/// The session of the next run of the same executor. It publishes the
/// rollback and the receipts of the new stream.
const NEW: ReplicaId = ReplicaId::from_session(33);

/// A receipt buffer, a divergence latch, and a sink that checks the local
/// receipts against the buffer.
struct Fixture {
    buf: Arc<ReceiptBuffer>,
    div: Arc<Divergence>,
    sink: ValidatorReceiptSink,
}

impl Fixture {
    fn new() -> Self {
        let buf = ReceiptBuffer::new();
        let div = Divergence::new();
        let sink = ValidatorReceiptSink::new(buf.clone(), div.clone())
            .with_wait(Duration::from_millis(50));
        Self { buf, div, sink }
    }

    /// A receipt at position 3 in block 7, with the write-set hash `wsh`.
    fn in_block(wsh: u8) -> Receipt {
        Receipt {
            block_number: 7,
            ..receipt(3, true, 21_000, wsh)
        }
    }

    /// The old session publishes a stale receipt. Then `run` publishes a
    /// rollback to block 6 and the receipt of the new stream.
    fn roll_back_on(&self, run: ReplicaId) {
        publish(&self.buf, OLD, Self::in_block(0xee));
        self.buf.roll_back(6);
        publish(&self.buf, run, Self::in_block(0xab));
    }

    /// Check the local receipt of the new stream, and return whether the
    /// validator halted.
    fn check_local(&mut self) -> bool {
        self.sink
            .publish(CMessage::Receipt(Self::in_block(0xab)))
            .unwrap();
        self.div.is_halted()
    }
}

/// An executor publishes a receipt of a lost stream, then rolls back and
/// publishes the receipt of the new stream at the same position. After
/// the rollback, the old receipt does not meet the local receipt: the
/// check passes and the validator does not halt.
#[test]
fn a_receipt_republished_after_a_rollback_checks_equal() {
    let mut f = Fixture::new();
    f.roll_back_on(OLD);
    assert!(!f.check_local());
}

/// The old session of an executor publishes a stale receipt. The next run
/// publishes the rollback and the new receipt on a new session. The
/// rollback withdraws the stale receipt of the old session too, so the
/// check passes and the validator does not halt.
#[test]
fn a_rollback_on_a_new_session_withdraws_the_old_session() {
    let mut f = Fixture::new();
    f.roll_back_on(NEW);
    assert!(!f.check_local());
}

/// A late copy of a rollback arrives after the new receipt. It withdraws
/// the new receipt, so the position goes unverified. The validator does
/// not halt.
#[test]
fn a_late_copy_of_a_rollback_leaves_the_position_unverified() {
    let mut f = Fixture::new();
    f.roll_back_on(NEW);
    f.buf.roll_back(6);
    let wait = Duration::ZERO;
    assert!(
        f.buf
            .receipts
            .take(BPosition::from_index(3), wait)
            .current
            .is_empty()
    );
    assert!(!f.check_local());
}
