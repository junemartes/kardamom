//! The markers: the durable rollback record, the clean end of a run, and
//! the record after a run that published receipts above its committed
//! block.

use kardamom_types::{Receipt, ReceiptRows};

use super::*;

fn markers() -> (tempfile::TempDir, Markers) {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    (dir, markers)
}

const RESTART: Cause = Cause::Restart { recorder: 2 };

fn receipt_in(block_number: u64) -> ReceiptRows {
    ReceiptRows::bare(Receipt {
        block_number,
        ..Receipt::default()
    })
}

#[test]
fn a_pending_record_keeps_the_lowest_block() {
    let (_dir, markers) = markers();
    assert_eq!(markers.pending().unwrap(), None);
    let high = RESTART.record(9);
    let low = Cause::Diverged {
        stream: B256::repeat_byte(4),
    }
    .record(5);
    markers.raise(high).unwrap();
    assert_eq!(markers.pending().unwrap(), Some(high));
    markers.raise(low).unwrap();
    markers.raise(RESTART.record(7)).unwrap();
    assert_eq!(
        markers.pending().unwrap(),
        Some(low),
        "the lower block stays"
    );
    markers.clear(high).unwrap();
    assert_eq!(
        markers.pending().unwrap(),
        Some(low),
        "another record stays"
    );
    markers.clear(low).unwrap();
    assert_eq!(markers.pending().unwrap(), None);
}

/// Every executor that finds one divergence names it with one identity. A
/// restart names a new event each time.
#[test]
fn a_divergence_has_one_identity_and_a_restart_a_new_one() {
    let diverged = Cause::Diverged {
        stream: B256::repeat_byte(4),
    };
    assert_eq!(diverged.record(5), diverged.record(5));
    assert_ne!(diverged.record(5).id, diverged.record(6).id);
    assert_ne!(RESTART.record(5).id, RESTART.record(5).id);
}

/// A first start with no clean marker follows a crash, so it publishes a
/// rollback to its committed block. A start after a clean end does not,
/// and a later run of the same process does not check.
#[test]
fn a_start_after_a_crash_publishes_a_rollback_to_the_committed_block() {
    let (_dir, markers) = markers();
    let after_crash = markers.open(true, 12, RESTART).unwrap().unwrap();
    assert_eq!(after_crash.to_block, 12);
    markers.clear(after_crash).unwrap();

    markers.mark_clean().unwrap();
    assert_eq!(markers.open(true, 15, RESTART).unwrap(), None);
    assert_eq!(markers.open(false, 15, RESTART).unwrap(), None);
    assert_eq!(
        markers.open(true, 15, RESTART).unwrap().map(|r| r.to_block),
        Some(15),
        "the clean marker is taken once"
    );
}

/// The lost tail is only in the open block: the comparison finds the
/// committed history equal, but the run published a receipt of the open
/// block. The next run publishes a rollback to the committed block, and
/// the publish of the record removes the marker.
#[test]
fn a_receipt_of_the_open_block_leaves_a_rollback_to_the_committed_block() {
    let (_dir, markers) = markers();
    let ledger = RunLedger::new(markers.clone());
    ledger.published(&[receipt_in(4), receipt_in(5)]);
    ledger.settle(5, RESTART).unwrap();
    assert_eq!(
        markers.pending().unwrap(),
        None,
        "every receipt is committed"
    );

    ledger.published(&[receipt_in(6)]);
    ledger.settle(5, RESTART).unwrap();
    let pending = markers.pending().unwrap().expect("a rollback");
    assert_eq!(pending.to_block, 5);
    assert_eq!(markers.open(false, 5, RESTART).unwrap(), Some(pending));

    RunLedger::new(markers.clone()).rollback_published(pending);
    assert_eq!(markers.pending().unwrap(), None);
}

/// A park of the state DB leaves the markers in the state directory.
#[test]
fn a_park_keeps_the_markers() {
    let (dir, markers) = markers();
    let env = kardamom_state::StateEnvBuilder::new(dir.path())
        .open()
        .unwrap();
    drop(env);
    let record = RESTART.record(3);
    markers.raise(record).unwrap();
    markers.mark_clean().unwrap();
    kardamom_state::park_state_db(dir.path()).unwrap();
    assert!(!kardamom_state::has_state_db(dir.path()).unwrap());
    assert_eq!(markers.pending().unwrap(), Some(record));
    assert!(markers.take_clean().unwrap());
}
