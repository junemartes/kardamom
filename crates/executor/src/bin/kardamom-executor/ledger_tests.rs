//! The receipt ledger: the tail above the committed block, the void of a
//! rollback, an untrusted ledger, and the ring limit.

use kardamom_types::BPosition;

use super::*;

fn ledger() -> (tempfile::TempDir, ReceiptLedger) {
    let dir = tempfile::tempdir().unwrap();
    let ledger = ReceiptLedger::open(Markers::new(dir.path())).unwrap();
    (dir, ledger)
}

/// The receipt at `index` of `block`.
fn receipt(index: u64, block: u64) -> ReceiptRows {
    ReceiptRows::bare(Receipt {
        tx_idx: BPosition::from_index(index),
        block_number: block,
        tx_hash: B256::repeat_byte(u8::try_from(index % 200 + 1).unwrap()),
        ..Receipt::default()
    })
}

fn known(tail: PublishedTail) -> Vec<(u64, u64)> {
    match tail {
        PublishedTail::Known(published) => published
            .into_iter()
            .map(|(index, (block, _))| (index, block))
            .collect(),
        PublishedTail::Unknown => panic!("expected a known tail"),
    }
}

#[test]
fn the_tail_holds_the_receipts_at_and_above_the_cursor() {
    let (dir, ledger) = ledger();
    ledger
        .record(&[receipt(3, 2), receipt(4, 3), receipt(5, 3)])
        .unwrap();
    assert_eq!(known(ledger.tail(4, true).unwrap()), [(4, 3), (5, 3)]);
    let again = ReceiptLedger::open(Markers::new(dir.path())).unwrap();
    assert_eq!(
        known(again.tail(4, true).unwrap()),
        [(4, 3), (5, 3)],
        "a new open reads the same file"
    );
}

#[test]
fn a_ledger_that_is_not_whole_is_unknown() {
    let (_dir, ledger) = ledger();
    assert_eq!(ledger.tail(0, false).unwrap(), PublishedTail::Unknown);
}

#[test]
fn receipts_that_span_more_than_the_ring_are_unknown() {
    let (_dir, ledger) = ledger();
    ledger
        .record(&[receipt(10, 2), receipt(10 + SLOTS, 9)])
        .unwrap();
    assert_eq!(ledger.tail(10, true).unwrap(), PublishedTail::Unknown);
    assert_eq!(known(ledger.tail(11, true).unwrap()), [(10 + SLOTS, 9)]);
}

#[test]
fn a_void_removes_the_receipts_of_its_block_and_above() {
    let (_dir, ledger) = ledger();
    ledger
        .record(&[receipt(3, 2), receipt(4, 3), receipt(5, 4)])
        .unwrap();
    ledger.void(3).unwrap();
    assert_eq!(known(ledger.tail(0, true).unwrap()), [(3, 2)]);
}

/// A run writes the running marker at its start and removes it at its
/// end. A start that finds the marker of this boot trusts the ledger; a
/// marker of another boot means the host restarted during a run.
#[test]
fn the_running_marker_tells_whether_the_ledger_is_whole() {
    let (dir, ledger) = ledger();
    let markers = &ledger.markers;
    assert!(markers.start_run().unwrap(), "no run before");
    assert!(markers.start_run().unwrap(), "a crash on this boot");
    std::fs::write(dir.path().join("marker-running"), "another boot\n").unwrap();
    assert!(!markers.start_run().unwrap(), "a restart of the host");
    ledger.end_run().unwrap();
    assert!(!dir.path().join("marker-running").exists());
    assert!(markers.start_run().unwrap(), "a clean end");
}

/// The slot layout is frozen: formats.toml holds its fingerprint.
#[test]
fn the_slot_layout_is_in_the_registry() {
    let slot = Slot {
        index: 1,
        block: 2,
        hash: B256::repeat_byte(3),
    };
    let fingerprint = format!(
        "slots {SLOTS} {}",
        alloy_primitives::hex::encode(slot.encode())
    );
    kardamom_formats::Registry::assert_layout("receipt-ledger", "Slot", &fingerprint);
    assert_eq!(Slot::decode(&slot.encode()), Some(slot));
}
