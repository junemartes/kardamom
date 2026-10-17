//! The markers: the durable rollback record, its identity, and the park.

use super::*;

fn markers() -> (tempfile::TempDir, Markers) {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    (dir, markers)
}

fn void(kept: u64, byte: u8) -> ReceiptRollback {
    Void {
        kept,
        stream: B256::repeat_byte(byte),
    }
    .record()
}

/// A pending record with a lower block stays. One with the same block
/// gives way to the latest divergence. A clear removes only the record
/// that went out.
#[test]
fn a_pending_record_keeps_the_lowest_block() {
    let (_dir, markers) = markers();
    assert_eq!(markers.pending().unwrap(), None);
    let high = void(9, 1);
    let low = void(5, 4);
    markers.raise(high).unwrap();
    assert_eq!(markers.pending().unwrap(), Some(high));
    markers.raise(low).unwrap();
    markers.raise(void(7, 2)).unwrap();
    assert_eq!(
        markers.pending().unwrap(),
        Some(low),
        "the lower block stays"
    );
    let latest = void(5, 6);
    markers.raise(latest).unwrap();
    assert_eq!(markers.pending().unwrap(), Some(latest), "the same block");
    markers.clear(low).unwrap();
    assert_eq!(
        markers.pending().unwrap(),
        Some(latest),
        "another record stays"
    );
    markers.clear(latest).unwrap();
    assert_eq!(markers.pending().unwrap(), None);
}

/// Every executor that finds one divergence names it with one identity:
/// the block it keeps and the first frame of the next block.
#[test]
fn a_divergence_has_one_identity() {
    assert_eq!(void(5, 4), void(5, 4));
    assert_ne!(void(5, 4).id, void(6, 4).id);
    assert_ne!(void(5, 4).id, void(5, 3).id);
}

/// A park of the state DB leaves the markers and the receipt ledger in the
/// state directory.
#[test]
fn a_park_keeps_the_markers() {
    let (dir, markers) = markers();
    let env = kardamom_state::StateEnvBuilder::new(dir.path())
        .open()
        .unwrap();
    drop(env);
    let record = void(3, 1);
    markers.raise(record).unwrap();
    markers.start_run().unwrap();
    let ledger = crate::ledger::ReceiptLedger::open(markers.clone()).unwrap();
    kardamom_state::park_state_db(dir.path()).unwrap();
    assert!(!kardamom_state::has_state_db(dir.path()).unwrap());
    assert_eq!(markers.pending().unwrap(), Some(record));
    assert!(dir.path().join("marker-running").exists());
    assert!(markers.ledger_path().exists());
    drop(ledger);
}

/// The text of the rollback marker is frozen: formats.toml holds it.
#[test]
fn the_marker_layout_is_in_the_registry() {
    let record = ReceiptRollback {
        to_block: 7,
        id: B256::repeat_byte(7),
    };
    let text = Markers::text(record);
    kardamom_formats::Registry::assert_layout("executor-markers", "RollbackMarker", &text);
    assert_eq!(Markers::parse(&text).unwrap(), record);
}
