use alloy_primitives::{Address, B256};

use super::{Cursor, OutboxReader, OutboxRecord, OutboxWriter};
use crate::dto::{Stage, TxStatusEvent};

fn event(b: u8) -> TxStatusEvent {
    TxStatusEvent {
        tx_hash: B256::repeat_byte(b),
        sender: Some(Address::repeat_byte(b)),
        nonce: Some(u64::from(b)),
        stage: Stage::Rejected,
        status: None,
        reason: Some("expired".to_string()),
        expected_nonce: Some(3),
    }
}

#[test]
fn records_round_trip_through_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.outbox");
    let mut w = OutboxWriter::open(&path).unwrap();
    let first = OutboxRecord::new(1, 10, &event(1));
    let second = OutboxRecord::new(2, 20, &event(2));
    let after_first = w.append(&first).unwrap();
    assert_eq!(after_first % 8, 0, "frames are 8-byte aligned");
    w.append(&second).unwrap();

    let mut r = OutboxReader::open(&path, 0).unwrap();
    assert_eq!(r.read_next().unwrap(), Some(first.clone()));
    assert_eq!(r.pos(), after_first);
    assert_eq!(r.read_next().unwrap(), Some(second));
    assert_eq!(r.read_next().unwrap(), None, "the tail is the end");
    assert_eq!(first.event(), Some(event(1)));
}

#[test]
fn a_partial_tail_is_cut_on_open_and_skipped_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.outbox");
    let mut w = OutboxWriter::open(&path).unwrap();
    let whole = w.append(&OutboxRecord::new(1, 10, &event(1))).unwrap();
    w.append(&OutboxRecord::new(2, 20, &event(2))).unwrap();
    // Cut the second frame in half: a crash mid-write.
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(whole + 5).unwrap();
    let mut r = OutboxReader::open(&path, 0).unwrap();
    assert!(r.read_next().unwrap().is_some());
    assert_eq!(r.read_next().unwrap(), None);

    let w = OutboxWriter::open(&path).unwrap();
    assert_eq!(w.len(), whole, "the writer trims the partial frame");
    assert_eq!(std::fs::metadata(&path).unwrap().len(), whole);
}

#[test]
fn truncate_only_when_nothing_was_appended_since() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.outbox");
    let mut w = OutboxWriter::open(&path).unwrap();
    let len = w.append(&OutboxRecord::new(1, 10, &event(1))).unwrap();
    w.append(&OutboxRecord::new(2, 20, &event(2))).unwrap();
    assert!(!w.truncate_if_len(len).unwrap());
    assert!(w.truncate_if_len(w.len()).unwrap());
    assert_eq!(w.len(), 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}

#[test]
fn the_cursor_persists_and_defaults_to_zero() {
    let dir = tempfile::tempdir().unwrap();
    let cursor = Cursor::new(dir.path().join("d.cursor"));
    assert_eq!(cursor.load(), 0);
    cursor.store(4096).unwrap();
    assert_eq!(cursor.load(), 4096);
    assert!(!dir.path().join("d.cursor.tmp").exists());
}

#[test]
fn the_outbox_layout_matches_the_registry() {
    kardamom_formats::Registry::assert_rkyv_layout::<OutboxRecord>(
        "notifier-outbox",
        "OutboxRecord",
    );
}
