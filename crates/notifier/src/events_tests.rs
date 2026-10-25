use std::time::Instant;

use alloy_primitives::{Address, B256};
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::tx_events::{TxEventKind, TxEventsFrame, TxEventsView};
use kardamom_types::{Receipt, ReceiptBatch, ReceiptRollback};

use super::{EventsPath, Send};
use crate::dto::Stage;

fn sends(
    path: &mut EventsPath,
    take: impl FnOnce(&mut EventsPath, &mut Vec<Send>),
) -> Vec<(B256, Stage)> {
    let mut out = Vec::new();
    take(path, &mut out);
    out.iter()
        .map(|s| (s.event.tx_hash, s.event.stage))
        .collect()
}

fn frame(kind: TxEventKind, origin: u16, hashes: &[B256]) -> Vec<u8> {
    let mut frame = TxEventsFrame::new(kind, origin);
    for hash in hashes {
        match kind {
            TxEventKind::Accepted => frame.push_accepted(hash),
            TxEventKind::Sent => frame.push_sent(hash, 0),
        };
    }
    frame.bytes().to_vec()
}

fn receipt(hash: B256, block: u64) -> Receipt {
    Receipt {
        tx_hash: hash,
        block_number: block,
        from: Address::repeat_byte(9),
        nonce: 4,
        status: true,
        ..Receipt::default()
    }
}

fn batch(receipts: Vec<Receipt>) -> ReceiptFrame {
    ReceiptFrame::Batch(ReceiptBatch {
        receipts,
        accounts: Vec::new(),
    })
}

#[test]
fn the_path_sends_the_latest_state_of_each_hash() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let a = B256::repeat_byte(1);
    let b = B256::repeat_byte(2);
    let accepted = frame(TxEventKind::Accepted, 1, &[a, b]);
    let sent = frame(TxEventKind::Sent, 0x0103, &[a]);
    let got = sends(&mut path, |p, out| {
        p.on_frame(&accepted, now, |s| out.push(s));
    });
    assert_eq!(got, [(a, Stage::Accepted), (b, Stage::Accepted)]);
    let got = sends(&mut path, |p, out| p.on_frame(&sent, now, |s| out.push(s)));
    assert_eq!(got, [(a, Stage::Sent)]);
    // The receipt of b skips `sent`; the twin's `sent` of a is a copy.
    let merged = batch(vec![receipt(b, 5)]);
    let got = sends(&mut path, |p, out| {
        p.on_merged(&merged, now, |s| out.push(s));
    });
    assert_eq!(got, [(b, Stage::Receipt)]);
    let got = sends(&mut path, |p, out| p.on_frame(&sent, now, |s| out.push(s)));
    assert!(got.is_empty());
}

#[test]
fn a_receipt_carries_its_sender_nonce_and_status() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let hash = B256::repeat_byte(3);
    let mut out = Vec::new();
    path.on_merged(&batch(vec![receipt(hash, 1)]), now, |s| out.push(s));
    let event = &out[0].event;
    assert_eq!(event.sender, Some(Address::repeat_byte(9)));
    assert_eq!(event.nonce, Some(4));
    assert_eq!(event.status, Some(1));
    assert_eq!(out[0].identity, Some((Address::repeat_byte(9), 4)));
}

#[test]
fn an_executor_copy_is_sent_once_and_the_receipt_follows() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let hash = B256::repeat_byte(4);
    let copy = batch(vec![receipt(hash, 2)]);
    let got = sends(&mut path, |p, out| {
        for session in [10, 11, 12] {
            p.on_copy(session, &copy, now, |s| out.push(s));
        }
    });
    assert_eq!(got, [(hash, Stage::Executed)], "three executors, one event");
    let got = sends(&mut path, |p, out| p.on_merged(&copy, now, |s| out.push(s)));
    assert_eq!(got, [(hash, Stage::Receipt)]);
}

#[test]
fn a_rollback_sends_rolled_back_and_the_new_receipt_again() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let hash = B256::repeat_byte(5);
    let merged = batch(vec![receipt(hash, 8)]);
    path.on_merged(&merged, now, |_| {});
    let rollback = ReceiptFrame::Rollback(ReceiptRollback {
        to_block: 7,
        id: B256::repeat_byte(0xAA),
    });
    let got = sends(&mut path, |p, out| {
        p.on_merged(&rollback, now, |s| out.push(s));
    });
    assert_eq!(got, [(hash, Stage::RolledBack)]);
    let got = sends(&mut path, |p, out| {
        p.on_merged(&merged, now, |s| out.push(s));
    });
    assert_eq!(got, [(hash, Stage::Receipt)]);
}

#[test]
fn a_jump_in_seq_is_a_gap_and_each_origin_counts_alone() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let mut twin_a = TxEventsFrame::new(TxEventKind::Sent, 0x0003);
    let mut twin_b = TxEventsFrame::new(TxEventKind::Sent, 0x0103);
    let mut gaps = Vec::new();
    for _ in 0..3 {
        gaps.push(path.note_seq(&TxEventsView::parse(twin_a.bytes()).unwrap()));
        gaps.push(path.note_seq(&TxEventsView::parse(twin_b.bytes()).unwrap()));
        twin_a.advance();
        twin_b.advance();
    }
    assert_eq!(gaps, [false; 6], "the twins have their own seq");
    twin_a.advance();
    assert!(
        path.note_seq(&TxEventsView::parse(twin_a.bytes()).unwrap()),
        "a lost frame"
    );
}

#[test]
fn a_frame_that_does_not_read_sends_nothing() {
    let now = Instant::now();
    let mut path = EventsPath::new(64, now);
    let mut out = Vec::new();
    path.on_frame(&[0x45, 0x4B, 9], now, |s| out.push(s));
    assert!(out.is_empty());
}
