use alloy_primitives::{B256, hex};

use super::{
    FrameError, HEADER_LEN, MAX_RECORDS, TxEvent, TxEventKind, TxEventsFrame, TxEventsView,
};

const HASH: B256 = B256::repeat_byte(0x11);

fn sent_frame() -> TxEventsFrame {
    let mut frame = TxEventsFrame::new(TxEventKind::Sent, 0x0102);
    frame.advance();
    frame.push_sent(&HASH, 3);
    frame
}

#[test]
fn a_frame_keeps_its_golden_layout() {
    let mut sent = sent_frame();
    kardamom_formats::Registry::assert_layout("tx-events", "Sent", &hex::encode(sent.bytes()));
    let mut accepted = TxEventsFrame::new(TxEventKind::Accepted, 7);
    accepted.push_accepted(&HASH);
    kardamom_formats::Registry::assert_layout(
        "tx-events",
        "Accepted",
        &hex::encode(accepted.bytes()),
    );
}

#[test]
fn each_kind_reads_back() {
    let mut accepted = TxEventsFrame::new(TxEventKind::Accepted, 5);
    accepted.push_accepted(&HASH);
    accepted.push_accepted(&HASH);
    let bytes = accepted.bytes().to_vec();
    let view = TxEventsView::parse(&bytes).unwrap();
    assert_eq!(
        (view.kind, view.origin, view.seq, view.len()),
        (TxEventKind::Accepted, 5, 0, 2)
    );
    assert_eq!(
        view.events().collect::<Vec<_>>(),
        vec![TxEvent::Accepted { hash: HASH }; 2]
    );
    let mut sent = sent_frame();
    let bytes = sent.bytes().to_vec();
    let view = TxEventsView::parse(&bytes).unwrap();
    assert_eq!((view.origin, view.seq), (0x0102, 1));
    assert_eq!(
        view.events().collect::<Vec<_>>(),
        vec![TxEvent::Sent {
            hash: HASH,
            lane: 3
        }]
    );
}

#[test]
fn a_full_frame_reports_full_and_the_next_one_starts_empty() {
    let mut frame = TxEventsFrame::new(TxEventKind::Accepted, 0);
    let full = (0..MAX_RECORDS)
        .map(|_| frame.push_accepted(&HASH))
        .collect::<Vec<_>>();
    assert_eq!(full.iter().filter(|f| **f).count(), 1);
    assert!(full[MAX_RECORDS - 1]);
    assert_eq!(frame.bytes().len(), HEADER_LEN + MAX_RECORDS * 32);
    frame.advance();
    assert!(frame.is_empty());
    let bytes = frame.bytes().to_vec();
    let view = TxEventsView::parse(&bytes).unwrap();
    assert_eq!((view.seq, view.len()), (1, 0));
}

#[test]
fn a_bad_frame_and_a_reserved_kind_are_refused() {
    let mut sent = sent_frame();
    let good = sent.bytes().to_vec();
    assert_eq!(
        TxEventsView::parse(&good[..10]).unwrap_err(),
        FrameError::Short
    );
    let mut bad = good.clone();
    bad[0] = 0;
    assert_eq!(TxEventsView::parse(&bad).unwrap_err(), FrameError::Magic);
    let mut bad = good.clone();
    bad[2] = 2;
    assert_eq!(
        TxEventsView::parse(&bad).unwrap_err(),
        FrameError::Version(2)
    );
    for reserved in [3, 4, 5] {
        let mut bad = good.clone();
        bad[3] = reserved;
        assert_eq!(
            TxEventsView::parse(&bad).unwrap_err(),
            FrameError::Kind(reserved)
        );
    }
    assert_eq!(
        TxEventsView::parse(&good[..good.len() - 1]).unwrap_err(),
        FrameError::Length { got: 32, want: 33 }
    );
}
