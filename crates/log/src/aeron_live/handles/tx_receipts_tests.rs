use super::*;

/// One raw batch frame must fan out to every receipt it carries, in
/// order, before the receiver goes back to the raw channel. This is
/// consumer-side behavior (`TxReceiptsReceiver`, what
/// `into_receiver()` returns), so the test needs no `AeronRuntime`.
#[test]
fn one_batch_frame_fans_out_to_every_receipt_in_order() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = TxReceiptsReceiver {
        rx,
        pending: VecDeque::new(),
    };
    let pos = BPosition {
        term_id: 1,
        term_offset: 2,
    };
    let batch = vec![
        Receipt {
            nonce: 0,
            ..Default::default()
        },
        Receipt {
            nonce: 1,
            ..Default::default()
        },
        Receipt {
            nonce: 2,
            ..Default::default()
        },
    ];
    let frame = ReceiptBatch {
        receipts: batch.clone(),
        accounts: Vec::new(),
    };
    tx.send(RawFrame {
        bytes: codec::encode(&frame).unwrap().to_vec(),
        pos,
        session: 0,
    })
    .unwrap();

    for expected in &batch {
        let (got_pos, got) = receiver.try_recv().expect("fanned-out receipt");
        assert_eq!(got_pos, pos);
        assert_eq!(&got, expected);
    }
    assert!(
        receiver.try_recv().is_none(),
        "every receipt in the one frame was already drained"
    );
}

/// A rollback frame is shorter than every batch frame. `recv_frame`
/// reads it as a rollback, `recv_batch` and `try_recv` skip it, and a
/// batch after it still arrives.
#[tokio::test]
async fn a_rollback_frame_reads_as_a_rollback_and_batch_readers_skip_it() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = TxReceiptsReceiver {
        rx,
        pending: VecDeque::new(),
    };
    let pos = BPosition::from_index(3);
    let rollback = ReceiptRollback { to_block: 41 };
    let batch = ReceiptBatch {
        receipts: vec![Receipt::default()],
        accounts: Vec::new(),
    };
    let raw = |bytes: rkyv::util::AlignedVec, session| RawFrame {
        bytes: bytes.to_vec(),
        pos,
        session,
    };
    let rollback_bytes = codec::encode(&rollback).unwrap();
    assert_eq!(rollback_bytes.len(), ReceiptFrame::ROLLBACK_LEN);
    tx.send(raw(rollback_bytes.clone(), 7)).unwrap();
    tx.send(raw(codec::encode(&batch).unwrap(), 7)).unwrap();
    assert_eq!(
        receiver.recv_frame().await,
        Some((7, ReceiptFrame::Rollback(rollback)))
    );
    assert_eq!(
        receiver.recv_frame().await,
        Some((7, ReceiptFrame::Batch(batch.clone())))
    );

    tx.send(raw(rollback_bytes.clone(), 7)).unwrap();
    tx.send(raw(codec::encode(&batch).unwrap(), 9)).unwrap();
    assert_eq!(receiver.recv_batch().await, Some((9, batch)));

    tx.send(raw(rollback_bytes, 7)).unwrap();
    assert_eq!(receiver.try_recv(), None);
}

/// `recv_batch` hands the whole frame over, rows included, and skips a
/// frame that does not decode.
#[tokio::test]
async fn recv_batch_keeps_the_rows_and_skips_a_bad_frame() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = TxReceiptsReceiver {
        rx,
        pending: VecDeque::new(),
    };
    let pos = BPosition {
        term_id: 1,
        term_offset: 2,
    };
    let frame = ReceiptBatch {
        receipts: vec![Receipt::default()],
        accounts: vec![kardamom_types::AccountRow {
            address: alloy_primitives::Address::repeat_byte(0xAA),
            nonce: 7,
            balance: alloy_primitives::U256::from(9u64),
        }],
    };
    tx.send(RawFrame {
        bytes: vec![0xFF; 3],
        pos,
        session: 0,
    })
    .unwrap();
    tx.send(RawFrame {
        bytes: codec::encode(&frame).unwrap().to_vec(),
        pos,
        session: 41,
    })
    .unwrap();

    let (got_session, got) = receiver.recv_batch().await.expect("the good frame");
    assert_eq!(got_session, 41);
    assert_eq!(got, frame);
}
