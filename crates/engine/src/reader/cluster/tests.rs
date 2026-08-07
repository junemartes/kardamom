use super::*;
use alloy_primitives::B256;
use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
use kardamom_cluster_adapter::wire::{
    encode_egress_boundary, encode_egress_record, encode_ingress_txref, split_ingress,
};
use kardamom_types::TxRef;

fn relayed_txref(shard: u8, off: i32) -> Vec<u8> {
    let r = TxRef::new(
        B256::repeat_byte(off as u8),
        shard,
        BPosition {
            term_id: 0,
            term_offset: off,
        },
        0,
    );
    let ingress = encode_ingress_txref(&r, alloy_primitives::Address::ZERO, 0);
    let (_cid, relayed) = split_ingress(&ingress).unwrap();
    relayed.to_vec()
}

#[test]
fn replay_gap_is_reordered_and_deduped() {
    // Live-ahead frames arrive FIRST (the session reconnected mid-stream);
    // the replayed range then fills the gap. Canonical order: r0 r1 b1(end2)
    // r2 r3 b2(end4) r4 r5.
    let egress = FakeEgress::new();
    // Live-ahead: record 5 and boundary 2 arrive before the replay.
    egress.push(encode_egress_record(5, &relayed_txref(1, 5)));
    egress.push(encode_egress_boundary(2, 4, 2_000, 0));
    // Replayed frames (emission order), incl. a duplicate of record 5's
    // predecessor range and both boundaries.
    for i in 0..5 {
        egress.push(encode_egress_record(i, &relayed_txref(1, i as i32)));
    }
    egress.push(encode_egress_boundary(1, 2, 1_000, 0));
    egress.push(encode_egress_boundary(2, 4, 2_000, 0)); // duplicate boundary
    egress.push(wire::encode_replay_done(6, 3));
    egress.close();

    let mut sub = ClusterTxOrderingSubscription::new(egress);
    let mut got = Vec::new();
    while let Ok((pos, msg)) = sub.next() {
        let tag = match msg {
            TxOrderingMessage::TxRef(_) => format!("r{}", pos.as_index()),
            TxOrderingMessage::BoundaryStart(b) => format!("b{}", b.block_number),
            other => format!("?{other:?}"),
        };
        got.push(tag);
    }
    assert_eq!(got, vec!["r0", "r1", "b1", "r2", "r3", "b2", "r4", "r5"]);
}

#[test]
fn boundary_first_stream_delivers_in_emission_order() {
    // An idle chain emits boundaries with no records: b1(end0) b2(end0).
    let egress = FakeEgress::new();
    egress.push(encode_egress_boundary(1, 0, 1_000, 0));
    egress.push(encode_egress_boundary(2, 0, 2_000, 0));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    let (p1, m1) = sub.next().unwrap();
    assert!(matches!(m1, TxOrderingMessage::BoundaryStart(b) if b.block_number == 1));
    assert_eq!(p1.as_index(), 0);
    let (_p2, m2) = sub.next().unwrap();
    assert!(matches!(m2, TxOrderingMessage::BoundaryStart(b) if b.block_number == 2));
}

#[test]
fn duplicates_below_cursor_are_skipped() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_record(0, &relayed_txref(1, 10)));
    egress.push(encode_egress_record(0, &relayed_txref(1, 10))); // dup
    egress.push(encode_egress_record(1, &relayed_txref(1, 11)));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    assert_eq!(sub.next().unwrap().0, BPosition::from_index(0));
    assert_eq!(sub.next().unwrap().0, BPosition::from_index(1));
    assert!(matches!(sub.next(), Err(ExecutorError::TxOrderingClosed)));
}

#[test]
fn replay_unavailable_is_fatal() {
    let egress = FakeEgress::new();
    egress.push(wire::encode_replay_unavailable(100, 7));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    assert!(matches!(
        sub.next(),
        Err(ExecutorError::ClusterReplayUnavailable {
            oldest_index: 100,
            oldest_block: 7,
            ..
        })
    ));
}

#[test]
fn resume_cursor_skips_already_applied_range() {
    // Consumer resumes at (records=3, next block=2): replayed frames below
    // the cursor are dropped, delivery starts exactly at the cursor.
    let egress = FakeEgress::new();
    for i in 0..5 {
        egress.push(encode_egress_record(i, &relayed_txref(1, i as i32)));
    }
    egress.push(encode_egress_boundary(1, 2, 1_000, 0)); // below cursor: dup
    egress.push(encode_egress_boundary(2, 5, 2_000, 0));
    egress.push(wire::encode_replay_done(5, 3));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(3, 2));
    let mut got = Vec::new();
    while let Ok((pos, msg)) = sub.next() {
        got.push(match msg {
            TxOrderingMessage::TxRef(_) => format!("r{}", pos.as_index()),
            TxOrderingMessage::BoundaryStart(b) => format!("b{}", b.block_number),
            other => format!("?{other:?}"),
        });
    }
    assert_eq!(got, vec!["r3", "r4", "b2"]);
}

#[test]
fn yields_records_with_monotonic_bposition() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_record(0, &relayed_txref(1, 10)));
    egress.push(encode_egress_record(1, &relayed_txref(2, 20)));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);

    let (p0, m0) = sub.next().unwrap();
    assert_eq!(p0, BPosition::from_index(0));
    assert!(matches!(m0, TxOrderingMessage::TxRef(_)));
    let (p1, _m1) = sub.next().unwrap();
    assert_eq!(p1, BPosition::from_index(1));
    // Stream closed ⇒ TxOrderingClosed (the reader treats this as clean EOF).
    assert!(matches!(sub.next(), Err(ExecutorError::TxOrderingClosed)));
}

#[test]
fn yields_boundary_with_fields_intact() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_boundary(7, 42, 1_700_000_000_250, 0));
    egress.close();
    // Consumer resumed at (42 records applied, next block 7) — the
    // boundary is the next in-order item and must decode field-intact.
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(42, 7));
    let (pos, msg) = sub.next().unwrap();
    match msg {
        TxOrderingMessage::BoundaryStart(b) => {
            assert_eq!(b.block_number, 7);
            assert_eq!(b.end_tx_idx.as_index(), 42);
            assert_eq!(b.l2_timestamp, 1_700_000_000_250);
            assert_eq!(pos, b.end_tx_idx);
        }
        other => panic!("expected boundary, got {other:?}"),
    }
}

// F07.2 regression: a boundary-only gap across a session reconnect.
// Cursor at (records=2, next block=1): records 0..1 delivered, block 1 not
// yet sealed. Boundary b1 was emitted during a brief session outage; the
// reconnect's first live frame is record 2 — exactly the next index, so no
// key gap is observed and live mode delivers it. The replayed boundary
// b1(end=2) then arrives; it canonically precedes record 2, so delivering
// it now would seal block 1 with block 2's first record inside. That
// inversion must FAIL-STOP (restart + cursor replay recovers gaplessly),
// never deliver.
#[test]
fn late_boundary_sealing_below_cursor_is_fatal() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_record(2, &relayed_txref(1, 2)));
    egress.push(encode_egress_boundary(1, 2, 1_000, 0));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(2, 1));
    // Live mode: record 2 is next-index and delivers immediately.
    let (p, m) = sub.next().unwrap();
    assert_eq!(p.as_index(), 2);
    assert!(matches!(m, TxOrderingMessage::TxRef(_)));
    // The late replayed boundary proves the inversion → fatal.
    assert!(matches!(
        sub.next(),
        Err(ExecutorError::BoundaryMisaligned { .. })
    ));
}

#[test]
fn malformed_frame_is_skipped_not_fatal() {
    let egress = FakeEgress::new();
    egress.push(vec![0xFF, 0x00]); // bad egress kind
    egress.push(encode_egress_boundary(1, 0, 0, 0));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    // The malformed frame is skipped; the next good frame is returned.
    let (_pos, msg) = sub.next().unwrap();
    assert!(matches!(msg, TxOrderingMessage::BoundaryStart(_)));
}
