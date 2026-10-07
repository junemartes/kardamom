use super::*;
use alloy_primitives::B256;
use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
use kardamom_cluster_adapter::wire::{
    GuardHeader, encode_egress_boundary, encode_egress_record, encode_ingress_txref, split_ingress,
};
use kardamom_types::TxRef;

/// Format one delivered record as a short tag, for order assertions.
fn label((pos, msg): (BPosition, TxOrderingMessage)) -> String {
    match msg {
        TxOrderingMessage::TxRef(_) => format!("r{}", pos.as_index()),
        TxOrderingMessage::BoundaryStart(b) => format!("b{}", b.block_number),
        other => format!("?{other:?}"),
    }
}

fn relayed_txref(shard: u8, off: i32) -> Vec<u8> {
    let r = TxRef::new(
        B256::repeat_byte(
            u8::try_from(off).expect("test fixture: off is a small non-negative offset"),
        ),
        shard,
        BPosition {
            term_id: 0,
            term_offset: off,
        },
        0,
    );
    let ingress = encode_ingress_txref(&r, GuardHeader::EXEMPT);
    let (_cid, relayed) = split_ingress(&ingress).unwrap();
    relayed.to_vec()
}

#[test]
fn replay_gap_is_reordered_and_deduped() {
    // Live-ahead frames arrive first, because the session reconnected
    // mid-stream. The replayed range then fills the gap. Canonical order:
    // r0 r1 b1(end2) r2 r3 b2(end4) r4 r5.
    let egress = FakeEgress::new();
    // Live-ahead: record 5 and boundary 2 arrive before the replay.
    egress.push(encode_egress_record(5, &relayed_txref(1, 5)).unwrap());
    egress.push(encode_egress_boundary(2, 4, 2_000, 0));
    // Replayed frames, in emission order. This includes a duplicate of
    // record 5's predecessor range, and both boundaries.
    for i in 0..5 {
        egress.push(
            encode_egress_record(
                i,
                &relayed_txref(1, i32::try_from(i).expect("small test fixture")),
            )
            .unwrap(),
        );
    }
    egress.push(encode_egress_boundary(1, 2, 1_000, 0));
    egress.push(encode_egress_boundary(2, 4, 2_000, 0)); // duplicate boundary
    egress.push(wire::encode_replay_done(6, 3));
    egress.close();

    let mut sub = ClusterTxOrderingSubscription::new(egress);
    let got: Vec<String> = std::iter::from_fn(|| sub.next().ok()).map(label).collect();
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
    egress.push(encode_egress_record(0, &relayed_txref(1, 10)).unwrap());
    egress.push(encode_egress_record(0, &relayed_txref(1, 10)).unwrap()); // dup
    egress.push(encode_egress_record(1, &relayed_txref(1, 11)).unwrap());
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

/// A head below the delivery cursor means the sealer lost records that
/// this consumer applied. The consumer stops, instead of dropping each new
/// record below its cursor.
#[test]
fn replay_done_below_the_cursor_is_fatal() {
    let behind = |up_to_index, up_to_block| {
        let egress = FakeEgress::new();
        egress.push(wire::encode_replay_done(up_to_index, up_to_block));
        egress.push(encode_egress_record(0, &relayed_txref(1, 0)).unwrap());
        egress.close();
        ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(5, 3)).next()
    };
    assert!(matches!(
        behind(0, 1),
        Err(ExecutorError::ClusterBehindCursor {
            next_index: 5,
            next_block: 3,
            head_index: 0,
            head_block: 1,
        })
    ));
    assert!(matches!(
        behind(5, 2),
        Err(ExecutorError::ClusterBehindCursor { head_block: 2, .. })
    ));
}

/// The sealer's `REPLAY_AHEAD` refusal stops the consumer with the head
/// in the error. No repair starts: the replay-refused fallback does not
/// take this error, so it fetches no checkpoint and parks no state.
#[test]
fn replay_ahead_is_fatal_and_starts_no_repair() {
    let egress = FakeEgress::new();
    egress.push(wire::encode_replay_ahead(0, 1));
    egress.push(encode_egress_record(0, &relayed_txref(1, 0)).unwrap());
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(5, 3));
    let err = sub.next().unwrap_err();
    assert!(matches!(
        err,
        ExecutorError::ClusterBehindCursor {
            next_index: 5,
            next_block: 3,
            head_index: 0,
            head_block: 1,
        }
    ));

    let state_dir = tempfile::tempdir().unwrap();
    std::fs::write(state_dir.path().join("mdbx.dat"), b"state").unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let repaired = crate::bin_support::replay_unavailable_fallback(
        Some(&err),
        Some(checkpoint_dir.path()),
        &["127.0.0.1:1".to_string()],
        state_dir.path(),
        None,
        false,
    )
    .unwrap();
    assert_eq!(repaired, None, "no resync outcome");
    assert!(!state_dir.path().join("stale").exists(), "no state parked");
    assert!(state_dir.path().join("mdbx.dat").exists());
}

/// A head at the delivery cursor ends the replay with nothing to deliver.
#[test]
fn replay_done_at_the_cursor_ends_the_catch_up() {
    let egress = FakeEgress::new();
    egress.push(wire::encode_replay_done(5, 3));
    egress.push(encode_egress_record(5, &relayed_txref(1, 5)).unwrap());
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(5, 3));
    assert_eq!(label(sub.next().unwrap()), "r5");
}

#[test]
fn resume_cursor_skips_already_applied_range() {
    // Consumer resumes at (records=3, next block=2). Replayed frames below
    // the cursor are dropped. Delivery starts exactly at the cursor.
    let egress = FakeEgress::new();
    for i in 0..5 {
        egress.push(
            encode_egress_record(
                i,
                &relayed_txref(1, i32::try_from(i).expect("small test fixture")),
            )
            .unwrap(),
        );
    }
    egress.push(encode_egress_boundary(1, 2, 1_000, 0)); // below cursor: dup
    egress.push(encode_egress_boundary(2, 5, 2_000, 0));
    egress.push(wire::encode_replay_done(5, 3));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(3, 2));
    let got: Vec<String> = std::iter::from_fn(|| sub.next().ok()).map(label).collect();
    assert_eq!(got, vec!["r3", "r4", "b2"]);
}

#[test]
fn yields_records_with_monotonic_bposition() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_record(0, &relayed_txref(1, 10)).unwrap());
    egress.push(encode_egress_record(1, &relayed_txref(2, 20)).unwrap());
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);

    let (p0, m0) = sub.next().unwrap();
    assert_eq!(p0, BPosition::from_index(0));
    assert!(matches!(m0, TxOrderingMessage::TxRef(_)));
    let (p1, _m1) = sub.next().unwrap();
    assert_eq!(p1, BPosition::from_index(1));
    // Stream closed gives TxOrderingClosed. The reader treats this as a
    // clean EOF.
    assert!(matches!(sub.next(), Err(ExecutorError::TxOrderingClosed)));
}

#[test]
fn yields_boundary_with_fields_intact() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_boundary(7, 42, 1_700_000_000_250, 0));
    egress.close();
    // Consumer resumed at (42 records applied, next block 7). The
    // boundary is the next in-order item, and must decode with its fields
    // intact.
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

// Regression test: a boundary-only gap across a session reconnect.
// Cursor at (records=2, next block=1). Records 0..1 are delivered; block 1
// is not yet sealed. Boundary b1 was emitted during a brief session
// outage. The reconnect's first live frame is record 2, exactly the next
// index, so no key gap is seen, and live mode delivers it. The replayed
// boundary b1(end=2) then arrives. It canonically precedes record 2, so
// delivering it now would seal block 1 with block 2's first record inside.
// This inversion must fail-stop instead of deliver; a restart plus cursor
// replay recovers with no gaps.
#[test]
fn late_boundary_sealing_below_cursor_is_fatal() {
    let egress = FakeEgress::new();
    egress.push(encode_egress_record(2, &relayed_txref(1, 2)).unwrap());
    egress.push(encode_egress_boundary(1, 2, 1_000, 0));
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(2, 1));
    // Live mode: record 2 is next-index and delivers immediately.
    let (p, m) = sub.next().unwrap();
    assert_eq!(p.as_index(), 2);
    assert!(matches!(m, TxOrderingMessage::TxRef(_)));
    // The late replayed boundary proves the inversion. This is fatal.
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
    // The malformed frame is skipped. The next good frame is returned.
    let (_pos, msg) = sub.next().unwrap();
    assert!(matches!(msg, TxOrderingMessage::BoundaryStart(_)));
}

/// The batcher's cursor is one system record on the session's ingress:
/// kind 7, then the posted head. The sealer reads it by fixed offsets.
#[test]
fn the_posted_cursor_publisher_offers_the_kind_7_record() {
    use kardamom_cluster_adapter::gateway::fakes::FakeIngress;

    let ingress = FakeIngress::new();
    let sub = ClusterTxOrderingSubscription::new(FakeEgress::new()).with_ingress(ingress.clone());
    let mut publisher = sub.posted_cursor_publisher();
    assert_eq!(publisher.publish(0x0102), OfferOutcome::Accepted);
    assert_eq!(
        ingress.accepted(),
        vec![vec![7u8, 0x02, 0x01, 0, 0, 0, 0, 0, 0]],
        "kind 7, then the head as u64 LE"
    );
    ingress.set_outcome(OfferOutcome::NotConnected);
    assert_eq!(publisher.publish(3), OfferOutcome::NotConnected);
}

/// An executor's recorded cursor is one system record on the session's
/// ingress: kind 9, the executor id, then the cursor. The sealer reads it
/// by fixed offsets.
#[test]
fn the_recorded_cursor_publisher_offers_the_kind_9_record() {
    use kardamom_cluster_adapter::gateway::fakes::FakeIngress;

    let ingress = FakeIngress::new();
    let sub = ClusterTxOrderingSubscription::new(FakeEgress::new()).with_ingress(ingress.clone());
    let mut publisher = sub.recorded_cursor_publisher(2);
    assert_eq!(publisher.publish(0x0102), OfferOutcome::Accepted);
    assert_eq!(
        ingress.accepted(),
        vec![vec![9u8, 2, 0x02, 0x01, 0, 0, 0, 0, 0, 0]],
        "kind 9, the executor id, then the cursor as u64 LE"
    );
    ingress.set_outcome(OfferOutcome::NotConnected);
    assert_eq!(publisher.publish(3), OfferOutcome::NotConnected);
}

/// The status and the lag rejects are not records of the ordering: a
/// consumer skips them and delivers the stream around them.
#[test]
fn status_and_lag_reject_frames_are_skipped() {
    use kardamom_cluster_adapter::wire::{
        encode_da_lag_reject, encode_record_lag_reject, encode_status,
    };

    let egress = FakeEgress::new();
    egress.push(encode_status(&kardamom_types::ClusterStatus::default()));
    egress.push(encode_da_lag_reject(
        alloy_primitives::Address::ZERO,
        0,
        &kardamom_types::ClusterStatus::default(),
    ));
    egress.push(encode_record_lag_reject(
        alloy_primitives::Address::ZERO,
        0,
        0,
        &kardamom_types::cluster_status::RecordLagStatus::default(),
    ));
    egress.push(encode_egress_record(0, &relayed_txref(0, 1)).unwrap());
    egress.push(encode_egress_boundary(1, 1, 250, 0));
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    assert_eq!(label(sub.next().unwrap()), "r0");
    assert_eq!(label(sub.next().unwrap()), "b1");
}
