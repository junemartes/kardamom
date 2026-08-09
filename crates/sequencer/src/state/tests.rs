use super::*;

fn s(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[test]
fn match_publishes_and_advances() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    let out = st.process(s(1), 0, 100);
    assert_eq!(
        out.actions,
        vec![ProcessAction::Publish {
            nonce: 0,
            payload: 100
        }]
    );
    assert_eq!(out.outcome, NonceOutcome::Matched);
    assert_eq!(st.next_nonce(s(1)), 1);
}

#[test]
fn match_drains_subsequent_buffered() {
    let mut st: PartitionState<u32> = PartitionState::new(8);
    assert!(matches!(
        st.process(s(1), 1, 11).outcome,
        NonceOutcome::Buffered
    ));
    assert!(matches!(
        st.process(s(1), 2, 22).outcome,
        NonceOutcome::Buffered
    ));
    let out = st.process(s(1), 0, 0);
    assert_eq!(
        out.actions,
        vec![
            ProcessAction::Publish {
                nonce: 0,
                payload: 0
            },
            ProcessAction::Publish {
                nonce: 1,
                payload: 11
            },
            ProcessAction::Publish {
                nonce: 2,
                payload: 22
            },
        ]
    );
    assert_eq!(st.next_nonce(s(1)), 3);
}

#[test]
fn past_reports_duplicate() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.process(s(1), 0, 0);
    st.process(s(1), 1, 1);
    let out = st.process(s(1), 0, 999);
    assert_eq!(
        out.actions,
        vec![ProcessAction::ReportDuplicate {
            nonce: 0,
            expected_nonce: 2
        }]
    );
    assert_eq!(out.outcome, NonceOutcome::Past);
    assert_eq!(st.next_nonce(s(1)), 2);
}

#[test]
fn future_is_buffered() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    let out = st.process(s(1), 5, 55);
    assert_eq!(out.actions, vec![]);
    assert_eq!(out.outcome, NonceOutcome::Buffered);
    assert_eq!(st.next_nonce(s(1)), 0);
}

#[test]
fn buffer_full_rejects_furthest_future() {
    // Capacity 2, buffer {5,6}; incoming 7 is the furthest-future → rejected,
    // NOT evicting the low run. (Old behaviour evicted 5 and wedged.)
    let mut st: PartitionState<u32> = PartitionState::new(2);
    st.process(s(1), 5, 5);
    st.process(s(1), 6, 6);
    let out = st.process(s(1), 7, 7);
    assert_eq!(out.outcome, NonceOutcome::RejectedTooFar { nonce: 7 });
}

#[test]
fn overflow_then_expected_arrives_drains_full_run_no_wedge() {
    // The end-to-end regression guard: a sender floods far-future nonces past
    // capacity, then its expected nonce (0) finally arrives. With lowest-wins
    // the contiguous run 0..=3 survives and publishes in order — the sender
    // is never permanently wedged. (Old evict-oldest would have dropped 0's
    // successors and stalled the sender forever.)
    let mut st: PartitionState<u32> = PartitionState::new(4);
    // expected is 0; buffer the near run 1..=4 (fills capacity 4).
    for n in 1..=4u64 {
        st.process(s(1), n, n as u32);
    }
    // Flood far-future nonces — all rejected, near run untouched.
    for n in 50..70u64 {
        assert_eq!(
            st.process(s(1), n, n as u32).outcome,
            NonceOutcome::RejectedTooFar { nonce: n }
        );
    }
    // Expected 0 arrives → publishes 0,1,2,3,4 in order (the retained run).
    let out = st.process(s(1), 0, 0);
    let published: Vec<u64> = out
        .actions
        .iter()
        .filter_map(|a| match a {
            ProcessAction::Publish { nonce, .. } => Some(*nonce),
            _ => None,
        })
        .collect();
    assert_eq!(published, vec![0, 1, 2, 3, 4]);
    assert_eq!(st.next_nonce(s(1)), 5);
}

// CI first-record audit: rebuffering a backpressured batch must NEVER
// lose a ref. A FULL future-run (capacity items) drained by `process`
// plus the in-flight ingress item is capacity+1 items; re-inserting them
// through the capacity-enforcing `insert` evicted the lowest nonce — a
// silent, permanent per-sender gap. The reinsert path is now unbounded.
#[test]
fn full_buffer_backpressure_rebuffer_loses_nothing() {
    let cap = 4;
    let mut st: PartitionState<u32> = PartitionState::new(cap);
    // Fill the buffer to capacity with the future run 1..=cap.
    for n in 1..=cap as u64 {
        assert!(matches!(
            st.process(s(1), n, 100 + n as u32).outcome,
            NonceOutcome::Buffered
        ));
    }
    // Nonce 0 arrives: the full run drains for publishing (cap+1 items).
    let out = st.process(s(1), 0, 100);
    assert_eq!(out.actions.len(), cap + 1);
    // Backpressure on the FIRST publish: rebuffer the whole batch in
    // reverse, exactly as `flush_drained` does.
    let mut batch: Vec<(u64, u32)> = (0..=cap as u64).map(|n| (n, 100 + n as u32)).collect();
    while let Some((n, p)) = batch.pop() {
        st.reinsert_for_retry(s(1), n, p);
    }
    assert_eq!(st.next_nonce(s(1)), 0, "floor rewound to lowest");
    // The retry drain must return ALL cap+1 refs — nothing evicted.
    let drained = st.drain_pending();
    let nonces: Vec<u64> = drained.iter().map(|(_, n, _)| *n).collect();
    assert_eq!(nonces, (0..=cap as u64).collect::<Vec<_>>());
    assert_eq!(st.next_nonce(s(1)), cap as u64 + 1);
}

// CI first-record audit: a capacity-0 (buffering disabled) config must
// still not lose a MATCHED ref that hit backpressure — the rebuffer path
// bypasses the disabled-buffer drop too.
#[test]
fn disabled_buffer_still_rebuffers_backpressured_match() {
    let mut st: PartitionState<u32> = PartitionState::new(0);
    let out = st.process(s(1), 0, 100);
    assert_eq!(out.actions.len(), 1);
    st.reinsert_for_retry(s(1), 0, 100);
    let drained = st.drain_pending();
    assert_eq!(drained, vec![(s(1), 0, 100)]);
}

#[test]
fn advance_floor_drops_proven_and_advances() {
    let mut st: PartitionState<u32> = PartitionState::new(8);
    // Cold-rejoin shape (F02.1): expected is 0, but the twin already
    // ordered 0..=4 (executed). The replica buffered 3,4 (stale dupes)
    // and 5,6 (live traffic it must regain coverage of).
    for n in [3u64, 4, 5, 6] {
        st.process(s(1), n, n as u32);
    }
    let (from, dropped) = st.advance_floor(s(1), 5).expect("floor must advance");
    assert_eq!(from, 0);
    assert_eq!(dropped, 2, "3 and 4 are receipt-proven duplicates");
    assert_eq!(st.next_nonce(s(1)), 5);
    // The stuck run unsticks: 5,6 drain as contiguous from the floor.
    let drained: Vec<u64> = st.drain_pending().iter().map(|(_, n, _)| *n).collect();
    assert_eq!(drained, vec![5, 6]);
}

#[test]
fn advance_floor_never_regresses() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.process(s(1), 0, 0);
    st.process(s(1), 1, 1);
    assert_eq!(st.next_nonce(s(1)), 2);
    // A floor at/behind next (late receipt) is a no-op.
    assert_eq!(st.advance_floor(s(1), 2), None);
    assert_eq!(st.advance_floor(s(1), 1), None);
    assert_eq!(st.next_nonce(s(1)), 2);
}

#[test]
fn reinsert_for_retry_rewinds_next_nonce() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.process(s(1), 0, 100);
    assert_eq!(st.next_nonce(s(1)), 1);
    // Simulate backpressure: roll back, putting payload 100 back in the buffer.
    st.reinsert_for_retry(s(1), 0, 100);
    assert_eq!(st.next_nonce(s(1)), 0);
    // Retry: state machine re-publishes the buffered payload (100), not the
    // payload arg (999) — exactly-once at the canonical layer.
    let out = st.process(s(1), 0, 999);
    assert_eq!(
        out.actions,
        vec![ProcessAction::Publish {
            nonce: 0,
            payload: 100
        }]
    );
    assert_eq!(st.next_nonce(s(1)), 1);
}
