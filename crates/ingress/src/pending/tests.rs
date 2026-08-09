use super::*;
use alloy_primitives::B256;

fn dummy_receipt(pos: BPosition) -> Receipt {
    Receipt {
        tx_idx: pos,
        tx_hash: B256::ZERO,
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }
}

fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

// --- OnQuorum (default, the original behavior) -----------------------

#[tokio::test]
async fn quorum_parks_until_receipt_and_watermark_both_arrive() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
    let sender = Address::repeat_byte(0x11);
    let nonce = 7u64;
    let position = pos(100);

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_receipt(sender, nonce, dummy_receipt(position)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(p.len(), 1, "must not release before quorum catches up");

    p.update_quorum_watermark(QuorumWatermark { position })
        .await;
    let res = waiter.await.unwrap().unwrap();
    assert_eq!(res.receipt.tx_idx, position);
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn tx_error_releases_parked_client_with_duplicate() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
    let sender = Address::repeat_byte(0x55);
    let nonce = 3u64;

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_tx_error(
        sender,
        nonce,
        TxErrorReason::DuplicatedTx { expected_nonce: 9 },
    )
    .await;

    let err = waiter
        .await
        .expect("join")
        .expect_err("on_tx_error must release with Err");
    assert!(
        matches!(err, IngressError::Duplicate((s, n)) if s == sender && n == nonce),
        "got {err:?}"
    );
    assert_eq!(p.len(), 0, "entry removed on release");
}

#[tokio::test]
async fn eviction_releases_the_parked_wait_with_an_evicted_error() {
    // The wedge fix: a sequencer overload-shed (Evicted) must error the
    // parked submit instead of leaving it waiting for a receipt that can
    // never arrive (the pre-fix silent evict permanently gapped the
    // sender and pinned the connection until timeout).
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
    let sender = Address::repeat_byte(0x77);
    let nonce = 21u64;

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_tx_error(sender, nonce, TxErrorReason::Evicted { expected_nonce: 18 })
        .await;

    let res = waiter.await.expect("join");
    match res {
        Err(IngressError::Evicted((s, n))) => {
            assert_eq!((s, n), (sender, nonce));
        }
        other => panic!("expected Evicted release, got {other:?}"),
    }
    assert_eq!(p.len(), 0, "entry removed on release");
}

#[tokio::test]
async fn success_arriving_within_the_grace_overrides_a_rejection() {
    // F02.6: with racing sequencer replicas, replica A's DuplicatedTx can
    // arrive BEFORE replica B's receipt for the same tx. The rejection is
    // held for the grace window; the receipt must win.
    let p = Arc::new(PendingReceipts::with_error_grace(
        AckPolicy::OnOffer,
        Duration::from_millis(200),
    ));
    let sender = Address::repeat_byte(0x88);
    let nonce = 5u64;
    let position = pos(77);

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Rejection first (replica A, stale floor)...
    p.on_tx_error(
        sender,
        nonce,
        TxErrorReason::DuplicatedTx { expected_nonce: 6 },
    )
    .await;
    // ...then the twin's success, well inside the grace window.
    tokio::time::sleep(Duration::from_millis(20)).await;
    p.on_receipt(sender, nonce, dummy_receipt(position)).await;

    let res = waiter
        .await
        .expect("join")
        .expect("success must override the racing rejection");
    assert_eq!(res.receipt.tx_idx, position);
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn rejection_releases_after_the_grace_when_no_success_arrives() {
    // A genuine duplicate (both replicas reject, no receipt ever) still
    // reaches the client — just delayed by the grace window.
    let p = Arc::new(PendingReceipts::with_error_grace(
        AckPolicy::OnOffer,
        Duration::from_millis(50),
    ));
    let sender = Address::repeat_byte(0x99);
    let nonce = 2u64;

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_tx_error(
        sender,
        nonce,
        TxErrorReason::DuplicatedTx { expected_nonce: 9 },
    )
    .await;
    let err = waiter
        .await
        .expect("join")
        .expect_err("no success arrived — the rejection must be released");
    assert!(
        matches!(err, IngressError::Duplicate((s, n)) if s == sender && n == nonce),
        "got {err:?}"
    );
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn watermark_gated_receipt_suppresses_a_rejection() {
    // A receipt that arrived but is still parked on the durability gate
    // already proves the tx landed — a late rejection must not evict it.
    let p = Arc::new(PendingReceipts::with_error_grace(
        AckPolicy::OnQuorum,
        Duration::ZERO, // inline release path; the stored receipt must gate it
    ));
    let sender = Address::repeat_byte(0xAA);
    let nonce = 1u64;
    let position = pos(9);

    let wait = p.register(sender, nonce);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Receipt arrives but quorum hasn't caught up — stored, not released.
    p.on_receipt(sender, nonce, dummy_receipt(position)).await;
    // Twin's rejection must be suppressed by the stored receipt.
    p.on_tx_error(
        sender,
        nonce,
        TxErrorReason::DuplicatedTx { expected_nonce: 2 },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(p.len(), 1, "entry must survive the rejection");

    // Quorum catches up → the success is delivered.
    p.update_quorum_watermark(QuorumWatermark { position })
        .await;
    let res = waiter.await.expect("join").expect("receipt must win");
    assert_eq!(res.receipt.tx_idx, position);
}

#[tokio::test]
async fn tx_error_for_unparked_key_is_noop() {
    // No client parked for this (sender, nonce) → on_tx_error returns
    // silently without panicking.
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
    p.on_tx_error(
        Address::repeat_byte(0x77),
        42,
        TxErrorReason::DuplicatedTx {
            expected_nonce: 100,
        },
    )
    .await;
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn quorum_releases_immediately_when_watermark_already_past() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
    let sender = Address::repeat_byte(0x22);
    let position = pos(5);
    p.update_quorum_watermark(QuorumWatermark {
        position: pos(1000),
    })
    .await;

    let wait = p.register(sender, 1);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    p.on_receipt(sender, 1, dummy_receipt(position)).await;
    let res = waiter.await.unwrap().unwrap();
    assert_eq!(res.receipt.tx_idx, position);
}

#[tokio::test]
async fn quorum_does_not_release_on_local_watermark_alone() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
    let sender = Address::repeat_byte(0x33);
    let position = pos(50);

    let wait = p.register(sender, 0);
    let waiter =
        tokio::spawn(async move { wait.await_with_timeout(Duration::from_millis(50)).await });
    tokio::time::sleep(Duration::from_millis(5)).await;

    p.on_receipt(sender, 0, dummy_receipt(position)).await;
    p.update_local_watermark(FsyncWatermark {
        recorder_id: 0,
        position,
    })
    .await;

    // Local advanced but quorum didn't — must time out.
    let err = waiter.await.unwrap().unwrap_err();
    assert!(matches!(err, IngressError::Timeout));
}

// --- OnOffer (no durability gate) ------------------------------------

#[tokio::test]
async fn on_offer_releases_as_soon_as_receipt_arrives() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
    let sender = Address::repeat_byte(0x44);
    let position = pos(1);

    let wait = p.register(sender, 0);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // No watermark updates at all — must still release on receipt.
    p.on_receipt(sender, 0, dummy_receipt(position)).await;
    let res = waiter.await.unwrap().unwrap();
    assert_eq!(res.receipt.tx_idx, position);
}

// --- OnLocalFsync (local-only, ignores quorum) -----------------------

#[tokio::test]
async fn local_releases_on_local_watermark_only() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnLocalFsync));
    let sender = Address::repeat_byte(0x55);
    let position = pos(20);

    let wait = p.register(sender, 0);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_receipt(sender, 0, dummy_receipt(position)).await;
    // Quorum advances but policy ignores it — still parked.
    p.update_quorum_watermark(QuorumWatermark { position })
        .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(p.len(), 1);

    // Local advances → releases.
    p.update_local_watermark(FsyncWatermark {
        recorder_id: 0,
        position,
    })
    .await;
    let res = waiter.await.unwrap().unwrap();
    assert_eq!(res.receipt.tx_idx, position);
}

// --- OnLocalFsyncAndQuorum (both required) ---------------------------

#[tokio::test]
async fn both_requires_local_and_quorum_to_advance() {
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnLocalFsyncAndQuorum));
    let sender = Address::repeat_byte(0x66);
    let position = pos(33);

    let wait = p.register(sender, 0);
    let waiter = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    p.on_receipt(sender, 0, dummy_receipt(position)).await;
    // Only local — still parked.
    p.update_local_watermark(FsyncWatermark {
        recorder_id: 0,
        position,
    })
    .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(p.len(), 1);

    // Now quorum too → releases.
    p.update_quorum_watermark(QuorumWatermark { position })
        .await;
    let res = waiter.await.unwrap().unwrap();
    assert_eq!(res.receipt.tx_idx, position);
}

// --- Timeout still works regardless of policy ------------------------

#[tokio::test]
async fn times_out_when_neither_event_arrives() {
    let p = PendingReceipts::default();
    let wait = p.register(Address::ZERO, 0);
    let err = wait
        .await_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::Timeout));
    assert_eq!(p.len(), 0);
}

// --- Cancelled-future cleanup (the #81 follow-up) ---------------------

#[tokio::test]
async fn dropping_an_unresolved_wait_removes_its_entry() {
    // The core leak: a client disconnect drops the RPC handler future,
    // which drops the PendingWait without any of the release paths
    // running. The entry must leave the map right there.
    let p = PendingReceipts::new(AckPolicy::OnOffer);
    let wait = p.register(Address::repeat_byte(0xB1), 4);
    assert_eq!(p.len(), 1);
    drop(wait);
    assert_eq!(p.len(), 0, "dropped wait must unregister its entry");
}

#[tokio::test]
async fn aborting_a_parked_await_cleans_up() {
    // Same, through the real shape: the wait is parked inside
    // await_with_timeout when the owning task is aborted (the future is
    // dropped mid-await, exactly what jsonrpsee does on disconnect).
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
    let wait = p.register(Address::repeat_byte(0xB2), 7);
    let task = tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(60)).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(p.len(), 1, "parked");
    task.abort();
    let _ = task.await;
    assert_eq!(p.len(), 0, "abort must unregister the parked entry");
}

#[tokio::test]
async fn stale_wait_drop_spares_a_replacement_registration() {
    // (sender, nonce) re-registered while an old wait still exists: the
    // old wait's Drop is identity-guarded and must NOT evict the new
    // registration.
    let p = PendingReceipts::new(AckPolicy::OnOffer);
    let sender = Address::repeat_byte(0xB3);
    let stale = p.register(sender, 1);
    let fresh = p.register(sender, 1); // replaces the slot
    assert_eq!(p.len(), 1);
    drop(stale);
    assert_eq!(p.len(), 1, "stale drop must spare the replacement");
    drop(fresh);
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn a_dead_slot_reads_as_absent() {
    // Structurally unreachable through the public API (Drop removes the
    // slot BEFORE its entry dies) — simulated directly to pin the reader
    // contract anyway: a dead Weak means "no client parked", never a
    // panic or a release.
    let p = PendingReceipts::new(AckPolicy::OnOffer);
    let sender = Address::repeat_byte(0xB5);
    p.map.insert((sender, 3), Weak::new());
    p.on_receipt(sender, 3, dummy_receipt(pos(1))).await;
    p.on_tx_error(sender, 3, TxErrorReason::DuplicatedTx { expected_nonce: 9 })
        .await;
}

#[tokio::test]
async fn grace_task_does_not_keep_a_disconnected_entry_alive() {
    // The deferred rejection-release holds only a Weak across its grace
    // sleep: a client that disconnects mid-grace must free its entry
    // immediately, not after the grace fires.
    let p = Arc::new(PendingReceipts::with_error_grace(
        AckPolicy::OnOffer,
        Duration::from_millis(100),
    ));
    let sender = Address::repeat_byte(0xB6);
    let wait = p.register(sender, 6);
    let weak = Arc::downgrade(&wait.entry);
    p.on_tx_error(sender, 6, TxErrorReason::DuplicatedTx { expected_nonce: 9 })
        .await;
    drop(wait); // disconnect mid-grace
    assert_eq!(
        weak.strong_count(),
        0,
        "the pending grace task must not keep the entry alive"
    );
    assert_eq!(p.len(), 0);
    // Let the grace fire against the dead weak: must be a clean no-op.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn replacement_registration_still_releases_on_receipt() {
    // End-to-end on the replacement: after a stale wait is dropped, the
    // fresh registration still parks and releases normally.
    let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
    let sender = Address::repeat_byte(0xB4);
    let position = pos(12);
    let stale = p.register(sender, 0);
    let fresh = p.register(sender, 0);
    drop(stale);
    let waiter =
        tokio::spawn(async move { fresh.await_with_timeout(Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    p.on_receipt(sender, 0, dummy_receipt(position)).await;
    let res = waiter.await.unwrap().expect("fresh wait must release");
    assert_eq!(res.receipt.tx_idx, position);
    assert_eq!(p.len(), 0);
}
