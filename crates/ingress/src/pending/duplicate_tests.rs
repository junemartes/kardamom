use super::*;
use crate::test_support::{dummy_receipt, pos};

#[tokio::test(start_paused = true)]
async fn delayed_receipt_still_waits_for_quorum_after_duplicate() {
    let pending = PendingReceipts::new(AckPolicy::OnQuorum);
    let sender = Address::repeat_byte(0xC1);
    let wait = pending.register(sender, 2);
    let task = tokio::spawn(wait.await_with_timeout(Duration::from_secs(5)));
    pending
        .on_tx_error(sender, 2, TxErrorReason::DuplicatedTx { expected_nonce: 3 })
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!task.is_finished());
    pending.on_receipt(sender, 2, dummy_receipt(pos(17))).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "a receipt cannot bypass its durability gate"
    );
    pending
        .update_quorum_watermark(QuorumWatermark { position: pos(17) })
        .await;
    assert_eq!(task.await.unwrap().unwrap().receipt.tx_idx, pos(17));
    assert!(pending.is_empty());
}

#[tokio::test(start_paused = true)]
async fn receipt_without_quorum_times_out_instead_of_returning_duplicate() {
    let pending = PendingReceipts::new(AckPolicy::OnQuorum);
    let sender = Address::repeat_byte(0xC2);
    let wait = pending.register(sender, 2);
    pending
        .on_tx_error(sender, 2, TxErrorReason::DuplicatedTx { expected_nonce: 3 })
        .await;
    pending.on_receipt(sender, 2, dummy_receipt(pos(17))).await;
    let result = wait.await_with_timeout(Duration::from_secs(5)).await;
    assert!(matches!(result, Err(IngressError::Timeout)), "{result:?}");
    assert!(pending.is_empty());
}

#[tokio::test]
async fn duplicate_does_not_keep_a_disconnected_entry_alive() {
    let pending = PendingReceipts::new(AckPolicy::OnOffer);
    let sender = Address::repeat_byte(0xC3);
    let weak = {
        let wait = pending.register(sender, 2);
        pending
            .on_tx_error(sender, 2, TxErrorReason::DuplicatedTx { expected_nonce: 3 })
            .await;
        Arc::downgrade(&wait.entry)
    };
    assert_eq!(weak.strong_count(), 0);
    assert!(pending.is_empty());
}
