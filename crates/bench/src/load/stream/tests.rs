use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use super::*;
use crate::load::tracker::{ReceiptStatus, Tracker};
use crate::load::{NonceStart, SenderRange};

fn config() -> LoadConfig {
    LoadConfig {
        chaos_mode: true,
        duration: Duration::from_secs(7200),
        sender_range: SenderRange::new(0, NonZeroU32::new(2).unwrap()).unwrap(),
        nonce_start: NonceStart::Fixed(0),
        ..LoadConfig::default()
    }
}

#[tokio::test]
async fn bounded_batches_match_the_full_signed_sequence_and_register_late_receipts() {
    let cfg = config();
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 2).unwrap();
    let plan = || QueuePlan {
        chain_id: 1,
        per_sender: 768,
        starts: vec![7, 93],
    };
    let mut reference = cfg.build_queues(&signers, &plan()).unwrap().queues;
    let mut streamed = SignedBatches::prepare(cfg, signers.clone(), plan())
        .unwrap()
        .queues;
    assert_eq!(streamed.planned().count(), BATCH_PER_SENDER * 2);
    let mut tracker = Tracker::new().unwrap();
    tracker.expect(&signers, streamed.planned());
    for index in 0..1536 {
        let tx = streamed.next(&tracker).await.unwrap().unwrap();
        let expected = reference.pop_next().unwrap();
        assert_eq!(
            (tx.hash, tx.raw, tx.sender, tx.nonce),
            (expected.hash, expected.raw, expected.sender, expected.nonce)
        );
        let receipt = ReceiptStatus {
            status: 1,
            gas: 21_000,
            from: Some(signers[expected.sender].address),
            block: Some(10),
        };
        tracker.confirm_from_feed(tx.hash, &receipt);
        tracker.await_feed(tx.hash, Instant::now());
        assert_eq!(tracker.counts().receipted, index + 1);
    }
    let tx = streamed.next(&tracker).await.unwrap().unwrap();
    tracker.confirm(
        tx.hash,
        &ReceiptStatus {
            status: 1,
            gas: 21_000,
            from: Some(signers[1].address),
            block: Some(10),
        },
        Duration::ZERO,
    );
    let tx = streamed.next(&tracker).await.unwrap().unwrap();
    tracker.confirm(
        tx.hash,
        &ReceiptStatus {
            status: 1,
            gas: 21_000,
            from: Some(signers[1].address),
            block: Some(9),
        },
        Duration::ZERO,
    );
    assert_eq!(
        tracker.counts().bad_receipt,
        2,
        "late batches retain sender and nonce placement checks"
    );
    streamed.stop_refill();
}

#[tokio::test]
async fn signer_failure_and_unexpected_worker_exit_are_errors() {
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 2).unwrap();
    let mut queues = SignedBatches::prepare(
        config(),
        signers.clone(),
        QueuePlan {
            chain_id: 1,
            per_sender: usize::MAX,
            starts: vec![u64::MAX - 255; 2],
        },
    )
    .unwrap()
    .queues;
    assert_eq!(queues.planned().count(), 512);
    let tracker = Tracker::new().unwrap();
    for _ in 0..512 {
        queues.next(&tracker).await.unwrap().unwrap();
    }
    assert!(
        queues
            .next(&tracker)
            .await
            .unwrap_err()
            .to_string()
            .contains("nonce overflows")
    );
    let receiver = {
        let (_sender, receiver) = mpsc::channel(2);
        receiver
    };
    let mut dead = Queues::new(Vec::new()).with_refill(SignedBatches { receiver, signers });
    assert!(
        dead.next(&tracker)
            .await
            .unwrap_err()
            .to_string()
            .contains("worker stopped")
    );
}

#[tokio::test]
async fn a_stalled_signer_is_cancellable_and_stopping_closes_its_bounded_channel() {
    let (sender, receiver) = mpsc::channel(2);
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 2).unwrap();
    let mut queues = Queues::new(Vec::new()).with_refill(SignedBatches { receiver, signers });
    let tracker = Tracker::new().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), queues.next(&tracker))
            .await
            .is_err()
    );
    assert_eq!(tracker.counts().offered, 0);
    queues.stop_refill();
    assert!(sender.is_closed());
    assert!(queues.next(&tracker).await.unwrap().is_none());
}
