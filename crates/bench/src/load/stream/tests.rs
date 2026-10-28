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

#[tokio::test]
async fn a_signer_failure_cannot_write_a_passing_run_report() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(Err(anyhow::anyhow!("signer failed")))
        .await
        .unwrap();
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 1).unwrap();
    let ingress = crate::load::prepared::tests::Ingress::start().await;
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    let config = ingress.config(Duration::from_secs(30), &output);
    let first = config
        .build_queues(
            &signers,
            &QueuePlan {
                chain_id: 1,
                per_sender: 1,
                starts: vec![0],
            },
        )
        .unwrap();
    let queues = first.queues.with_refill(SignedBatches {
        receiver,
        signers: signers.clone(),
    });
    let setup = crate::load::RunSetup {
        client: std::sync::Arc::new(
            crate::config::rpc_client(&config.rpc, config.max_in_flight.get()).unwrap(),
        ),
        signers,
        queues,
    };
    let prepared = crate::load::Prepared::new(config, setup).unwrap();
    let progress = prepared.progress();
    let error = tokio::time::timeout(Duration::from_secs(20), prepared.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("signer failed"));
    let counts = progress.tracker.counts();
    assert_eq!(
        (counts.offered, counts.accepted, counts.receipted),
        (1, 1, 1)
    );
    assert!(!output.exists());
}

#[tokio::test]
async fn closing_a_real_producers_consumer_stops_signing() {
    let (sender, mut receiver) = mpsc::channel(BUFFERED_BATCHES);
    let producer = Producer {
        cfg: config(),
        signers: SignerSet::derive(crate::ANVIL_MNEMONIC, 2).unwrap(),
        plan: QueuePlan {
            chain_id: 1,
            per_sender: BATCH_PER_SENDER,
            starts: vec![0, 0],
        },
        sender,
    };
    let task = tokio::task::spawn_blocking(move || producer.run());
    assert!(receiver.recv().await.unwrap().is_ok());
    receiver.close();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn streamed_expectations_check_receipts_when_the_ack_precedes_the_feed() {
    let cfg = config();
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 2).unwrap();
    let batch = cfg
        .build_queues(
            &signers,
            &QueuePlan {
                chain_id: 1,
                per_sender: 1,
                starts: vec![0, 0],
            },
        )
        .unwrap();
    let tracker = Tracker::new().unwrap();
    tracker.expect_streamed(&signers, batch.queues.planned());
    let tx = batch.queues.planned().next().unwrap();
    tracker.await_feed(tx.hash, Instant::now());
    tracker.confirm_from_feed(
        tx.hash,
        &ReceiptStatus {
            status: 1,
            gas: 21_000,
            from: Some(signers[1].address),
            block: Some(10),
        },
    );
    assert_eq!(
        (tracker.counts().receipted, tracker.counts().bad_receipt),
        (1, 1)
    );
}
