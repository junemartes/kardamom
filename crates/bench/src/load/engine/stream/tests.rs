use super::*;
use crate::load::engine::Tracker;
use crate::load::tracker::ReceiptStatus;
use alloy_primitives::{Address, U256};
use std::time::Duration;

fn producer(per_sender: usize, starts: Vec<u64>) -> Producer {
    Producer {
        mix: TransferMix::single(TransferShape::plain(Address::ZERO, U256::from(1))),
        signers: SignerSet::derive(crate::ANVIL_MNEMONIC, u32::try_from(starts.len()).unwrap())
            .unwrap(),
        params: TxPlanParams {
            chain_id: 1,
            nonce_start: 0,
            gas_price: 1,
        },
        plan: QueuePlan {
            chain_id: 1,
            per_sender,
            starts,
        },
        offset: 0,
    }
}

#[tokio::test]
async fn batches_match_presigning_bytes_hashes_and_nonce_order() {
    let producer = producer(BATCH_SIZE + 3, vec![17, 203]);
    let signers = producer.signers.clone();
    let mut expected = Queues::new(
        producer
            .mix
            .pregenerate(
                &signers,
                producer.plan.per_sender,
                producer.params,
                &producer.plan.starts,
            )
            .unwrap(),
    );
    let mut stream = producer.start();
    let first = stream.receive().await.unwrap().unwrap();
    assert_eq!(first.iter().map(Vec::len).sum::<usize>(), BATCH_SIZE);
    let mut actual = Queues::new(first);
    let mut tracker = Tracker::new().unwrap();
    tracker.expect(&signers, actual.planned());
    actual.more = Some(stream);
    let mut seen = 0;
    while let Some(tx) = actual.next(&tracker).await.unwrap() {
        let old = expected.pop_next().unwrap();
        assert_eq!(
            (tx.sender, tx.nonce, tx.hash, tx.raw),
            (old.sender, old.nonce, old.hash, old.raw)
        );
        // Receipt identity and nonce ordering must still be checked after refill.
        tracker.confirm(
            tx.hash,
            &ReceiptStatus {
                status: 1,
                gas: 21_000,
                from: Some(signers[tx.sender].address),
                block: Some(tx.nonce),
            },
            Duration::ZERO,
        );
        seen += 1;
    }
    assert_eq!(seen, 2 * (BATCH_SIZE + 3));
    assert!(expected.pop_next().is_none());
    assert_eq!(tracker.counts().bad_receipt, 0);
}

#[tokio::test]
async fn streamed_expectations_check_identity_on_both_sides_of_the_feed_race() {
    let producer = producer(2, vec![1]);
    let signers = producer.signers.clone();
    let mut stream = producer.start();
    let first = stream.receive().await.unwrap().unwrap();
    let tracker = Tracker::new().unwrap();
    tracker.expect_more(&signers, first.iter().flatten());
    let receipt = ReceiptStatus {
        status: 1,
        gas: 21_000,
        from: Some(Address::ZERO),
        block: Some(1),
    };
    let first_hash = first[0][0].hash;
    let second_hash = first[0][1].hash;
    tracker.confirm_from_feed(first_hash, &receipt);
    tracker.await_feed(first_hash, std::time::Instant::now());
    tracker.await_feed(second_hash, std::time::Instant::now());
    tracker.confirm_from_feed(second_hash, &receipt);
    assert_eq!(
        (tracker.counts().receipted, tracker.counts().bad_receipt),
        (2, 2)
    );
}

#[tokio::test]
async fn a_later_signing_failure_is_an_error_not_a_drained_queue() {
    let producer = producer(BATCH_SIZE + 1, vec![u64::MAX - BATCH_SIZE as u64 + 1]);
    let signers = producer.signers.clone();
    let mut stream = producer.start();
    let first = stream.receive().await.unwrap().unwrap();
    let mut queues = Queues::new(first);
    let mut tracker = Tracker::new().unwrap();
    tracker.expect(&signers, queues.planned());
    queues.more = Some(stream);
    // Drain without submitting; max nonce is deliberately the last valid one.
    while queues.pop_next().is_some() {}
    assert!(
        queues
            .next(&tracker)
            .await
            .unwrap_err()
            .to_string()
            .contains("overflows")
    );
}

#[tokio::test]
async fn a_lost_producer_cannot_look_like_a_complete_load() {
    let (sender, receiver) = mpsc::channel(1);
    let mut stream = Stream {
        receiver,
        signers: producer(1, vec![0]).signers,
    };
    sender.send(Ok(Some(vec![vec![]]))).await.unwrap();
    // Closing without the explicit terminal message is not successful exhaustion.
    let task = tokio::spawn(async move { sender.closed().await });
    task.abort();
    let _ = task.await;
    assert!(stream.receive().await.unwrap().is_some());
    assert!(
        stream
            .receive()
            .await
            .unwrap_err()
            .to_string()
            .contains("final batch")
    );
}

#[tokio::test]
async fn dropping_the_consumer_stops_a_blocked_signer() {
    let producer = producer(usize::MAX, vec![0]);
    let (sender, mut receiver) = mpsc::channel(1);
    let task = tokio::task::spawn_blocking(move || producer.produce(&sender));
    assert!(receiver.recv().await.unwrap().unwrap().is_some());
    receiver.close();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn streamed_receipts_cannot_regress_the_first_batch_nonce_order() {
    let mut producer = producer(2, vec![5]);
    let batch = producer.next().unwrap().unwrap();
    let mut tracker = Tracker::new().unwrap();
    tracker.expect(&producer.signers, batch[0][..1].iter());
    tracker.expect_more(&producer.signers, batch[0][1..].iter());
    let receipt = ReceiptStatus {
        status: 1,
        gas: 21_000,
        from: Some(producer.signers[0].address),
        block: Some(10),
    };
    tracker.confirm(batch[0][0].hash, &receipt, Duration::ZERO);
    tracker.confirm(
        batch[0][1].hash,
        &ReceiptStatus {
            block: Some(9),
            ..receipt
        },
        Duration::ZERO,
    );
    assert_eq!(tracker.counts().bad_receipt, 1);
}

#[tokio::test]
async fn a_signer_failure_cannot_write_a_passing_run_report() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(Err(anyhow::anyhow!("signer failed")))
        .await
        .unwrap();
    let signers = producer(1, vec![0]).signers;
    let mut queues = Queues::new(Vec::new());
    queues.more = Some(Stream {
        receiver,
        signers: signers.clone(),
    });
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    let config = LoadConfig {
        rpc: "http://127.0.0.1:9".into(),
        chaos_mode: true,
        scrape: Vec::new(),
        output: Some(output.clone()),
        ..LoadConfig::default()
    };
    let setup = crate::load::RunSetup {
        client: std::sync::Arc::new(
            crate::config::rpc_client(&config.rpc, config.max_in_flight.get()).unwrap(),
        ),
        signers,
        queues,
    };
    let prepared = crate::load::Prepared::new(config, setup).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), prepared.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("signer failed"));
    assert!(!output.exists());
}
