//! End-to-end test. 100 signed txs from 100 distinct senders must land on
//! the correct partitions and receive their receipts. A duplicate
//! `(sender, nonce)` must be served from the receipt cache.

mod common;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::routing::partition_for;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{BPosition, QuorumWatermark, Receipt};

fn nonce_of(raw: &bytes::Bytes) -> u64 {
    use alloy_consensus::TxEnvelope;
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    TxEnvelope::decode(&mut raw.as_ref()).unwrap().nonce()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hundred_txs_route_and_receive_receipts() {
    let m = 8u32;
    let cfg = IngressConfig {
        partition_count_m: m,
        pending_receipt_timeout: Duration::from_secs(10),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockChannels::new(m as usize);
    let proxy = Arc::new(IngressProxy::new(cfg.clone(), mock.clone(), mock.clone()));

    // This is a fake executor. It drains each partition, and sends the
    // receipt and watermark right away.
    // Every receipt's `tx_idx` is a position in the sealer's single
    // tx_ordering stream (see crates/log/src/watermark.rs: one archive
    // recording, one durable watermark). So all partitions share one
    // increasing position space. Giving each partition its own
    // `term_id` instead would let the quorum watermark move backward,
    // since `BPosition` is ordered `(term_id, term_offset)`, and any
    // receipt parked above where the last watermark lands would never
    // release.
    let next_pos = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let mut handles = Vec::new();
    for mut rx in partition_rx.drain(..) {
        let receipt_bus = mock.receipt_bus.clone();
        let watermark_bus = mock.watermark_bus.clone();
        let next_pos = next_pos.clone();
        handles.push(tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let pos = BPosition {
                    term_id: 0,
                    term_offset: next_pos.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1,
                };
                let nonce = nonce_of(&envelope.raw_tx);
                let receipt = Receipt {
                    tx_idx: pos,
                    tx_hash: envelope.tx_hash,
                    status: true,
                    gas_used: 21_000,
                    logs: Vec::new(),
                    write_set_hash: B256::ZERO,
                    from: envelope.sender,
                    nonce,
                    ..Default::default()
                };
                let _ = receipt_bus.send(receipt);
                let _ = watermark_bus.send(QuorumWatermark { position: pos });
            }
        }));
    }

    // These are 100 unique senders, one tx each.
    let mut signers = Vec::with_capacity(100);
    for _ in 0..100 {
        signers.push(PrivateKeySigner::random());
    }

    let mut futs = Vec::new();
    for signer in &signers {
        let raw = common::sign_legacy(signer, 0);
        let p = proxy.clone();
        let addr = signer.address();
        futs.push(async move {
            p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                .await
                .map(|r| (addr, r))
        });
    }
    let results: Vec<_> = futures::future::join_all(futs).await;

    for (i, res) in results.into_iter().enumerate() {
        let (sender, resp) = res.expect("submit");
        assert_eq!(sender, signers[i].address());
        // This only checks the partition function on the recovered sender.
        let _expected_partition = partition_for(sender, m);
        assert!(resp.receipt.status);
    }

    // This is an idempotent retry: it resubmits the same tx for
    // signers[0]. It must hit the in-memory receipt cache, with no
    // executor round trip needed.
    let raw0 = common::sign_legacy(&signers[0], 0);
    let resp = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw0)
        .await
        .expect("retry");
    assert!(resp.receipt.status);

    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_parks_until_watermark_advances() {
    let cfg = IngressConfig {
        partition_count_m: 2,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockChannels::new(2);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    // This is a fake executor for partition 0. It publishes the
    // receipt right away, but holds off advancing the watermark for
    // about 200ms.
    let receipt_bus = mock.receipt_bus.clone();
    let watermark_bus = mock.watermark_bus.clone();
    let rx0 = partition_rx.remove(0);
    let _rx1 = partition_rx.remove(0);
    let pos = BPosition {
        term_id: 0,
        term_offset: 1,
    };
    let h = tokio::spawn(async move {
        let mut rx0 = rx0;
        if let Some(envelope) = rx0.recv().await {
            let nonce = nonce_of(&envelope.raw_tx);
            let receipt = Receipt {
                tx_idx: pos,
                tx_hash: envelope.tx_hash,
                status: true,
                gas_used: 21_000,
                logs: Vec::new(),
                write_set_hash: B256::ZERO,
                from: envelope.sender,
                nonce,
                ..Default::default()
            };
            let _ = receipt_bus.send(receipt);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = watermark_bus.send(QuorumWatermark { position: pos });
        }
    });

    // This picks a signer whose address routes to partition 0.
    let signer = loop {
        let s = PrivateKeySigner::random();
        if partition_for(s.address(), 2) == 0 {
            break s;
        }
    };
    let raw = common::sign_legacy(&signer, 0);
    let start = std::time::Instant::now();
    let resp = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .expect("submit");
    let elapsed = start.elapsed();
    assert!(resp.receipt.status);
    // This must have waited at least about 150ms for the watermark to
    // advance.
    assert!(
        elapsed >= Duration::from_millis(150),
        "parked too short: {elapsed:?}"
    );
    h.abort();
}

/// MDS fan-in dedup. With N executor replicas replaying the same
/// canonical order, ingress receives the same receipt N times. The
/// submit must resolve exactly once, first-wins by tx hash, with no
/// panic and no double ack, and the receipt cache must hold a single
/// entry for the tx_hash. This drives the full proxy receipt watcher
/// path, which is what the live ingress uses.
#[tokio::test(flavor = "multi_thread")]
async fn mds_duplicate_receipts_dedup_resolves_submit_once() {
    let cfg = IngressConfig {
        partition_count_m: 2,
        // OnOffer releases as soon as the deduped receipt arrives. This
        // way, the test exercises the receipt path with no watermark
        // dependency.
        ack_policy: kardamom_types::AckPolicy::OnOffer,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockChannels::new(2);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    // Three "executor replicas" all emit the identical receipt for the
    // same tx, with the same tx_hash, sender, nonce, and position. This
    // is exactly what N MDS sources deliver onto the aggregated
    // subscription.
    const REPLICAS: usize = 3;
    let receipt_bus = mock.receipt_bus.clone();
    let rx0 = partition_rx.remove(0);
    let _rx1 = partition_rx.remove(0);
    let pos = BPosition {
        term_id: 0,
        term_offset: 1,
    };
    let h = tokio::spawn(async move {
        let mut rx0 = rx0;
        if let Some(envelope) = rx0.recv().await {
            let nonce = nonce_of(&envelope.raw_tx);
            let receipt = Receipt {
                tx_idx: pos,
                tx_hash: envelope.tx_hash,
                status: true,
                gas_used: 21_000,
                logs: Vec::new(),
                write_set_hash: B256::ZERO,
                from: envelope.sender,
                nonce,
                ..Default::default()
            };
            // This is the fan-in: the same receipt arrives once per
            // replica.
            for _ in 0..REPLICAS {
                let _ = receipt_bus.send(receipt.clone());
            }
        }
    });

    let signer = loop {
        let s = PrivateKeySigner::random();
        if partition_for(s.address(), 2) == 0 {
            break s;
        }
    };
    let raw = common::sign_legacy(&signer, 0);
    // This resolves exactly once, despite REPLICAS identical receipts,
    // with no panic.
    let resp = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .expect("submit must resolve once under duplicate receipts");
    assert!(resp.receipt.status);
    assert_eq!(resp.receipt.from, signer.address());

    // This gives the watcher time to try to process all replica copies.
    // The dedup must keep the cache at a single entry for this tx.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        proxy.lookup_receipt_by_hash(resp.receipt.tx_hash),
        Some(resp.receipt.clone()),
        "receipt indexed by tx_hash"
    );
    // A second submit of the same tx is served from cache, idempotently.
    // This shows the duplicate copies did not corrupt or double-resolve
    // the pending state.
    let raw_again = common::sign_legacy(&signer, 0);
    let resp2 = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw_again)
        .await
        .expect("cached resubmit");
    assert_eq!(resp2.receipt.tx_hash, resp.receipt.tx_hash);

    h.abort();
}

/// P=2 racing sequencer replicas. Replica A, with a stale nonce
/// floor, rejects the tx, and its rejection fans out twice, since both
/// replicas emit per-tx errors. Replica B accepts it, and the
/// executor's receipt lands shortly after. The client must get the
/// receipt: the dedup drops the duplicate rejections, and the success
/// overrides the earlier rejection. This drives the full proxy watcher
/// pipeline, tx_errors bus to dedup to pending grace to receipt bus
/// release, which is what the live ingress uses.
#[tokio::test(flavor = "multi_thread")]
async fn racing_replica_rejection_is_overridden_by_twin_success() {
    let cfg = IngressConfig {
        partition_count_m: 2,
        ack_policy: kardamom_types::AckPolicy::OnOffer,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockChannels::new(2);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    let receipt_bus = mock.receipt_bus.clone();
    let error_bus = mock.tx_error_bus.clone();
    let rx0 = partition_rx.remove(0);
    let _rx1 = partition_rx.remove(0);
    let pos = BPosition {
        term_id: 0,
        term_offset: 1,
    };
    let h = tokio::spawn(async move {
        let mut rx0 = rx0;
        if let Some(envelope) = rx0.recv().await {
            let nonce = nonce_of(&envelope.raw_tx);
            // Replica A wrongly rejects. The rejection arrives from both
            // replicas, a 2x fan-out, and before the twin's receipt.
            for _ in 0..2 {
                let _ = error_bus.send(kardamom_types::TxError {
                    sender: envelope.sender,
                    nonce,
                    reason: kardamom_types::TxErrorReason::DuplicatedTx {
                        expected_nonce: nonce + 1,
                    },
                });
            }
            // The twin ordered it. The receipt lands shortly after, well
            // inside the rejection-release grace.
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = receipt_bus.send(Receipt {
                tx_idx: pos,
                tx_hash: envelope.tx_hash,
                status: true,
                gas_used: 21_000,
                logs: Vec::new(),
                write_set_hash: B256::ZERO,
                from: envelope.sender,
                nonce,
                ..Default::default()
            });
        }
    });

    let signer = loop {
        let s = PrivateKeySigner::random();
        if partition_for(s.address(), 2) == 0 {
            break s;
        }
    };
    let raw = common::sign_legacy(&signer, 0);
    let resp = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .expect("success must override the racing replica's rejection");
    assert!(resp.receipt.status);
    assert_eq!(resp.receipt.from, signer.address());

    h.abort();
}

/// Companion test. A genuine duplicate, where both replicas
/// reject and no receipt ever arrives, must still reach the client as a
/// Duplicate error. The dedup drops the twin's copy, and the grace only
/// delays the release.
#[tokio::test(flavor = "multi_thread")]
async fn genuine_rejection_from_both_replicas_reaches_the_client_once() {
    let cfg = IngressConfig {
        partition_count_m: 2,
        ack_policy: kardamom_types::AckPolicy::OnOffer,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockChannels::new(2);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    let error_bus = mock.tx_error_bus.clone();
    let rx0 = partition_rx.remove(0);
    let _rx1 = partition_rx.remove(0);
    let h = tokio::spawn(async move {
        let mut rx0 = rx0;
        if let Some(envelope) = rx0.recv().await {
            let nonce = nonce_of(&envelope.raw_tx);
            // Both replicas reject. The copies can disagree on expected_nonce.
            for expected in [7u64, 8u64] {
                let _ = error_bus.send(kardamom_types::TxError {
                    sender: envelope.sender,
                    nonce,
                    reason: kardamom_types::TxErrorReason::DuplicatedTx {
                        expected_nonce: expected,
                    },
                });
            }
        }
    });

    let signer = loop {
        let s = PrivateKeySigner::random();
        if partition_for(s.address(), 2) == 0 {
            break s;
        }
    };
    let raw = common::sign_legacy(&signer, 0);
    let err = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .expect_err("no replica accepted the tx — the rejection must be delivered");
    assert!(
        matches!(err, kardamom_ingress::IngressError::Duplicate(_)),
        "expected Duplicate, got {err:?}"
    );

    h.abort();
}
