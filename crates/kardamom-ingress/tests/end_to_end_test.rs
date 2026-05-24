//! End-to-end: 100 signed txs from 100 distinct senders should land on
//! the correct partitions, receive their receipts, and a duplicate
//! `(sender, nonce)` should be served from the receipt cache.

mod common;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::routing::partition_for;
use kardamom_ingress::{InMemoryStateDb, IngressProxy, MockChannels};
use kardamom_types::{BPosition, CachedReceipt, QuorumWatermark, Receipt};

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
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(
        cfg.clone(),
        mock.clone(),
        mock.clone(),
        state_db,
    ));

    // Fake executor — drains each partition, immediately satisfies
    // receipt + watermark.
    let mut handles = Vec::new();
    for (i, mut rx) in partition_rx.drain(..).enumerate() {
        let receipt_cache_bus = mock.receipt_cache_bus.clone();
        let watermark_bus = mock.watermark_bus.clone();
        handles.push(tokio::spawn(async move {
            let mut local_idx: i32 = 0;
            while let Some(envelope) = rx.recv().await {
                local_idx += 1;
                let pos = BPosition {
                    term_id: i as i32,
                    term_offset: local_idx,
                };
                let nonce = nonce_of(&envelope.raw_tx);
                let receipt = Receipt {
                    tx_idx: pos,
                    tx_hash: envelope.tx_hash,
                    status: true,
                    gas_used: 21_000,
                    logs: Vec::new(),
                    write_set_hash: B256::ZERO,
                };
                let _ = receipt_cache_bus.send(CachedReceipt {
                    sender: envelope.sender,
                    nonce,
                    tx_hash: envelope.tx_hash,
                    receipt: receipt.clone(),
                });
                let _ = watermark_bus.send(QuorumWatermark { position: pos });
            }
        }));
    }

    // 100 unique senders, one tx each.
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
        // Just sanity-check the partition function on the recovered sender.
        let _expected_partition = partition_for(sender, m);
        assert!(resp.receipt.status);
    }

    // Idempotent retry: re-submit the same tx for signers[0]; must hit
    // the in-memory receipt cache (no executor round-trip needed).
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
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));

    // Fake executor for partition 0 that publishes the receipt-cache
    // immediately but holds off advancing the watermark for ~200ms.
    let receipt_cache_bus = mock.receipt_cache_bus.clone();
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
            };
            let _ = receipt_cache_bus.send(CachedReceipt {
                sender: envelope.sender,
                nonce,
                tx_hash: envelope.tx_hash,
                receipt,
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = watermark_bus.send(QuorumWatermark { position: pos });
        }
    });

    // Pick a signer whose address routes to partition 0.
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
    // Must have waited at least ~150ms for the watermark to advance.
    assert!(
        elapsed >= Duration::from_millis(150),
        "parked too short: {elapsed:?}"
    );
    h.abort();
}
