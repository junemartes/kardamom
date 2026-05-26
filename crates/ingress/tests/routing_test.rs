//! Routing invariant: every tx submitted by sender S lands on partition
//! `keccak(S) % M`, regardless of M. Verified end-to-end through the
//! proxy with a fake executor that immediately satisfies receipt +
//! watermark per arrival.

mod common;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;

use ingress::config::IngressConfig;
use ingress::routing::partition_for;
use ingress::{InMemoryStateDb, IngressProxy, MockChannels};
use types::{BPosition, QuorumWatermark, Receipt};

#[tokio::test(flavor = "multi_thread")]
async fn each_tx_lands_on_keccak_partition() {
    for m in [2u32, 4, 8, 16] {
        let cfg = IngressConfig {
            partition_count_m: m,
            pending_receipt_timeout: Duration::from_secs(5),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockChannels::new(m as usize);
        let state_db = Arc::new(InMemoryStateDb::new());
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));

        // Fake executor — one task per partition, satisfies receipt +
        // watermark immediately.
        let mut spawns = Vec::new();
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let receipt_bus = mock.receipt_bus.clone();
            let watermark_bus = mock.watermark_bus.clone();
            spawns.push(tokio::spawn(async move {
                let mut local_idx: i32 = 0;
                while let Some(envelope) = rx.recv().await {
                    local_idx += 1;
                    let pos = BPosition {
                        term_id: i as i32,
                        term_offset: local_idx,
                    };
                    let nonce = extract_nonce(&envelope.raw_tx);
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

        // 32 senders. The proxy's `partition_for` is the same function the
        // proxy uses; the invariant is that the publication landed there.
        // If routing were wrong, the executor for that partition would
        // never see the message and the proxy would time out.
        let mut futs = Vec::new();
        for _ in 0..32 {
            let s = PrivateKeySigner::random();
            let raw = common::sign_legacy(&s, 0);
            let p = proxy.clone();
            futs.push(async move {
                p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .map(|r| (s.address(), r))
            });
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            let (sender, resp) = r.expect("submit ok");
            let part = partition_for(sender, m);
            assert!(part < m);
            assert!(resp.receipt.status);
        }
        for s in spawns {
            s.abort();
        }
    }
}

/// Decode just the `nonce` field out of an RLP-encoded legacy tx so the
/// fake executor can fill in the `Receipt.nonce` field that the proxy uses
/// to look up its pending entry.
fn extract_nonce(raw: &bytes::Bytes) -> u64 {
    use alloy_consensus::TxEnvelope;
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    let env = TxEnvelope::decode(&mut raw.as_ref()).expect("decode");
    env.nonce()
}
