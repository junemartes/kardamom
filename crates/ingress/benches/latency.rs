//! End-to-end latency. The path is: client, proxy, mock executor, receipt.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Bytes;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, criterion_group, criterion_main};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::test_support::{receipt_for, sign_legacy};
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{BPosition, QuorumWatermark};

fn bench_e2e_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: 8,
            pending_receipt_timeout: Duration::from_secs(2),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockChannels::new(8);
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let receipt_bus = mock.receipt_bus.clone();
            let watermark_bus = mock.watermark_bus.clone();
            tokio::spawn(async move {
                let mut local: i32 = 0;
                while let Some(envelope) = rx.recv().await {
                    local += 1;
                    let pos = BPosition {
                        term_id: i32::try_from(i).expect("shard count fits in i32"),
                        term_offset: local,
                    };
                    let receipt = receipt_for(&envelope, pos);
                    let _ = receipt_bus.send(receipt);
                    let _ = watermark_bus.send(QuorumWatermark { position: pos });
                }
            });
        }
        proxy
    });

    // Sign 1000 unique-sender txs ahead of time, so signing is not on the
    // hot path.
    let pre: Vec<Bytes> = (0..1000)
        .map(|_| sign_legacy(&PrivateKeySigner::random(), 0))
        .collect();
    let mut idx = 0usize;

    c.bench_function("ingress/e2e_latency_simple_transfer", |b| {
        b.to_async(&rt).iter(|| {
            let raw = pre[idx % pre.len()].clone();
            idx = idx.wrapping_add(1);
            let proxy = proxy.clone();
            async move {
                let _ = proxy
                    .submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_e2e_latency);
criterion_main!(benches);
