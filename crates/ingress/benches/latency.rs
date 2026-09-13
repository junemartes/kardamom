//! End-to-end latency. The path is: client, proxy, mock executor, receipt.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Bytes;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, criterion_group, criterion_main};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::test_support::{sign_legacy, spawn_fake_executor};
use kardamom_ingress::{IngressProxy, MockChannels};

const SHARDS: NonZeroU32 = NonZeroU32::new(8).unwrap();
const MOCK_SHARDS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(8).unwrap();

fn bench_e2e_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: SHARDS,
            pending_receipt_timeout: Duration::from_secs(2),
            ..IngressConfig::default()
        };
        let (mock, rx_vec) = MockChannels::new(MOCK_SHARDS);
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
        // One shared position space across shards, so no receipt parks
        // above a watermark that a later shard moves backward.
        let _echo_tasks = spawn_fake_executor(&mock, rx_vec);
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
            submit_one(proxy.clone(), raw)
        });
    });
}

/// Submits one signed transaction and waits for its receipt.
async fn submit_one(proxy: Arc<IngressProxy<MockChannels, MockChannels>>, raw: Bytes) {
    let _ = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .unwrap();
}

criterion_group!(benches, bench_e2e_latency);
criterion_main!(benches);
