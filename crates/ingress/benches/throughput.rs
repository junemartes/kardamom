//! Sustained throughput per proxy. This measures the txs/sec a single
//! proxy can ingest, with everything past the sequencer mocked.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Bytes;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::test_support::{sign_legacy, spawn_fake_executor};
use kardamom_ingress::{IngressProxy, MockChannels};

const BATCH: usize = 1024;
const SHARDS: NonZeroU32 = NonZeroU32::new(8).unwrap();
const MOCK_SHARDS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(8).unwrap();

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: SHARDS,
            pending_receipt_timeout: Duration::from_secs(5),
            ..IngressConfig::default()
        };
        let (mock, rx_vec) = MockChannels::new(MOCK_SHARDS);
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
        // One shared position space across shards, so no receipt parks
        // above a watermark that a later shard moves backward.
        let _echo_tasks = spawn_fake_executor(&mock, rx_vec);
        proxy
    });
    let pre: Vec<Bytes> = (0..BATCH)
        .map(|_| sign_legacy(&PrivateKeySigner::random(), 0))
        .collect();

    let mut group = c.benchmark_group("ingress/throughput");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("submit_raw_batch_1024", |b| {
        b.to_async(&rt).iter(|| {
            let proxy = proxy.clone();
            let pre = pre.clone();
            async move {
                let mut futs = Vec::with_capacity(BATCH);
                for raw in pre {
                    let p = proxy.clone();
                    futs.push(async move {
                        p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                            .await
                            .unwrap()
                    });
                }
                let _ = futures::future::join_all(futs).await;
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
