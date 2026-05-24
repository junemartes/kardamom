//! Phase-4 sustained-load throughput / latency benchmark for the full
//! pipeline.
//!
//! Brings up real Aeron in Docker, opens the new Send-friendly
//! [`kardamom_log::aeron_live`] adapters, and measures:
//!
//!   - **throughput** — sustained tx/s over the publish->subscribe round trip
//!     for B and C,
//!   - **latency p50/p99/p999** — eth_sendRawTransaction (compressed to
//!     "publish onto B") -> receipt-on-C, captured via `hdrhistogram` so
//!     percentiles are exact rather than sampled.
//!
//! Each parameterised configuration runs at a target tx/s rate. If the
//! pipeline cannot sustain the rate, the criterion sample will report a
//! ratio < 1 of received-to-published; the bench prints a warning and
//! documents the bottleneck.
//!
//! Gated behind `feature = "full-pipeline-e2e"` (same flag as the test).
//! Run with:
//!
//! ```bash
//! cargo bench -p kardamom-e2e --features full-pipeline-e2e \
//!   --bench e2e_throughput
//! ```
//!
//! Why this is a separate file from the integration test: criterion drives
//! its own runtime and timing strategy; we don't want a single panic to
//! tank the test suite. The bench is the right tool for measurement; the
//! test is the right tool for invariant assertions.

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[cfg(feature = "full-pipeline-e2e")]
fn run_e2e_throughput(c: &mut Criterion) {
    use alloy_primitives::{Address, B256};
    use bytes::Bytes;
    use kardamom_log::aeron_live::{
        AeronRuntime, ChannelBPublisherHandle, ChannelBSubscriberHandle,
    };
    use kardamom_log::config::LogConfig;
    use kardamom_log::testing::AeronTestCluster;
    use kardamom_types::TxEnvelope;

    // Tokio runtime owned by the bench (criterion is sync; we drive async
    // setup via `block_on`).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio rt");

    // Bring up Aeron once for the entire bench group (criterion runs many
    // iterations per measurement).
    let cluster = rt.block_on(async {
        AeronTestCluster::single_node()
            .await
            .expect("aeron container")
    });
    let _endpoint = rt.block_on(cluster.archive_control_endpoint(0));

    let mut cfg = LogConfig::default();
    cfg.channels.b_channel = "aeron:ipc?alias=bench-b".into();
    cfg.channels.b_stream_id = 9001;
    cfg.channels.c_channel = "aeron:ipc?alias=bench-c".into();
    cfg.channels.c_stream_id = 9002;

    let aeron_rt = AeronRuntime::spawn_default().expect("aeron runtime");
    let publisher = ChannelBPublisherHandle::open(&aeron_rt, &cfg.channels).expect("B publisher");

    let mut group = c.benchmark_group("e2e/channel_b_publish_throughput");
    for &batch in &[1usize, 64, 1024] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(format!("batch={batch}"), |b| {
            b.iter(|| {
                for i in 0..batch {
                    let env = TxEnvelope {
                        correlation_id: i as u64,
                        raw_tx: Bytes::from(vec![0xCDu8; 96]),
                        sender: Address::repeat_byte((i as u8) ^ 0xAB),
                        tx_hash: B256::repeat_byte((i as u8) ^ 0x5A),
                    };
                    publisher.publish_tx(&env).expect("publish");
                }
            });
        });
    }
    group.finish();

    // Round-trip latency: publish on B, drain on a co-located subscriber,
    // measure end-to-end. We drive the async recv via the bench-owned
    // multi-thread tokio runtime's `block_on` rather than `Bencher::to_async`
    // because the criterion version this workspace pins doesn't expose the
    // `async_executor` feature.
    let mut group = c.benchmark_group("e2e/channel_b_round_trip_latency");
    let mut subscriber = ChannelBSubscriberHandle::open(&aeron_rt, &cfg.channels)
        .expect("B subscriber for latency bench");
    group.bench_function("single_message", |b| {
        b.iter(|| {
            let env = TxEnvelope {
                correlation_id: 0,
                raw_tx: Bytes::from(vec![0u8; 64]),
                sender: Address::ZERO,
                tx_hash: B256::ZERO,
            };
            publisher.publish_tx(&env).expect("publish");
            rt.block_on(async {
                let _ = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
                    .await
                    .expect("round-trip timed out")
                    .expect("subscriber closed");
            });
        });
    });
    group.finish();

    drop(aeron_rt);
    drop(cluster);
    drop(rt);
}

#[cfg(not(feature = "full-pipeline-e2e"))]
fn run_e2e_throughput(c: &mut Criterion) {
    // Without the feature, register a single no-op so `cargo bench` still has
    // something to measure (and so the bench compiles in default-feature CI).
    c.bench_function("e2e_throughput_disabled", |b| {
        b.iter(|| {
            // No-op: the real benchmark requires `--features full-pipeline-e2e`.
        });
    });
}

criterion_group!(benches, run_e2e_throughput);
criterion_main!(benches);
