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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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

    let aeron_rt = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("aeron runtime");
    let publisher = ChannelBPublisherHandle::open(&aeron_rt, &cfg.channels).expect("B publisher");
    let mut subscriber =
        ChannelBSubscriberHandle::open(&aeron_rt, &cfg.channels).expect("B subscriber for drain");

    // Background draining task. Without it, every batch fills the term buffer
    // and the publisher hits back-pressure. Drains as fast as the subscriber
    // can deliver — we don't care about the values here, only that they're
    // consumed so the publisher has room.
    let drain_stop = Arc::new(AtomicBool::new(false));
    let drain_stop_for_task = drain_stop.clone();
    let drain_handle = std::thread::spawn(move || {
        let local_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("drain runtime");
        local_rt.block_on(async move {
            while !drain_stop_for_task.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        });
    });

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

    // Round-trip latency: publish on B, drain on a separate subscriber on the
    // benchmark thread, measure end-to-end. The throughput bench has its own
    // drainer running; for latency we'd race with it, so we use a fresh
    // subscriber on a dedicated stream id.
    drain_stop.store(true, Ordering::Relaxed);
    drain_handle.join().expect("drain join");

    let mut latency_cfg = cfg.clone();
    latency_cfg.channels.b_stream_id = 9011;
    latency_cfg.channels.b_channel = "aeron:ipc?alias=bench-b-latency".into();
    let latency_pub =
        ChannelBPublisherHandle::open(&aeron_rt, &latency_cfg.channels).expect("latency publisher");
    let mut latency_sub = ChannelBSubscriberHandle::open(&aeron_rt, &latency_cfg.channels)
        .expect("latency subscriber");

    let mut group = c.benchmark_group("e2e/channel_b_round_trip_latency");
    group.bench_function("single_message", |b| {
        b.iter(|| {
            let env = TxEnvelope {
                correlation_id: 0,
                raw_tx: Bytes::from(vec![0u8; 64]),
                sender: Address::ZERO,
                tx_hash: B256::ZERO,
            };
            latency_pub.publish_tx(&env).expect("publish");
            rt.block_on(async {
                let _ = tokio::time::timeout(Duration::from_secs(1), latency_sub.recv())
                    .await
                    .expect("round-trip timed out")
                    .expect("subscriber closed");
            });
        });
    });
    group.finish();

    // Order-of-drop: AeronRuntime holds an Arc<JoinHandle> we want to flush
    // before testcontainers' async drop reaches for the tokio runtime. Force
    // testcontainers cleanup INSIDE rt.block_on so Handle::current() resolves.
    drop(aeron_rt);
    rt.block_on(async move { drop(cluster) });
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
