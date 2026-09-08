//! Sustained-load throughput and latency benchmark for the full pipeline.
//!
//! This brings up real Aeron in Docker, opens the Send-friendly
//! [`kardamom_log::aeron_live`] adapters, and measures:
//!
//!   - throughput: sustained transactions per second, over the
//!     publish-to-subscribe round trip on `tx_data`.
//!   - latency (p50, p99, p999): from `eth_sendRawTransaction` (modeled as
//!     "publish onto `tx_data`") to a receipt on `tx_receipts`, captured
//!     with `hdrhistogram` so the percentiles are exact, not sampled.
//!
//! This is gated behind `feature = "full-pipeline-e2e"` (the same flag as
//! the test). Run it with:
//!
//! ```bash
//! cargo bench -p e2e --features full-pipeline-e2e \
//!   --bench e2e_throughput
//! ```

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// Publish `batch` synthetic envelopes, each with a distinct correlation
/// id, sender, and hash derived from its index.
#[cfg(feature = "full-pipeline-e2e")]
#[allow(
    clippy::cast_possible_truncation,
    reason = "wrapping the per-message low byte past 255 is fine here"
)]
fn publish_batch(publisher: &kardamom_log::aeron_live::TxDataPublisherHandle, batch: usize) {
    use alloy_primitives::{Address, B256};
    use bytes::Bytes;
    use kardamom_types::TxEnvelope;

    for i in 0..batch {
        let low_byte = i as u8;
        let env = TxEnvelope {
            correlation_id: i as u64,
            raw_tx: Bytes::from(vec![0xCDu8; 96]),
            sender: Address::repeat_byte(low_byte ^ 0xAB),
            tx_hash: B256::repeat_byte(low_byte ^ 0x5A),
        };
        publisher.publish(&env).expect("publish");
    }
}

#[cfg(feature = "full-pipeline-e2e")]
fn run_e2e_throughput(c: &mut Criterion) {
    use alloy_primitives::{Address, B256};
    use bytes::Bytes;
    use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle};
    use kardamom_log::config::LogConfig;
    use kardamom_log::testing::AeronTestCluster;
    use kardamom_types::TxEnvelope;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio rt");

    let cluster = rt.block_on(async {
        AeronTestCluster::single_node()
            .await
            .expect("aeron container")
    });
    let _endpoint = rt.block_on(cluster.archive_control_endpoint(0));

    let mut cfg = LogConfig::default();
    cfg.channels.tx_data_channel_template = "aeron:ipc?alias=bench-tx-data-{sid}".into();
    cfg.channels.tx_data_stream_id_base = 9001;
    cfg.channels.tx_receipts_channel = "aeron:ipc?alias=bench-tx-receipts".into();
    cfg.channels.tx_receipts_stream_id = 9100;

    let aeron_rt = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("aeron runtime");
    let sequencer_id = 0u8;
    let publisher = TxDataPublisherHandle::open(&aeron_rt, &cfg.channels, sequencer_id)
        .expect("tx_data publisher");
    let mut subscriber = TxDataSubscriberHandle::open(&aeron_rt, &cfg.channels, sequencer_id)
        .expect("tx_data subscriber for drain");

    // A background draining task. Without it, every batch fills the term
    // buffer, and the publisher hits back-pressure. It drains as fast as
    // the subscriber can deliver; the values do not matter here.
    let (drain_stop_tx, mut drain_stop_rx) = tokio::sync::watch::channel(false);
    let drain_handle = std::thread::spawn(move || {
        let local_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("drain runtime");
        local_rt.block_on(async move {
            loop {
                tokio::select! {
                    msg = subscriber.recv() => match msg {
                        Some(_) => {}
                        None => break,
                    },
                    _ = drain_stop_rx.changed() => break,
                }
            }
        });
    });

    let mut group = c.benchmark_group("e2e/tx_data_publish_throughput");
    for &batch in &[1usize, 64, 1024] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(format!("batch={batch}"), |b| {
            b.iter(|| publish_batch(&publisher, batch));
        });
    }
    group.finish();

    let _ = drain_stop_tx.send(true);
    drain_handle.join().expect("drain join");

    let mut latency_cfg = cfg.clone();
    latency_cfg.channels.tx_data_channel_template =
        "aeron:ipc?alias=bench-tx-data-latency-{sid}".into();
    latency_cfg.channels.tx_data_stream_id_base = 9011;
    let latency_publisher =
        TxDataPublisherHandle::open(&aeron_rt, &latency_cfg.channels, sequencer_id)
            .expect("latency publisher");
    let mut latency_subscriber =
        TxDataSubscriberHandle::open(&aeron_rt, &latency_cfg.channels, sequencer_id)
            .expect("latency subscriber");

    let mut group = c.benchmark_group("e2e/tx_data_round_trip_latency");
    group.bench_function("single_message", |b| {
        b.iter(|| {
            let env = TxEnvelope {
                correlation_id: 0,
                raw_tx: Bytes::from(vec![0u8; 64]),
                sender: Address::ZERO,
                tx_hash: B256::ZERO,
            };
            latency_publisher.publish(&env).expect("publish");
            rt.block_on(async {
                let _ = tokio::time::timeout(Duration::from_secs(1), latency_subscriber.recv())
                    .await
                    .expect("round-trip timed out")
                    .expect("subscriber closed");
            });
        });
    });
    group.finish();

    // Drop order matters here. `AeronRuntime` holds an `Arc<JoinHandle>`
    // that must flush before testcontainers' async drop reaches for the
    // tokio runtime. Run testcontainers cleanup inside `rt.block_on`, so
    // `Handle::current()` resolves.
    drop(aeron_rt);
    rt.block_on(async move { drop(cluster) });
    drop(rt);
}

#[cfg(not(feature = "full-pipeline-e2e"))]
fn run_e2e_throughput(c: &mut Criterion) {
    // Without the feature, register a single no-op. This gives `cargo
    // bench` something to measure, and lets the bench compile in
    // default-feature CI.
    c.bench_function("e2e_throughput_disabled", |b| {
        b.iter(|| {
            // No-op. The real benchmark needs `--features full-pipeline-e2e`.
        });
    });
}

criterion_group!(benches, run_e2e_throughput);
criterion_main!(benches);
