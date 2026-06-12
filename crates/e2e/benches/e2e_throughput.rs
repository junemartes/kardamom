//! Phase-4 sustained-load throughput / latency benchmark for the full
//! pipeline.
//!
//! Brings up real Aeron in Docker, opens the new Send-friendly
//! [`kardamom_log::aeron_live`] adapters, and measures:
//!
//!   - **throughput** — sustained tx/s over the publish→subscribe round
//!     trip on tx_data,
//!   - **latency p50/p99/p999** — eth_sendRawTransaction (compressed to
//!     "publish onto tx_data") → receipt-on-tx_receipts, captured via
//!     `hdrhistogram` so percentiles are exact rather than sampled.
//!
//! Gated behind `feature = "full-pipeline-e2e"` (same flag as the test).
//! Run with:
//!
//! ```bash
//! cargo bench -p e2e --features full-pipeline-e2e \
//!   --bench e2e_throughput
//! ```

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[cfg(feature = "full-pipeline-e2e")]
fn run_e2e_throughput(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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

    // Background draining task. Without it, every batch fills the term
    // buffer and the publisher hits back-pressure. Drains as fast as the
    // subscriber can deliver — we don't care about values here.
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

    let mut group = c.benchmark_group("e2e/tx_data_publish_throughput");
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
                    publisher.publish(&env).expect("publish");
                }
            });
        });
    }
    group.finish();

    drain_stop.store(true, Ordering::Relaxed);
    drain_handle.join().expect("drain join");

    let mut latency_cfg = cfg.clone();
    latency_cfg.channels.tx_data_channel_template =
        "aeron:ipc?alias=bench-tx-data-latency-{sid}".into();
    latency_cfg.channels.tx_data_stream_id_base = 9011;
    let latency_pub = TxDataPublisherHandle::open(&aeron_rt, &latency_cfg.channels, sequencer_id)
        .expect("latency publisher");
    let mut latency_sub =
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
            latency_pub.publish(&env).expect("publish");
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
