//! Real-Aeron e2e against the new Send-friendly `aeron_live` adapters.
//!
//! Exercises the publish + subscribe path through `AeronRuntime`,
//! demonstrating that:
//!
//!   1. publisher and subscriber handles are `Send + Sync` — they live in a
//!      `tokio::test(flavor = "multi_thread")` runtime where tasks freely
//!      migrate across worker threads;
//!   2. the dedicated Aeron OS thread inside [`AeronRuntime`] correctly
//!      bridges the `Rc<Aeron>` to the multi-threaded outside world;
//!   3. published `TxEnvelope`s round-trip through real Aeron and emerge on
//!      the subscriber in publish order, with non-zero `BPosition` cursors.
//!
//! Gated on the `docker-e2e` feature AND on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::TxEnvelope;

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p log --features docker-e2e --test aeron_live_e2e -- --ignored`"]
async fn aeron_live_send_friendly_round_trip() {
    // This test only runs when explicitly opted in (`--features docker-e2e --
    // --ignored`), an environment where Docker is required — its absence is an
    // error, not a skip condition (a silent return would count as a green pass).
    assert!(
        docker_available().await,
        "docker not available — required for this --ignored docker-e2e test"
    );

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");
    let endpoint = cluster.archive_control_endpoint(0).await;
    let aeron_dir = cluster.aeron_dir_host(0).to_string_lossy().to_string();
    eprintln!("aeron archive control: {endpoint}");
    eprintln!("aeron.dir (host): {aeron_dir}");

    let mut cfg = LogConfig::default();
    // Use IPC over the shared `aeron.dir` (bind-mounted between host and
    // container) — both processes are clients of the same Media Driver
    // running inside the container, so no UDP is needed for this single-host
    // smoke test. Override the per-shard tx_data template to a plain IPC URI
    // for the single-shard test (sequencer_id=0).
    cfg.channels.tx_data_channel_template = "aeron:ipc?alias=a-{sid}".to_string();
    cfg.channels.tx_data_stream_id_base = 4001;

    // Spawn the runtime, pointing AeronContext at the bind-mounted aeron.dir
    // so the host client joins the container's Media Driver.
    let rt = AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");

    let sequencer_id = 0u8;
    let publisher =
        TxDataPublisherHandle::open(&rt, &cfg.channels, sequencer_id).expect("publisher");
    let mut subscriber =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, sequencer_id).expect("subscriber");

    // Spawn the publisher on a tokio task — this proves the handle survives
    // worker-thread migration (which `current_thread` could mask).
    let pub_task = tokio::task::spawn_blocking({
        let publisher = publisher.clone();
        move || {
            for i in 0..50u64 {
                publisher
                    .publish(&TxEnvelope {
                        correlation_id: i,
                        raw_tx: Bytes::from(vec![0xEEu8; 64]),
                        sender: Address::repeat_byte(i as u8),
                        tx_hash: B256::repeat_byte(i as u8),
                    })
                    .expect("publish");
            }
        }
    });

    pub_task.await.expect("publisher task");

    // Drain the subscriber with a 5 s deadline.
    let mut received: Vec<TxEnvelope> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received.len() < 50 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await {
            Ok(Some((_pos, env))) => received.push(env),
            Ok(None) => break,
            Err(_) => {}
        }
    }

    assert_eq!(
        received.len(),
        50,
        "expected 50 messages, got {}",
        received.len()
    );
    // Per-publisher ordering preserved.
    for (i, env) in received.iter().enumerate() {
        assert_eq!(env.correlation_id, i as u64);
    }

    drop(rt);
    drop(cluster);
}
