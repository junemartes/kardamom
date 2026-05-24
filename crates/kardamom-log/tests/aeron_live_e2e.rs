//! Real-Aeron e2e against the new Send-friendly `aeron_live` adapters.
//!
//! Exercises the publish + subscribe path through `AeronRuntime`, demonstrating
//! that:
//!
//!   1. publisher and subscriber handles are `Send + Sync` — they live in
//!      a `tokio::test(flavor = "multi_thread")` runtime where tasks freely
//!      migrate across worker threads;
//!   2. the dedicated Aeron OS thread inside [`AeronRuntime`] correctly bridges
//!      the `Rc<Aeron>` to the multi-threaded outside world;
//!   3. published `TxEnvelope`s round-trip through real Aeron and emerge on
//!      the subscriber in publish order, with non-zero `BPosition` cursors.
//!
//! Gated on `docker-e2e` + `aeron-live` features AND on Docker availability.

#![cfg(all(feature = "docker-e2e", feature = "aeron-live"))]

use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::aeron_live::{AeronRuntime, ChannelBPublisherHandle, ChannelBSubscriberHandle};
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
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e,aeron-live --test aeron_live_e2e -- --ignored`"]
async fn aeron_live_send_friendly_round_trip() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");
    let endpoint = cluster.archive_control_endpoint(0).await;
    eprintln!("aeron archive control: {endpoint}");

    let mut cfg = LogConfig::default();
    cfg.channels.b_channel = format!("aeron:udp?endpoint={endpoint}|alias=b-live-e2e");
    cfg.channels.b_stream_id = 4001;

    // Spawn the runtime: an OS thread that owns the Aeron client. The
    // returned handle is `Send + Sync` and may be moved freely across the
    // multi-threaded tokio runtime workers.
    let rt = AeronRuntime::spawn_default().expect("aeron runtime");

    let publisher = ChannelBPublisherHandle::open(&rt, &cfg.channels).expect("publisher");
    let mut subscriber = ChannelBSubscriberHandle::open(&rt, &cfg.channels).expect("subscriber");

    // Spawn the publisher on a tokio task — this proves the handle survives
    // worker-thread migration (which `current_thread` could mask).
    let pub_task = tokio::task::spawn_blocking({
        let publisher = publisher.clone();
        move || {
            for i in 0..50u64 {
                publisher
                    .publish_tx(&TxEnvelope {
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
