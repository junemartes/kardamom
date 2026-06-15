//! Regression test for the publisher connect-race (the multi-host cluster-e2e
//! tx-flow stall).
//!
//! Aeron does not replay pre-subscription history, and the previous offer loop
//! gave up after a fixed ~1024-spin burst (microseconds). So a frame published
//! before the subscriber's image formed was silently dropped — which is exactly
//! why a transaction accepted by the cluster ingress never reached the
//! sequencer over UDP multicast (the single-host IPC e2e only worked because it
//! wraps bring-up in a fixed `sleep`).
//!
//! This test publishes a message **before any subscriber exists**, then opens
//! the subscriber after a delay. With the old loop the publish returns
//! NOT_CONNECTED and the frame is lost; with the deadline-based offer
//! ([`kardamom_log::offer_retry`]) the publish *waits* for the subscriber to
//! connect and the message is delivered.
//!
//! Gated on the `docker-e2e` feature AND on Docker availability (the real Aeron
//! Media Driver runs in a container), same as `aeron_live_e2e.rs`.

#![cfg(feature = "docker-e2e")]

use std::time::{Duration, Instant};

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
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test offer_connect_race -- --ignored`"]
async fn publish_waits_for_a_late_joining_subscriber() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");
    let aeron_dir = cluster.aeron_dir_host(0).to_string_lossy().to_string();

    let mut cfg = LogConfig::default();
    // Plain IPC over the shared (bind-mounted) aeron.dir; distinct stream id so
    // this test can't collide with the other e2e tests' streams.
    cfg.channels.tx_data_channel_template = "aeron:ipc?alias=a-{sid}".to_string();
    cfg.channels.tx_data_stream_id_base = 5101;

    let rt = AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");
    let sid = 0u8;
    let publisher = TxDataPublisherHandle::open(&rt, &cfg.channels, sid).expect("publisher");

    // Publish BEFORE any subscriber exists. With the old fixed-spin offer this
    // returns NOT_CONNECTED in microseconds and drops the frame (the `.expect`
    // below would fail); with the deadline-based offer it blocks until the
    // subscriber connects and then succeeds.
    let pub_task = tokio::task::spawn_blocking({
        let publisher = publisher.clone();
        move || {
            publisher
                .publish(&TxEnvelope {
                    correlation_id: 7,
                    raw_tx: Bytes::from(vec![0xABu8; 48]),
                    sender: Address::repeat_byte(7),
                    tx_hash: B256::repeat_byte(7),
                })
                .expect("publish must succeed once the late subscriber connects");
        }
    });

    // The subscriber's image forms only AFTER a delay — simulating the slow
    // multicast join that breaks the cluster (here forced even over fast IPC by
    // simply opening it late).
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut subscriber =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, sid).expect("subscriber");

    // The publisher must have waited (not dropped) and now succeed.
    pub_task.await.expect("publisher task");

    // The late subscriber must receive exactly the frame the offer waited to
    // deliver — proving no pre-subscription drop occurred.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got: Option<TxEnvelope> = None;
    while got.is_none() && Instant::now() < deadline {
        if let Ok(Some((_pos, env))) =
            tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await
        {
            got = Some(env);
        }
    }

    let env = got.expect("late subscriber received nothing — the frame was dropped pre-connect");
    assert_eq!(
        env.correlation_id, 7,
        "delivered frame must be the one the publisher waited to send"
    );

    drop(rt);
    drop(cluster);
}
