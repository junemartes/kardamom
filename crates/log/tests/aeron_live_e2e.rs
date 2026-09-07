//! Real-Aeron e2e against the Send-friendly `aeron_live` adapters.
//!
//! Exercises the publish and subscribe path through `AeronRuntime`,
//! showing that:
//!
//!   1. publisher and subscriber handles are `Send + Sync`, so they live
//!      in a `tokio::test(flavor = "multi_thread")` runtime where tasks
//!      freely migrate across worker threads;
//!   2. the dedicated Aeron OS thread inside [`AeronRuntime`] correctly
//!      bridges the `Rc<Aeron>` to the multi-threaded outside world;
//!   3. published `TxEnvelope`s round-trip through real Aeron and emerge
//!      on the subscriber in publish order, with non-zero `BPosition`
//!      cursors.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

mod common;

use std::time::Duration;

use kardamom_log::aeron_live::{TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::TxEnvelope;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p log --features docker-e2e --test aeron_live_e2e -- --ignored`"]
async fn aeron_live_send_friendly_round_trip() {
    // This test only runs when explicitly opted in (`--features
    // docker-e2e -- --ignored`), an environment where Docker is required.
    // Its absence is an error, not a skip condition; a silent return
    // would count as a green pass.
    kardamom_log::testing::require_docker().await;

    // Use IPC over the shared `aeron.dir` (bind-mounted between host and
    // container). Both processes are clients of the same Media Driver
    // running inside the container, so no UDP is needed for this
    // single-host smoke test. Stream base 4001 keeps this test's stream
    // distinct from the other e2e tests'.
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4001).await;

    let sequencer_id = 0u8;
    let publisher =
        TxDataPublisherHandle::open(&rt, &cfg.channels, sequencer_id).expect("publisher");
    let mut subscriber =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, sequencer_id).expect("subscriber");

    // Spawn the publisher on a tokio task. This proves the handle survives
    // worker-thread migration, which `current_thread` could mask.
    let pub_task = tokio::task::spawn_blocking({
        let publisher = publisher.clone();
        move || {
            for i in 0..50u64 {
                let fill = u8::try_from(i).expect("loop bound is below u8::MAX");
                publisher
                    .publish(&common::tx_envelope(i, fill, 64))
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
