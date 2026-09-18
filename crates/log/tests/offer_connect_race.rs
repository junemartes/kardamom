//! Regression test for the publisher connect race (the multi-host
//! cluster-e2e tx-flow stall).
//!
//! Aeron does not replay pre-subscription history, so a frame published
//! before the subscriber's image forms is silently dropped unless the
//! offer waits for the connection.
//!
//! This test publishes a message before any subscriber exists, then opens
//! the subscriber after a delay. The deadline-based offer
//! ([`kardamom_log::offer_retry`]) must wait for the subscriber to
//! connect, so the message is delivered.
//!
//! Gated on the `docker-e2e` feature and on Docker availability (the real
//! Aeron Media Driver runs in a container), same as `aeron_live_e2e.rs`.

#![cfg(feature = "docker-e2e")]

mod common;

use std::time::Duration;

use kardamom_log::aeron_live::{TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test offer_connect_race -- --ignored`"]
async fn publish_waits_for_a_late_joining_subscriber() {
    // Explicit opt-in test (`--features docker-e2e -- --ignored`). A
    // missing Docker must fail loudly, not silently pass through an early
    // return.
    kardamom_log::testing::require_docker().await;

    // Plain IPC over the shared (bind-mounted) aeron.dir. Stream base
    // 5101 keeps this test from colliding with the other e2e tests'
    // streams.
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5101).await;
    let sid = 0u8;
    let publisher = TxDataPublisherHandle::open(&rt, &cfg.channels, sid).expect("publisher");

    // Publish before any subscriber exists. The deadline-based offer must
    // block until the subscriber connects, then succeed (otherwise the
    // `.expect` below fails).
    let pub_task = tokio::task::spawn_blocking({
        let publisher = publisher.clone();
        move || {
            publisher
                .publish(&common::tx_envelope(7, 7, 48))
                .expect("publish must succeed once the late subscriber connects");
        }
    });

    // The subscriber's image forms only after a delay, simulating the
    // slow multicast join that breaks the cluster. This test forces it
    // even over fast IPC, simply by opening the subscriber late.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut subscriber = TxDataSubscriberHandle::open(&rt, &cfg.channels, sid).expect("subscriber");

    // The publisher must have waited (not dropped) and now succeed.
    pub_task.await.expect("publisher task");

    // The late subscriber must receive exactly the frame the offer waited
    // to deliver, proving no pre-subscription drop occurred.
    let got =
        kardamom_log::testing::recv_within(&mut subscriber, Duration::from_secs(5), |_| true).await;

    let env = got.expect("late subscriber received nothing — the frame was dropped pre-connect");
    assert_eq!(
        env.correlation_id, 7,
        "delivered frame must be the one the publisher waited to send"
    );

    drop(rt);
    drop(cluster);
}
