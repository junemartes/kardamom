//! A publication on loan, against a real media driver.
//!
//! - A lease offers from a thread of its own, on the session of the
//!   publication.
//! - During the lease, a publish through the handle fails at once, and a
//!   second lease is refused.
//! - The drop of the lease gives the publication back, and the handle
//!   publishes again on the same session, in order after the lease.
//! - A close during the lease closes the publication when the lease ends.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

mod common;

use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{LentPublication, TxDataSubscriberHandle};
use kardamom_log::parked::Offered;
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::TxEnvelope;

/// Offer envelope `id` on `lease` until the publication takes it, for at
/// most 5 s.
fn offer_until_delivered(lease: &LentPublication, id: u64) {
    let fill = u8::try_from(id).expect("small test ids");
    let bytes = kardamom_log::codec::encode(&common::tx_envelope(id, fill, 64)).expect("encode");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !delivered(lease.offer(bytes.as_slice())) {
        assert!(
            Instant::now() < deadline,
            "the lease did not deliver envelope {id} in 5 s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Whether the offer delivered its frame. A lease holds its publication,
/// so an unknown publication is a failure of the test.
fn delivered(offered: Offered) -> bool {
    match offered {
        Offered::Delivered(position) => {
            position.expect("the position decodes");
            true
        }
        Offered::Refused(_) => false,
        Offered::UnknownPublication => panic!("the lease holds its publication"),
    }
}

/// The next `count` envelopes of `subscriber`, with their session ids.
async fn receive(subscriber: &mut TxDataSubscriberHandle, count: usize) -> Vec<(i32, TxEnvelope)> {
    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while received.len() < count && Instant::now() < deadline {
        let step = tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await;
        let Ok(Some((loc, env))) = step else {
            continue;
        };
        received.push((loc.session_id, env));
    }
    received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test lent_publication -- --ignored`"]
async fn a_lease_offers_on_the_session_and_gives_the_publication_back() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5501).await;
    let channel = cfg.channels.tx_data_channel(0);
    let stream = cfg.channels.tx_data_stream_id(0);
    let mut subscriber = TxDataSubscriberHandle::open(&rt, &cfg.channels, 0).expect("subscriber");
    let handle = rt.open_publication(&channel, stream).expect("publication");
    handle
        .publish(&common::tx_envelope(0, 0, 64))
        .expect("the handle publishes before the lease");

    let lease = handle.lend().expect("lend");
    assert_eq!(lease.session_id(), handle.session_id());
    assert!(handle.lend().is_err(), "a second lease is refused");
    let (lent_tx, lent_rx) = std::sync::mpsc::channel();
    let (end_tx, end_rx) = std::sync::mpsc::channel::<()>();
    let worker = std::thread::spawn(move || {
        (1..=10).for_each(|id| offer_until_delivered(&lease, id));
        lent_tx.send(()).expect("main waits");
        end_rx.recv().expect("main ends the lease");
        drop(lease);
    });
    tokio::task::spawn_blocking(move || lent_rx.recv())
        .await
        .expect("join")
        .expect("the worker offered");
    let refused = handle.publish(&common::tx_envelope(99, 99, 64));
    assert!(
        refused.is_err(),
        "a publish during the lease fails: {refused:?}"
    );
    end_tx.send(()).expect("the worker waits");
    tokio::task::spawn_blocking(move || worker.join())
        .await
        .expect("join")
        .expect("the worker ends");

    // The return is a command to the Aeron thread, so the first publish
    // after it can still find the publication on loan.
    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.publish(&common::tx_envelope(11, 11, 64)).is_err() {
        assert!(
            Instant::now() < deadline,
            "the publication came back in 5 s"
        );
    }
    let received = receive(&mut subscriber, 12).await;
    let ids: Vec<u64> = received.iter().map(|(_, env)| env.correlation_id).collect();
    assert_eq!(ids, (0..=11).collect::<Vec<_>>(), "one session, in order");
    assert!(
        received
            .iter()
            .all(|(session, _)| *session == handle.session_id()),
        "every envelope is on the session of the publication"
    );
    drop(rt);
    drop(cluster);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test lent_publication -- --ignored`"]
async fn a_close_during_a_lease_closes_the_publication_when_the_lease_ends() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5511).await;
    let channel = cfg.channels.tx_data_channel(0);
    let stream = cfg.channels.tx_data_stream_id(0);
    let handle = rt.open_publication(&channel, stream).expect("publication");
    let lease = handle.lend().expect("lend");
    handle.close().expect("a close during the lease");
    drop(lease);
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        assert!(
            handle.publish(&common::tx_envelope(1, 1, 64)).is_err(),
            "the closed publication never comes back"
        );
    }
    assert!(handle.close().is_err(), "the publication is closed");
    drop(rt);
    drop(cluster);
}
