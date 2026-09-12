//! Real-Aeron check of the stream plane over the in-memory catalog: a
//! discovered `tx_errors` subscriber receives from a publisher that
//! registered before it opened, and from one that joins later. A
//! publisher that shuts down is detached after the removal grace.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, TxErrorsPublisherHandle, TxErrorsSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::memory::MemoryCatalog;
use kardamom_log::discovery::{Catalog, Instance, StreamPlane};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::{TxError, TxErrorReason};

const GRACE: Duration = Duration::from_millis(500);

fn config(base: &LogConfig) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(200).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(50).unwrap();
    cfg.discovery.removal_grace_ms = u64::try_from(GRACE.as_millis()).unwrap();
    cfg.discovery.check_ttl_ms = NonZeroU64::new(2_000).unwrap();
    cfg
}

fn plane(cfg: &LogConfig, label: &str, catalog: &MemoryCatalog) -> StreamPlane {
    let instance = Instance {
        id: format!("alloc-{label}"),
        ports: None,
    };
    StreamPlane::with_catalog(
        cfg,
        label,
        Catalog::Memory(catalog.clone()),
        instance,
        Ipv4Addr::LOCALHOST,
    )
}

fn tx_error(tag: u64) -> TxError {
    TxError {
        sender: alloy_primitives::Address::ZERO,
        nonce: tag,
        reason: TxErrorReason::DuplicatedTx { expected_nonce: 0 },
    }
}

/// Publish `tag` until it is acknowledged or `budget` runs out: a
/// dynamic MDC publication is not connected until a subscriber joins.
async fn publish_until_connected(publisher: &TxErrorsPublisherHandle, tag: u64, budget: Duration) {
    let deadline = Instant::now() + budget;
    let p = publisher.clone();
    let connected = tokio::task::spawn_blocking(move || {
        while Instant::now() < deadline {
            if p.publish(&tx_error(tag)).is_ok() {
                return true;
            }
        }
        false
    })
    .await
    .expect("publish task");
    assert!(connected, "publisher {tag} never connected");
}

async fn recv_tag(sub: &mut TxErrorsSubscriberHandle, budget: Duration) -> Option<u64> {
    tokio::time::timeout(budget, sub.recv())
        .await
        .ok()
        .flatten()
        .map(|(_pos, e)| e.nonce)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_errors -- --ignored`"]
async fn a_discovered_subscriber_follows_publishers_that_join_and_leave() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4600).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();

    // A publisher registered before the subscriber exists.
    let rt_a = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("runtime a");
    let mut plane_a = plane(&cfg, "seq-a", &catalog);
    let pub_a: TxErrorsPublisherHandle = plane_a.publisher(&rt_a).await.expect("publisher a");

    let mut plane_sub = plane(&cfg, "ingress", &catalog);
    let mut sub: TxErrorsSubscriberHandle = plane_sub.subscriber(&rt).expect("subscriber");

    publish_until_connected(&pub_a, 1, Duration::from_secs(10)).await;
    let mut seen = Vec::new();
    while let Some(tag) = recv_tag(&mut sub, Duration::from_secs(5)).await {
        seen.push(tag);
        if seen.contains(&1) {
            break;
        }
    }
    assert!(seen.contains(&1), "the pre-registered publisher is joined");

    // A publisher that joins while the subscriber is live.
    let rt_b = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("runtime b");
    let mut plane_b = plane(&cfg, "seq-b", &catalog);
    let pub_b: TxErrorsPublisherHandle = plane_b.publisher(&rt_b).await.expect("publisher b");
    publish_until_connected(&pub_b, 2, Duration::from_secs(10)).await;
    let mut seen = Vec::new();
    while let Some(tag) = recv_tag(&mut sub, Duration::from_secs(5)).await {
        seen.push(tag);
        if seen.contains(&2) {
            break;
        }
    }
    assert!(
        seen.contains(&2),
        "a late publisher is joined without a reopen"
    );

    // Publisher b leaves gracefully: after the grace, its frames stop.
    plane_b.shutdown().await;
    tokio::time::sleep(GRACE * 4).await;
    let late = tokio::task::spawn_blocking(move || pub_b.publish(&tx_error(3)))
        .await
        .expect("publish task");
    let mut late_seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(tag) = recv_tag(&mut sub, Duration::from_millis(200)).await {
            late_seen.push(tag);
        }
    }
    assert!(
        !late_seen.contains(&3),
        "detached publisher still delivered (publish result {late:?})"
    );

    // Publisher a still delivers.
    publish_until_connected(&pub_a, 4, Duration::from_secs(10)).await;
    let mut seen = Vec::new();
    while let Some(tag) = recv_tag(&mut sub, Duration::from_secs(5)).await {
        seen.push(tag);
        if seen.contains(&4) {
            break;
        }
    }
    assert!(seen.contains(&4));

    plane_a.shutdown().await;
    plane_sub.shutdown().await;
}
