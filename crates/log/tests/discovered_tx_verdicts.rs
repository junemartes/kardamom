//! Real-Aeron check of the `tx_verdicts` addressing over the in-memory
//! catalog: a sequencer publishes to two ingress ids, and each ingress
//! receives only the verdicts of its own id.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_log::aeron_live::{AeronRuntime, TxStatusSubscriberHandle, TxVerdictsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::memory::MemoryCatalog;
use kardamom_log::discovery::{Catalog, Instance, StreamPlane};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::TxStatus;

fn config(base: &LogConfig) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(200).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(50).unwrap();
    cfg.discovery.removal_grace_ms = 500;
    cfg.discovery.check_ttl_ms = NonZeroU64::new(2_000).unwrap();
    cfg
}

fn plane(cfg: &LogConfig, label: &str, catalog: &MemoryCatalog) -> StreamPlane {
    let instance = Instance {
        id: format!("alloc-{label}"),
    };
    StreamPlane::with_catalog(
        cfg,
        label,
        Catalog::Memory(catalog.clone()),
        instance,
        Ipv4Addr::LOCALHOST,
    )
}

fn verdict(nonce: u64) -> TxStatus {
    TxStatus::accepted(B256::ZERO, Address::ZERO, nonce)
}

/// The nonces that `sub` receives within `budget`.
async fn nonces_within(sub: &mut TxStatusSubscriberHandle, budget: Duration) -> Vec<u64> {
    let deadline = Instant::now() + budget;
    let mut seen = Vec::new();
    while let Ok(Some((_, status))) = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        sub.recv(),
    )
    .await
    {
        seen.extend(status.identity().map(|(_, nonce)| nonce));
    }
    seen
}

/// Offer `nonce` to `publisher` until `sub` receives it: a lossy offer
/// drops the frames that precede the join of the subscriber.
async fn deliver(
    publisher: &TxVerdictsPublisherHandle,
    sub: &mut TxStatusSubscriberHandle,
    nonce: u64,
) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    while Instant::now() < deadline && !seen.contains(&nonce) {
        publisher.publish_status(&verdict(nonce)).expect("encode");
        seen.extend(nonces_within(sub, Duration::from_millis(200)).await);
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_verdicts -- --ignored`"]
async fn each_ingress_receives_only_its_own_verdicts() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4700).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();

    let mut plane_0 = plane(&cfg, "ingress-0", &catalog);
    let mut sub_0 = plane_0
        .tx_verdicts_subscriber(&rt, 0)
        .expect("subscriber 0");
    let mut plane_1 = plane(&cfg, "ingress-1", &catalog);
    let mut sub_1 = plane_1
        .tx_verdicts_subscriber(&rt, 1)
        .expect("subscriber 1");

    let rt_seq = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("runtime");
    let mut plane_seq = plane(&cfg, "sequencer", &catalog);
    let to_0 = plane_seq
        .tx_verdicts_publisher(&rt_seq, 0)
        .await
        .expect("publisher to 0");
    let to_1 = plane_seq
        .tx_verdicts_publisher(&rt_seq, 1)
        .await
        .expect("publisher to 1");

    let got_1 = deliver(&to_1, &mut sub_1, 11).await;
    let got_0 = deliver(&to_0, &mut sub_0, 10).await;
    assert!(got_1.contains(&11), "ingress 1 receives its verdict");
    assert!(got_0.contains(&10), "ingress 0 receives its verdict");

    let late_0 = nonces_within(&mut sub_0, Duration::from_millis(500)).await;
    let late_1 = nonces_within(&mut sub_1, Duration::from_millis(500)).await;
    assert!(
        got_0.iter().chain(&late_0).all(|&n| n == 10),
        "ingress 0 received another ingress's verdict: {got_0:?} {late_0:?}"
    );
    assert!(
        got_1.iter().chain(&late_1).all(|&n| n == 11),
        "ingress 1 received another ingress's verdict: {got_1:?} {late_1:?}"
    );

    plane_seq.shutdown().await;
    plane_0.shutdown().await;
    plane_1.shutdown().await;
}
