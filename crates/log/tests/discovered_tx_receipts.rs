//! Real-Aeron check of `tx_receipts` over the stream plane and the
//! in-memory catalog: two executor-like publishers register their receipt
//! and boundary publications, and one ingress-like subscriber receives
//! both streams from both, each as its own image.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::memory::MemoryCatalog;
use kardamom_log::discovery::{Catalog, Instance, StreamPlane};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::{BlockBoundary, Receipt};

fn config(base: &LogConfig) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(200).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(50).unwrap();
    cfg.discovery.removal_grace_ms = 500;
    cfg
}

fn plane(cfg: &LogConfig, label: &str, catalog: &MemoryCatalog) -> StreamPlane {
    StreamPlane::with_catalog(
        cfg,
        label,
        Catalog::Memory(catalog.clone()),
        Instance {
            id: format!("alloc-{label}"),
            ports: None,
        },
        Ipv4Addr::LOCALHOST,
    )
}

fn receipt(nonce: u64) -> Receipt {
    Receipt {
        nonce,
        ..Receipt::default()
    }
}

fn boundary(block: u64) -> BlockBoundary {
    BlockBoundary {
        block_number: block,
        ..BlockBoundary::default()
    }
}

/// Publish until the offer is acknowledged: a dynamic MDC publication
/// is not connected until the subscriber joins it.
async fn publish_receipt_until_connected(publisher: &TxReceiptsPublisherHandle, nonce: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let p = publisher.clone();
    let connected = tokio::task::spawn_blocking(move || {
        while Instant::now() < deadline {
            if p.publish_receipt(&receipt(nonce)).is_ok() {
                return true;
            }
        }
        false
    })
    .await
    .expect("publish task");
    assert!(connected, "receipt publisher {nonce} never connected");
}

async fn publish_boundary_until_connected(publisher: &TxReceiptsPublisherHandle, block: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let p = publisher.clone();
    let connected = tokio::task::spawn_blocking(move || {
        while Instant::now() < deadline {
            if p.publish_boundary(&boundary(block)).is_ok() {
                return true;
            }
        }
        false
    })
    .await
    .expect("publish task");
    assert!(connected, "boundary publisher {block} never connected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_receipts -- --ignored`"]
async fn two_discovered_executors_reach_one_subscriber_on_both_streams() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4700).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();

    let mut plane_sub = plane(&cfg, "ingress", &catalog);
    let mut receipts = plane_sub
        .tx_receipts_subscriber(&rt, None)
        .expect("receipts subscriber");
    let mut boundaries = plane_sub
        .tx_receipt_boundaries_subscriber(&rt, None)
        .expect("boundary subscriber");

    let rt_a = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("runtime a");
    let mut plane_a = plane(&cfg, "executor-0", &catalog);
    let pub_a = plane_a
        .tx_receipts_publisher(&rt_a, 0)
        .await
        .expect("publisher a");
    let rt_b = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("runtime b");
    let mut plane_b = plane(&cfg, "executor-1", &catalog);
    let pub_b = plane_b
        .tx_receipts_publisher(&rt_b, 1)
        .await
        .expect("publisher b");

    publish_receipt_until_connected(&pub_a, 1).await;
    publish_receipt_until_connected(&pub_b, 2).await;
    publish_boundary_until_connected(&pub_a, 10).await;
    publish_boundary_until_connected(&pub_b, 20).await;

    let mut nonces = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !nonces.is_superset(&BTreeSet::from([1, 2])) && Instant::now() < deadline {
        if let Ok(Some((_pos, r))) =
            tokio::time::timeout(Duration::from_millis(200), receipts.recv()).await
        {
            nonces.insert(r.nonce);
        }
    }
    assert!(nonces.contains(&1) && nonces.contains(&2), "got {nonces:?}");

    let mut blocks = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !blocks.is_superset(&BTreeSet::from([10, 20])) && Instant::now() < deadline {
        if let Ok(Some((_pos, b))) =
            tokio::time::timeout(Duration::from_millis(200), boundaries.recv()).await
        {
            blocks.insert(b.block_number);
        }
    }
    assert!(
        blocks.contains(&10) && blocks.contains(&20),
        "got {blocks:?}"
    );

    plane_a.shutdown().await;
    plane_b.shutdown().await;
    plane_sub.shutdown().await;
}
