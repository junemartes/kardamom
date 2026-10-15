//! Real-Aeron check of the executor stream over the in-memory catalog: a
//! discovered `exec_txs` subscriber receives the record of a discovered
//! `exec_txs` publisher unchanged. A start-up publication open waits
//! through a frozen media driver with one add, and the driver then holds
//! one publication only.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

mod common;

use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{ADD_PUB_TIMEOUT, ExecTxsPublisherHandle, ExecTxsSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::memory::MemoryCatalog;
use kardamom_log::discovery::{Catalog, Instance, StreamPlane};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::{BPosition, ExecTxRecord};

fn config(base: &LogConfig) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(200).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(50).unwrap();
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

fn record() -> ExecTxRecord {
    let tx_data_position = BPosition {
        term_id: 2,
        term_offset: 4096,
    };
    ExecTxRecord {
        index: 41,
        tx_ref: common::tx_ref(1, tx_data_position),
        envelope: common::tx_envelope(9, 0x5A, 64),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_exec_txs -- --ignored`"]
async fn a_discovered_subscriber_receives_the_executor_record() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig {
        cluster: _cluster,
        rt,
        cfg,
    } = AeronTestCluster::single_node_runtime(5301).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();

    let mut executor_plane = plane(&cfg, "executor", &catalog);
    let publisher: ExecTxsPublisherHandle = executor_plane.publisher(&rt).await.expect("publisher");
    let mut validator_plane = plane(&cfg, "validator", &catalog);
    let mut subscriber: ExecTxsSubscriberHandle =
        validator_plane.subscriber(&rt).expect("subscriber");

    // A dynamic MDC publication refuses offers until a subscriber joins,
    // so the first accepted offer is the first record the subscriber sees.
    let deadline = Instant::now() + Duration::from_secs(10);
    let sent = tokio::task::spawn_blocking(move || {
        std::iter::repeat_with(|| publisher.publish(&record()))
            .take_while(|_| Instant::now() < deadline)
            .any(|offer| offer.is_ok())
    })
    .await
    .expect("publish task");
    assert!(sent, "the publisher never connected");

    let (_pos, got) = tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
        .await
        .expect("a record within the budget")
        .expect("an open subscription");
    assert_eq!(got, record());

    executor_plane.shutdown().await;
    validator_plane.shutdown().await;
}

/// How long the driver stays frozen: past the add timeout of the first
/// try (`ADD_PUB_TIMEOUT`), and below the 10 s default driver timeout of
/// the client.
const FREEZE: Duration = Duration::from_secs(7);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_exec_txs -- --ignored`"]
async fn a_publication_open_outlasts_a_driver_stall() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5311).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();
    let cluster = Arc::new(cluster);

    cluster
        .set_frozen(0, true)
        .await
        .expect("freeze the driver");
    let thaw = {
        let cluster = cluster.clone();
        tokio::spawn(async move {
            tokio::time::sleep(FREEZE).await;
            cluster.set_frozen(0, false).await
        })
    };
    let start = Instant::now();
    let mut executor_plane = plane(&cfg, "executor", &catalog);
    let opened = executor_plane
        .publisher::<ExecTxsPublisherHandle>(&rt)
        .await;
    thaw.await.expect("thaw task").expect("thaw the driver");
    opened.expect("the open outlasts the stall");
    assert!(
        start.elapsed() > ADD_PUB_TIMEOUT,
        "the start-up add waits past the run-time add timeout"
    );
    let publications: Vec<String> = cluster
        .driver_counter_labels(0)
        .await
        .expect("driver counters")
        .into_iter()
        .filter(|label| label.starts_with("pub-lmt:") && label.contains("control-mode=dynamic"))
        .collect();
    assert_eq!(
        publications.len(),
        1,
        "one add, so one publication in the driver: {publications:?}"
    );

    executor_plane.shutdown().await;
}
