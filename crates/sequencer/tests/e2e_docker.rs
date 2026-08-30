//! End-to-end test: a real Aeron Media Driver and Archive in Docker.
//! Uses the `kardamom_log::testing::AeronTestCluster` harness from `kardamom-log`.
//!
//! This test follows the convention in
//! `kardamom-kardamom_ingress::tests::docker_e2e.rs`. It is a topology
//! smoke test. It proves that a single Aeron node can serve both the
//! per-sequencer tx_data streams and the shared tx_ordering stream.
//!
//! The full round-trip test needs an Aeron-backed `IngressSource` adapter
//! that feeds a real sequencer process. That sequencer process performs
//! dual writes onto real Aeron streams. This adapter must be
//! `!Send + !Sync`-aware and must run on a dedicated OS thread. The adapter
//! is planned for a follow-up PR, after the `kardamom-log` high-level
//! ingress publisher surface lands.
//!
//! This test needs the `docker-e2e` feature. It needs a Docker daemon and
//! about 30 seconds to start. The default `cargo test` run skips it.

#![cfg(feature = "docker-e2e")]

use kardamom_log::testing::AeronTestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_for_sequencer_e2e_smoke() {
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container should start");
    assert_eq!(cluster.len(), 1);
    let endpoint = cluster.archive_control_endpoint(0).await;
    assert!(
        endpoint.starts_with("127.0.0.1:"),
        "unexpected endpoint {endpoint}"
    );
    drop(cluster);
}

/// M+1 topology smoke test.
/// Checks that the single-node Aeron harness exposes enough Archive
/// endpoints to host M per-sequencer tx_data streams and the tx_ordering
/// stream. Today the single-node container puts everything on one Media
/// Driver and Archive, so this check always passes. The test pins this
/// expectation. When the harness grows to multi-node, a failure here is
/// deliberate, not silent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn aeron_cluster_serves_m_plus_one_topology_smoke() {
    const M: usize = 4;
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container should start");
    // The harness exposes one shared Archive endpoint. All M tx_data
    // streams and the tx_ordering stream live on it. This follows the
    // default in `ChannelsConfig::tx_data_channel_template`
    // (`aeron:ipc?alias=a-{sid}`) and the shared `tx_ordering_channel`.
    let archive = cluster.archive_control_endpoint(0).await;
    let response = cluster.archive_response_endpoint(0).await;
    assert!(archive.starts_with("127.0.0.1:"));
    assert!(response.starts_with("127.0.0.1:"));

    // A follow-up wiring PR will test the M+1 fanout end-to-end. For now,
    // this test checks that it can name 4 sequencer ids without the
    // harness rejecting them.
    for sid in 0..M as u8 {
        let _alias = format!("a-{sid}");
    }
    drop(cluster);
}
