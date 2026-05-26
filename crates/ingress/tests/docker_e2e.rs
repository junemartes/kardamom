//! E2E test scaffold: real Aeron Media Driver + Aeron Archive in Docker
//! via the [`kardamom_log::testing::AeronTestCluster`] harness from S3.
//!
//!— mock-based unit and integration tests in this crate
//! stay; this is *additional* coverage that brings up the real Aeron
//! containers so we catch wire-format / IPC / back-pressure bugs the
//! mocks cannot surface.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker
//! daemon and ~30s startup; default `cargo test` skips it.
//!
//! **v0 scope:** brings up the Aeron container and asserts the harness
//! resolves its host ports. The full proxy↔real-Aeron round-trip
//! requires an adapter that wraps `kardamom-log`'s `aeron-live`
//! `TxOrderingPublisher` / `ReceiptCacheSubscriber` / etc. as an
//! `IngressPublication` / `IngressSubscription` implementor — those
//! types are `!Send + !Sync` (per S3's `rusteron` quirks doc) so the
//! adapter has to live on a dedicated OS thread and forward messages
//! over Send-able channels. That adapter is left for a follow-up PR;
//! the test here proves the Docker harness is reachable from this
//! crate's test target so that follow-up is purely additive.

#![cfg(feature = "docker-e2e")]

use kardamom_log::testing::AeronTestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_and_exposes_ports() {
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container should start");
    assert_eq!(cluster.len(), 1);
    let endpoint = cluster.archive_control_endpoint(0).await;
    // Sanity: "127.0.0.1:<port>".
    assert!(
        endpoint.starts_with("127.0.0.1:"),
        "unexpected endpoint {endpoint}"
    );
    let response = cluster.archive_response_endpoint(0).await;
    assert!(
        response.starts_with("127.0.0.1:"),
        "unexpected response endpoint {response}"
    );
    drop(cluster);
}
