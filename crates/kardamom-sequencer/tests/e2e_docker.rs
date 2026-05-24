//! D-Sh8 e2e: real Aeron Media Driver + Archive in Docker via the
//! `kardamom_log::testing::AeronTestCluster` harness from S3. Per the same
//! convention used by `kardamom-ingress::tests::docker_e2e.rs`, this is a
//! smoke test that proves the container starts and exposes its ports — the
//! full round-trip (Aeron-backed `IngressSource` adapter feeding a real
//! sequencer process publishing onto a real channel B) requires a
//! `!Send + !Sync`-aware adapter that lives on a dedicated OS thread; the
//! adapter is staged for a follow-up PR once the S3 high-level ingress
//! publisher surface lands.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker daemon
//! and ~30s startup; default `cargo test` skips it.

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
