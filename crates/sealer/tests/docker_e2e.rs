//! E2E test scaffold: real Aeron Media Driver + Aeron Archive in Docker
//! via the [`log::testing::AeronTestCluster`] harness from S3.
//!
//!— mock-based unit and integration tests in this crate
//! stay (single_emitter, failover, chaos_isolation); this is *additional*
//! coverage that brings up the real Aeron container so we catch
//! wire-format / IPC / back-pressure bugs the FakeBus cannot surface.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker
//! daemon and ~30s startup; default `cargo test` skips it.
//!
//! **v0 scope:** brings up the Aeron container and asserts the harness
//! resolves its host ports. The full sealer ↔ real-Aeron round-trip
//! requires a `BoundaryPublisher` adapter wrapping `kardamom-log`'s
//! `aeron-live` tx_ordering concurrent publisher. The current
//! `kardamom-log` exposes the low-level rusteron primitives but not yet
//! a high-level `TxOrderingPublisher` async wrapper (see
//! crates/kardamom-log/src/publisher.rs); building that wrapper is a
//! cross-component task tracked in the executor branch's Task 19
//! ("pending S3 channel wrappers"). When the wrapper lands, this file
//! gains the full live-publish round-trip; the harness call below is
//! the gating proof that this crate's test target can reach it.

#![cfg(feature = "docker-e2e")]

use log::testing::AeronTestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-sealer --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_and_exposes_ports() {
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container should start");
    assert_eq!(cluster.len(), 1);
    let endpoint = cluster.archive_control_endpoint(0).await;
    assert!(
        endpoint.starts_with("127.0.0.1:"),
        "unexpected endpoint {endpoint}"
    );
    let response = cluster.archive_response_endpoint(0).await;
    assert!(
        response.starts_with("127.0.0.1:"),
        "unexpected response endpoint {response}"
    );
}
