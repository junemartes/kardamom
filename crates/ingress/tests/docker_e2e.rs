//! E2E test scaffold. It uses a real Aeron Media Driver and Aeron Archive
//! in Docker, through the [`kardamom_log::testing::AeronTestCluster`]
//! harness from `kardamom-log`.
//!
//! This test adds to the mock-based unit and integration tests in this
//! crate. It does not replace them. It brings up real Aeron containers, so
//! it can catch wire-format, IPC, and back-pressure bugs that mocks
//! cannot find.
//!
//! The test needs `feature = "docker-e2e"`. It needs a Docker daemon and
//! about 30 seconds to start. The default `cargo test` skips it.
//!
//! v0 scope: the test starts the Aeron container and checks that the
//! harness resolves its host ports. A full proxy-to-real-Aeron round trip
//! needs an adapter. The adapter must wrap `kardamom-log`'s `aeron-live`
//! `TxOrderingPublisher`, `TxReceiptsSubscriber`, and similar types as an
//! `IngressPublication` or `IngressSubscription` implementor. These types
//! are `!Send + !Sync` (see `kardamom-log`'s `rusteron` quirks doc). So the adapter
//! must run on its own OS thread and forward messages over Send-able
//! channels. A follow-up PR will add this adapter. This test proves the
//! Docker harness works from this crate's test target, so the follow-up
//! only adds code.

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
    // Check the format: "127.0.0.1:<port>".
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
