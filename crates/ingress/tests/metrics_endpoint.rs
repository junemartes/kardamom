//! Smoke test. A call to `kardamom_obs::init("ingress", ...)` exposes
//! `/metrics` with the expected counters.

use kardamom_obs::testkit::{free_port, scrape};

#[tokio::test]
async fn ingress_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("ingress", addr, "local", "test", "test")
        .await
        .expect("init");

    // Touch the counter that the ingress crate must publish. This lets
    // describe_counter run without the full binary.
    // The test uses the crate's constants, so a rename in src/metrics.rs
    // makes this test fail.
    metrics::counter!(kardamom_ingress::metrics::TX_RECEIVED_TOTAL).increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains(kardamom_ingress::metrics::TX_RECEIVED_TOTAL),
        "missing ingress counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"ingress\""),
        "missing service label"
    );
}
