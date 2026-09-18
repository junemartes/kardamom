//! Smoke test: calling `kardamom_obs::init("executor", ...)` exposes
//! `/metrics` with the expected counters.

use kardamom_obs::testkit::{free_port, scrape};

#[tokio::test]
async fn executor_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("executor", addr, "local", "test", "test")
        .await
        .expect("init");

    // Touch the counter that the executor crate publishes. This means
    // describe_counter calls do not also need us to run the binary.
    // The test uses the crate's constants, so a rename in src/metrics.rs
    // breaks this test.
    metrics::counter!(kardamom_engine::metrics::TX_APPLIED_TOTAL, "outcome" => "ok").increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains(kardamom_engine::metrics::TX_APPLIED_TOTAL),
        "missing executor counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"executor\""),
        "missing service label"
    );
}
