//! Smoke test: calling `kardamom_obs::init("sequencer", ...)` exposes
//! `/metrics` with the expected counters.

use kardamom_obs::testkit::{free_port, scrape};

#[tokio::test]
async fn sequencer_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("sequencer", addr, "local", "test", "test")
        .await
        .expect("init");

    // Touch every counter that the sequencer crate publishes. This lets
    // describe_counter run without the binary. The test uses the crate's
    // constants, so a rename in src/metrics.rs breaks this test.
    metrics::counter!(kardamom_sequencer::metrics::TX_INGESTED, "partition" => "0").increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains(kardamom_sequencer::metrics::TX_INGESTED),
        "missing sequencer counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"sequencer\""),
        "missing service label"
    );
}
