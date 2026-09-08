//! Smoke test: calling `kardamom_obs::init("da-watcher", ...)` exposes
//! `/metrics` with the expected counters.

use kardamom_obs::testkit::{free_port, scrape};

#[tokio::test]
async fn da_watcher_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("da-watcher", addr, "local", "test", "test")
        .await
        .expect("init");

    // Touch the counter so the describe call registers it.
    ::metrics::counter!(
        kardamom_da_watcher::metrics::TICK_TOTAL,
        "outcome" => "ok"
    )
    .increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains(kardamom_da_watcher::metrics::TICK_TOTAL),
        "missing da-watcher counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"da-watcher\""),
        "missing service label; got:\n{body}"
    );
}
