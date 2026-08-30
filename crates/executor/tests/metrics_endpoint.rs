//! Smoke test: calling `kardamom_obs::init("executor", ...)` exposes
//! `/metrics` with the expected counters.

use std::net::{SocketAddr, TcpListener};

#[tokio::test]
async fn executor_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("executor", addr, "local", "test", "test")
        .await
        .expect("init");

    // Touch the counter the executor crate is expected to publish so
    // describe_counter calls don't require us to also drive the binary.
    // Uses the crate's constants so a rename in src/metrics.rs fails here.
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

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

async fn scrape(url: &str) -> String {
    for _ in 0..40 {
        if let Ok(r) = reqwest::get(url).await
            && r.status().is_success()
        {
            return r.text().await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("exporter not ready at {url}");
}
