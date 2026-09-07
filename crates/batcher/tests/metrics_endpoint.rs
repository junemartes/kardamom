//! Smoke test: calling `kardamom_obs::init("batcher", ...)` exposes
//! `/metrics` with the expected counters.

use std::net::{SocketAddr, TcpListener};

#[tokio::test]
async fn batcher_metrics_endpoint_serves_expected_counters() {
    // Touch every counter the batcher crate is expected to publish. This
    // way, describe_counter calls do not require running the binary too.
    // Use the crate's constants, so a rename in metric_names fails here.
    use kardamom_batcher::batcher::metric_names;

    let addr = free_port();
    kardamom_obs::init("batcher", addr, "local", "test", "test")
        .await
        .expect("init");

    metrics::counter!(metric_names::BLOCKS_OBSERVED).increment(0);
    metrics::counter!(metric_names::BATCHES_POSTED).increment(0);
    metrics::counter!(metric_names::BLOBS_POSTED).increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains(metric_names::BLOCKS_OBSERVED),
        "missing blocks_observed counter; got:\n{body}"
    );
    assert!(
        body.contains(metric_names::BATCHES_POSTED),
        "missing batches_posted counter; got:\n{body}"
    );
    assert!(
        body.contains(metric_names::BLOBS_POSTED),
        "missing blobs_posted counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"batcher\""),
        "missing service label"
    );
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
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
