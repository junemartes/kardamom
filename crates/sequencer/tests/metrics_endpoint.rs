//! Smoke test: calling `kardamom_obs::init("sequencer", ...)` exposes
//! `/metrics` with the expected counters.

use std::net::{SocketAddr, TcpListener};

#[tokio::test]
async fn sequencer_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("sequencer", addr, "local", "test", "test").expect("init");

    // Touch every counter the sequencer crate is expected to publish so
    // describe_counter calls don't require us to also drive the binary.
    metrics::counter!("kardamom_sequencer_tx_ingested_total", "partition" => "0").increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains("kardamom_sequencer_tx_ingested_total"),
        "missing sequencer counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"sequencer\""),
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
