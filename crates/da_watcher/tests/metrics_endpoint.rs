//! Smoke test: calling `kardamom_obs::init("da-watcher", ...)` exposes
//! `/metrics` with the expected counters.

use std::net::{SocketAddr, TcpListener};

#[tokio::test]
async fn da_watcher_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("da-watcher", addr, "local", "test", "test").expect("init");

    // Touch the counter so the describe call registers it.
    ::metrics::counter!(
        kardamom_da_watcher::metrics::TICK_TOTAL,
        "outcome" => "ok"
    )
    .increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains("kardamom_da_watcher_tick_total"),
        "missing da-watcher counter; got:\n{body}"
    );
    assert!(
        body.contains("service=\"da-watcher\""),
        "missing service label; got:\n{body}"
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
