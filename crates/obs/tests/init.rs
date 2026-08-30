//! End-to-end smoke test for `kardamom_obs::init`: spin up the exporter on an
//! ephemeral port, scrape `/metrics`, and assert the heartbeat + build_info
//! show up with the correct global labels.

use std::net::{SocketAddr, TcpListener};

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = l.local_addr().expect("local_addr");
    // Dropping the listener releases the port before the exporter binds it.
    drop(l);
    addr
}

#[tokio::test]
async fn init_exposes_build_info_and_service_up() {
    let addr = free_port();
    kardamom_obs::init("test-service", addr, "test-host", "0.0.0", "deadbeef")
        .await
        .expect("init succeeds on a free port");

    // The exporter binds asynchronously — give it a short retry budget.
    let url = format!("http://{}/metrics", addr);
    let body = scrape_with_retry(&url).await;

    assert!(
        body.contains("kardamom_service_up{"),
        "expected kardamom_service_up in:\n{body}"
    );
    assert!(
        body.contains("service=\"test-service\""),
        "expected service label in:\n{body}"
    );
    assert!(
        body.contains("host_id=\"test-host\""),
        "expected host_id label in:\n{body}"
    );
    assert!(
        body.contains("kardamom_build_info"),
        "expected kardamom_build_info in:\n{body}"
    );
    assert!(
        body.contains("version=\"0.0.0\""),
        "expected version label in:\n{body}"
    );
}

async fn scrape_with_retry(url: &str) -> String {
    for _ in 0..40 {
        match reqwest::get(url).await {
            Ok(r) if r.status().is_success() => return r.text().await.expect("text"),
            _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    panic!("exporter did not become ready at {url}");
}
