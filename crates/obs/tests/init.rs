//! End-to-end smoke test for `kardamom_obs::init`: start the exporter on an
//! ephemeral port, scrape `/metrics`, and check that the heartbeat and
//! `build_info` show up with the correct global labels.

mod common;

use std::time::Duration;

#[tokio::test]
async fn init_exposes_build_info_and_service_up() {
    let addr = common::free_port();
    kardamom_obs::init("test-service", addr, "test-host", "0.0.0", "deadbeef")
        .await
        .expect("init succeeds on a free port");

    // The exporter binds asynchronously, so give it a short retry budget.
    let body = common::scrape(addr, Duration::from_secs(2)).await;

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
