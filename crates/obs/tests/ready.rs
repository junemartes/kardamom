//! The `/ready` route beside `/metrics`: 503 while the rule's gauge is
//! unset, 200 once the service sets it, and 404 elsewhere.

mod common;

use std::time::Duration;

#[tokio::test]
async fn ready_follows_the_rule_and_metrics_stay_served() {
    let addr = common::free_port();
    let rule = kardamom_obs::Readiness::up().equals("kardamom_test_serving", 1.0);
    kardamom_obs::init_with_readiness("ready-test", addr, "host", "0.0.0", "deadbeef", rule)
        .await
        .expect("init succeeds on a free port");
    let budget = Duration::from_secs(2);

    let not_ready = common::get(addr, "/ready", budget).await;
    assert!(not_ready.contains(" 503 "), "{not_ready}");
    assert!(not_ready.contains("kardamom_test_serving"), "{not_ready}");

    metrics::gauge!("kardamom_test_serving").set(1.0);
    let ready = common::get(addr, "/ready", budget).await;
    assert!(ready.contains(" 200 "), "{ready}");

    let metrics = common::get(addr, "/metrics", budget).await;
    assert!(metrics.contains("kardamom_test_serving{"), "{metrics}");

    let missing = common::get(addr, "/other", budget).await;
    assert!(missing.contains(" 404 "), "{missing}");
}
