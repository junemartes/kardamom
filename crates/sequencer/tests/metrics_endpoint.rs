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
    readiness_gauges(&format!("http://{addr}/metrics")).await;
}

/// Readiness must distinguish a fresh idle replica from a missing exporter,
/// and a replica that re-enters RESYNC must not retain its old zero gauge.
async fn readiness_gauges(url: &str) {
    use kardamom_sequencer::resync::{ResyncChannel, ResyncConfig};
    use std::time::{Duration, Instant};

    let _seq = kardamom_sequencer::Sequencer::new(kardamom_sequencer::testkit::one_partition_cfg())
        .unwrap();
    let cfg = ResyncConfig::default();
    let hold = Duration::from_millis(cfg.exit_hold_ms);
    let stall = Duration::from_millis(cfg.publish_stall_ms);
    let mut channel = ResyncChannel::open(cfg, 0).unwrap();
    let body = scrape(url).await;
    let depths: Vec<_> = body
        .lines()
        .filter(|line| line.starts_with("kardamom_sequencer_pending_depth{"))
        .collect();
    assert_eq!(depths.len(), 256);
    assert!(depths.iter().all(|line| line.ends_with(" 0")));
    assert_mode(&body, "1");

    let now = Instant::now();
    channel.watermark.store(1);
    channel.controller.observe(now);
    channel.controller.observe(now + hold);
    assert!(!channel.controller.active());
    assert_mode(&scrape(url).await, "0");

    channel.controller.note_publish_stall(now + hold);
    channel.controller.note_publish_stall(now + hold + stall);
    assert!(channel.controller.active());
    assert_mode(&scrape(url).await, "1");
}

fn assert_mode(body: &str, value: &str) {
    let mode = body
        .lines()
        .find(|line| line.starts_with("kardamom_sequencer_resync_mode{"))
        .expect("RESYNC gauge exists");
    assert_eq!(mode.split_whitespace().last(), Some(value));
}
