//! Exported cumulative counts distinguish target boundaries without interpolation.

mod common;

use std::time::Duration;

const CANONICAL: &str = "kardamom_ingress_seal_latency_seconds";
const RECEIPT: &str = "kardamom_ingress_execute_latency_seconds";

struct Scrape(String);

impl Scrape {
    fn record(ns: u64) {
        let seconds = Duration::from_nanos(ns).as_secs_f64();
        metrics::histogram!(CANONICAL).record(seconds);
        metrics::histogram!(RECEIPT).record(seconds);
    }

    fn count(&self, metric: &str, upper: &str) -> u64 {
        self.0
            .lines()
            .find(|line| {
                line.starts_with(&format!("{metric}_bucket{{"))
                    && line.contains(&format!("le=\"{upper}\""))
            })
            .unwrap_or_else(|| panic!("missing {metric} bucket {upper}: {}", self.0))
            .rsplit(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
}

#[tokio::test]
async fn canonical_targets_keep_exact_boundaries_and_the_slow_tail() {
    let addr = common::free_port();
    kardamom_obs::init("ingress", addr, "test", "0.0.0", "test")
        .await
        .unwrap();
    let samples = [
        149_999,
        150_000,
        150_001,
        998_999,
        999_000,
        999_001,
        1_000_000,
        1_000_001,
        10_000_000,
        6_000_000_000,
    ];
    samples.into_iter().for_each(Scrape::record);
    let scrape = Scrape(common::scrape(addr, Duration::from_secs(2)).await);
    assert_eq!(scrape.count(CANONICAL, "0.00015"), 2);
    assert_eq!(scrape.count(CANONICAL, "0.000999"), 5);
    assert_eq!(scrape.count(CANONICAL, "0.001"), 7);
    assert_eq!(scrape.count(CANONICAL, "5"), 9);
    assert_eq!(scrape.count(CANONICAL, "+Inf"), 10);
    assert_eq!(scrape.count(RECEIPT, "0.0001"), 0);
    assert_eq!(scrape.count(RECEIPT, "0.00025"), 3);
    assert_eq!(scrape.count(RECEIPT, "+Inf"), 10);
    assert!(scrape.0.lines().all(|line| {
        !line.starts_with(&format!("{RECEIPT}_bucket{{")) || !line.contains("le=\"0.00015\"")
    }));
}
