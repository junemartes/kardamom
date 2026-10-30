use std::future;
use std::time::Duration;

use super::*;

struct Fixture;

impl Fixture {
    fn recovery() -> Recovery {
        Recovery::new(
            ["ingress-0", "ingress-1"]
                .map(|name| Reader {
                    name: name.into(),
                    target: Target::bridged(std::net::Ipv4Addr::LOCALHOST, name, 1),
                    scrape: Scrape::new(),
                    rpc: Rpc::new("http://127.0.0.1:1", 1).unwrap(),
                })
                .into(),
        )
        .unwrap()
    }

    fn reading(name: &str, redis: i64, degraded: i64) -> Reading {
        Reading {
            name: name.into(),
            result: Ok(Transition {
                before: Readers {
                    redis: 10,
                    degraded: 7,
                },
                after: Readers { degraded, redis },
                head_lag: None,
            }),
        }
    }

    fn complete() -> Vec<Reading> {
        vec![
            Self::reading("ingress-0", 11, 7),
            Self::reading("ingress-1", 11, 7),
        ]
    }
}

#[test]
fn every_reader_must_prove_redis_use_without_degradation() {
    let recovery = Fixture::recovery();
    for (redis, degraded) in [(10, 7), (11, 8), (1, 7), (11, 0)] {
        let readings = vec![
            Fixture::reading("ingress-0", 11, 7),
            Fixture::reading("ingress-1", redis, degraded),
        ];
        assert!(recovery.observe(&readings).is_none());
    }
    assert_eq!(recovery.observe(&Fixture::complete()), Some(()));
}

#[test]
fn missing_duplicate_foreign_and_failed_readers_cannot_certify_recovery() {
    let recovery = Fixture::recovery();
    for readings in [
        vec![],
        vec![Fixture::reading("ingress-0", 11, 7)],
        vec![
            Fixture::reading("ingress-0", 11, 7),
            Fixture::reading("ingress-0", 11, 7),
        ],
        vec![
            Fixture::reading("ingress-0", 11, 7),
            Fixture::reading("other", 11, 7),
        ],
        vec![
            Fixture::reading("ingress-0", 11, 7),
            Reading {
                name: "ingress-1".into(),
                result: Err(anyhow::anyhow!("exporter unavailable")),
            },
        ],
    ] {
        assert!(recovery.observe(&readings).is_none());
    }
    assert!(recovery.last.borrow().contains("exporter unavailable"));
    assert!(Recovery::new(vec![]).is_err());
}

#[tokio::test(start_paused = true)]
async fn separate_partial_observations_do_not_accumulate_into_recovery() {
    let recovery = Fixture::recovery();
    let mut attempt = 0;
    let started = tokio::time::Instant::now();
    let error = recovery
        .wait_with("cache", Budget::secs(120, 3), || {
            let mut readings = Fixture::complete();
            readings[attempt % 2].result.as_mut().unwrap().after.redis = 10;
            attempt += 1;
            future::ready(readings)
        })
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert_eq!(attempt, 40);
    let diagnostic = error.to_string();
    assert!(diagnostic.contains("ingress-1"), "{diagnostic}");
    assert!(diagnostic.contains("redis: 10"), "{diagnostic}");
    assert!(!diagnostic.contains("no completed observation"));
}

#[tokio::test(start_paused = true)]
async fn a_fresh_complete_reading_recovers_after_a_failed_probe() {
    let recovery = Fixture::recovery();
    let mut first = Fixture::complete();
    first[1].result = Err(anyhow::anyhow!("cold read: RPC unavailable"));
    let mut attempts = [first, Fixture::complete()].into_iter();
    let started = tokio::time::Instant::now();
    recovery
        .wait_with("cache", Budget::secs(120, 3), || {
            future::ready(attempts.next().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn a_hung_probe_preserves_the_original_recovery_deadline() {
    let recovery = Fixture::recovery();
    let started = tokio::time::Instant::now();
    let error = recovery
        .wait_with("cache", Budget::secs(120, 3), future::pending)
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("no completed observation"));
}

mod http;
