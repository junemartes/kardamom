use super::*;

const HOLD: Duration = Duration::from_secs(30);
const BUDGET: Duration = Duration::from_secs(60);

#[tokio::test(start_paused = true)]
async fn fast_alert_samples_require_the_full_hold_duration() {
    let observation = Held::new(HOLD);
    let outcome = observation
        .run(BUDGET, || async { Ok(Some("pending".into())) })
        .await
        .unwrap();
    let Outcome::Ready { elapsed, value } = outcome else {
        panic!("a continuously active alert must pass");
    };
    assert_eq!(elapsed, HOLD);
    assert_eq!(value, "pending");
}

#[tokio::test(start_paused = true)]
async fn slow_alert_samples_use_elapsed_time_within_the_original_budget() {
    let observation = Held::new(HOLD);
    let outcome = observation
        .run(BUDGET, || async {
            tokio::time::sleep(Duration::from_secs(8)).await;
            Ok(Some("firing".into()))
        })
        .await
        .unwrap();
    let Outcome::Ready { elapsed, .. } = outcome else {
        panic!("observations at 8, 21, 34 and 47 seconds span 39 active seconds");
    };
    assert_eq!(elapsed, Duration::from_secs(47));
}

struct Reset {
    broken: Result<Option<String>, &'static str>,
}

impl Reset {
    async fn verify(self) {
        let observation = Held::new(HOLD);
        let reads = Cell::new(0_u32);
        let outcome = observation
            .run(BUDGET, || {
                let read = reads.get();
                reads.set(read.checked_add(1).unwrap());
                let response = if read == 2 {
                    self.broken.clone().map_err(anyhow::Error::msg)
                } else {
                    Ok(Some(if read < 2 { "pending" } else { "firing" }.into()))
                };
                async move { response }
            })
            .await
            .unwrap();
        let Outcome::Ready { elapsed, .. } = outcome else {
            panic!("the new run from 15 to 45 seconds must pass");
        };
        assert_eq!(elapsed, Duration::from_secs(45));
    }
}

#[tokio::test(start_paused = true)]
async fn inactive_unknown_and_failed_reads_each_reset_the_hold() {
    for broken in [Ok(None), Ok(Some("unknown".into())), Err("unavailable")] {
        Reset { broken }.verify().await;
    }
}

#[tokio::test(start_paused = true)]
async fn first_slow_response_does_not_certify_unobserved_time() {
    let observation = Held::new(HOLD);
    let first = Cell::new(true);
    let outcome = observation
        .run(BUDGET, || {
            let initial = first.replace(false);
            async move {
                if initial {
                    tokio::time::sleep(Duration::from_secs(31)).await;
                }
                Ok(Some("pending".into()))
            }
        })
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == BUDGET));
    assert!(observation.evidence().contains("25.000s"));
}

#[tokio::test(start_paused = true)]
async fn failed_alert_reads_preserve_the_cause_at_the_total_deadline() {
    let observation = Held::new(HOLD);
    let outcome = observation
        .run(BUDGET, || async {
            anyhow::bail!("HTTP 503 from Prometheus")
        })
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == BUDGET));
    assert!(observation.evidence().contains("HTTP 503 from Prometheus"));
    assert!(observation.evidence().contains("0.000s"));
}

#[tokio::test(start_paused = true)]
async fn hanging_alert_read_cannot_extend_the_total_budget() {
    let observation = Held::new(HOLD);
    let outcome = observation.run(BUDGET, std::future::pending).await.unwrap();
    assert!(matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == BUDGET));
    assert!(observation.evidence().contains("no completed observation"));
}
