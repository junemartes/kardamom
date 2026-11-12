use super::*;

struct Exporter;

impl Exporter {
    fn healthy() -> String {
        format!("{PIPELINE_ACTIVE} 1\n{COMMITTED} 10\n{VERIFIED} 5\n{SHADOW_CHECKS} 3\n")
    }
}

#[test]
fn verdict_needs_positive_evidence_and_checks_errors_in_the_same_scrape() {
    let healthy = Exporter::healthy();
    assert!(Sample::read(&healthy).unwrap().is_some());
    assert!(Sample::read("").unwrap().is_none());
    assert!(
        Sample::read(&format!("{COMMITTED} 10\n"))
            .unwrap()
            .is_none()
    );
    assert!(Sample::read(&format!("{healthy}{DIVERGENCE} 1\n")).is_err());
    assert!(Sample::read(&format!("{healthy}{SHADOW_MISMATCH} 1\n")).is_err());
    assert!(Sample::read(&format!("{DIVERGENCE} 1\n")).is_err());
    assert!(Sample::read(&format!("{SHADOW_MISMATCH} 1\n")).is_err());
}

#[test]
fn a_closed_pipeline_cannot_reuse_its_previous_verification_counts() {
    let stale = format!("{COMMITTED} 10\n{VERIFIED} 5\n{SHADOW_CHECKS} 3\n");
    assert!(Sample::read(&stale).unwrap().is_none());
    assert!(
        Sample::read(&format!("{stale}{PIPELINE_ACTIVE} 0\n"))
            .unwrap()
            .is_none()
    );
    assert!(
        Sample::read(&format!("{stale}{PIPELINE_ACTIVE} 1\n"))
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn a_restarted_validator_must_verify_again_within_the_sync_budget() {
    let mut calls = 0;
    let outcome = poll::until(Budget::secs(10, 2), |_| {
        calls += 1;
        let body = match calls {
            1 => String::new(),
            2 => format!("{COMMITTED} 10\n{VERIFIED} 0\n{SHADOW_CHECKS} 0\n"),
            _ => Exporter::healthy(),
        };
        async move {
            Ok(Sample::read(&body)?.filter(|sample| sample.synced_with(Some(10), 10).is_some()))
        }
    })
    .await
    .unwrap();
    let (sample, elapsed) = outcome.or_fail(|_| anyhow::anyhow!("no evidence")).unwrap();
    assert_eq!(sample.verified, 5);
    assert_eq!(elapsed, Duration::from_secs(4));
    assert_eq!(sample.synced_with(None, 10), None);
    assert_eq!(sample.synced_with(Some(21), 10), None);
}

#[tokio::test(start_paused = true)]
async fn a_caught_up_cursor_without_verification_never_passes() {
    let outcome = poll::until(Budget::secs(5, 1), |_| async {
        Sample::read(&format!("{COMMITTED} 10\n"))
    })
    .await
    .unwrap();
    assert!(matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == Duration::from_secs(5)));
}
