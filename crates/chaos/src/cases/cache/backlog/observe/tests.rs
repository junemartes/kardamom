use std::time::Duration;

use super::*;

#[tokio::test(start_paused = true)]
async fn a_transient_read_failure_retries_until_complete_evidence() {
    let mut replies = [Err(anyhow::anyhow!("timed out")), Ok(42)].into_iter();
    let started = tokio::time::Instant::now();
    let value = Observe::new("baseline", Budget::secs(120, 2))
        .wait(|| std::future::ready(replies.next().unwrap()))
        .await
        .unwrap();
    assert_eq!(value, 42);
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn persistent_missing_heads_fail_at_the_recovery_deadline_with_the_cause() {
    let started = tokio::time::Instant::now();
    let error = Observe::new("two later coverage heads", Budget::secs(300, 5))
        .wait::<(), _, _>(|| async { anyhow::bail!("writer2 has no second coverage head") })
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(300));
    assert!(error.to_string().contains("writer2"), "{error}");
}

#[tokio::test(start_paused = true)]
async fn a_hanging_baseline_read_cannot_extend_its_budget() {
    let started = tokio::time::Instant::now();
    let error = Observe::new("baseline", Budget::secs(120, 2))
        .wait(std::future::pending::<anyhow::Result<()>>)
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("after 120s"), "{error}");
}
