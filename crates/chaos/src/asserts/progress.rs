use std::cell::Cell;
use std::future::Future;

use crate::poll::{self, Budget, Outcome};

#[derive(Default)]
pub(super) struct ExecutorProgress {
    baseline: Cell<Option<i64>>,
}

impl ExecutorProgress {
    pub(super) async fn wait<F, Fut>(
        &self,
        budget: Budget,
        mut read: F,
    ) -> anyhow::Result<Outcome<(i64, i64)>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<i64>>,
    {
        poll::until(budget, |_| {
            let reading = read();
            async move { Ok(self.observe(reading.await)) }
        })
        .await
    }

    fn observe(&self, current: Option<i64>) -> Option<(i64, i64)> {
        let current = current?;
        if let Some(base) = self.baseline.get() {
            (current > base).then_some((base, current))
        } else {
            self.baseline.set(Some(current));
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_late_first_metric_must_advance_inside_the_same_recovery_budget() {
        let started = tokio::time::Instant::now();
        let outcome = ExecutorProgress::default()
            .wait(Budget::secs(60, 5), || {
                let elapsed = started.elapsed().as_secs();
                async move {
                    match elapsed {
                        0..20 => None,
                        20..25 => Some(349),
                        _ => Some(350),
                    }
                }
            })
            .await
            .unwrap();
        let (blocks, elapsed) = outcome.or_fail(|_| anyhow::anyhow!("timeout")).unwrap();
        assert_eq!(blocks, (349, 350));
        assert_eq!(elapsed, Duration::from_secs(25));
    }

    #[tokio::test(start_paused = true)]
    async fn a_returning_exporter_without_block_progress_cannot_pass() {
        let started = tokio::time::Instant::now();
        let outcome = ExecutorProgress::default()
            .wait(Budget::secs(60, 5), || {
                let visible = started.elapsed() >= Duration::from_secs(20);
                async move { visible.then_some(349) }
            })
            .await
            .unwrap();
        assert!(matches!(outcome, Outcome::TimedOut { .. }));
        assert_eq!(started.elapsed(), Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn absent_metrics_cannot_supply_a_zero_baseline() {
        let started = tokio::time::Instant::now();
        let outcome = ExecutorProgress::default()
            .wait(Budget::secs(20, 5), || async { None })
            .await
            .unwrap();
        assert!(matches!(outcome, Outcome::TimedOut { .. }));
        assert_eq!(started.elapsed(), Duration::from_secs(20));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hanging_probe_cannot_extend_the_recovery_budget() {
        let started = tokio::time::Instant::now();
        let outcome = ExecutorProgress::default()
            .wait(Budget::secs(20, 5), std::future::pending)
            .await
            .unwrap();
        assert!(matches!(outcome, Outcome::TimedOut { .. }));
        assert_eq!(started.elapsed(), Duration::from_secs(20));
    }
}
