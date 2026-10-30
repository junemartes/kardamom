//! The one polling loop of the suite. Every wait is a step function
//! called until it yields a value or the budget runs out, so no
//! assertion carries its own loop.

use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;

use tokio::time::Instant;

/// How long the last probe of a hold may run past the end of the hold:
/// a direct read of an exporter (5 s), its `docker exec` fallback
/// (10 s), and a margin. The answer of that probe counts. A probe that
/// runs longer is cancelled.
const PROBE_GRACE: Duration = Duration::from_secs(20);

/// A wait's budget and cadence.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub timeout: Duration,
    pub interval: Duration,
}

impl Budget {
    #[must_use]
    pub fn new(timeout: Duration, interval: Duration) -> Self {
        Self { timeout, interval }
    }

    /// A budget of `secs` seconds, polled every `every` seconds.
    #[must_use]
    pub fn secs(secs: u64, every: u64) -> Self {
        Self::new(Duration::from_secs(secs), Duration::from_secs(every))
    }
}

/// How a wait ended.
#[derive(Debug)]
pub enum Outcome<T> {
    /// The step yielded a value after `elapsed`.
    Ready { value: T, elapsed: Duration },
    /// The budget ran out; `elapsed` is the time spent.
    TimedOut { elapsed: Duration },
}

impl<T> Outcome<T> {
    /// The value, or the error `on_timeout` builds from the elapsed
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the built error when the wait timed out.
    pub fn or_fail(
        self,
        on_timeout: impl FnOnce(Duration) -> anyhow::Error,
    ) -> anyhow::Result<(T, Duration)> {
        match self {
            Self::Ready { value, elapsed } => Ok((value, elapsed)),
            Self::TimedOut { elapsed } => Err(on_timeout(elapsed)),
        }
    }
}

/// Call `step` with the elapsed time until it yields `Some`, sleeping
/// `interval` between calls, for at most `timeout`. The first call is
/// immediate; a step error ends the wait. The budget includes the time
/// of the calls: a call that runs past `timeout` is cancelled.
///
/// # Errors
///
/// Returns the step's error.
pub async fn until<T, F, Fut>(budget: Budget, step: F) -> anyhow::Result<Outcome<T>>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    Wait::new(budget, Duration::ZERO, step).run(false).await
}

/// Sample a condition that must hold for the whole budget. A step yields
/// `Some` with the evidence of a break. The last step can start inside
/// the budget and end after it: its answer counts, so a clean hold stays
/// clean. A step that does not end within [`PROBE_GRACE`] of the end of
/// the budget cannot certify that the condition holds.
///
/// # Errors
///
/// Returns the step's error, or an error when a probe does not end
/// within the grace.
pub async fn first_break<T, F, Fut>(
    budget: Budget,
    step: F,
) -> anyhow::Result<Option<(T, Duration)>>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let mut wait = Wait::new(budget, PROBE_GRACE, step);
    match wait.run(false).await? {
        Outcome::Ready { value, elapsed } => Ok(Some((value, elapsed))),
        Outcome::TimedOut { .. } => {
            anyhow::ensure!(
                !wait.probing,
                "condition probe does not finish within {}s of the end of its {}s hold",
                PROBE_GRACE.as_secs(),
                budget.timeout.as_secs_f64()
            );
            Ok(None)
        }
    }
}

/// Sleep before the first observation. The initial sleep, probes, and
/// later sleeps all consume the same budget.
///
/// # Errors
///
/// Returns the step's error.
pub async fn after_sleep<T, F, Fut>(budget: Budget, step: F) -> anyhow::Result<Outcome<T>>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    Wait::new(budget, Duration::ZERO, step).run(true).await
}

struct Wait<F> {
    budget: Budget,
    /// How long a probe that started inside the budget may run past it.
    grace: Duration,
    started: Instant,
    probing: bool,
    step: F,
}

impl<F> Wait<F> {
    fn new(budget: Budget, grace: Duration, step: F) -> Self {
        Self {
            budget,
            grace,
            started: Instant::now(),
            probing: false,
            step,
        }
    }

    async fn run<T, Fut>(&mut self, delay: bool) -> anyhow::Result<Outcome<T>>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        let bound = self.budget.timeout.saturating_add(self.grace);
        let result = tokio::time::timeout(bound, self.drive(delay)).await;
        let elapsed = self.started.elapsed();
        Ok(match result {
            Ok(Ok(Some(value))) => Outcome::Ready { value, elapsed },
            Ok(Err(error)) => return Err(error),
            Ok(Ok(None)) | Err(_) => Outcome::TimedOut { elapsed },
        })
    }

    async fn drive<T, Fut>(&mut self, delay: bool) -> anyhow::Result<Option<T>>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        if delay {
            self.sleep().await;
        }
        loop {
            match self.tick().await? {
                ControlFlow::Break(value) => return Ok(value),
                ControlFlow::Continue(()) => (),
            }
        }
    }

    async fn tick<T, Fut>(&mut self) -> anyhow::Result<ControlFlow<Option<T>>>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        let elapsed = self.started.elapsed();
        if elapsed >= self.budget.timeout {
            return Ok(ControlFlow::Break(None));
        }
        self.probing = true;
        let value = (self.step)(elapsed).await?;
        self.probing = false;
        if value.is_some() || self.started.elapsed() >= self.budget.timeout {
            return Ok(ControlFlow::Break(value));
        }
        self.sleep().await;
        Ok(ControlFlow::Continue(()))
    }

    async fn sleep(&self) {
        let remaining = self.budget.timeout.saturating_sub(self.started.elapsed());
        tokio::time::sleep(self.budget.interval.min(remaining)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_budget_includes_the_probe_and_reports_real_elapsed_time() {
        let started = tokio::time::Instant::now();
        let outcome: Outcome<()> = until(Budget::secs(5, 1), |_| async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(None)
        })
        .await
        .unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert!(
            matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == Duration::from_secs(5))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_never_finishes_cannot_extend_the_deadline() {
        let outcome = tokio::time::timeout(
            Duration::from_secs(6),
            until(Budget::secs(5, 1), |_| async {
                std::future::pending::<()>().await;
                Ok::<Option<()>, anyhow::Error>(None)
            }),
        )
        .await
        .expect("the five-second poll must finish before six seconds")
        .unwrap();
        assert!(
            matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == Duration::from_secs(5))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_hold_whose_last_probe_straddles_the_deadline_holds() {
        let mut calls = 0;
        let held: Option<((), Duration)> = first_break(Budget::secs(20, 2), |_| {
            calls += 1;
            let slow = calls == 4;
            async move {
                if slow {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }
                Ok(None)
            }
        })
        .await
        .unwrap();
        assert_eq!(held, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_success_reports_probe_time() {
        let outcome = until(Budget::secs(5, 1), |_| async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(Some("ready"))
        })
        .await
        .unwrap();
        let (value, elapsed) = outcome.or_fail(|_| anyhow::anyhow!("timeout")).unwrap();
        assert_eq!((value, elapsed), ("ready", Duration::from_secs(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hanging_hold_probe_cannot_certify_a_clean_window() {
        let result = first_break(Budget::secs(5, 1), |_| async {
            std::future::pending::<()>().await;
            Ok::<Option<()>, anyhow::Error>(None)
        })
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("probe does not finish")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_initial_sleep_consumes_the_budget() {
        let started = Instant::now();
        let mut calls = 0;
        let outcome: Outcome<()> = after_sleep(Budget::secs(5, 8), |_| {
            calls += 1;
            async { Ok(None) }
        })
        .await
        .unwrap();
        assert_eq!(calls, 0);
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert!(matches!(outcome, Outcome::TimedOut { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn yields_when_the_step_does_and_times_out_otherwise() {
        let budget = Budget::secs(10, 2);
        let mut calls = 0;
        let outcome = until(budget, |_| {
            calls += 1;
            let ready = calls == 3;
            async move { Ok(ready.then_some("done")) }
        })
        .await
        .unwrap();
        let (value, elapsed) = outcome.or_fail(|_| anyhow::anyhow!("x")).unwrap();
        assert_eq!(value, "done");
        assert_eq!(elapsed, Duration::from_secs(4));

        let outcome: Outcome<()> = until(budget, |_| async move { Ok(None) }).await.unwrap();
        assert!(
            matches!(outcome, Outcome::TimedOut { elapsed } if elapsed == Duration::from_secs(10))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_hold_reports_its_first_break_or_none() {
        let budget = Budget::secs(10, 2);
        let mut calls = 0;
        let broken = first_break(budget, |_| {
            calls += 1;
            let value = (calls >= 2).then_some(calls);
            async move { Ok(value) }
        })
        .await
        .unwrap();
        assert_eq!(broken, Some((2, Duration::from_secs(2))));

        let held: Option<((), Duration)> = first_break(budget, |_| async move { Ok(None) })
            .await
            .unwrap();
        assert_eq!(held, None);
    }
}
