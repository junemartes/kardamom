//! The one polling loop of the suite. Every wait is a step function
//! called until it yields a value or the budget runs out, so no
//! assertion carries its own loop.

use std::future::Future;
use std::time::Duration;

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
/// immediate; a step error ends the wait.
///
/// # Errors
///
/// Returns the step's error.
pub async fn until<T, F, Fut>(budget: Budget, mut step: F) -> anyhow::Result<Outcome<T>>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let mut elapsed = Duration::ZERO;
    loop {
        match step(elapsed).await? {
            Some(value) => return Ok(Outcome::Ready { value, elapsed }),
            None if elapsed >= budget.timeout => return Ok(Outcome::TimedOut { elapsed }),
            None => elapsed = sleep_step(budget.interval, elapsed).await,
        }
    }
}

/// Wait `timeout` after the first sleep, so a wait that must sleep
/// before its first observation (a freeze, a window) starts late.
///
/// # Errors
///
/// Returns the step's error.
pub async fn after_sleep<T, F, Fut>(budget: Budget, step: F) -> anyhow::Result<Outcome<T>>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    tokio::time::sleep(budget.interval).await;
    until(budget, step).await
}

async fn sleep_step(interval: Duration, elapsed: Duration) -> Duration {
    tokio::time::sleep(interval).await;
    elapsed.saturating_add(interval)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
