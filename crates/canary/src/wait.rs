//! A bounded poll: ask until an answer comes or a deadline passes.

use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;

use tokio::time::Instant;

/// A poll of one check, every `every`, until `deadline`. A poll with no
/// deadline asks until an answer comes.
#[derive(Debug, Clone, Copy)]
pub struct Poll {
    pub deadline: Option<Instant>,
    pub every: Duration,
}

impl Poll {
    /// A poll that ends `within` from now, or never when the clock cannot
    /// hold that time.
    #[must_use]
    pub fn within(within: Duration, every: Duration) -> Self {
        Self {
            deadline: Instant::now().checked_add(within),
            every,
        }
    }

    /// The first answer of `check`, or `None` once the deadline passes.
    /// The check runs at least once.
    pub async fn until<T, F, Fut>(self, mut check: F) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        loop {
            if let ControlFlow::Break(answer) = self.step(&mut check).await {
                return answer;
            }
        }
    }

    async fn step<T, F, Fut>(self, check: &mut F) -> ControlFlow<Option<T>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        if let Some(answer) = check().await {
            return ControlFlow::Break(Some(answer));
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return ControlFlow::Break(None);
        }
        tokio::time::sleep(self.every).await;
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_poll_returns_the_first_answer_or_none() {
        let mut calls = 0;
        let found = Poll::within(Duration::from_secs(1), Duration::from_millis(10))
            .until(|| {
                calls += 1;
                let answer = (calls == 3).then_some(calls);
                async move { answer }
            })
            .await;
        assert_eq!(found, Some(3));
        let none: Option<()> = Poll::within(Duration::from_millis(50), Duration::from_millis(10))
            .until(|| async { None })
            .await;
        assert_eq!(none, None);
    }
}
