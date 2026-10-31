//! Elapsed time between consecutive active observations certifies an alert hold.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

use crate::poll::{self, Budget, Outcome};

pub(super) struct Held {
    hold: Duration,
    since: Cell<Option<Instant>>,
    longest: Cell<Duration>,
    last: RefCell<String>,
}

impl Held {
    pub(super) fn new(hold: Duration) -> Self {
        Self {
            hold,
            since: Cell::new(None),
            longest: Cell::new(Duration::ZERO),
            last: RefCell::new("no completed observation".into()),
        }
    }

    pub(super) async fn run<F, Fut>(
        &self,
        budget: Duration,
        mut read: F,
    ) -> anyhow::Result<Outcome<String>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<String>>>,
    {
        poll::until(Budget::new(budget, Duration::from_secs(5)), |_| {
            let response = read();
            async move { Ok(self.observe(response.await)) }
        })
        .await
    }

    fn observe(&self, response: anyhow::Result<Option<String>>) -> Option<String> {
        let state = match response {
            Ok(state) => state,
            Err(error) => {
                self.since.set(None);
                *self.last.borrow_mut() = format!("unread ({error:#})");
                return None;
            }
        };
        *self.last.borrow_mut() = state.clone().unwrap_or_else(|| "inactive".into());
        let Some(state) = state.filter(|state| matches!(state.as_str(), "pending" | "firing"))
        else {
            self.since.set(None);
            return None;
        };
        let now = Instant::now();
        let since = self.since.get().unwrap_or(now);
        self.since.set(Some(since));
        let elapsed = now.duration_since(since);
        self.longest.set(self.longest.get().max(elapsed));
        (elapsed >= self.hold).then_some(state)
    }

    pub(super) fn evidence(&self) -> String {
        format!(
            "last observation: {}; longest observed active span: {:.3}s",
            self.last.borrow(),
            self.longest.get().as_secs_f64()
        )
    }
}

#[cfg(test)]
mod tests;
