//! Only a completed observation certifies the backlog fixture's state.

use std::cell::RefCell;
use std::future::Future;

use crate::poll::{self, Budget};

pub(super) struct Observe {
    evidence: &'static str,
    budget: Budget,
}

#[cfg(test)]
mod tests;

impl Observe {
    pub(super) fn new(evidence: &'static str, budget: Budget) -> Self {
        Self { evidence, budget }
    }

    pub(super) async fn wait<T, F, Fut>(&self, mut read: F) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        let last = RefCell::new(String::from("no completed observation"));
        let last_ref = &last;
        let outcome = poll::until(self.budget, |_| {
            let response = read();
            async move {
                Ok(match response.await {
                    Ok(value) => Some(value),
                    Err(error) => {
                        *last_ref.borrow_mut() = format!("{error:#}");
                        None
                    }
                })
            }
        })
        .await?;
        let (value, _) = outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "{}: {} unavailable after {}s: {}",
                super::CTX,
                self.evidence,
                elapsed.as_secs(),
                last.borrow()
            )
        })?;
        Ok(value)
    }
}
