//! A complete observation of the current primary proves exact account coverage.

use std::cell::RefCell;
use std::future::Future;

use crate::poll::{self, Budget};
use crate::rpc::AccountState;

pub(super) struct Projection<'a> {
    expected: &'a [AccountState],
    last: RefCell<String>,
}

impl<'a> Projection<'a> {
    pub(super) fn new(expected: &'a [AccountState]) -> Self {
        Self {
            expected,
            last: RefCell::new("no completed observation".into()),
        }
    }

    pub(super) async fn wait<F, Fut>(
        &self,
        ctx: &str,
        budget: Budget,
        mut read: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<String>>>,
    {
        let outcome = poll::until(budget, |_| {
            let response = read();
            async move { Ok(self.observe(response.await)) }
        })
        .await?;
        outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "{ctx}: exact account coverage did not return after {}s: {}",
                elapsed.as_secs(),
                self.last.borrow()
            )
        })?;
        Ok(())
    }

    fn observe(&self, response: anyhow::Result<Vec<String>>) -> Option<()> {
        let rows = match response {
            Ok(rows) => rows,
            Err(error) => {
                *self.last.borrow_mut() = format!("unread ({error:#})");
                return None;
            }
        };
        let complete = rows.len() == self.expected.len()
            && self
                .expected
                .iter()
                .zip(&rows)
                .all(|(account, row)| account.matches_projection(row));
        *self.last.borrow_mut() = format!("expected {:?}, observed {rows:?}", self.expected);
        complete.then_some(())
    }
}

#[cfg(test)]
mod tests;
