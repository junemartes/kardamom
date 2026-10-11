//! The start handshake of the Aeron thread. The thread builds the
//! `AeronContext`, reports the start budget that the context's driver
//! timeout gives, then connects the client and reports the result.
//! [`AeronRuntime::spawn_with`](super::AeronRuntime::spawn_with) waits for
//! each report in turn.

use std::rc::Rc;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender as CbSender};
use rusteron_client::AeronContext;

use super::{AeronClient, build_aeron};
use crate::driver_budget::DriverBudget;
use crate::error::LogError;

/// The longest wait for the Aeron thread to build its `AeronContext`. The
/// build reads the environment and sets the directory. It does not touch
/// the media driver.
const CONTEXT_BUDGET: Duration = Duration::from_secs(10);

/// The Aeron thread's side of the handshake.
pub(super) struct StartReport {
    budget: CbSender<Result<DriverBudget, LogError>>,
    started: CbSender<Result<(), LogError>>,
}

impl StartReport {
    /// Build the context with `make_ctx`, report the budget, then build
    /// and start the client and report the result. Returns the client and
    /// its budget, or `None` after a failure report.
    pub(super) fn start<F>(&self, make_ctx: F) -> Option<(Rc<AeronClient>, DriverBudget)>
    where
        F: FnOnce() -> Result<AeronContext, LogError>,
    {
        let built = make_ctx().and_then(|ctx| Ok((DriverBudget::of_client(&ctx)?, ctx)));
        let (budget, ctx) = Self::step(&self.budget, built, |(budget, _)| *budget)?;
        let aeron = Self::step(&self.started, build_aeron(&ctx), |_| ())?;
        Some((aeron, budget))
    }

    /// Send the outcome of one start step on `tx`: the error, or the
    /// report that `report` makes from the value. Returns the value.
    /// The waiter can be gone after its own timeout, so a failed send is
    /// not an error.
    fn step<T, R>(
        tx: &CbSender<Result<R, LogError>>,
        outcome: Result<T, LogError>,
        report: impl FnOnce(&T) -> R,
    ) -> Option<T> {
        match outcome {
            Ok(value) => {
                let _ = tx.send(Ok(report(&value)));
                Some(value)
            }
            Err(e) => {
                let _ = tx.send(Err(e));
                None
            }
        }
    }
}

/// The caller's side of the handshake.
pub(super) struct StartWait {
    budget: Receiver<Result<DriverBudget, LogError>>,
    started: Receiver<Result<(), LogError>>,
}

impl StartWait {
    /// A connected pair. Each channel carries one report, so a send never
    /// blocks the Aeron thread.
    pub(super) fn channel() -> (StartReport, Self) {
        let (budget_tx, budget_rx) = crossbeam_channel::bounded(1);
        let (started_tx, started_rx) = crossbeam_channel::bounded(1);
        let report = StartReport {
            budget: budget_tx,
            started: started_tx,
        };
        let wait = Self {
            budget: budget_rx,
            started: started_rx,
        };
        (report, wait)
    }

    /// Wait [`CONTEXT_BUDGET`] for the context, then the [`DriverBudget`]
    /// of that context for the client start. The Aeron C client waits up
    /// to its driver timeout for a live driver: for the `CnC` file, and
    /// for a heartbeat younger than the timeout after a driver restart.
    /// Returns the budget, which the runtime keeps for its users.
    pub(super) fn wait(&self) -> Result<DriverBudget, LogError> {
        let budget = Self::recv(&self.budget, CONTEXT_BUDGET, "build its context")?;
        Self::recv(&self.started, budget.duration(), "signal start")?;
        Ok(budget)
    }

    /// One report, or an error that names the step and the wait.
    fn recv<T>(
        rx: &Receiver<Result<T, LogError>>,
        within: Duration,
        step: &str,
    ) -> Result<T, LogError> {
        rx.recv_timeout(within).map_err(|e| match e {
            RecvTimeoutError::Timeout => {
                LogError::Aeron(format!("aeron thread did not {step} within {within:?}"))
            }
            RecvTimeoutError::Disconnected => {
                LogError::Aeron(format!("aeron thread ended before it could {step}"))
            }
        })?
    }
}

#[cfg(test)]
mod tests;
