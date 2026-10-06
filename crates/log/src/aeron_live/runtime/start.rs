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
use crate::error::LogError;

/// The longest wait for the Aeron thread to build its `AeronContext`. The
/// build reads the environment and sets the directory. It does not touch
/// the media driver.
const CONTEXT_BUDGET: Duration = Duration::from_secs(10);

/// How long `spawn_with` waits for the client to connect to the media
/// driver and start. The Aeron C client waits up to its driver timeout for
/// a live driver: for the `CnC` file, and for a heartbeat younger than the
/// timeout after a driver restart. So the budget is the driver timeout of
/// the context plus [`Self::MARGIN`], and at least [`Self::FLOOR`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StartBudget(Duration);

impl StartBudget {
    /// The time past the driver timeout for the conductor start.
    const MARGIN: Duration = Duration::from_secs(5);

    /// The least budget, for a short driver timeout on a slow host.
    const FLOOR: Duration = Duration::from_secs(10);

    /// The budget for a client with a driver timeout of `ms`.
    fn from_driver_timeout_ms(ms: u64) -> Result<Self, LogError> {
        Duration::from_millis(ms)
            .checked_add(Self::MARGIN)
            .map(|budget| Self(budget.max(Self::FLOOR)))
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "driver timeout {ms} ms overflows the aeron start budget"
                ))
            })
    }

    /// The budget for the driver timeout that `ctx` uses: the
    /// `AERON_DRIVER_TIMEOUT` value, or a value that `make_ctx` sets.
    fn of(ctx: &AeronContext) -> Result<Self, LogError> {
        Self::from_driver_timeout_ms(ctx.get_driver_timeout_ms())
    }
}

/// The Aeron thread's side of the handshake.
pub(super) struct StartReport {
    budget: CbSender<Result<StartBudget, LogError>>,
    started: CbSender<Result<(), LogError>>,
}

impl StartReport {
    /// Build the context with `make_ctx`, report the budget, then build
    /// and start the client and report the result. Returns the client, or
    /// `None` after a failure report.
    pub(super) fn start<F>(&self, make_ctx: F) -> Option<Rc<AeronClient>>
    where
        F: FnOnce() -> Result<AeronContext, LogError>,
    {
        let built = make_ctx().and_then(|ctx| Ok((StartBudget::of(&ctx)?, ctx)));
        let (_, ctx) = Self::step(&self.budget, built, |(budget, _)| *budget)?;
        Self::step(&self.started, build_aeron(&ctx), |_| ())
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
    budget: Receiver<Result<StartBudget, LogError>>,
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

    /// Wait [`CONTEXT_BUDGET`] for the context, then the start budget of
    /// that context for the client start.
    pub(super) fn wait(&self) -> Result<(), LogError> {
        let budget = Self::recv(&self.budget, CONTEXT_BUDGET, "build its context")?;
        Self::recv(&self.started, budget.0, "signal start")
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
