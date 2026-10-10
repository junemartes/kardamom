//! The wait of the Aeron thread for one add of a publication or a
//! subscription.
//!
//! The thread starts the add and polls the driver for its answer. An add
//! at run time waits the add timeout. An add at start-up waits the
//! start-up limit ([`AeronRuntime::start_open_limit`]) in one go, and a
//! stop token ends that wait at once. A wait that ends with no answer
//! cancels the add, so the driver keeps no late publication or
//! subscription. A start-up add never tries again: a second add during a
//! stall can only queue behind the first.
//!
//! [`AeronRuntime::start_open_limit`]: super::AeronRuntime::start_open_limit

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use rusteron_client::{AeronCError, AeronErrorType};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{ACK_TIMEOUT, AeronClient, Pub, Sub};
use crate::error::LogError;

/// The pause between two polls of an add in flight.
const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// How long the Aeron thread waits for one add, and the token that ends
/// the wait early.
#[derive(Clone, Debug)]
pub struct AddWait {
    within: Duration,
    stop: Option<CancellationToken>,
}

impl AddWait {
    /// The wait of an add at run time: `within`, with no stop.
    pub(super) fn run_time(within: Duration) -> Self {
        Self { within, stop: None }
    }

    /// The wait of a start-up add: `within`, ended early by a cancel of
    /// `stop`.
    #[must_use]
    pub fn start_up(within: Duration, stop: CancellationToken) -> Self {
        Self {
            within,
            stop: Some(stop),
        }
    }

    /// How long the add waits for the driver.
    #[must_use]
    pub fn within(&self) -> Duration {
        self.within
    }

    /// How long the caller waits for the reply of the Aeron thread: the
    /// add wait plus the round trip budget of any command.
    pub(super) fn reply_wait(&self) -> Duration {
        // A wait past `Duration::MAX` has no end, which is the meaning of
        // so long an add wait.
        self.within.saturating_add(ACK_TIMEOUT)
    }

    /// Poll `add` until the driver answers, the wait ends, or the stop
    /// token is cancelled. `op` and `uri` name the add in the error. A
    /// wait that ends with no answer cancels the add.
    pub(super) fn complete<A: AsyncAdd>(
        &self,
        add: &A,
        op: &str,
        uri: &str,
    ) -> Result<A::Out, LogError> {
        let start = Instant::now();
        let end = loop {
            if let ControlFlow::Break(end) = self.poll_once(add, start) {
                break end;
            }
        };
        let detail = match end {
            AddEnd::Added(out) => return Ok(out),
            AddEnd::Failed(e) => return Err(LogError::Aeron(format!("{op} {uri}: {e}"))),
            AddEnd::TimedOut => AeronCError::from(AeronErrorType::TimedOut).to_string(),
            AddEnd::Stopped => "stopped during start-up open".to_string(),
        };
        if let Err(e) = add.cancel() {
            warn!(op, uri, error = %e, "aeron: the cancel of an unanswered add failed");
        }
        Err(LogError::Aeron(format!("{op} {uri}: {detail}")))
    }

    /// One poll of `add`, for [`Self::complete`]'s loop. `Continue` means
    /// the caller polls again, after this waits [`POLL_INTERVAL`].
    fn poll_once<A: AsyncAdd>(&self, add: &A, start: Instant) -> ControlFlow<AddEnd<A::Out>> {
        match add.poll() {
            Ok(Some(out)) => return ControlFlow::Break(AddEnd::Added(out)),
            Ok(None) => {}
            Err(e) => return ControlFlow::Break(AddEnd::Failed(e)),
        }
        if self
            .stop
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return ControlFlow::Break(AddEnd::Stopped);
        }
        if start.elapsed() >= self.within {
            return ControlFlow::Break(AddEnd::TimedOut);
        }
        std::thread::sleep(POLL_INTERVAL);
        ControlFlow::Continue(())
    }
}

/// How the wait for one add ended.
enum AddEnd<T> {
    Added(T),
    Failed(AeronCError),
    TimedOut,
    Stopped,
}

/// One add in flight: the poll for its answer and its cancel.
pub(super) trait AsyncAdd {
    type Out;
    fn poll(&self) -> Result<Option<Self::Out>, AeronCError>;
    fn cancel(&self) -> Result<(), AeronCError>;
}

/// An add of a publication, with the client that cancels it.
pub(super) struct PubAdd<'a> {
    pub(super) aeron: &'a AeronClient,
    pub(super) poller: rusteron_client::AeronAsyncAddPublication,
}

impl AsyncAdd for PubAdd<'_> {
    type Out = Pub;

    fn poll(&self) -> Result<Option<Pub>, AeronCError> {
        self.poller.poll()
    }

    fn cancel(&self) -> Result<(), AeronCError> {
        self.aeron
            .async_add_publication_cancel(&self.poller)
            .map(|_| ())
    }
}

/// An add of a subscription, with the client that cancels it.
pub(super) struct SubAdd<'a> {
    pub(super) aeron: &'a AeronClient,
    pub(super) poller: rusteron_client::AeronAsyncAddSubscription,
}

impl AsyncAdd for SubAdd<'_> {
    type Out = Sub;

    fn poll(&self) -> Result<Option<Sub>, AeronCError> {
        self.poller.poll()
    }

    fn cancel(&self) -> Result<(), AeronCError> {
        self.aeron
            .async_add_subscription_cancel(&self.poller)
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests;
