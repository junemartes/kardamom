//! The watcher's start inputs and its handle: the resume block an
//! operator gives, and the handle of a spawned watcher task.

use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The last L1 block whose epoch the chain already holds: the L1 origin
/// of the head that a sealer cluster starts from. The watcher then
/// publishes every finalized block after it, so no epoch between that
/// origin and the tip is lost. The value is never 0: a head at origin 0
/// holds no epoch, and its watcher starts at the finalized tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1ResumeAfter(NonZeroU64);

impl L1ResumeAfter {
    /// The L1 block number.
    #[must_use]
    pub fn block(self) -> u64 {
        self.0.get()
    }
}

/// Why a resume block does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResumeAfterError {
    /// The text is not a `u64`.
    #[error("not an L1 block number: {0}")]
    NotANumber(#[from] ParseIntError),
    /// Block 0: the chain holds no epoch to resume after.
    #[error(
        "L1 block 0 holds no epoch; omit the resume block, and the watcher starts at the finalized tip"
    )]
    Zero,
}

impl From<NonZeroU64> for L1ResumeAfter {
    fn from(block: NonZeroU64) -> Self {
        Self(block)
    }
}

impl FromStr for L1ResumeAfter {
    type Err = ResumeAfterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NonZeroU64::new(s.parse()?)
            .map(Self)
            .ok_or(ResumeAfterError::Zero)
    }
}

/// Handle to a running watcher task. [`WatcherHandle::join`] asks the
/// loop to exit and waits for it. Dropping the handle also asks the loop
/// to exit, without the wait. `task` is the underlying tokio `JoinHandle`.
pub struct WatcherHandle {
    /// Underlying tokio task. Tests read `is_finished` on it, and hold
    /// `shutdown` while they await it, to see a fail-stop.
    pub task: JoinHandle<()>,
    /// Cooperative shutdown signal. Dropping this asks the watcher loop
    /// to exit at the next tick boundary.
    pub shutdown: oneshot::Sender<()>,
}

impl WatcherHandle {
    /// Ask the loop to exit, then wait for the task. Ending the sender is
    /// the request; the block scope ends it before the await.
    ///
    /// # Errors
    ///
    /// Returns the join error when the watcher task panicked.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        let Self { task, shutdown } = self;
        {
            let _request = shutdown;
        }
        task.await
    }
}
