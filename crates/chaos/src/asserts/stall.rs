//! A bounded head observation retains every exporter reading that
//! completes within a short settle after the first answer.

use std::future::Future;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

/// The longest wait for any answer: a direct read and its fallback.
const FIRST_ANSWER: Duration = Duration::from_secs(12);

/// How long the observation waits for the other readings after the first
/// answer. A live exporter answers within a second or two; a reading of
/// a killed node only times out, so waiting for it adds nothing.
const SETTLE: Duration = Duration::from_secs(3);

#[derive(Default)]
pub(super) struct Samples {
    reads: JoinSet<Option<i64>>,
}

impl Samples {
    pub(super) fn start(&mut self, read: impl Future<Output = Option<i64>> + Send + 'static) {
        self.reads.spawn(read);
    }

    pub(super) async fn maximum(mut self) -> Option<i64> {
        let mut deadline = Instant::now() + FIRST_ANSWER;
        let mut best = None;
        while let Ok(Some(read)) = timeout_at(deadline, self.reads.join_next()).await {
            deadline = Self::settle(deadline, best, read.as_ref().ok().copied().flatten());
            best = best.max(read.ok().flatten());
        }
        best
    }

    /// The deadline after a reading: the first answer moves it to the
    /// end of the settle, when that comes sooner.
    fn settle(deadline: Instant, best: Option<i64>, read: Option<i64>) -> Instant {
        match (best, read) {
            (None, Some(_)) => deadline.min(Instant::now() + SETTLE),
            _ => deadline,
        }
    }
}

#[cfg(test)]
mod tests;
