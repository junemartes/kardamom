//! A bounded head observation retains every completed exporter reading.

use std::future::Future;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

#[derive(Default)]
pub(super) struct Samples {
    reads: JoinSet<Option<i64>>,
}

impl Samples {
    pub(super) fn start(&mut self, read: impl Future<Output = Option<i64>> + Send + 'static) {
        self.reads.spawn(read);
    }

    pub(super) async fn maximum(mut self) -> Option<i64> {
        let deadline = Instant::now() + Duration::from_secs(12);
        let mut best = None;
        while let Ok(Some(read)) = timeout_at(deadline, self.reads.join_next()).await {
            best = best.max(read.ok().flatten());
        }
        best
    }
}

#[cfg(test)]
mod tests;
