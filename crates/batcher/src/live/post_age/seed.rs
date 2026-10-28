//! One bounded archive seed read, owned by the post-age actor.

use std::time::Duration;

use anyhow::Context;
use tokio::task::JoinHandle;

use crate::indexer::IndexerClient;

pub(super) struct ArchiveSeed {
    indexer: Option<IndexerClient>,
    pending: Option<JoinHandle<anyhow::Result<Option<u64>>>>,
}

impl ArchiveSeed {
    pub(super) fn new(indexer: Option<IndexerClient>) -> Self {
        Self {
            indexer,
            pending: None,
        }
    }

    /// Keep one attempt across live records; the complete RPC sequence shares the budget.
    pub(super) fn start(&mut self, budget: Duration) {
        if self.pending.is_some() {
            return;
        }
        let Some(indexer) = self.indexer.clone() else {
            return;
        };
        self.pending = Some(tokio::spawn(async move {
            tokio::time::timeout(budget, indexer.last_post_time())
                .await
                .context("post age archive seed exceeded its observation interval")?
                .map_err(anyhow::Error::from)
        }));
    }

    pub(super) async fn next(&mut self) -> anyhow::Result<Option<u64>> {
        let Some(task) = &mut self.pending else {
            return std::future::pending().await;
        };
        let result = task.await.context("post age archive task stopped");
        self.pending = None;
        result?
    }
}

impl Drop for ArchiveSeed {
    fn drop(&mut self) {
        if let Some(task) = &self.pending {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::post_age::component_tests::SilentArchive;

    #[tokio::test]
    async fn a_seed_attempt_times_out_and_can_start_again_without_overlapping() {
        let mut archive = SilentArchive::new().await;
        let mut seed = ArchiveSeed::new(Some(IndexerClient::new(&archive.url)));
        let budget = Duration::from_secs(10);
        seed.start(budget);
        let original = seed.pending.as_ref().unwrap().id();
        archive.wait_for_request().await;
        tokio::time::pause();
        seed.start(budget);
        assert_eq!(seed.pending.as_ref().unwrap().id(), original);
        let error = tokio::time::timeout(budget, seed.next())
            .await
            .expect("the seed must enforce its own complete-read deadline")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exceeded its observation interval")
        );
        assert!(seed.pending.is_none());
        seed.start(budget);
        assert_ne!(seed.pending.as_ref().unwrap().id(), original);
    }

    #[tokio::test]
    async fn closing_the_owner_cancels_the_inflight_archive_task() {
        let mut archive = SilentArchive::new().await;
        let task = {
            let mut seed = ArchiveSeed::new(Some(IndexerClient::new(&archive.url)));
            seed.start(Duration::from_secs(10));
            archive.wait_for_request().await;
            seed.pending.as_ref().unwrap().abort_handle()
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the seed task must stop when its actor ends");
    }
}
