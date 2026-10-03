//! The batcher's confirmed cursor on the cluster ingress: the last L2
//! block posted to L1, published as a system record at start and after
//! every confirmed post. The sealer adopts it as the floor of its DA-lag
//! guard and of its egress retention; the ingress serves `safe` from it.

use anyhow::{Context, Result};
use kardamom_engine::bin_support::LiveCursorPublisher;
use kardamom_engine::reader::cluster::OfferOutcome;
use tracing::warn;

/// The publisher the feed loop holds. The offer blocks on the session
/// thread's reply, so it runs off the async feed task.
#[derive(Clone)]
pub(crate) struct PostedCursor {
    inner: LiveCursorPublisher,
}

impl PostedCursor {
    pub(crate) fn new(inner: LiveCursorPublisher) -> Self {
        Self { inner }
    }

    /// Publish `posted_head`. A refused offer is logged, not fatal: the
    /// next confirmed post publishes again, and the sealer sends its
    /// status to a session that announces itself.
    ///
    /// # Errors
    ///
    /// Returns an error when the blocking task panics.
    pub(crate) async fn publish(&self, posted_head: u64) -> Result<()> {
        let mut publisher = self.inner.clone();
        let outcome = tokio::task::spawn_blocking(move || publisher.publish(posted_head))
            .await
            .context("posted cursor publication task")?;
        match outcome {
            OfferOutcome::Accepted => {
                tracing::info!(posted_head, "posted cursor published to the cluster");
            }
            refused @ (OfferOutcome::BackPressured | OfferOutcome::NotConnected) => {
                warn!(
                    posted_head,
                    ?refused,
                    "posted cursor not published; the next post retries"
                );
            }
        }
        Ok(())
    }
}
