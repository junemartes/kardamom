//! The batcher's lifecycle on the `events` stream, and its post age from
//! the L1 follower's `l1_blocks` stream. Each start of the service opens
//! and closes its own stream plane, and a held halt runs between two
//! starts, so these have a runtime and a plane of their own for the whole
//! process: the chain status shows the batcher halted, not gone, while it
//! holds, and the post age grows through a halt.

use anyhow::{Context, Result};
use std::time::Duration;

use kardamom_log::aeron_live::{
    AeronRuntime, L1BlocksSubscriberHandle, ServiceEventsPublisherHandle,
    ServiceEventsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;

use super::post_age::PostAge;
use super::run::LiveArgs;
use crate::indexer::IndexerClient;

/// How often the post age is exported and the follower checked.
const POST_AGE_EVERY: Duration = Duration::from_secs(10);

/// The beacon's transport. Field order is drop order: the plane ends in
/// [`Self::close`], then the runtime drops.
pub(crate) struct EventsBeacon {
    plane: StreamPlane,
    _rt: AeronRuntime,
}

impl EventsBeacon {
    /// Open the transport, publish the process's lifecycle, and watch the
    /// post age on `l1_blocks`.
    ///
    /// # Errors
    ///
    /// Returns an error when the log config, the runtime, or the
    /// publication fails.
    pub(crate) async fn open(args: &LiveArgs) -> Result<Self> {
        let log_cfg =
            LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
        let mut plane = StreamPlane::from_config(&log_cfg, "batcher-events")
            .context("build the events stream plane")?;
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn events runtime")?;
        plane
            .publisher::<ServiceEventsPublisherHandle>(&rt)
            .await
            .context("open events")?
            .spawn_process_beacon();
        let blocks = plane
            .subscriber::<L1BlocksSubscriberHandle>(&rt)
            .context("open the l1_blocks subscription")?;
        let board = plane
            .subscriber::<ServiceEventsSubscriberHandle>(&rt)
            .context("open the events subscription")?
            .spawn_board();
        let indexer = args.indexer_url.as_deref().map(IndexerClient::new);
        tokio::spawn(PostAge::new(blocks, board, indexer, args.l1_silence, POST_AGE_EVERY).run());
        Ok(Self { plane, _rt: rt })
    }

    /// End the plane's registrations before the runtime drops.
    pub(crate) async fn close(self) {
        self.plane.shutdown().await;
    }
}
