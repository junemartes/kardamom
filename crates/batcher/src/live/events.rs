//! The batcher's lifecycle on the `events` stream. Each start of the
//! service opens and closes its own stream plane, and a held halt runs
//! between two starts, so the beacon has a runtime and a plane of its
//! own for the whole process: the chain status shows the batcher halted,
//! not gone, while it holds.

use anyhow::{Context, Result};
use kardamom_log::aeron_live::{AeronRuntime, ServiceEventsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;

use super::run::LiveArgs;

/// The beacon's transport. Field order is drop order: the plane ends in
/// [`Self::close`], then the runtime drops.
pub(crate) struct EventsBeacon {
    plane: StreamPlane,
    _rt: AeronRuntime,
}

impl EventsBeacon {
    /// Open the transport and publish the process's lifecycle.
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
        Ok(Self { plane, _rt: rt })
    }

    /// End the plane's registrations before the runtime drops.
    pub(crate) async fn close(self) {
        self.plane.shutdown().await;
    }
}
