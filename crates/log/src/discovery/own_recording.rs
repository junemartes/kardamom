//! The archive recording a producer waits for before it publishes: the
//! discovery-driven recorder of one topic on the local archive, and the
//! barrier on this process's own publication of it.
//!
//! A producer whose stream a consumer replays from the archive (the
//! da-watcher's `tx_deposits`, the L1 follower's `l1_blocks`) must not
//! publish a record before the archive records its publication. A record
//! published earlier is on no archive, and a replay that needs it fails
//! for good.

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::oneshot;

use super::plane::StreamPlane;
use super::record::Topic;
use super::recording::{DiscoveredRecorder, RecorderProgress};
use crate::config::AeronConfig;
use crate::error::LogError;
use crate::recorder::RecorderThreads;

/// How long a start waits for its own recording. One catalog poll is
/// about 500 ms; the budget only bounds a stuck or unreachable archive.
const READY_BUDGET: Duration = Duration::from_mins(1);

/// What the recorder of one topic needs from the process.
pub struct OwnRecording {
    pub topic: Topic,
    /// The media driver directory the archive session connects through.
    pub aeron_dir: Option<PathBuf>,
    pub aeron_cfg: AeronConfig,
}

impl StreamPlane {
    /// Start the recorder thread of `rec.topic`, and return once this
    /// process's own publication of it records. The thread records every
    /// publisher of the topic, so each archive that runs it is a full
    /// mirror. Dropping the returned value stops the thread. `None` on a
    /// static plane: it has no catalog to follow.
    ///
    /// # Errors
    ///
    /// Returns an error when the thread cannot be spawned, when the
    /// recorder fails, or when the own recording is not live within one
    /// minute.
    pub async fn record_own(
        &mut self,
        rec: OwnRecording,
    ) -> Result<Option<RecorderThreads>, LogError> {
        let (Some(membership), Some(local_ip), Some(own_instance)) = (
            self.watch_topic(rec.topic),
            self.local_ip(),
            self.instance_id().map(str::to_string),
        ) else {
            return Ok(None);
        };
        let mut recorders = RecorderThreads::new();
        let recorder = DiscoveredRecorder {
            aeron_dir: rec.aeron_dir,
            aeron_cfg: rec.aeron_cfg,
            local_ip,
            own_instance,
            expected_own: 1,
            removal_grace: self.removal_grace(),
            membership,
            stop: recorders.stop_token(),
            runtime: tokio::runtime::Handle::current(),
        };
        let topic = rec.topic;
        let (ready_tx, ready_rx) = oneshot::channel::<RecorderProgress>();
        recorders.spawn(format!("{topic}-recorder"), move |_stop| {
            Self::run_own_recorder(recorder, topic, ready_tx);
        })?;
        Self::await_own(topic, ready_rx).await?;
        Ok(Some(recorders))
    }

    /// Run the recorder on its thread, and report its progress on
    /// `ready_tx`. The recorder holds the threads' stop token.
    fn run_own_recorder(
        recorder: DiscoveredRecorder,
        topic: Topic,
        ready_tx: oneshot::Sender<RecorderProgress>,
    ) {
        let outcome = recorder.run(|progress| {
            let _ = ready_tx.send(progress);
        });
        if let Err(e) = outcome {
            tracing::error!(error = %e, "{topic} recorder exited with error");
        }
    }

    /// Wait for the own recording of `topic` to go live.
    async fn await_own(
        topic: Topic,
        ready_rx: oneshot::Receiver<RecorderProgress>,
    ) -> Result<(), LogError> {
        match tokio::time::timeout(READY_BUDGET, ready_rx).await {
            Ok(Ok(RecorderProgress::Ready { .. })) => {
                tracing::info!("{topic} recording confirmed active");
                Ok(())
            }
            Ok(Ok(RecorderProgress::Failed(reason))) => Err(LogError::Aeron(format!(
                "archive durability requested but the {topic} recorder failed to start: {reason}"
            ))),
            Ok(Err(_)) => Err(LogError::Aeron(format!(
                "archive durability requested but the {topic} recorder thread exited before \
                 reporting readiness"
            ))),
            Err(_) => Err(LogError::Aeron(format!(
                "archive durability requested but the {topic} recording did not become active \
                 within 60s"
            ))),
        }
    }
}
