//! The production executor stream: the two Aeron publications, the local
//! recording, and the publisher thread.

use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::{Sender, bounded};
use kardamom_engine::ExecStreamItem;
use kardamom_log::aeron_live::{AeronRuntime, ExecTxsPublisherHandle, PubHandle};
use kardamom_log::config::AeronConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_log::error::LogError;
use kardamom_log::recorder::{
    PositionReport, RecordedStream, RecorderKind, RecorderThreads, record_stream_reporting,
};
use rkyv::util::AlignedVec;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::locators::LocatorLog;
use super::publisher::{ExecStreamPublisher, PublisherInputs, StreamPublications};

/// The recorded publication. An IPC publication cannot run ahead of its
/// slowest subscriber, the local archive, so the recording never loses a
/// frame. The publication is exclusive: every executor has its own
/// session, also when several executors share one media driver.
const RECORDED_CHANNEL: &str = "aeron:ipc?alias=exec-txs";

/// How often the recorder thread reads the recording position.
const POSITION_EVERY: Duration = Duration::from_millis(20);

/// The depth of the channel from the reader to the publisher. A full
/// channel blocks the reader.
const ITEMS_DEPTH: usize = 4096;

/// The time the executor waits for its recording before it gives up.
const READY_TIMEOUT: Duration = Duration::from_mins(1);

/// The production [`StreamPublications`]: the recorded IPC publication,
/// and the live publication when it is a separate one.
struct AeronPublications {
    recorded: PubHandle,
    live: Option<ExecTxsPublisherHandle>,
}

impl StreamPublications for AeronPublications {
    fn session_id(&self) -> i32 {
        self.recorded.session_id()
    }

    fn offer_recorded(&self, bytes: &AlignedVec) -> Result<i64, LogError> {
        let end = self.recorded.publish_bytes(bytes.clone())?;
        self.recorded.stream_position(end)
    }

    fn offer_live(&self, bytes: &AlignedVec) {
        if let Some(live) = &self.live {
            live.publish_lossy(bytes.clone());
        }
    }
}

/// What [`ExecStream::open`] needs.
pub struct ExecStreamConfig<'a> {
    /// The publication runtime of the executor.
    pub rt_pub: &'a AeronRuntime,
    pub plane: &'a mut StreamPlane,
    pub aeron_dir: Option<&'a Path>,
    pub aeron_cfg: &'a AeronConfig,
    /// The state directory. The locator log is
    /// `<state_dir>/exec_stream/locators.log`.
    pub state_dir: &'a Path,
    /// Cancelled at shutdown.
    pub stop: CancellationToken,
}

/// The open executor stream: the reader's sink, and the threads behind it.
pub struct ExecStream {
    /// The sink of the `tx_ordering` reader.
    pub sink: Sender<ExecStreamItem>,
    pub threads: ExecStreamThreads,
}

/// The publisher thread and the recorder thread of the executor stream.
pub struct ExecStreamThreads {
    publisher: JoinHandle<anyhow::Result<()>>,
    recorder: RecorderThreads,
}

impl ExecStreamThreads {
    /// Join the publisher, then stop and join the recorder. The publisher
    /// ends when the reader has dropped its sink, so call this after the
    /// engine has ended.
    pub fn join(self) {
        match self.publisher.join() {
            Ok(Ok(())) => tracing::info!("exec stream publisher ended"),
            Ok(Err(e)) => tracing::error!(error = %e, "exec stream publisher failed"),
            Err(_) => tracing::error!("exec stream publisher panicked"),
        }
        self.recorder.join();
    }
}

impl ExecStream {
    /// Open the recorded and the live publication on the publication
    /// runtime, start the local recording, wait until the recording is
    /// active, and spawn the publisher thread.
    ///
    /// # Errors
    ///
    /// Returns an error when a publication does not open, when the
    /// recording does not start within one minute, when the locator log
    /// cannot open, or when a thread cannot spawn.
    pub async fn open(cfg: ExecStreamConfig<'_>) -> Result<Self> {
        let stream_id = cfg.plane.channels().exec_txs_stream_id;
        let ipc = cfg
            .rt_pub
            .open_exclusive_publication(RECORDED_CHANNEL, stream_id)
            .context("open the recorded exec_txs publication")?;
        let live = Self::open_live(cfg.rt_pub, cfg.plane).await?;
        let session_id = ipc.session_id();
        let (positions_tx, positions) = bounded(1);
        let (ready_tx, ready_rx) = oneshot::channel();
        let recorder = RecorderBody {
            aeron_dir: cfg.aeron_dir.map(Path::to_path_buf),
            aeron_cfg: cfg.aeron_cfg.clone(),
            stream_id,
            session_id,
        }
        .spawn(ready_tx, positions_tx)?;
        Self::wait_ready(ready_rx, session_id).await?;
        let locators = LocatorLog::open(&cfg.state_dir.join("exec_stream").join("locators.log"))
            .context("open the exec stream locator log")?;
        let (sink, items) = bounded(ITEMS_DEPTH);
        let publisher = ExecStreamPublisher::spawn(PublisherInputs {
            items,
            positions,
            publications: AeronPublications {
                recorded: ipc,
                live,
            },
            locators,
            stop: cfg.stop,
        })
        .context("spawn the exec stream publisher")?;
        Ok(Self {
            sink,
            threads: ExecStreamThreads {
                publisher,
                recorder,
            },
        })
    }

    /// The live publication: a dynamic MDC publication with discovery. A
    /// static plane whose `exec_txs` channel is IPC shares the recorded
    /// publication, so it opens no second one.
    async fn open_live(
        rt_pub: &AeronRuntime,
        plane: &mut StreamPlane,
    ) -> Result<Option<ExecTxsPublisherHandle>> {
        let shared = !plane.is_discovered()
            && plane
                .channels()
                .exec_txs_channel
                .as_str()
                .starts_with("aeron:ipc");
        if shared {
            return Ok(None);
        }
        let live = plane
            .publisher::<ExecTxsPublisherHandle>(rt_pub)
            .await
            .context("open the live exec_txs publication")?;
        Ok(Some(live))
    }

    /// Wait until the recorder reports an active recording of this
    /// session.
    async fn wait_ready(
        ready: oneshot::Receiver<Result<i64, String>>,
        session_id: i32,
    ) -> Result<()> {
        match tokio::time::timeout(READY_TIMEOUT, ready).await {
            Ok(Ok(Ok(recording_id))) => {
                tracing::info!(
                    session_id,
                    recording_id,
                    "exec stream: the local archive records the recorded publication"
                );
                Ok(())
            }
            Ok(Ok(Err(reason))) => {
                anyhow::bail!("the exec_txs recording failed to start: {reason}")
            }
            Ok(Err(_)) => anyhow::bail!("the exec_txs recorder thread ended before readiness"),
            Err(_) => anyhow::bail!(
                "timed out ({READY_TIMEOUT:?}) waiting for the exec_txs recording to become active"
            ),
        }
    }
}

/// The recorder thread of the recorded publication: it records the
/// session on the local archive and reports the recording position.
struct RecorderBody {
    aeron_dir: Option<PathBuf>,
    aeron_cfg: AeronConfig,
    stream_id: i32,
    session_id: i32,
}

impl RecorderBody {
    fn spawn(
        self,
        ready: oneshot::Sender<Result<i64, String>>,
        positions: Sender<i64>,
    ) -> Result<RecorderThreads> {
        let mut recorders = RecorderThreads::new();
        recorders
            .spawn("exec-txs-recorder".into(), move |stop| {
                self.run(&stop, ready, &positions);
            })
            .context("spawn the exec_txs recorder thread")?;
        Ok(recorders)
    }

    /// A full position channel keeps its older value: the publisher reads
    /// it, and the next report brings the newer one.
    fn run(
        &self,
        stop: &CancellationToken,
        ready: oneshot::Sender<Result<i64, String>>,
        positions: &Sender<i64>,
    ) {
        let outcome = record_stream_reporting(
            self.aeron_dir.as_deref(),
            &self.aeron_cfg,
            RecordedStream {
                channel: RECORDED_CHANNEL,
                stream_id: self.stream_id,
                kind: RecorderKind::ExecTxs {
                    session_id: self.session_id,
                },
            },
            stop,
            |outcome| {
                let _ = ready.send(outcome);
            },
            PositionReport {
                every: POSITION_EVERY,
                send: |position| {
                    let _ = positions.try_send(position);
                },
            },
        );
        if let Err(e) = outcome {
            tracing::error!(
                error = %e,
                "exec_txs recorder ended: the local recording is lost; the executor stops"
            );
        }
    }
}
