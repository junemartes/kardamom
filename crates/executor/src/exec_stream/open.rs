//! The production executor stream: the two Aeron publications, the local
//! recording, and the publisher thread.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_cluster_adapter::LiveIngress;
use kardamom_engine::{
    Escalation, ExecStreamItem, ExecutorError, PUBLICATION_DEAD_EXIT_CODE, Step,
};
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

use super::cadence::CursorHandoff;
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

/// The pause between two attempts to start the recording.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

/// The topic of the recorded publication, for the log lines and the
/// typed error.
const TOPIC: &str = "exec_txs";

/// The production [`StreamPublications`]: the recorded IPC publication,
/// and the live publication when it is a separate one.
struct AeronPublications {
    recorded: PubHandle,
    live: Option<ExecTxsPublisherHandle>,
}

impl StreamPublications for AeronPublications {
    type CursorIngress = LiveIngress;

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

/// The open executor stream: the reader's sink, the hand-off of the
/// recorded cursor, and the threads behind them.
pub struct ExecStream {
    /// The sink of the `tx_ordering` reader.
    pub sink: Sender<ExecStreamItem>,
    /// Starts the recorded cursor once the cluster session is up.
    pub cursor: CursorHandoff<LiveIngress>,
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
        let Recording {
            ipc,
            recorder,
            positions,
        } = RecordingStart {
            cfg: &cfg,
            stream_id,
            ipc,
            budget: cfg.rt_pub.stall_budget(),
            escalation: Escalation::from_stall_budget(cfg.rt_pub.stall_budget()),
        }
        .run()
        .await?;
        let locators = LocatorLog::open(&cfg.state_dir.join("exec_stream").join("locators.log"))
            .context("open the exec stream locator log")?;
        let (sink, items) = bounded(ITEMS_DEPTH);
        let (cursor, cursor_sender) = CursorHandoff::new();
        let publisher = ExecStreamPublisher::spawn(PublisherInputs {
            items,
            positions,
            publications: AeronPublications {
                recorded: ipc,
                live,
            },
            locators,
            stop: cfg.stop,
            cursor: cursor_sender,
        })
        .context("spawn the exec stream publisher")?;
        Ok(Self {
            sink,
            cursor,
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

    /// Wait up to `within` until the recorder reports an active recording
    /// of this session.
    async fn wait_ready(
        ready: oneshot::Receiver<Result<i64, String>>,
        session_id: i32,
        within: Duration,
    ) -> Result<()> {
        match tokio::time::timeout(within, ready).await {
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
                "timed out ({within:?}) waiting for the exec_txs recording to become active"
            ),
        }
    }
}

/// The recorded publication with its active recording and the thread
/// that holds it.
struct Recording {
    ipc: PubHandle,
    recorder: RecorderThreads,
    positions: Receiver<i64>,
}

/// The start of the recording, under the escalation of the stall budget.
/// A media driver that crashed under the executor leaves the archive
/// unreachable for a while, and a recording of a stale session never
/// starts. Each attempt connects the archive again, and each attempt is
/// bounded by the budget, the archive connect included. The clock starts
/// with the first attempt. After one budget the start opens a new
/// session. After five, it ends with [`ExecutorError::PublicationDead`],
/// and the process exits with [`PUBLICATION_DEAD_EXIT_CODE`] so the
/// supervisor restarts it.
struct RecordingStart<'a> {
    cfg: &'a ExecStreamConfig<'a>,
    stream_id: i32,
    ipc: PubHandle,
    /// The stall budget: the bound of one attempt.
    budget: Duration,
    escalation: Escalation,
}

impl RecordingStart<'_> {
    async fn run(mut self) -> Result<Recording> {
        self.escalation.start(Instant::now());
        loop {
            if let ControlFlow::Break(done) = self.attempt().await {
                return done;
            }
        }
    }

    /// One attempt: a recorder thread for the current session, and the
    /// wait for its report. `Break` carries the recording or the final
    /// error. `Continue` means another attempt, after the escalation
    /// step. The recorder of a failed attempt ends with this scope.
    async fn attempt(&mut self) -> ControlFlow<Result<Recording>> {
        let (positions_tx, positions) = bounded(1);
        let (ready_tx, ready_rx) = oneshot::channel();
        let session_id = self.ipc.session_id();
        let spawned = RecorderBody {
            aeron_dir: self.cfg.aeron_dir.map(Path::to_path_buf),
            aeron_cfg: self.cfg.aeron_cfg.clone(),
            stream_id: self.stream_id,
            session_id,
            connect_timeout: self.budget,
        }
        .spawn(ready_tx, positions_tx);
        let recorder = match spawned {
            Ok(recorder) => recorder,
            Err(e) => return ControlFlow::Break(Err(e)),
        };
        match ExecStream::wait_ready(ready_rx, session_id, self.budget).await {
            Ok(()) => ControlFlow::Break(Ok(Recording {
                ipc: self.ipc.clone(),
                recorder,
                positions,
            })),
            Err(e) => self.failed(e).await,
        }
    }

    /// The escalation step after one failed attempt.
    async fn failed(&mut self, e: anyhow::Error) -> ControlFlow<Result<Recording>> {
        let now = Instant::now();
        let step = self.escalation.unconnected(now);
        tracing::warn!(
            error = %e,
            session_id = self.ipc.session_id(),
            unconnected_s = self.escalation.unconnected_for(now).as_secs(),
            "the exec_txs recording did not start; retrying"
        );
        match step {
            Step::Wait => {
                tokio::time::sleep(RETRY_PAUSE).await;
                ControlFlow::Continue(())
            }
            Step::Reopen => self.reopen(),
            Step::Exit { unconnected } => ControlFlow::Break(Err(self.dead(unconnected))),
        }
    }

    /// Open the recorded publication again: a new session, which the
    /// archive records as a new recording.
    fn reopen(&mut self) -> ControlFlow<Result<Recording>> {
        let opened = self
            .cfg
            .rt_pub
            .open_exclusive_publication(RECORDED_CHANNEL, self.stream_id)
            .context("reopen the recorded exec_txs publication");
        let ipc = match opened {
            Ok(ipc) => ipc,
            Err(e) => return ControlFlow::Break(Err(e)),
        };
        let old = std::mem::replace(&mut self.ipc, ipc);
        tracing::warn!(
            topic = TOPIC,
            old_session = old.session_id(),
            session = self.ipc.session_id(),
            exit_after_s = self.escalation.exit_after().as_secs(),
            "the recording stays unstarted; the recorded publication reopened on a new session"
        );
        if let Err(e) = old.close() {
            tracing::warn!(error = %e, session = old.session_id(), "the old exec_txs publication did not close");
        }
        ControlFlow::Continue(())
    }

    /// The error that ends the start after `unconnected` without a
    /// recording. The line names the topic, the durations and the exit
    /// code.
    fn dead(&self, unconnected: Duration) -> anyhow::Error {
        let reopen_after_s = self.escalation.reopen_after().as_secs();
        let unconnected_s = unconnected.as_secs();
        tracing::error!(
            topic = TOPIC,
            unconnected_s,
            reopen_after_s,
            exit_after_s = self.escalation.exit_after().as_secs(),
            exit_code = PUBLICATION_DEAD_EXIT_CODE,
            "the exec_txs recording stayed unstarted past its budget; the process exits so the supervisor restarts it"
        );
        anyhow::Error::new(ExecutorError::PublicationDead {
            topic: TOPIC.into(),
            unconnected_s,
            reopen_after_s,
        })
    }
}

/// The recorder thread of the recorded publication: it records the
/// session on the local archive and reports the recording position.
struct RecorderBody {
    aeron_dir: Option<PathBuf>,
    aeron_cfg: AeronConfig,
    stream_id: i32,
    session_id: i32,
    /// The bound of the archive connect: the stall budget.
    connect_timeout: Duration,
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
                connect_timeout: self.connect_timeout,
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
