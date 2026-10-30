//! The executor process of the recovery tests. It runs the production
//! state startup (`kardamom_executor::state`), the production engine
//! (reader, exec and commit threads) on durable MDBX, the production
//! cluster reader and replay repair, and the checkpoint server. Only the
//! transport is a controlled neighbour: the canonical stream comes from
//! the [`SealerSession`], and the receipts go to a file.

use std::io::Write;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kardamom_engine::actor::fixtures::ChanTxDataSub;
use kardamom_engine::bin_support;
use kardamom_engine::reader::cluster::ClusterTxOrderingSubscription;
use kardamom_engine::{
    CMessage, EngineWiring, ExecPorts, ExecStreamSink, Executor, ExecutorConfig, ExecutorError,
    Inbound, MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal, NoBlockExec, NoEpochCheck,
    NoExecStream, NoRemoteEpochCheck, NoTxHook, Outbound, ResumePoint, RoleHooks,
    TxReceiptsPublication,
};
use kardamom_executor::state::{BlockLogConfig, CheckpointLog, StateConfig};
use kardamom_state::{BlockRange, Durability, seed_genesis};
use serde::{Deserialize, Serialize};

use super::sealer::{ChannelEgress, SealerSession};

/// The settings of one executor process, written by the parent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeConfig {
    pub(crate) state_dir: PathBuf,
    pub(crate) checkpoint_dir: PathBuf,
    pub(crate) peers: Vec<String>,
    pub(crate) serve: SocketAddr,
    /// The nonce, balance and receipt query address.
    pub(crate) rpc: SocketAddr,
    /// Checkpoint rounds every second, or none.
    pub(crate) checkpoints: bool,
    pub(crate) block_log: bool,
    /// With the block log: a new base when the newest one is older than
    /// this, in seconds.
    pub(crate) base_max_age_secs: NonZeroU64,
    pub(crate) sealer: PathBuf,
    pub(crate) genesis: PathBuf,
    pub(crate) receipts: PathBuf,
    /// The Prometheus exporter address, with `/metrics` and `/ready`.
    pub(crate) metrics: SocketAddr,
    /// The media driver of the executor stream. `None` runs no stream.
    pub(crate) aeron_dir: Option<PathBuf>,
}

impl NodeConfig {
    fn state(&self) -> StateConfig {
        StateConfig {
            state_dir: self.state_dir.clone(),
            durability: Durability::Durable,
            checkpoint_dir: Some(self.checkpoint_dir.clone()),
            checkpoint_peers: self.peers.clone(),
            checkpoint_interval_secs: self.checkpoints.then_some(NonZeroU64::MIN),
            checkpoint_keep: NonZeroUsize::new(3).unwrap(),
            block_log: self.block_log.then_some(BlockLogConfig {
                base_log_percent: NonZeroU64::new(100).unwrap(),
                base_max_age_secs: self.base_max_age_secs,
                copy_mib_per_sec: NonZeroU64::new(16).unwrap(),
                segment_mib: NonZeroU64::MIN,
            }),
        }
    }
}

/// The port types of the process: the production MDBX seams and cluster
/// reader, with the controlled egress and receipt file. `S` is the
/// executor stream: the production publisher on a real archive, or none.
struct RecoveryWiring<S>(std::marker::PhantomData<S>);

impl<S> ExecPorts for RecoveryWiring<S> {
    type Snapshots = MdbxSnapshotSource;
    type WriterSignal = MdbxWriterSignal;
    type WriterQueue = MdbxWriterQueue;
    type Epoch = NoEpochCheck;
    type RemoteEpoch = NoRemoteEpochCheck;
    type BlockExec = NoBlockExec;
    type TxHook = NoTxHook;
}

impl<S: ExecStreamSink> EngineWiring for RecoveryWiring<S> {
    type TxData = ChanTxDataSub;
    type TxOrdering = ClusterTxOrderingSubscription<ChannelEgress>;
    type TxReceipts = FileReceipts;
    type ExecStream = S;
}

/// The receipt stream as lines: `R <index> <hash>` per receipt and
/// `B <block> <end>` per boundary. One write per line, so a kill leaves
/// whole lines.
pub(crate) struct FileReceipts(std::fs::File);

impl FileReceipts {
    fn open(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self(file))
    }

    fn line(&mut self, line: &str) -> Result<(), ExecutorError> {
        self.0
            .write_all(line.as_bytes())
            .map_err(|_| ExecutorError::TxReceiptsClosed)
    }
}

impl TxReceiptsPublication for FileReceipts {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        let line = match msg {
            CMessage::Receipt(r) => format!("R {} {}\n", r.tx_idx.as_index(), r.tx_hash),
            CMessage::BlockBoundary(b) => {
                format!("B {} {}\n", b.block_number, b.end_tx_idx.as_index())
            }
        };
        self.line(&line)
    }

    fn publish_rollback(
        &mut self,
        _rollback: kardamom_types::ReceiptRollback,
    ) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// `/ready` passes while the applied block is at most this many blocks
/// behind the sealer's head: the binary's default.
const READY_LAG_BLOCKS: f64 = 8.0;

/// What ends one run of the pipeline.
enum RunEnd {
    /// The operator stopped the process.
    Stopped,
    /// The engine ended by itself, with its error.
    Ended(Option<ExecutorError>),
}

/// One executor process.
pub(crate) struct Node {
    cfg: NodeConfig,
    state: StateConfig,
    genesis: Option<kardamom_types::Genesis>,
    chain_id: NonZeroU64,
    expected_genesis: Option<alloy_primitives::B256>,
    stop: tokio_util::sync::CancellationToken,
}

impl Node {
    /// Run the process until a stop signal or a failure the process
    /// cannot repair.
    pub(crate) fn main(cfg: NodeConfig) -> Result<()> {
        bin_support::init_tracing();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let (genesis, chain_id) = bin_support::resolve_genesis(Some(&cfg.genesis), 1)?;
            let node = Self {
                state: cfg.state(),
                expected_genesis: bin_support::expected_genesis_digest(genesis.as_ref()),
                genesis,
                chain_id,
                cfg,
                stop: tokio_util::sync::CancellationToken::new(),
            };
            node.run().await
        })
    }

    async fn run(self) -> Result<()> {
        kardamom_obs::init_service!(
            "executor",
            self.cfg.metrics,
            "recovery",
            kardamom_obs::Readiness::up().within(
                kardamom_engine::metrics::BLOCK_NUMBER,
                kardamom_engine::metrics::SEALER_BLOCK_NUMBER,
                READY_LAG_BLOCKS,
            )
        )
        .await?;
        kardamom_engine::metrics::describe();
        kardamom_executor::exec_stream::ExecStreamMetrics::describe();
        let _server =
            kardamom_state::serve_checkpoints(self.cfg.serve, self.cfg.checkpoint_dir.clone())
                .context("bind checkpoint serve address")?;
        let signal = self.stop.clone();
        tokio::spawn(async move {
            bin_support::wait_for_shutdown().await;
            signal.cancel();
        });
        FileReceipts::open(&self.cfg.receipts)?.line(&format!("S {}\n", std::process::id()))?;
        while self.revolve().await? {
            tracing::info!(
                "the pipeline starts again in-process from the state DB or a staged checkpoint"
            );
        }
        Ok(())
    }

    /// One run and its repair. Returns whether to run again.
    async fn revolve(&self) -> Result<bool> {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let prepared =
            self.state
                .prepare(self.expected_genesis, BlockRange::ANY, shutdown.clone())?;
        let alloc = bin_support::build_genesis_alloc(self.genesis.as_ref());
        seed_genesis(&prepared.env, &alloc).context("seed genesis into state env")?;
        let mut queries = kardamom_state::serve_nonce_queries(self.cfg.rpc, prepared.env.clone())
            .context("bind nonce query address")?;
        let mut writer = CheckpointLog::spawn_writer(&self.state, prepared.env)?;
        let engine_error = match self.cfg.aeron_dir.as_deref() {
            Some(dir) => {
                self.run_streamed(dir, &writer, prepared.start, shutdown.clone())
                    .await?
            }
            None => {
                self.run_engine(&writer, prepared.start, NoExecStream)
                    .await?
            }
        };
        writer.shutdown().context("state writer shutdown")?;
        queries.task.abort();
        let _ = (&mut queries.task).await;
        shutdown.cancel();
        if let Some(checkpointer) = prepared.checkpointer {
            checkpointer.await?;
        }
        self.verdict(engine_error)
    }

    /// Run the engine with the production executor stream: the recorded
    /// publication on the driver of `dir`, its local recording, and the
    /// publisher thread. The stream threads end after the engine.
    async fn run_streamed(
        &self,
        dir: &Path,
        writer: &kardamom_state::WriterHandle,
        start: ResumePoint,
        stop: tokio_util::sync::CancellationToken,
    ) -> Result<Option<ExecutorError>> {
        let log = kardamom_log::config::LogConfig::default();
        let rt_pub = kardamom_log::aeron_live::AeronRuntime::spawn(Some(dir))?;
        let mut plane = kardamom_log::discovery::StreamPlane::static_only(log.channels.clone());
        let stream = kardamom_executor::exec_stream::ExecStream::open(
            kardamom_executor::exec_stream::ExecStreamConfig {
                rt_pub: &rt_pub,
                plane: &mut plane,
                aeron_dir: Some(dir),
                aeron_cfg: &log.aeron,
                state_dir: &self.cfg.state_dir,
                stop,
            },
        )
        .await?;
        stream.cursor.start(None);
        let threads = stream.threads;
        let engine_error = self.run_engine(writer, start, stream.sink).await?;
        tokio::task::spawn_blocking(move || threads.join()).await?;
        Ok(engine_error)
    }

    /// Run the engine until a stop signal or its own end, and return its
    /// error.
    async fn run_engine<S: ExecStreamSink + Send + 'static>(
        &self,
        writer: &kardamom_state::WriterHandle,
        start: ResumePoint,
        exec_stream: S,
    ) -> Result<Option<ExecutorError>> {
        let cursor = bin_support::cluster_replay_cursor(&start);
        let (session, egress) = SealerSession::open(&self.cfg.sealer, cursor.position());
        let engine = Executor::<RecoveryWiring<S>>::new(
            ExecutorConfig {
                chain_id: self.chain_id,
                ..ExecutorConfig::default()
            },
            Inbound {
                tx_data: Vec::new(),
                tx_ordering: ClusterTxOrderingSubscription::with_cursor(egress, cursor),
                join_recovery: None,
                exec_stream,
            },
            Outbound {
                tx_receipts: FileReceipts::open(&self.cfg.receipts)?,
                snapshots: MdbxSnapshotSource::new(writer.snapshot_rx.clone()),
                writer_signal: MdbxWriterSignal::new(writer.snapshot_rx.clone()),
                writer_queue: MdbxWriterQueue::new(writer.delta_tx.clone()),
            },
            start,
            RoleHooks::none(),
        );
        let mut join = tokio::task::spawn_blocking(move || engine.run());
        let end = tokio::select! {
            () = self.stop.cancelled() => RunEnd::Stopped,
            joined = &mut join => RunEnd::Ended(joined?.err()),
        };
        session.close();
        Ok(match end {
            RunEnd::Stopped => join.await?.err(),
            RunEnd::Ended(error) => error,
        })
    }

    /// The verdict of one run: done, run again after a staged peer
    /// checkpoint, or fail.
    fn verdict(&self, engine_error: Option<ExecutorError>) -> Result<bool> {
        let repaired = bin_support::replay_unavailable_fallback(
            engine_error.as_ref(),
            Some(&self.cfg.checkpoint_dir),
            &self.cfg.peers,
            &self.cfg.state_dir,
            self.expected_genesis,
            false,
        )?;
        match (engine_error, repaired) {
            (None, _) => Ok(false),
            (Some(_), Some("peer-checkpoint")) => Ok(!self.stop.is_cancelled()),
            (Some(e), _) if self.stop.is_cancelled() => {
                tracing::info!(error = %e, "the engine ended on the stop signal");
                Ok(false)
            }
            (Some(e), _) => Err(anyhow::Error::new(e).context("executor pipeline failed")),
        }
    }
}
