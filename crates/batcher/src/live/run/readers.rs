//! The batcher's reader startup and failure classification.

use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_engine::reader::cluster::PostedCursorPublisher;
use kardamom_engine::reader::{
    JoinBuffer, JoinRecoveryFactory, NoExecStream, ReaderConfig, ReaderToExec, TxDataReader,
    TxOrderingInputs, TxOrderingReader,
};
use kardamom_engine::{ExecutorError, bin_support};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, LogConfig};
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::halt::{Halt, HaltCause};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};

use super::super::cursor::BatchCursor;
use super::super::posted_cursor::PostedCursor;
use super::super::sender::PostExhausted;
use super::{BatcherFileConfig, LiveArgs};

/// The batcher's `[cluster]` config, and the log's channel/Aeron config,
/// resolved from `--config` and the CLI overrides.
pub(super) struct RunConfig {
    file_cfg: BatcherFileConfig,
    aeron_cfg: AeronConfig,
    /// The plane the `tx_data` lanes open through.
    pub(super) plane: StreamPlane,
}

impl RunConfig {
    /// Resolve the batcher's `[cluster]` config and the log's
    /// channel/Aeron config, applying the `--cluster-egress-endpoint` and
    /// `--aeron-dir` overrides.
    pub(super) fn resolve(args: &LiveArgs) -> Result<Self> {
        let raw = std::fs::read_to_string(&args.config).context("read batcher config")?;
        let mut file_cfg: BatcherFileConfig =
            toml::from_str(&raw).context("parse batcher config")?;
        file_cfg
            .cluster
            .set_egress_endpoint(args.cluster_egress_endpoint.as_deref());
        let log_cfg =
            LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
        let plane =
            StreamPlane::from_config(&log_cfg, "batcher").context("build the stream plane")?;
        let mut aeron_cfg = log_cfg.aeron;
        if let Some(dir) = args.aeron_dir.as_ref() {
            aeron_cfg.aeron_dir.clone_from(dir);
        }
        Ok(Self {
            file_cfg,
            aeron_cfg,
            plane,
        })
    }

    /// Replace the static `[cluster]` ingress endpoints with the members
    /// the catalog lists, when discovery is on and lists any. A
    /// transient catalog error makes the read try again for up to
    /// `limit`.
    async fn resolve_cluster_ingress(&mut self, limit: Duration) -> Result<()> {
        if let Some(endpoints) = self.plane.cluster_ingress_endpoints(limit).await? {
            self.file_cfg.cluster.ingress_endpoints = endpoints;
        }
        Ok(())
    }

    /// The join-miss refetch factory: the `tx_data` archives, reached
    /// from this node's refetch endpoints. `None` when the deployment
    /// configures no archive or no local endpoint.
    pub(super) fn join_recovery(&mut self, args: &LiveArgs) -> Option<JoinRecoveryFactory> {
        bin_support::archive_join_recovery(
            &mut self.plane,
            &self.aeron_cfg,
            args.aeron_dir.as_deref(),
            args.archive_control_response_endpoint.as_deref(),
            args.replay_destination_endpoint.as_deref(),
        )
    }

    /// Open the `tx_data` and cluster `tx_ordering` subscriptions, and
    /// spawn their reader threads. The cluster members come from the
    /// catalog first, within the start-up limit of the runtime.
    pub(super) async fn spawn_reader_stack(
        &mut self,
        args: &LiveArgs,
        cursor: BatchCursor,
    ) -> Result<ReaderStack> {
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        self.resolve_cluster_ingress(rt.start_open_limit()).await?;
        let replay_cursor = kardamom_engine::reader::cluster::ReplayCursor::new(
            cursor.next_index,
            cursor.next_block,
        );
        let Ordering {
            tx_data_subs,
            join_recovery,
            cluster_guard,
            tx_ordering_sub,
            posted_cursor,
        } = match &args.egress {
            Some(base) => self.egress_ordering(args, &rt, base, replay_cursor)?,
            None => self.cluster_ordering(args, &rt, replay_cursor)?,
        };

        let join_buffer = JoinBuffer::new();
        let join_handles = tx_data_subs
            .into_iter()
            .map(|sub| TxDataReader::new(sub, join_buffer.clone()).spawn())
            .collect();
        // There is no tx_deposits reader. Deposits ride inside the epoch
        // record on the canonical stream, so there is nothing to join
        // against. The channel is bounded. The reader thread calls
        // `blocking_send`. The feed task calls `recv`.
        let (feed_tx, feed_rx) = tokio::sync::mpsc::channel(1 << 14);
        // The default 100 ms join timeout assumes IPC locality. On the
        // cluster's UDP multicast, a transient frame drop needs the
        // archive refetch to repair it, and refetch only engages after
        // `join_refetch_after` (10 s). Use the same bounded budget here as
        // the executor and validator, or the batcher dies before refetch
        // can fire.
        let reader_cfg = ReaderConfig {
            join_timeout: bin_support::bounded_join_timeout(cursor.next_index > 0),
            voter_id: args.void_voter_id,
            ..ReaderConfig::default()
        };
        let ordering_handle = TxOrderingReader::spawn(TxOrderingInputs {
            sub: tx_ordering_sub,
            buffer: join_buffer,
            cfg: reader_cfg,
            exec_out: feed_tx,
            exec_stream: NoExecStream,
            recovery_factory: join_recovery,
        });

        Ok(ReaderStack {
            handles: ReaderHandles {
                rt,
                cluster_guard,
                join_handles,
                ordering_handle,
            },
            feed_rx,
            posted_cursor,
        })
    }
}

/// The ordering source of the reader stack, and what goes with it.
struct Ordering {
    tx_data_subs: Vec<bin_support::LiveTxDataSub>,
    join_recovery: Option<JoinRecoveryFactory>,
    cluster_guard: bin_support::LiveClusterGuard,
    tx_ordering_sub: bin_support::LiveOrdering,
    posted_cursor: PostedCursor,
}

impl RunConfig {
    /// The references of the cluster egress, joined with `tx_data`, and the
    /// posted cursor over the same cluster session.
    fn cluster_ordering(
        &mut self,
        args: &LiveArgs,
        rt: &AeronRuntime,
        cursor: kardamom_engine::reader::cluster::ReplayCursor,
    ) -> Result<Ordering> {
        let tx_data_subs = bin_support::open_tx_data_subs(rt, &mut self.plane)?;
        let join_recovery = self.join_recovery(args);
        // A dedicated cluster runtime, exactly as in the executor and
        // validator. The cluster session must never contend with the
        // tx_data work on `rt`.
        let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
            args.aeron_dir.as_deref(),
            self.file_cfg.cluster.to_live(),
            cursor,
        )?;
        // The kardamom_sealer_* re-export is the executor's job.
        let cluster_sub = cluster_sub.suppress_sealer_metrics();
        let posted_cursor = PostedCursor::new(cluster_sub.posted_cursor_publisher());
        info!("kardamom-batcher: tx_ordering via Aeron Cluster");
        Ok(Ordering {
            tx_data_subs,
            join_recovery,
            cluster_guard,
            tx_ordering_sub: bin_support::LiveOrdering::Cluster(cluster_sub),
            posted_cursor,
        })
    }

    /// The entries with their bytes, from the egress at `base`, and the
    /// posted cursor over a cluster session that only publishes.
    fn egress_ordering(
        &mut self,
        args: &LiveArgs,
        rt: &AeronRuntime,
        base: &str,
        cursor: kardamom_engine::reader::cluster::ReplayCursor,
    ) -> Result<Ordering> {
        let source = bin_support::EgressSource {
            base: base.to_owned(),
            cursor_gauge: super::live_metric_names::EGRESS_CURSOR_INDEX,
        };
        let sub = bin_support::open_egress(rt, &mut self.plane, source, cursor)?;
        let (cluster_guard, ingress) = bin_support::connect_cluster_publisher(
            args.aeron_dir.as_deref(),
            self.file_cfg.cluster.to_live(),
        )?;
        info!(%base, "kardamom-batcher: the canonical stream via the egress");
        Ok(Ordering {
            tx_data_subs: Vec::new(),
            join_recovery: None,
            cluster_guard,
            tx_ordering_sub: bin_support::LiveOrdering::Egress(Box::new(sub)),
            posted_cursor: PostedCursor::new(PostedCursorPublisher::new(ingress)),
        })
    }
}

/// The engine reader stack: the `tx_data` join-buffer readers, the cluster
/// ordering subscription, the channel the feed loop reads from, and the
/// posted-cursor publisher over the same cluster session.
pub(super) struct ReaderStack {
    pub(super) handles: ReaderHandles,
    pub(super) feed_rx: Receiver<ReaderToExec>,
    pub(super) posted_cursor: PostedCursor,
}

/// The reader-thread handles retain the data runtime and cluster guard
/// through the feed loop and its post-failure diagnosis.
pub(super) struct ReaderHandles {
    // The `tx_data` runtime. Its Aeron thread owns the eight lane
    // subscriptions, and ends when the last `AeronRuntime` clone drops:
    // the discovery reconciler holds a command-only handle, which keeps
    // nothing alive. Without this field the runtime ends when the setup
    // function returns, every lane closes, and the batcher joins every
    // TxRef through the archive refetch alone, which fails when an
    // ingress archive is lost. Declared first so the lanes close before
    // the cluster session does, the order `LiveStreams` uses. Never read.
    #[allow(
        dead_code,
        reason = "held only for its Drop impl, which ends the Aeron thread"
    )]
    rt: AeronRuntime,
    // Held only for its Drop impl (closes the Aeron cluster session when
    // the handles are dropped after a feed failure); its value is never
    // read.
    #[allow(
        dead_code,
        reason = "held only for its Drop impl, which closes the Aeron cluster session"
    )]
    cluster_guard: bin_support::LiveClusterGuard,
    join_handles: Vec<JoinHandle<Result<(), ExecutorError>>>,
    ordering_handle: JoinHandle<Result<(), ExecutorError>>,
}

/// Why a reader stack ended, as [`ReaderHandles::end`] classifies it.
pub(super) enum ReaderEnd {
    /// The sealer refused the replay: the cursor is below its retention
    /// floor, and `oldest_block` is the oldest block it still holds.
    ReplayRefused { oldest_block: u64 },
    /// A post failed on every attempt: the `l1_unreachable` halt.
    Halted(Halt),
    /// Every other failure, with the reader thread's error for context.
    Failed(anyhow::Error),
}

impl ReaderEnd {
    fn exhausted_post(error: &anyhow::Error) -> Option<Self> {
        error
            .is::<PostExhausted>()
            .then(|| Self::Halted(Halt::new(HaltCause::L1Unreachable, format!("{error:#}"))))
    }
}

impl ReaderHandles {
    /// The feed loop returns only on failure (channel closed, or a post
    /// that stopped it). Classify the end from the reader threads'
    /// errors: the channel-closed case's root cause lives there. A post
    /// that failed on every attempt is a halt, not an end. The handles
    /// drop here, so the cluster session and the runtime close before a
    /// new stack opens.
    pub(super) fn end(self, feed_err: anyhow::Error) -> ReaderEnd {
        warn!(error = %format!("{feed_err:#}"), "feed loop exited");
        if self.ordering_handle.is_finished()
            && let Ok(Err(re)) = self.ordering_handle.join()
        {
            if let ExecutorError::ClusterReplayUnavailable { oldest_block, .. } = re {
                return ReaderEnd::ReplayRefused { oldest_block };
            }
            return ReaderEnd::Failed(anyhow::anyhow!(
                "tx_ordering reader failed: {re:#} (feed loop: {feed_err:#})"
            ));
        }
        if let Some(halted) = ReaderEnd::exhausted_post(&feed_err) {
            return halted;
        }
        ReaderEnd::Failed(
            self.join_handles
                .into_iter()
                .find_map(|h| Self::stream_reader_failure(h, &feed_err))
                .unwrap_or(feed_err),
        )
    }

    /// `h`'s error, if it already finished and failed.
    fn stream_reader_failure(
        h: JoinHandle<Result<(), ExecutorError>>,
        feed_err: &anyhow::Error,
    ) -> Option<anyhow::Error> {
        if h.is_finished()
            && let Ok(Err(re)) = h.join()
        {
            return Some(anyhow::anyhow!(
                "stream reader failed: {re:#} (feed loop: {feed_err:#})"
            ));
        }
        None
    }
}

#[cfg(test)]
mod tests;
