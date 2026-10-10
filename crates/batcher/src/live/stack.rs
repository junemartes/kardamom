//! The reader stack of the live batcher: the transaction source, the
//! cluster `tx_ordering` subscription, the `tx_ordering` reader thread,
//! and the classification of how the stack ended.

use anyhow::{Context, Result};
use kardamom_obs::halt::{Halt, HaltCause};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};

use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support::{self, ArchiveAccess, TxSourceArg, TxSourceConfig};
use kardamom_engine::reader::{
    FeedHandle, NoExecStream, ReaderConfig, ReaderToExec, SourceStart, TxOrderingInputs,
    TxOrderingReader, TxOrderingSubscription, TxSource,
};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, LogConfig};
use kardamom_log::discovery::StreamPlane;

use super::cursor::BatchCursor;
use super::posted_cursor::PostedCursor;
use super::rebuild::{ArchiveRebuilder, ExecArchiveRebuilder, SourceRebuilder};
use super::run::{BatcherFileConfig, LiveArgs};
use super::sender::PostExhausted;

/// The batcher's `[cluster]` config, and the log's channel/Aeron config,
/// resolved from `--config` and the CLI overrides.
pub(super) struct RunConfig {
    file_cfg: BatcherFileConfig,
    aeron_cfg: AeronConfig,
    /// The plane the transaction source opens through.
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
    /// the catalog lists, when discovery is on and lists any.
    pub(super) async fn resolve_cluster_ingress(&mut self) -> Result<()> {
        if let Some(endpoints) = self.plane.cluster_ingress_endpoints().await? {
            self.file_cfg.cluster.ingress_endpoints = endpoints;
        }
        Ok(())
    }

    /// This node's way to the archives: the plane's archive records, and
    /// the local refetch endpoints of the arguments.
    fn archives<'a>(&'a mut self, args: &'a LiveArgs) -> ArchiveAccess<'a> {
        ArchiveAccess {
            plane: &mut self.plane,
            aeron_cfg: &self.aeron_cfg,
            aeron_dir: args.aeron_dir.as_deref(),
            archive_control_response_endpoint: args.archive_control_response_endpoint.as_deref(),
            replay_destination_endpoint: args.replay_destination_endpoint.as_deref(),
        }
    }

    /// The rebuilder of a refused replay: the archives of the source that
    /// `--tx-source` names. The `tx_data` source reads the `tx_data`
    /// archives; the executor stream reads the executor archives.
    ///
    /// # Errors
    ///
    /// Returns an error that names the settings when the archives of the
    /// source are not reachable from this node.
    pub(super) fn rebuilder(&mut self, args: &LiveArgs) -> Result<SourceRebuilder> {
        let mut archives = self.archives(args);
        match args.tx_source {
            TxSourceArg::TxData => {
                let factory = archives.join_recovery().context(
                    "the sealer refused the replay, and the rebuild from references needs the \
                     tx_data archives: --replay-destination-endpoint, \
                     --archive-control-response-endpoint and the archive endpoints of \
                     channels.toml",
                )?;
                Ok(SourceRebuilder::TxData(ArchiveRebuilder { factory }))
            }
            TxSourceArg::ExecStream => {
                let seed = archives
                    .exec_archive(&args.executor_query_endpoints)?
                    .context(
                        "the sealer refused the replay, and the rebuild from references needs \
                         the executor archives: --executor-query-endpoints, \
                         --replay-destination-endpoint, --archive-control-response-endpoint \
                         and the exec_txs archive records",
                    )?;
                Ok(SourceRebuilder::ExecStream(ExecArchiveRebuilder { seed }))
            }
        }
    }

    /// Open the transaction source and the cluster `tx_ordering`
    /// subscription, and spawn their reader threads.
    pub(super) fn spawn_reader_stack(
        &mut self,
        args: &LiveArgs,
        cursor: BatchCursor,
    ) -> Result<ReaderStack<impl Send + use<>>> {
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        let source = TxSourceConfig {
            rt: &rt,
            archives: self.archives(args),
            tx_source: args.tx_source,
            executor_query_endpoints: &args.executor_query_endpoints,
            bin_name: "kardamom-batcher",
        }
        .open()?;

        // A dedicated cluster runtime, exactly as in the executor and
        // validator. The cluster session must never contend with the
        // transaction source's work on `rt`.
        let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
            args.aeron_dir.as_deref(),
            self.file_cfg.cluster.to_live(),
            kardamom_engine::reader::cluster::ReplayCursor::new(
                cursor.next_index,
                cursor.next_block,
            ),
        )?;
        // The kardamom_sealer_* re-export is the executor's job.
        let tx_ordering_sub = cluster_sub.suppress_sealer_metrics();
        let posted_cursor = PostedCursor::new(tx_ordering_sub.posted_cursor_publisher());
        info!("kardamom-batcher: tx_ordering via Aeron Cluster");

        // The default 100 ms join timeout assumes IPC locality. On the
        // cluster's UDP multicast, a transient frame drop needs the
        // archive refetch to repair it, and refetch only engages after
        // `join_refetch_after` (10 s). Use the same bounded budget here as
        // the executor and validator, or the batcher dies before refetch
        // can fire. The executor stream source never votes and has no
        // deadline, so it reads neither the voter id nor the timeout.
        let reader_cfg = ReaderConfig {
            join_timeout: bin_support::bounded_join_timeout(cursor.next_index > 0),
            voter_id: args.void_voter_id,
            ..ReaderConfig::default()
        };
        let FeedReaderRun {
            join_handles,
            ordering_handle,
            feed_rx,
        } = FeedReader {
            order: tx_ordering_sub,
            source,
            cfg: reader_cfg,
        }
        .spawn();

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

/// The `tx_ordering` reader of the batcher over one transaction source.
pub(super) struct FeedReader<O, S> {
    pub(super) order: O,
    pub(super) source: S,
    pub(super) cfg: ReaderConfig,
}

/// A started [`FeedReader`]: the feed threads of the source, the reader
/// thread, and the channel the feed loop reads.
pub(super) struct FeedReaderRun {
    pub(super) join_handles: Vec<FeedHandle>,
    pub(super) ordering_handle: FeedHandle,
    pub(super) feed_rx: Receiver<ReaderToExec>,
}

impl<O: TxOrderingSubscription + 'static, S: TxSource> FeedReader<O, S> {
    /// Start the source and spawn the reader thread. There is no
    /// `tx_deposits` reader: deposits ride inside the epoch record on the
    /// canonical stream, so there is nothing to join against. The channel
    /// is bounded. The reader thread calls `blocking_send`. The feed task
    /// calls `recv`.
    pub(super) fn spawn(self) -> FeedReaderRun {
        let SourceStart { feeds, seed } = self.source.start();
        let (feed_tx, feed_rx) = tokio::sync::mpsc::channel(1 << 14);
        let ordering_handle = TxOrderingReader::spawn(TxOrderingInputs {
            sub: self.order,
            cfg: self.cfg,
            exec_out: feed_tx,
            exec_stream: NoExecStream,
            join: seed,
        });
        FeedReaderRun {
            join_handles: feeds,
            ordering_handle,
            feed_rx,
        }
    }
}

/// The engine reader stack: the feed threads of the transaction source,
/// the cluster ordering subscription, the channel the feed loop reads
/// from, and the posted-cursor publisher over the same cluster session.
pub(super) struct ReaderStack<G> {
    pub(super) handles: ReaderHandles<G>,
    pub(super) feed_rx: Receiver<ReaderToExec>,
    pub(super) posted_cursor: PostedCursor,
}

/// The reader-thread handles, kept for post-failure diagnosis. The cluster
/// guard (`G`, kept opaque so this module names no direct dependency on
/// `kardamom-cluster-adapter`) must outlive the feed loop.
pub(super) struct ReaderHandles<G> {
    // The runtime of the transaction source. Its Aeron thread owns the
    // eight lane subscriptions, or the executor stream subscription, and
    // ends when the last `AeronRuntime` clone drops: the discovery
    // reconciler holds a command-only handle, which keeps nothing alive.
    // Without this field the runtime ends when the setup function
    // returns, every subscription closes, and the batcher gets every
    // record through the archive refetch alone, which fails when an
    // archive is lost. Declared first so the subscriptions close before
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
    cluster_guard: G,
    join_handles: Vec<FeedHandle>,
    ordering_handle: FeedHandle,
}

/// Why a reader stack ended, as [`ReaderHandles::end`] classifies it.
pub(super) enum ReaderEnd {
    /// The sealer refused the replay: the cursor is below its retention
    /// floor, and `oldest_block` is the oldest block it still holds.
    ReplayRefused { oldest_block: u64 },
    /// A post failed on every attempt (the `l1_unreachable` halt), or
    /// every executor archive holds a record at an index that fails the
    /// check (the `exec_record_mismatch` halt).
    Halted(Halt),
    /// Every other failure, with the reader thread's error for context.
    Failed(anyhow::Error),
}

impl ReaderEnd {
    /// How a failed `tx_ordering` reader ends the stack. Every executor
    /// archive with a record that fails the check is the
    /// `exec_record_mismatch` halt: the batcher posts only checked bytes,
    /// so it stays up, posts nothing past the entry, and waits for an
    /// operator.
    fn of_reader(re: ExecutorError, feed_err: &anyhow::Error) -> Self {
        match re {
            ExecutorError::ClusterReplayUnavailable { oldest_block, .. } => {
                Self::ReplayRefused { oldest_block }
            }
            e @ ExecutorError::ExecRecordMismatch { .. } => {
                Self::Halted(Halt::new(HaltCause::ExecRecordMismatch, format!("{e:#}")))
            }
            re => Self::Failed(anyhow::anyhow!(
                "tx_ordering reader failed: {re:#} (feed loop: {feed_err:#})"
            )),
        }
    }
}

impl<G> ReaderHandles<G> {
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
            return ReaderEnd::of_reader(re, &feed_err);
        }
        if feed_err
            .chain()
            .any(|c| c.downcast_ref::<PostExhausted>().is_some())
        {
            return ReaderEnd::Halted(Halt::new(HaltCause::L1Unreachable, format!("{feed_err:#}")));
        }
        ReaderEnd::Failed(
            self.join_handles
                .into_iter()
                .find_map(|h| Self::stream_reader_failure(h, &feed_err))
                .unwrap_or(feed_err),
        )
    }

    /// `h`'s error, if it already finished and failed.
    fn stream_reader_failure(h: FeedHandle, feed_err: &anyhow::Error) -> Option<anyhow::Error> {
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
#[path = "stack_tests.rs"]
mod tests;
