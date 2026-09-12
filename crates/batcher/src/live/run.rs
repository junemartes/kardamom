//! Live service wiring: CLI args, the reader stack, and the feed-loop task.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};

use kardamom_engine::bin_support;
use kardamom_engine::reader::{
    JoinBuffer, ReaderConfig, ReaderToExec, spawn_tx_data_reader, spawn_tx_ordering_reader,
};
use kardamom_engine::{ExecutorError, TxIndex};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_log::discovery::StreamPlane;

use crate::da_store::FsBlobStore;

use super::cursor::{BatchCursor, L1Truth, read_l1_truth, reconcile};
use super::feed::{FeedConfig, run_feed};
use super::sender::LiveSender;

/// Top-level config the batcher reads from `--config` in live mode. It uses
/// the same `[cluster]` section shape as the executor and the validator.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub(crate) struct BatcherFileConfig {
    pub cluster: kardamom_engine::reader::cluster::ClusterConfig,
}

/// Parse the batcher key. Connect the wallet-backed L1 provider and the
/// local DA blob store. The live service and the offline `--dry-run=false`
/// post path share this signer, provider, and blob-store setup.
///
/// # Errors
/// Returns an error when the key does not parse, or the L1 RPC connection
/// or the blob store fails to open.
pub async fn connect_l1(
    rpc: &str,
    key: &str,
    da_dir: &Path,
) -> Result<(impl Provider + 'static, FsBlobStore)> {
    let signer: PrivateKeySigner = key.parse().context("parse --l1-key")?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect(rpc)
        .await
        .with_context(|| format!("connect L1 RPC {rpc}"))?;
    let da_store = FsBlobStore::open(da_dir)?;
    Ok((provider, da_store))
}

/// Everything [`run`] needs from the CLI, already validated. The binary
/// checks the L1 flag tuple, `--config`, and `--cursor-file` presence
/// first, so its error messages can name the exact flag combination.
#[derive(Debug, Clone)]
pub struct LiveArgs {
    pub rpc: String,
    pub key: String,
    pub settlement: Address,
    pub da_store: PathBuf,
    /// TOML supplying the `[cluster]` section ([`BatcherFileConfig`]).
    pub config: PathBuf,
    pub cursor_file: PathBuf,
    pub log_config: Option<PathBuf>,
    pub aeron_dir: Option<PathBuf>,
    /// The L2 chain id. See [`BatcherConfig::chain_id`].
    pub chain_id: u64,
    pub cluster_egress_endpoint: Option<String>,
    pub replay_destination_endpoint: Option<String>,
    pub archive_control_response_endpoint: Option<String>,
    pub blocks_per_batch: NonZeroUsize,
    pub compress: bool,
    /// Post a partial group if the oldest pending block has waited this
    /// long. Nonzero at the type level: 0 makes the flush timeout expire
    /// at once, a busy loop.
    pub flush_ms: NonZeroU64,
    pub l1_retries: u32,
}

/// [`start_l1_side`]'s resolved view: the provider, the blob store, L1's
/// truth, the cursor to replay from, and the block to skip through
/// (already covered by L1).
struct L1Side<P> {
    provider: P,
    da_store: FsBlobStore,
    l1_truth: L1Truth,
    cursor: BatchCursor,
    skip_through_block: u64,
}

impl LiveArgs {
    /// Connect to L1 and reconcile the durable cursor against it.
    async fn start_l1_side(&self) -> Result<L1Side<impl Provider + 'static>> {
        let (provider, da_store) = connect_l1(&self.rpc, &self.key, &self.da_store).await?;
        let l1_truth = read_l1_truth(&provider, self.settlement).await?;
        let (cursor, skip_through_block) =
            reconcile(BatchCursor::load(&self.cursor_file)?, l1_truth)?;
        info!(
            settlement = %self.settlement,
            last_batch_index = l1_truth.last_batch_index,
            covered_through_block = l1_truth.covered_through_block,
            replay_from_index = cursor.next_index,
            replay_from_block = cursor.next_block,
            skip_through_block,
            chain_id = self.chain_id,
            "live batcher starting"
        );
        Ok(L1Side {
            provider,
            da_store,
            l1_truth,
            cursor,
            skip_through_block,
        })
    }
}

/// The batcher's `[cluster]` config, and the log's channel/Aeron config,
/// resolved from `--config` and the CLI overrides.
struct RunConfig {
    file_cfg: BatcherFileConfig,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    /// The plane the `tx_data` lanes open through.
    plane: StreamPlane,
}

impl RunConfig {
    /// Resolve the batcher's `[cluster]` config and the log's
    /// channel/Aeron config, applying the `--cluster-egress-endpoint` and
    /// `--aeron-dir` overrides.
    fn resolve(args: &LiveArgs) -> Result<Self> {
        let raw = std::fs::read_to_string(&args.config).context("read batcher config")?;
        let mut file_cfg: BatcherFileConfig =
            toml::from_str(&raw).context("parse batcher config")?;
        if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
            file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
        }
        let log_cfg =
            LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
        let plane =
            StreamPlane::from_config(&log_cfg, "batcher").context("build the stream plane")?;
        let channels = log_cfg.channels;
        let mut aeron_cfg = log_cfg.aeron;
        if let Some(dir) = args.aeron_dir.as_ref() {
            aeron_cfg.aeron_dir.clone_from(dir);
        }
        Ok(Self {
            file_cfg,
            channels,
            aeron_cfg,
            plane,
        })
    }

    /// Open the `tx_data` and cluster `tx_ordering` subscriptions, and
    /// spawn their reader threads.
    fn spawn_reader_stack(
        &mut self,
        args: &LiveArgs,
        cursor: BatchCursor,
    ) -> Result<ReaderStack<impl Send + use<>>> {
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        let tx_data_subs = bin_support::open_tx_data_subs(&rt, &mut self.plane)?;
        let join_recovery = bin_support::archive_join_recovery(
            &self.channels,
            &self.aeron_cfg,
            args.aeron_dir.as_deref(),
            args.archive_control_response_endpoint.as_deref(),
            args.replay_destination_endpoint.as_deref(),
        );

        // A dedicated cluster runtime, exactly as in the executor and
        // validator. The cluster session must never contend with the
        // tx_data work on `rt`.
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
        info!("kardamom-batcher: tx_ordering via Aeron Cluster");

        let join_buffer = JoinBuffer::new();
        let join_handles = tx_data_subs
            .into_iter()
            .map(|sub| spawn_tx_data_reader(sub, join_buffer.clone()))
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
            ..ReaderConfig::default()
        };
        let ordering_handle = spawn_tx_ordering_reader(
            tx_ordering_sub,
            join_buffer,
            reader_cfg,
            feed_tx,
            TxIndex(cursor.next_index),
            join_recovery,
        );

        Ok(ReaderStack {
            handles: ReaderHandles {
                cluster_guard,
                join_handles,
                ordering_handle,
            },
            feed_rx,
        })
    }
}

/// The engine reader stack: the `tx_data` join-buffer readers, the cluster
/// ordering subscription, and the channel the feed loop reads from.
struct ReaderStack<G> {
    handles: ReaderHandles<G>,
    feed_rx: Receiver<ReaderToExec>,
}

/// The reader-thread handles, kept for post-failure diagnosis. The cluster
/// guard (`G`, kept opaque so this module names no direct dependency on
/// `kardamom-cluster-adapter`) must outlive the feed loop.
struct ReaderHandles<G> {
    // Held only for its Drop impl (closes the Aeron cluster session when
    // the handles are dropped after a feed failure); its value is never
    // read.
    #[allow(
        dead_code,
        reason = "held only for its Drop impl, which closes the Aeron cluster session"
    )]
    cluster_guard: G,
    join_handles: Vec<JoinHandle<Result<(), ExecutorError>>>,
    ordering_handle: JoinHandle<Result<(), ExecutorError>>,
}

impl<G> ReaderHandles<G> {
    /// The feed loop returns only on failure (channel closed, or a post
    /// that stopped it). Surface the reader threads' errors for context
    /// before propagating `feed_err`. The channel-closed case's root
    /// cause lives there.
    fn surface_errors(self, feed_err: anyhow::Error) -> anyhow::Error {
        warn!(error = %format!("{feed_err:#}"), "feed loop exited");
        if self.ordering_handle.is_finished()
            && let Ok(Err(re)) = self.ordering_handle.join()
        {
            return anyhow::anyhow!("tx_ordering reader failed: {re:#} (feed loop: {feed_err:#})");
        }
        self.join_handles
            .into_iter()
            .find_map(|h| Self::stream_reader_failure(h, &feed_err))
            .unwrap_or(feed_err)
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

/// Live service mode: the batcher as a third cluster-egress consumer.
/// Front-end wiring mirrors the validator: M `tx_data` subscriptions and
/// `tx_deposits` feed the join buffers, archive refetch runs on a join miss,
/// and the canonical ordering comes from the Aeron Cluster egress, with the
/// replay request seeded from the durable cursor. Runs until SIGTERM,
/// Ctrl-C, or a feed-loop failure that stops it.
///
/// # Errors
/// Returns an error when L1 setup, cursor reconcile, config parsing, or the
/// reader stack fails to start, or when the feed loop exits with a failure.
pub async fn run(args: LiveArgs) -> Result<()> {
    let l1 = args.start_l1_side().await?;
    let mut run_cfg = RunConfig::resolve(&args)?;
    let ReaderStack { handles, feed_rx } = run_cfg.spawn_reader_stack(&args, l1.cursor)?;

    let sender = LiveSender::new(
        l1.provider,
        args.settlement,
        l1.da_store,
        l1.l1_truth.last_batch_index,
        args.l1_retries,
        args.cursor_file,
    );
    let feed_cfg = FeedConfig {
        blocks_per_batch: args.blocks_per_batch,
        compress: args.compress,
        chain_id: args.chain_id,
        flush: Duration::from_millis(args.flush_ms.get()),
        skip_through_block: l1.skip_through_block,
    };
    let mut feed = tokio::spawn(run_feed(feed_rx, sender, feed_cfg));
    let feed_result = tokio::select! {
        r = &mut feed => r.context("feed task panicked")?,
        () = bin_support::wait_for_shutdown() => {
            // Exit cleanly. The cursor is reconciled against L1 truth on
            // every restart, so tearing down mid-batch loses nothing.
            info!("shutdown signal received; stopping live batcher");
            run_cfg.plane.shutdown().await;
            return Ok(());
        }
    };
    match feed_result {
        Ok(()) => Ok(()),
        Err(e) => Err(handles.surface_errors(e)),
    }
}
