//! Live service wiring: CLI args, the reader stack, and the feed-loop task.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_signer_local::PrivateKeySigner;
use alloy_transport::layers::FallbackLayer;
use alloy_transport_http::Http;
use anyhow::{Context, Result, bail};
use tokio::sync::mpsc::Receiver;
use tower::Layer;
use tracing::{info, warn};

use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_engine::reader::{
    JoinBuffer, ReaderConfig, ReaderToExec, TxDataReader, TxOrderingInputs, TxOrderingReader,
};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, LogConfig};
use kardamom_log::discovery::StreamPlane;

use crate::da::DaProxy;

use super::cursor::{BatchCursor, L1Truth, read_l1_truth, reconcile, resume_from_indexer};
use super::payload_store::PayloadStore;
use super::spool::{Restored, Spool};
use crate::indexer::IndexerClient;

/// How a start waits for the indexer to reach the last posted batch: the
/// indexer follows the finalized L1, about 13 minutes behind the head on
/// Ethereum, and a batch posted just before the start is not there yet.
const INDEXER_POLL: Duration = Duration::from_secs(12);
const INDEXER_POLLS: u32 = 100;
use super::feed::{FeedConfig, FeedLoop};
use super::sender::LiveSender;

/// Top-level config the batcher reads from `--config` in live mode. It uses
/// the same `[cluster]` section shape as the executor and the validator.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub(crate) struct BatcherFileConfig {
    pub cluster: kardamom_engine::reader::cluster::ClusterConfig,
}

/// The methods the batcher sends, tried one endpoint at a time in score
/// order, so a failed or rate-limited endpoint falls back to the next
/// one inside the request. A method outside this set fans out to every
/// endpoint and takes the first answer, which is wrong for a nonce or a
/// send: two endpoints at different heights answer differently.
const SEQUENTIAL_METHODS: &[&str] = &[
    "eth_blockNumber",
    "eth_call",
    "eth_chainId",
    "eth_estimateGas",
    "eth_feeHistory",
    "eth_gasPrice",
    "eth_getBlockByNumber",
    "eth_getLogs",
    "eth_getTransactionCount",
    "eth_getTransactionReceipt",
    "eth_maxPriorityFeePerGas",
    "eth_sendRawTransaction",
    "eth_sendRawTransactionSync",
];

/// Parse the batcher key. Connect the wallet-backed L1 provider over
/// `rpcs`, and the local DA blob store. The live service and the offline
/// `--dry-run=false` post path share this signer, provider, and
/// blob-store setup.
///
/// With more than one endpoint, every request goes to the best-scored
/// endpoint first and falls back to the next on an error or an HTTP 429;
/// the scores rank a failing endpoint last for the requests after it.
///
/// # Errors
/// Returns an error when the key does not parse, when `rpcs` is empty,
/// or when an endpoint is not a URL.
pub fn connect_l1(rpcs: &[String], key: &str) -> Result<impl Provider + 'static> {
    let signer: PrivateKeySigner = key.parse().context("parse --l1-key")?;
    let transports = rpcs
        .iter()
        .map(|rpc| {
            rpc.parse::<reqwest::Url>()
                .map(Http::new)
                .with_context(|| format!("--l1-rpc {rpc} is not a URL"))
        })
        .collect::<Result<Vec<_>>>()?;
    let count = NonZeroUsize::new(transports.len()).context("--l1-rpc names no endpoint")?;
    let service = FallbackLayer::default()
        .with_active_transport_count(count)
        .with_sequential_methods(SEQUENTIAL_METHODS.iter().map(ToString::to_string).collect())
        .layer(transports);
    Ok(ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_client(RpcClient::new(service, false)))
}

/// Everything [`run`] needs from the CLI, already validated. The binary
/// checks the L1 flag tuple, `--config`, and `--cursor-file` presence
/// first, so its error messages can name the exact flag combination.
#[derive(Debug, Clone)]
pub struct LiveArgs {
    /// The L1 endpoints, best first. See [`connect_l1`].
    pub rpcs: Vec<String>,
    pub key: String,
    pub settlement: Address,
    /// The EigenDA proxy's URL (`http://host:port`).
    pub da_proxy: String,
    /// TOML supplying the `[cluster]` section ([`BatcherFileConfig`]).
    pub config: PathBuf,
    pub cursor_file: PathBuf,
    /// The spool of consumed, unposted blocks (`spool`).
    pub spool_dir: PathBuf,
    pub log_config: Option<PathBuf>,
    pub aeron_dir: Option<PathBuf>,
    /// The L2 chain id. See [`BatcherConfig::chain_id`].
    pub chain_id: u64,
    pub cluster_egress_endpoint: Option<String>,
    /// This batcher's voter id at the sealer; see
    /// [`ReaderConfig::voter_id`]. `None` never votes.
    pub void_voter_id: Option<u8>,
    pub replay_destination_endpoint: Option<String>,
    pub archive_control_response_endpoint: Option<String>,
    pub blocks_per_batch: NonZeroUsize,
    pub compress: bool,
    /// Post a partial group if the oldest pending block has waited this
    /// long. Nonzero at the type level: 0 makes the flush timeout expire
    /// at once, a busy loop.
    pub flush_ms: NonZeroU64,
    /// The flush wait for a group of empty blocks. See
    /// [`FeedConfig::idle_flush`].
    pub idle_flush_ms: NonZeroU64,
    /// See [`FeedConfig::target_payload_bytes`].
    pub target_payload_bytes: NonZeroUsize,
    pub l1_retries: u32,
    /// The inbox indexer's API. With it, a batcher without a cursor file
    /// resumes just past the last posted batch, and no start reads
    /// `BatchPosted` events from L1. Without it, a missing cursor file
    /// replays from genesis.
    pub indexer_url: Option<String>,
    /// The settlement contract's deployment block: where a `BatchPosted`
    /// scan starts when no indexer serves it.
    pub settlement_deploy_block: u64,
    /// The query endpoints of the executors and the validator
    /// (`http://host:port`), the block payload store. When the sealer
    /// refuses the replay, the gap up to its floor is read from here.
    /// Empty: a refused replay is a fail-stop.
    pub payload_sources: Vec<String>,
}

/// [`start_l1_side`]'s resolved view: the provider, the blob store, L1's
/// truth, the cursor to replay from, and the block to skip through
/// (already covered by L1).
struct L1Side<P> {
    provider: P,
    da: DaProxy,
    l1_truth: L1Truth,
    cursor: BatchCursor,
    skip_through_block: u64,
}

impl LiveArgs {
    /// Connect to L1 and reconcile the durable cursor against it.
    async fn start_l1_side(&self) -> Result<L1Side<impl Provider + 'static>> {
        let provider = connect_l1(&self.rpcs, &self.key)?;
        let da = DaProxy::new(&self.da_proxy)?;
        let indexer = self.indexer_url.as_deref().map(IndexerClient::new);
        let l1_truth = match &indexer {
            Some(ix) => {
                L1Truth::read_via_indexer(
                    &provider,
                    self.settlement,
                    ix,
                    INDEXER_POLL,
                    INDEXER_POLLS,
                )
                .await?
            }
            None => read_l1_truth(&provider, self.settlement, self.settlement_deploy_block).await?,
        };
        let (cursor, skip_through_block) = match (BatchCursor::load(&self.cursor_file)?, &indexer) {
            (None, Some(ix)) if l1_truth.last_batch_index > 0 => {
                resume_from_indexer(ix, l1_truth).await?
            }
            (loaded, _) => reconcile(loaded, l1_truth)?,
        };
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
            da,
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

    /// Open the `tx_data` and cluster `tx_ordering` subscriptions, and
    /// spawn their reader threads.
    /// Replace the static `[cluster]` ingress endpoints with the members
    /// the catalog lists, when discovery is on and lists any.
    async fn resolve_cluster_ingress(&mut self) -> Result<()> {
        if let Some(endpoints) = self.plane.cluster_ingress_endpoints().await? {
            self.file_cfg.cluster.ingress_endpoints = endpoints;
        }
        Ok(())
    }

    fn spawn_reader_stack(
        &mut self,
        args: &LiveArgs,
        cursor: BatchCursor,
    ) -> Result<ReaderStack<impl Send + use<>>> {
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        let tx_data_subs = bin_support::open_tx_data_subs(&rt, &mut self.plane)?;
        let join_recovery = bin_support::archive_join_recovery(
            &mut self.plane,
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
    cluster_guard: G,
    join_handles: Vec<JoinHandle<Result<(), ExecutorError>>>,
    ordering_handle: JoinHandle<Result<(), ExecutorError>>,
}

/// Why a reader stack ended, as [`ReaderHandles::end`] classifies it.
enum ReaderEnd {
    /// The sealer refused the replay: the cursor is below its retention
    /// floor, and `oldest_block` is the oldest block it still holds.
    ReplayRefused { oldest_block: u64 },
    /// Every other failure, with the reader thread's error for context.
    Failed(anyhow::Error),
}

impl<G> ReaderHandles<G> {
    /// The feed loop returns only on failure (channel closed, or a post
    /// that stopped it). Classify the end from the reader threads'
    /// errors: the channel-closed case's root cause lives there. The
    /// handles drop here, so the cluster session and the runtime close
    /// before a new stack opens.
    fn end(self, feed_err: anyhow::Error) -> ReaderEnd {
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

/// Live service mode: the batcher as a third cluster-egress consumer.
/// Front-end wiring mirrors the validator: M `tx_data` subscriptions and
/// `tx_deposits` feed the join buffers, archive refetch runs on a join miss,
/// and the canonical ordering comes from the Aeron Cluster egress, with the
/// replay request seeded from the durable cursor. Runs until SIGTERM,
/// What a restart continues: the spool's blocks, when they continue the
/// confirmed cursor, and the cursor the reader resumes at. A spool that
/// starts elsewhere (a cursor file restored from a backup, a spool from
/// another chain) is dropped, and the reader resumes at the confirmed
/// cursor. Blocks L1 already covers (through `skip_through_block`) are
/// dropped first: a post that confirmed after the spool write.
fn continue_from_spool(
    spool: &Spool,
    cursor: BatchCursor,
    skip_through_block: u64,
) -> Result<(Restored, BatchCursor)> {
    spool.clear_through(skip_through_block)?;
    let restored = spool.load()?;
    let expected_first = cursor.next_block.max(skip_through_block.saturating_add(1));
    let continues = restored
        .blocks
        .first()
        .is_some_and(|first| first.block_number == expected_first)
        && restored
            .blocks
            .windows(2)
            .all(|w| w[1].block_number == w[0].block_number.saturating_add(1));
    if !continues {
        if let Some(first) = restored.blocks.first() {
            warn!(
                spool_first = first.block_number,
                expected_first, "spool does not continue the confirmed cursor; dropping it"
            );
            spool.clear_through(u64::MAX)?;
        }
        return Ok((
            Restored {
                blocks: Vec::new(),
                oldest_written: None,
            },
            cursor,
        ));
    }
    let resume = restored.blocks.last().map_or(cursor, |last| BatchCursor {
        next_index: last.end_tx_idx.as_index(),
        next_block: last.block_number.saturating_add(1),
        last_batch_index: cursor.last_batch_index,
    });
    Ok((restored, resume))
}

/// Ctrl-C, or a feed-loop failure that stops it.
///
/// The resume sources, in order: the spool, the sealer's replay from the
/// cursor, and the block payload store for a gap the sealer no longer
/// retains. A refused replay is answered once: the store fills the gap
/// up to the sealer's floor, and the stack starts again at the floor. A
/// second refusal is a fail-stop.
///
/// # Errors
/// Returns an error when L1 setup, cursor reconcile, config parsing, or the
/// reader stack fails to start, or when the feed loop exits with a failure.
pub async fn run(args: LiveArgs) -> Result<()> {
    let l1 = args.start_l1_side().await?;
    let spool = Spool::open(&args.spool_dir)?;
    let (restored, resume) = continue_from_spool(&spool, l1.cursor, l1.skip_through_block)?;
    let mut run_cfg = RunConfig::resolve(&args)?;
    run_cfg.resolve_cluster_ingress().await?;

    let sender = LiveSender::new(
        l1.provider,
        args.settlement,
        l1.da,
        l1.l1_truth.last_batch_index,
        args.l1_retries,
        args.cursor_file.clone(),
        args.settlement_deploy_block,
    );
    let feed_cfg = FeedConfig {
        blocks_per_batch: args.blocks_per_batch,
        compress: args.compress,
        chain_id: args.chain_id,
        flush: Duration::from_millis(args.flush_ms.get()),
        idle_flush: Duration::from_millis(args.idle_flush_ms.get()),
        target_payload_bytes: args.target_payload_bytes,
        skip_through_block: resume.next_block.saturating_sub(1),
    };
    let feed = FeedLoop::new(sender, feed_cfg, spool, restored);
    let store = PayloadStore::new(args.payload_sources.clone());
    let mut service = Service { args, run_cfg };
    let served = match service.serve(feed, resume).await? {
        Served::Done => Served::Done,
        Served::Refused {
            mut feed,
            oldest_block,
        } => {
            let resume = recover_from_store(&store, &mut feed, resume, oldest_block).await?;
            service.serve(*feed, resume).await?
        }
    };
    if let Served::Refused { oldest_block, .. } = served {
        bail!(
            "the sealer refused the replay again after the store filled the gap: its floor \
             moved to block {oldest_block}; a restart recovers the new gap"
        );
    }
    // Exit cleanly. The cursor is reconciled against L1 truth on every
    // restart, so tearing down mid-batch loses nothing.
    info!("shutdown signal received; stopping live batcher");
    service.run_cfg.plane.shutdown().await;
    Ok(())
}

/// Read the gap between the cursor and the sealer's floor from the
/// payload store into the feed loop, and return the cursor the reader
/// resumes at: the end of the floor block, which the sealer holds.
///
/// # Errors
/// Returns an error when no payload source is configured, when a block
/// of the gap is not served, or when a block does not continue its
/// predecessor.
async fn recover_from_store<P: Provider>(
    store: &PayloadStore,
    feed: &mut FeedLoop<P>,
    resume: BatchCursor,
    oldest_block: u64,
) -> Result<BatchCursor> {
    if store.is_empty() {
        bail!(
            "the sealer refused the replay from block {} (its oldest retained block is \
             {oldest_block}) and no --payload-source is set; the gap is not recoverable \
             from here",
            resume.next_block
        );
    }
    warn!(
        from_block = resume.next_block,
        oldest_block, "sealer replay refused; recovering the gap from the payload store"
    );
    let blocks = store.blocks(resume, oldest_block).await?;
    let resumed = feed.absorb(blocks)?;
    info!(
        replay_from_index = resumed.next_index,
        replay_from_block = resumed.next_block,
        "gap recovered from the payload store; resuming at the sealer's floor"
    );
    Ok(resumed)
}

/// What one reader stack did: ran until shutdown, or ended on a refused
/// replay with the feed loop handed back for the recovery. The loop is
/// boxed: it is the one large value, and `Done` carries none.
enum Served<P> {
    Done,
    Refused {
        feed: Box<FeedLoop<P>>,
        oldest_block: u64,
    },
}

/// The live service's fixed parts across reader stacks: the arguments
/// and the resolved config.
struct Service {
    args: LiveArgs,
    run_cfg: RunConfig,
}

impl Service {
    /// Run `feed` on a reader stack that resumes at `resume`, until
    /// shutdown or a failure. A refused replay hands the feed loop back;
    /// every other failure is the error. The caller ends the plane after
    /// a shutdown.
    async fn serve<P: Provider + 'static>(
        &mut self,
        feed: FeedLoop<P>,
        resume: BatchCursor,
    ) -> Result<Served<P>> {
        let ReaderStack { handles, feed_rx } =
            self.run_cfg.spawn_reader_stack(&self.args, resume)?;
        let mut task = tokio::spawn(async move {
            let mut feed = feed;
            let why = feed.run(feed_rx).await;
            (feed, why)
        });
        let (feed, why) = tokio::select! {
            r = &mut task => r.context("feed task panicked")?,
            () = bin_support::wait_for_shutdown() => return Ok(Served::Done),
        };
        match handles.end(why) {
            ReaderEnd::ReplayRefused { oldest_block } => Ok(Served::Refused {
                feed: Box::new(feed),
                oldest_block,
            }),
            ReaderEnd::Failed(e) => Err(e),
        }
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
