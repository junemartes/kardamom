//! Live service wiring: CLI args, the reader stack, and the feed-loop task.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use kardamom_obs::halt::{self, Clears, Halt, HaltCause};
use tokio::sync::mpsc::Receiver;
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
use super::spool::{Restored, Spool};
use crate::indexer::IndexerClient;

/// How a start waits for the indexer to reach the last posted batch: the
/// indexer follows the finalized L1, about 13 minutes behind the head on
/// Ethereum, and a batch posted just before the start is not there yet.
const INDEXER_POLL: Duration = Duration::from_secs(12);
const INDEXER_POLLS: u32 = 100;
use super::feed::{FeedConfig, FeedLoop};
use super::posted_cursor::PostedCursor;
use super::sender::{LiveSender, PostExhausted};

/// How long an `l1_unreachable` halt waits before the batcher starts
/// again: long enough for a rate limit to lift, short enough that a post
/// follows an L1 recovery within a flush.
const HALT_RETRY: Duration = Duration::from_secs(30);

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
pub async fn connect_l1(rpc: &str, key: &str) -> Result<impl Provider + 'static> {
    let signer: PrivateKeySigner = key.parse().context("parse --l1-key")?;
    ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect(rpc)
        .await
        .with_context(|| format!("connect L1 RPC {rpc}"))
}

/// Everything [`run`] needs from the CLI, already validated. The binary
/// checks the L1 flag tuple, `--config`, and `--cursor-file` presence
/// first, so its error messages can name the exact flag combination.
#[derive(Debug, Clone)]
pub struct LiveArgs {
    pub rpc: String,
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
}

/// Why a start did not reach a running feed: L1 (or the indexer in front
/// of it) did not answer, or the resume was refused. The first clears by
/// itself when L1 answers; the second waits for an operator, who recovers
/// the range or reverts the chain.
enum StartError {
    L1(anyhow::Error),
    Resume(anyhow::Error),
}

impl StartError {
    /// The halt this start failure puts the batcher in.
    fn halt(&self) -> Halt {
        match self {
            Self::L1(e) => Halt::new(HaltCause::L1Unreachable, format!("{e:#}")),
            Self::Resume(e) => Halt::new(HaltCause::ReplayUnavailable, format!("{e:#}")),
        }
    }
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
    /// Connect to L1 and reconcile the durable cursor against it. The two
    /// failure classes are typed at this boundary, so the service holds
    /// the right halt for each.
    async fn start_l1_side(&self) -> Result<L1Side<impl Provider + 'static>, StartError> {
        let provider = connect_l1(&self.rpc, &self.key)
            .await
            .map_err(StartError::L1)?;
        let da = DaProxy::new(&self.da_proxy).map_err(|e| StartError::Resume(e.into()))?;
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
                .await
            }
            None => read_l1_truth(&provider, self.settlement, self.settlement_deploy_block).await,
        }
        .map_err(StartError::L1)?;
        let loaded = BatchCursor::load(&self.cursor_file).map_err(StartError::Resume)?;
        let (cursor, skip_through_block) = match (loaded, &indexer) {
            (None, Some(ix)) if l1_truth.last_batch_index > 0 => resume_from_indexer(ix, l1_truth)
                .await
                .map_err(StartError::Resume)?,
            (loaded, _) => reconcile(loaded, l1_truth).map_err(StartError::Resume)?,
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
        let posted_cursor = PostedCursor::new(tx_ordering_sub.posted_cursor_publisher());
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
            posted_cursor,
        })
    }
}

/// The engine reader stack: the `tx_data` join-buffer readers, the cluster
/// ordering subscription, the channel the feed loop reads from, and the
/// posted-cursor publisher over the same cluster session.
struct ReaderStack<G> {
    handles: ReaderHandles<G>,
    feed_rx: Receiver<ReaderToExec>,
    posted_cursor: PostedCursor,
}

/// How a run ended short of a shutdown: the halt to hold before the
/// next start, or the error that ends the process.
struct RunFailure {
    halt: Option<Halt>,
    error: anyhow::Error,
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

impl<G> ReaderHandles<G> {
    /// The feed loop returns only on failure (channel closed, or a post
    /// that stopped it). Surface the reader threads' errors for context
    /// before propagating `feed_err`. The channel-closed case's root
    /// cause lives there. Two failures are halts, not ends: a refused
    /// replay from the cursor (`replay_unavailable`), and a post that
    /// failed on every attempt (`l1_unreachable`).
    fn surface_errors(self, feed_err: anyhow::Error) -> RunFailure {
        warn!(error = %format!("{feed_err:#}"), "feed loop exited");
        if self.ordering_handle.is_finished()
            && let Ok(Err(re)) = self.ordering_handle.join()
        {
            let halt = matches!(re, ExecutorError::ClusterReplayUnavailable { .. })
                .then(|| Halt::new(HaltCause::ReplayUnavailable, re.to_string()));
            return RunFailure {
                halt,
                error: anyhow::anyhow!(
                    "tx_ordering reader failed: {re:#} (feed loop: {feed_err:#})"
                ),
            };
        }
        let halt = feed_err
            .chain()
            .any(|c| c.downcast_ref::<PostExhausted>().is_some())
            .then(|| Halt::new(HaltCause::L1Unreachable, format!("{feed_err:#}")));
        let error = self
            .join_handles
            .into_iter()
            .find_map(|h| Self::stream_reader_failure(h, &feed_err))
            .unwrap_or(feed_err);
        RunFailure { halt, error }
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
/// A failure the batcher can wait out is a halt, not an end: the process
/// stays up, serves its metrics and the `/halt` record, and starts again
/// when the halt clears. An `l1_unreachable` halt retries on a backoff;
/// a `replay_unavailable` halt waits for the operator's clear, after the
/// range is recovered or the chain reverted.
///
/// # Errors
/// Returns an error when config parsing or the reader stack fails to
/// start, or when the feed loop exits with a failure no halt names.
pub async fn run(args: LiveArgs) -> Result<()> {
    loop {
        match run_once(&args).await? {
            RunEnd::Shutdown => return Ok(()),
            RunEnd::Halted(halt) => {
                if hold(halt).await.is_break() {
                    return Ok(());
                }
            }
        }
    }
}

/// How one run ended: the shutdown signal, or a halt to hold.
enum RunEnd {
    Shutdown,
    Halted(Halt),
}

/// Hold `halt` until the batcher may start again: one backoff for a halt
/// that clears by itself, the operator's clear for the other. `Break` is
/// the shutdown signal during the hold.
async fn hold(halt: Halt) -> ControlFlow<()> {
    let clears = halt.clears;
    halt::raise(halt);
    tokio::select! {
        () = bin_support::wait_for_shutdown() => {
            info!("shutdown signal received while halted; stopping live batcher");
            ControlFlow::Break(())
        }
        () = wait_to_retry(clears) => ControlFlow::Continue(()),
    }
}

/// When a held halt is tried again.
async fn wait_to_retry(clears: Clears) {
    match clears {
        Clears::Auto => tokio::time::sleep(HALT_RETRY).await,
        Clears::Operator => halt::cleared().await,
    }
}

/// One start of the service: L1, the spool, the reader stack, and the
/// feed loop, until the shutdown signal or a failure.
async fn run_once(args: &LiveArgs) -> Result<RunEnd> {
    let l1 = match args.start_l1_side().await {
        Ok(l1) => l1,
        Err(e) => return Ok(RunEnd::Halted(e.halt())),
    };
    let spool = Spool::open(&args.spool_dir)?;
    let (restored, resume) = continue_from_spool(&spool, l1.cursor, l1.skip_through_block)?;
    let mut run_cfg = RunConfig::resolve(args)?;
    run_cfg.resolve_cluster_ingress().await?;
    let ReaderStack {
        handles,
        feed_rx,
        posted_cursor,
    } = run_cfg.spawn_reader_stack(args, resume)?;
    // The start reached the stream: a held halt is over. The sealer and
    // the ingress learn the confirmed cursor before the first post.
    halt::clear();
    posted_cursor
        .publish(l1.l1_truth.covered_through_block)
        .await?;

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
    let mut feed = tokio::spawn(
        FeedLoop::new(feed_rx, sender, feed_cfg, spool, restored, posted_cursor).run(),
    );
    let feed_result = tokio::select! {
        r = &mut feed => r.context("feed task panicked")?,
        () = bin_support::wait_for_shutdown() => {
            // Exit cleanly. The cursor is reconciled against L1 truth on
            // every restart, so tearing down mid-batch loses nothing.
            info!("shutdown signal received; stopping live batcher");
            run_cfg.plane.shutdown().await;
            return Ok(RunEnd::Shutdown);
        }
    };
    let Err(e) = feed_result else {
        return Ok(RunEnd::Shutdown);
    };
    let RunFailure { halt, error } = handles.surface_errors(e);
    run_cfg.plane.shutdown().await;
    match halt {
        Some(halt) => Ok(RunEnd::Halted(halt)),
        None => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use kardamom_types::BPosition;

    /// L1 silence clears by itself; a refused resume waits for an operator.
    #[test]
    fn a_start_failure_names_its_halt() {
        let l1 = StartError::L1(anyhow::anyhow!("connect L1 RPC http://l1: refused"));
        let halt = l1.halt();
        assert_eq!(halt.cause, HaltCause::L1Unreachable);
        assert!(halt.detail.contains("connect L1 RPC"), "{}", halt.detail);
        assert_eq!(halt.clears, kardamom_obs::halt::Clears::Auto);

        let resume = StartError::Resume(anyhow::anyhow!("spool does not continue the cursor"));
        let halt = resume.halt();
        assert_eq!(halt.cause, HaltCause::ReplayUnavailable);
        assert_eq!(halt.clears, kardamom_obs::halt::Clears::Operator);
    }

    use super::*;
    use crate::batch::ClosedBlock;

    fn block(number: u64, end: i32) -> ClosedBlock {
        ClosedBlock {
            block_number: number,
            l2_timestamp: 0,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: end,
            },
            l1_origin: 0,
            remote_epochs: Vec::new(),
            txs: Vec::new(),
        }
    }

    fn cursor(next_index: u64, next_block: u64) -> BatchCursor {
        BatchCursor {
            next_index,
            next_block,
            last_batch_index: 3,
        }
    }

    #[test]
    fn a_spool_that_continues_the_cursor_moves_the_resume_point() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        for n in 11..=13 {
            spool
                .append(&block(n, 100 + i32::try_from(n).unwrap()))
                .unwrap();
        }
        let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
        assert_eq!(restored.blocks.len(), 3);
        assert_eq!(resume.next_block, 14);
        assert_eq!(resume.next_index, block(13, 113).end_tx_idx.as_index());
        assert_eq!(resume.last_batch_index, 3);
    }

    #[test]
    fn a_spool_behind_a_confirmed_post_drops_the_covered_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        for n in 11..=13 {
            spool.append(&block(n, 0)).unwrap();
        }
        // L1 covers through 12 (a post confirmed after the spool write).
        let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 12).unwrap();
        assert_eq!(restored.blocks.len(), 1);
        assert_eq!(restored.blocks[0].block_number, 13);
        assert_eq!(resume.next_block, 14);
    }

    #[test]
    fn a_spool_that_does_not_continue_the_cursor_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        spool.append(&block(20, 0)).unwrap();
        let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
        assert!(restored.blocks.is_empty());
        assert_eq!(resume, cursor(90, 11));
        assert!(spool.load().unwrap().blocks.is_empty());
    }
}
