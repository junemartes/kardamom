//! Live service wiring: CLI args, the reader stack, and the feed-loop task.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_signer_local::PrivateKeySigner;
use alloy_transport::layers::FallbackLayer;
use alloy_transport_http::Http;
use anyhow::{Context, Result, bail};
use kardamom_obs::halt::{self, Clears, Halt, HaltCause};
use metrics::gauge;
use tower::Layer;
use tracing::{info, warn};

use kardamom_engine::bin_support;

use crate::da::DaProxy;

use super::cursor::{BatchCursor, L1Truth, reconcile};
use super::events::EventsBeacon;
use super::feed::{FeedConfig, FeedLoop};
use super::live_metric_names;
use super::rebuild::{ArchiveRebuilder, Rebuilder};
use super::refs_store::RefsStore;
use super::sender::LiveSender;
use super::spool::{Restored, Spool};

mod readers;
use readers::{ReaderEnd, ReaderStack, RunConfig};

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
/// The nonce of each send comes from the pending transaction count of
/// L1. A local nonce count would advance on a send that failed before it
/// reached L1, for example on a failed gas estimate. Every later send
/// then leaves a nonce gap, and L1 never mines it. The sends are serial
/// ([`LiveSender`]), so two sends never read the same count.
///
/// # Errors
/// Returns an error when the key does not parse, when `rpcs` is empty,
/// or when an endpoint is not a URL.
pub fn connect_l1(rpcs: &[String], key: &str) -> Result<impl Provider + Clone + use<>> {
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
        .disable_recommended_fillers()
        .with_gas_estimation()
        .with_simple_nonce_management()
        .fetch_chain_id()
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
    /// Read the canonical stream from the egress at this base URL, not from
    /// the cluster egress.
    pub egress: Option<String>,
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
    /// The L1 follower's API. A batcher without a cursor file reads the
    /// last posted batch's payload from its archive, and the post age
    /// starts from the last post it holds.
    pub indexer_url: Option<String>,
    /// How long `l1_blocks` may carry no record before the batcher pauses
    /// with the follower as its root.
    pub l1_silence: Duration,
    /// The sealer's DA-lag budget, in blocks; 0 turns the guard off. A
    /// group is due at half of it.
    pub da_lag_budget_blocks: u64,
    /// The query endpoints of the executors and the validator
    /// (`http://host:port`). When the sealer refuses the replay, the
    /// references of the gap up to its floor are read from here, and the
    /// bytes from the `tx_data` archives. Empty: a refused replay raises the
    /// `replay_unavailable` halt.
    pub block_refs_sources: Vec<String>,
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

impl<P: Provider> L1Side<P> {
    fn feed(
        self,
        args: &LiveArgs,
        spool: Spool,
        restored: Restored,
        resume: BatchCursor,
    ) -> FeedLoop<P> {
        gauge!(live_metric_names::IDLE_FLUSH_SECONDS)
            .set(Duration::from_millis(args.idle_flush_ms.get()).as_secs_f64());
        let sender = LiveSender::new(
            self.provider,
            args.settlement,
            self.da,
            self.l1_truth.last_batch_index,
            args.l1_retries,
            args.cursor_file.clone(),
        );
        let feed_cfg = FeedConfig {
            blocks_per_batch: args.blocks_per_batch,
            compress: args.compress,
            chain_id: args.chain_id,
            flush: Duration::from_millis(args.flush_ms.get()),
            idle_flush: Duration::from_millis(args.idle_flush_ms.get()),
            target_payload_bytes: args.target_payload_bytes,
            skip_through_block: resume.skip_through(self.skip_through_block),
            da_lag_due: NonZeroU64::new(args.da_lag_budget_blocks / 2),
        };
        // The sealer and the ingress learn the confirmed cursor before the
        // first post: each reader stack publishes the current value first.
        FeedLoop::new(
            sender,
            feed_cfg,
            spool,
            restored,
            self.l1_truth.covered_through_block,
        )
    }
}

impl LiveArgs {
    /// Connect to L1 and reconcile the durable cursor against it. The L1
    /// reads retry until L1 answers, under the `l1_unreachable` halt; the
    /// reconcile of a cursor file that is ahead of L1 is a refusal,
    /// decided once. The failure classes are typed at this boundary, so
    /// the service holds the right halt for each.
    async fn start_l1_side(&self) -> Result<L1Side<impl Provider + Clone + 'static>, StartError> {
        let provider = connect_l1(&self.rpcs, &self.key).map_err(StartError::L1)?;
        let da = DaProxy::new(&self.da_proxy).map_err(|e| StartError::Resume(e.into()))?;
        let loaded = BatchCursor::load(&self.cursor_file).map_err(StartError::Resume)?;
        let resumed = self.resume_until_l1_answers(&provider, loaded).await;
        let l1_truth = resumed.l1_truth;
        let (cursor, skip_through_block) = match resumed.rebuilt {
            Some(rebuilt) => rebuilt,
            None => reconcile(loaded, l1_truth).map_err(StartError::Resume)?,
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
            Spool::count_dropped("discontinuous");
            spool.clear_through(u64::MAX)?;
        }
        return Ok((Restored::empty(), cursor));
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
/// cursor, and a rebuild from the state databases' references and the
/// `tx_data` archives for a gap the sealer no longer retains. A refused
/// replay is answered once: the rebuild fills the gap up to the sealer's
/// floor, and the stack starts again at the floor.
///
/// A failure the batcher can wait out is a halt, not an end: the process
/// stays up, serves its metrics and the `/halt` record, and starts again
/// when the halt clears. An `l1_unreachable` halt retries on a backoff.
/// A `replay_unavailable` halt (a refused resume, a gap the rebuild cannot
/// fill, or a second refusal after the rebuild) waits for the operator's
/// clear, after the range is recovered or the chain reverted.
///
/// # Errors
/// Returns an error when config parsing or the reader stack fails to
/// start, or when the feed loop exits with a failure no halt names.
pub async fn run(args: LiveArgs) -> Result<()> {
    let events = EventsBeacon::open(&args).await?;
    let ended = run_until_shutdown(&args).await;
    events.close().await;
    ended
}

/// Start, hold each halt, and start again, until the shutdown signal or
/// a failure no halt names.
async fn run_until_shutdown(args: &LiveArgs) -> Result<()> {
    loop {
        match run_once(args).await? {
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

/// One start of the service: L1, the spool, the reader stacks, and the
/// feed loop, until the shutdown signal or a halt.
async fn run_once(args: &LiveArgs) -> Result<RunEnd> {
    let l1 = match args.start_l1_side().await {
        Ok(l1) => l1,
        Err(e) => return Ok(RunEnd::Halted(e.halt())),
    };
    let spool = Spool::open(&args.spool_dir)?;
    let (restored, resume) = continue_from_spool(&spool, l1.cursor, l1.skip_through_block)?;
    let run_cfg = RunConfig::resolve(args)?;

    let feed = l1.feed(args, spool, restored, resume);
    let mut service = Service { args, run_cfg };
    let end = match service.serve(feed, resume).await? {
        Served::Refused { feed, oldest_block } => {
            service
                .recover_and_serve(feed, resume, oldest_block)
                .await?
        }
        served => served.into_end(),
    };
    service.run_cfg.plane.shutdown().await;
    if matches!(end, RunEnd::Shutdown) {
        // The cursor is reconciled against L1 truth on every restart, so
        // tearing down mid-batch loses nothing.
        info!("shutdown signal received; stopping live batcher");
    }
    Ok(end)
}

/// Rebuild the gap between the cursor and the sealer's floor into the
/// feed loop: the references from the state databases, the bytes from the
/// archives through `rebuilder`. Returns the cursor the reader resumes
/// at: the end of the floor block, which the sealer holds.
///
/// # Errors
/// Returns an error when no query endpoint is configured, when a block
/// of the gap is not served, when a block does not continue its
/// predecessor, or when the rebuild fails.
async fn recover_from_refs<P: Provider, R: Rebuilder>(
    store: &RefsStore,
    rebuilder: R,
    feed: &mut FeedLoop<P>,
    resume: BatchCursor,
    oldest_block: u64,
) -> Result<BatchCursor> {
    if store.is_empty() {
        bail!(
            "the sealer refused the replay from block {} (its oldest retained block is \
             {oldest_block}) and no --block-refs-source is set; the gap is not recoverable \
             from here",
            resume.next_block
        );
    }
    warn!(
        from_block = resume.next_block,
        oldest_block,
        "sealer replay refused; rebuilding the gap from the state databases and the archives"
    );
    let refs = store.blocks(resume, oldest_block).await?;
    // The archive client is thread-bound and blocks on each replay; the
    // runtime's other tasks keep running on their own workers.
    let blocks = tokio::task::block_in_place(|| rebuilder.rebuild(refs))?;
    let resumed = feed.absorb(blocks)?;
    info!(
        replay_from_index = resumed.next_index,
        replay_from_block = resumed.next_block,
        "gap rebuilt; resuming at the sealer's floor"
    );
    Ok(resumed)
}

/// What one reader stack did: ran until shutdown, ended on a halt, or
/// ended on a refused replay with the feed loop handed back for the
/// recovery. The loop is boxed: it is the one large value, and the other
/// variants carry none.
enum Served<P> {
    Done,
    Halted(Halt),
    Refused {
        feed: Box<FeedLoop<P>>,
        oldest_block: u64,
    },
}

impl<P> Served<P> {
    /// How the run ends after this stack, when no recovery follows. A
    /// refusal here is the second one: the rebuild already ran, so the
    /// operator decides.
    fn into_end(self) -> RunEnd {
        match self {
            Self::Done => RunEnd::Shutdown,
            Self::Halted(halt) => RunEnd::Halted(halt),
            Self::Refused { oldest_block, .. } => RunEnd::Halted(Halt::new(
                HaltCause::ReplayUnavailable,
                format!(
                    "the sealer refused the replay again after the rebuild filled the gap: its \
                     floor moved to block {oldest_block}; a restart rebuilds the new gap"
                ),
            )),
        }
    }
}

/// The live service's fixed parts across reader stacks: the arguments
/// and the resolved config.
struct Service<'a> {
    args: &'a LiveArgs,
    run_cfg: RunConfig,
}

impl Service<'_> {
    /// Run `feed` on a reader stack that resumes at `resume`, until
    /// shutdown or a failure. A refused replay hands the feed loop back;
    /// a post that failed on every attempt is a halt; every other failure
    /// is the error. The caller ends the plane.
    async fn serve<P: Provider + 'static>(
        &mut self,
        feed: FeedLoop<P>,
        resume: BatchCursor,
    ) -> Result<Served<P>> {
        let ReaderStack {
            handles,
            feed_rx,
            posted_cursor,
        } = self.run_cfg.spawn_reader_stack(self.args, resume).await?;
        // The start reached the stream: a held halt is over.
        halt::clear();
        let follower = posted_cursor.follow(feed.posted());
        let mut task = tokio::spawn(async move {
            let mut feed = feed;
            let why = feed.run(feed_rx).await;
            (feed, why)
        });
        metrics::gauge!(super::live_metric_names::FEED_RUNNING).set(1.0);
        let ended = tokio::select! {
            r = &mut task => r.context("feed task panicked"),
            () = bin_support::wait_for_shutdown() => {
                follower.abort();
                return Ok(Served::Done);
            }
        };
        follower.abort();
        let (feed, why) = ended?;
        match handles.end(why) {
            ReaderEnd::ReplayRefused { oldest_block } => Ok(Served::Refused {
                feed: Box::new(feed),
                oldest_block,
            }),
            ReaderEnd::Halted(halt) => Ok(Served::Halted(halt)),
            ReaderEnd::Failed(e) => Err(e),
        }
    }

    /// Answer a refused replay: rebuild the gap from the references and
    /// the archives, then serve again from the sealer's floor. A gap the
    /// rebuild cannot fill is the `replay_unavailable` halt.
    async fn recover_and_serve<P: Provider + 'static>(
        &mut self,
        mut feed: Box<FeedLoop<P>>,
        resume: BatchCursor,
        oldest_block: u64,
    ) -> Result<RunEnd> {
        match self.rebuild(&mut feed, resume, oldest_block).await {
            Ok(resume) => Ok(self.serve(*feed, resume).await?.into_end()),
            Err(e) => Ok(RunEnd::Halted(Halt::new(
                HaltCause::ReplayUnavailable,
                format!("{e:#}"),
            ))),
        }
    }

    /// The rebuild of [`Self::recover_and_serve`]: the archive client,
    /// then [`recover_from_refs`].
    async fn rebuild<P: Provider>(
        &mut self,
        feed: &mut FeedLoop<P>,
        resume: BatchCursor,
        oldest_block: u64,
    ) -> Result<BatchCursor> {
        let factory = self.run_cfg.join_recovery(self.args).context(
            "the sealer refused the replay, and the rebuild from references needs the \
             tx_data archives: --replay-destination-endpoint, \
             --archive-control-response-endpoint and the archive endpoints of channels.toml",
        )?;
        let store = RefsStore::new(self.args.block_refs_sources.clone());
        recover_from_refs(
            &store,
            ArchiveRebuilder { factory },
            feed,
            resume,
            oldest_block,
        )
        .await
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;

#[cfg(test)]
mod null_receipts_tests;
#[cfg(test)]
mod prune_floor_tests;
