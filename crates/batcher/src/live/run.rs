//! Live service wiring: CLI args, the reader stack, and the feed-loop task.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use metrics::{counter, gauge};
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

use super::cursor::{BatchCursor, L1Truth, PayloadSources, reconcile};
use super::feed::{FeedConfig, FeedLoop};
use super::live_metric_names;
use super::post_age::PostAge;
use super::sender::LiveSender;
use super::spool::{Restored, Spool};
use crate::indexer::IndexerClient;

/// How often the post-age probe asks L1 for the last `BatchPosted` log.
/// Its own cadence, apart from the feed loop's tick: one log query and
/// one block read per probe, which a public endpoint tolerates at this
/// rate and not at the feed's one-second tick.
const POST_AGE_EVERY: Duration = Duration::from_secs(10);

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
pub async fn connect_l1(rpc: &str, key: &str) -> Result<impl Provider + Clone + 'static> {
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
    /// The inbox indexer's API. A batcher without a cursor file reads the
    /// last posted batch's blobs from it, when it holds them; a start
    /// never waits on it.
    pub indexer_url: Option<String>,
    /// The settlement contract's deployment block: where a `BatchPosted`
    /// scan starts, for the post-age probe and for a start without a
    /// cursor file that the indexer cannot serve.
    pub settlement_deploy_block: u64,
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

/// What a resume read from L1: its truth, and the cursor rebuilt from
/// the last batch's payload when no cursor file exists.
struct Resumed {
    l1_truth: L1Truth,
    rebuilt: Option<(BatchCursor, u64)>,
}

impl LiveArgs {
    /// Connect to L1 and reconcile the durable cursor against it. The L1
    /// reads retry until L1 answers; the reconcile of a cursor file that
    /// is ahead of L1 is a refusal, decided once.
    async fn start_l1_side(&self) -> Result<L1Side<impl Provider + Clone + 'static>> {
        let provider = connect_l1(&self.rpc, &self.key).await?;
        let da = DaProxy::new(&self.da_proxy)?;
        let loaded = BatchCursor::load(&self.cursor_file)?;
        let resumed = self.resume_until_l1_answers(&provider, &da, loaded).await;
        let l1_truth = resumed.l1_truth;
        let (cursor, skip_through_block) = match resumed.rebuilt {
            Some(rebuilt) => rebuilt,
            None => reconcile(loaded, l1_truth)?,
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

    /// One resume: L1's truth from the contract, and the cursor rebuilt
    /// from the last batch's payload when there is no cursor file.
    async fn resume_once<P: Provider>(
        &self,
        provider: &P,
        da: &DaProxy,
        loaded: Option<BatchCursor>,
    ) -> Result<Resumed> {
        let l1_truth = L1Truth::read(provider, self.settlement).await?;
        let indexer = self.indexer_url.as_deref().map(IndexerClient::new);
        let rebuilt = match loaded {
            None if l1_truth.last_batch_index > 0 => Some(
                PayloadSources {
                    indexer: indexer.as_ref(),
                    provider,
                    da,
                    settlement: self.settlement,
                    deploy_block: self.settlement_deploy_block,
                }
                .resume(l1_truth)
                .await?,
            ),
            _ => None,
        };
        Ok(Resumed { l1_truth, rebuilt })
    }

    /// Resume, again after every failure, until L1 answers. A batcher
    /// without L1 has nothing to do, and an exit would hide the failure
    /// behind the orchestrator's restart loop: the exporter stays up, each
    /// failure counts on the resume-failures metric, and the log names it.
    async fn resume_until_l1_answers<P: Provider>(
        &self,
        provider: &P,
        da: &DaProxy,
        loaded: Option<BatchCursor>,
    ) -> Resumed {
        let mut attempt: u32 = 0;
        loop {
            if let Some(resumed) = self.resume_step(provider, da, loaded, &mut attempt).await {
                return resumed;
            }
        }
    }

    /// One attempt; `None` after a counted failure and its backoff.
    async fn resume_step<P: Provider>(
        &self,
        provider: &P,
        da: &DaProxy,
        loaded: Option<BatchCursor>,
        attempt: &mut u32,
    ) -> Option<Resumed> {
        let error = match self.resume_once(provider, da, loaded).await {
            Ok(resumed) => return Some(resumed),
            Err(error) => error,
        };
        counter!(live_metric_names::RESUME_FAILURES).increment(1);
        // Bounded by the shift below; saturate so a long outage keeps
        // counting instead of wrapping to a short backoff.
        *attempt = attempt.saturating_add(1);
        let backoff = Duration::from_secs(1 << (*attempt).min(5));
        warn!(
            attempt = *attempt,
            backoff_s = backoff.as_secs(),
            error = %format!("{error:#}"),
            "resume failed; L1 did not answer; retrying"
        );
        tokio::time::sleep(backoff).await;
        None
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
/// # Errors
/// Returns an error when L1 setup, cursor reconcile, config parsing, or the
/// reader stack fails to start, or when the feed loop exits with a failure.
pub async fn run(args: LiveArgs) -> Result<()> {
    let l1 = args.start_l1_side().await?;
    let spool = Spool::open(&args.spool_dir)?;
    let (restored, resume) = continue_from_spool(&spool, l1.cursor, l1.skip_through_block)?;
    let mut run_cfg = RunConfig::resolve(&args)?;
    run_cfg.resolve_cluster_ingress().await?;
    let ReaderStack { handles, feed_rx } = run_cfg.spawn_reader_stack(&args, resume)?;

    gauge!(live_metric_names::IDLE_FLUSH_SECONDS)
        .set(Duration::from_millis(args.idle_flush_ms.get()).as_secs_f64());
    let age_probe = tokio::spawn(
        PostAge::new(
            l1.provider.clone(),
            args.settlement,
            args.settlement_deploy_block,
            POST_AGE_EVERY,
        )
        .run(),
    );
    let sender = LiveSender::new(
        l1.provider,
        args.settlement,
        l1.da,
        l1.l1_truth.last_batch_index,
        args.l1_retries,
        args.cursor_file,
    );
    let feed_cfg = FeedConfig {
        blocks_per_batch: args.blocks_per_batch,
        compress: args.compress,
        chain_id: args.chain_id,
        flush: Duration::from_millis(args.flush_ms.get()),
        idle_flush: Duration::from_millis(args.idle_flush_ms.get()),
        target_payload_bytes: args.target_payload_bytes,
        skip_through_block: resume.skip_through(l1.skip_through_block),
    };
    let mut feed = tokio::spawn(FeedLoop::new(feed_rx, sender, feed_cfg, spool, restored).run());
    let feed_result = tokio::select! {
        r = &mut feed => r.context("feed task panicked")?,
        () = bin_support::wait_for_shutdown() => {
            // Exit cleanly. The cursor is reconciled against L1 truth on
            // every restart, so tearing down mid-batch loses nothing.
            info!("shutdown signal received; stopping live batcher");
            age_probe.abort();
            run_cfg.plane.shutdown().await;
            return Ok(());
        }
    };
    age_probe.abort();
    match feed_result {
        Ok(()) => Ok(()),
        Err(e) => Err(handles.surface_errors(e)),
    }
}

#[cfg(test)]
mod tests {
    use kardamom_types::BPosition;

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
