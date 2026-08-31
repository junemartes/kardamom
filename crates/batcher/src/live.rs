//! Live batcher: tails the canonical ordering from the Aeron Cluster
//! egress, joins tx_data, packs batches, and posts them to L1 as a
//! long-lived service.
//!
//! The batcher is a third cluster-egress consumer, next to the executor and
//! the validator. It reuses the `kardamom-engine` reader stack (cluster
//! tx_ordering subscription, tx_data join buffer, archive refetch) as-is.
//! Only the sink differs: `ReaderToExec` records feed a [`BatchAccumulator`]
//! instead of an execution pipeline. `Deposit` records are skipped, because
//! deposits are absent from DA by design. A reconstructor re-derives them
//! from L1; see `docs/agents/l1-origin-deposit-derivation-spec.md`.
//!
//! Durability model (see `docs/agents/batcher-live-l1-spec.md`):
//! - L1 (`lastBatchIndex` and the `BatchPosted` event) is the authoritative
//!   record of what has been posted.
//! - The cursor file holds the ordering-stream position matching that
//!   truth. It is written only after a confirmed post (at-least-once, like
//!   the da-watcher's L1 cursor). A stale or lost cursor causes
//!   re-observation. The feed loop drops re-observed blocks
//!   (`block_number <= skip_through_block`) without posting. The contract's
//!   compare-and-swap check makes any double post revert loudly instead of
//!   landing.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, bail};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};

use kardamom_engine::bin_support;
use kardamom_engine::reader::ReaderToExec;

use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::batcher::{BatcherConfig, PostedBatch, metric_names, pack_blocks};
use crate::da_store::FsBlobStore;
use crate::l1::{post_batch, read_posted_batches};
use crate::settlement::IKardamomL2Settlement;

/// Top-level config the batcher reads from `--config` in live mode. It uses
/// the same `[cluster]` section shape as the executor and the validator.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct BatcherFileConfig {
    pub cluster: kardamom_engine::reader::cluster::ClusterConfig,
}

/// Live-mode metric names, alongside [`metric_names`]. In live mode,
/// `kardamom_batcher_batches_posted_total` and `_blobs_posted_total` count
/// confirmed L1 posts (receipt observed or reconciled on-chain), not packed
/// batches.
pub mod live_metric_names {
    /// L1 post attempts that failed and were retried. This includes
    /// transient transport errors and CAS races that reconciled as not
    /// ours.
    pub const L1_POST_RETRIES: &str = "kardamom_batcher_l1_post_retries_total";
    /// Highest L2 block confirmed on L1 by this batcher.
    pub const LAST_POSTED_BLOCK: &str = "kardamom_batcher_last_posted_block";
    /// The contract's `lastBatchIndex` after this batcher's latest confirmed
    /// post.
    pub const LAST_BATCH_INDEX: &str = "kardamom_batcher_last_batch_index";
    /// Closed blocks buffered, waiting for the group to fill or flush.
    pub const PENDING_BLOCKS: &str = "kardamom_batcher_pending_blocks";
    /// Re-observed blocks dropped because L1 already covers them (stale
    /// cursor replay after a crash between post and cursor write).
    pub const SKIPPED_POSTED_BLOCKS: &str = "kardamom_batcher_skipped_posted_blocks_total";
}

/// The durable cursor: the ordering-stream position matching the last
/// confirmed L1 post. `next_index` and `next_block` seed the cluster replay
/// request. `last_batch_index` ties the position to the contract's CAS
/// counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchCursor {
    pub next_index: u64,
    pub next_block: u64,
    pub last_batch_index: u64,
}

impl BatchCursor {
    /// A fresh consumer: no records seen, the first boundary is block 1,
    /// and nothing is posted (`lastBatchIndex` starts at 0 on-chain; batch
    /// indices start at 1).
    pub fn genesis() -> Self {
        Self {
            next_index: 0,
            next_block: 1,
            last_batch_index: 0,
        }
    }

    pub fn load(path: &Path) -> Result<Option<Self>> {
        match std::fs::read_to_string(path) {
            Ok(raw) => Ok(Some(serde_json::from_str(&raw).with_context(|| {
                format!("parse batcher cursor file {}", path.display())
            })?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read cursor file {}", path.display())),
        }
    }

    /// An atomic write: write to a temp file, then rename it into place in
    /// the same directory.
    pub fn store(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(self).expect("cursor serializes"))
            .with_context(|| format!("write cursor tmp {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename cursor into place {}", path.display()))?;
        Ok(())
    }
}

/// What L1 says has been posted: the CAS counter, and the block the chain
/// is covered through (the latest `BatchPosted.l2BlockEnd`; 0 when nothing
/// is posted yet, since L2 blocks start at 1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct L1Truth {
    pub last_batch_index: u64,
    pub covered_through_block: u64,
}

/// The settlement contract's CAS counter (`lastBatchIndex`). Both
/// [`read_l1_truth`] and the offline post path use this. The offline path
/// needs only the counter, not the `BatchPosted` event scan.
pub async fn read_last_batch_index<P: Provider>(provider: &P, settlement: Address) -> Result<u64> {
    IKardamomL2Settlement::new(settlement, provider)
        .lastBatchIndex()
        .call()
        .await
        .context("read lastBatchIndex")
}

/// Parse the batcher key. Connect the wallet-backed L1 provider and the
/// local DA blob store. The live service and the offline `--dry-run=false`
/// post path share this signer, provider, and blob-store setup.
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

/// Read the settlement contract's view. The event scan runs from L1 block
/// 0. This is fine against the dev-cluster anvil. A long-lived L1 should
/// pass a deployment block hint here.
pub async fn read_l1_truth<P: Provider>(provider: &P, settlement: Address) -> Result<L1Truth> {
    let last = read_last_batch_index(provider, settlement).await?;
    if last == 0 {
        return Ok(L1Truth {
            last_batch_index: 0,
            covered_through_block: 0,
        });
    }
    let posted = read_posted_batches(provider, settlement, 0)
        .await
        .context("read BatchPosted events")?;
    let head = posted
        .iter()
        .find(|d| d.index == last)
        .with_context(|| format!("lastBatchIndex={last} but no BatchPosted event with it"))?;
    Ok(L1Truth {
        last_batch_index: last,
        covered_through_block: head.l2_block_end,
    })
}

/// Reconcile the cursor file against L1 at startup. Returns the cursor to
/// replay from and the block to skip through (drop without posting).
///
/// - No cursor file: replay from genesis; skip L1 coverage instead of
///   re-posting it.
/// - Cursor behind L1 (a crash between post and cursor write): replay from
///   the cursor, and skip through L1's covered block.
/// - Cursor ahead of L1: the chain regressed under this batcher (for
///   example, an anvil reset while `/opt/kardamom` survived). Stop, and let
///   the operator decide which side is real. Silently re-posting would
///   fork the DA history.
pub fn reconcile(cursor: Option<BatchCursor>, l1: L1Truth) -> Result<(BatchCursor, u64)> {
    match cursor {
        None => {
            if l1.last_batch_index > 0 {
                warn!(
                    covered_through_block = l1.covered_through_block,
                    "no cursor file but L1 has batches; genesis replay will skip re-posting \
                     (requires cluster retention back to genesis)"
                );
            }
            Ok((BatchCursor::genesis(), l1.covered_through_block))
        }
        Some(c) if c.last_batch_index > l1.last_batch_index => bail!(
            "cursor file says batch {} was posted but L1 lastBatchIndex is {} — the L1 chain \
             regressed under a surviving cursor (anvil reset?); refusing to guess. Delete the \
             cursor file to re-derive from this L1, or restore the L1 state",
            c.last_batch_index,
            l1.last_batch_index,
        ),
        Some(c) => Ok((c, l1.covered_through_block)),
    }
}

/// A streaming L1 sender. It posts one packed group at a time, strictly
/// serialized by the contract's CAS check, and persists the cursor only
/// after a confirmed post.
pub struct LiveSender<P> {
    provider: P,
    settlement: Address,
    da_store: FsBlobStore,
    prev_index: u64,
    max_retries: u32,
    cursor_path: PathBuf,
}

impl<P: Provider> LiveSender<P> {
    pub fn new(
        provider: P,
        settlement: Address,
        da_store: FsBlobStore,
        prev_index: u64,
        max_retries: u32,
        cursor_path: PathBuf,
    ) -> Self {
        Self {
            provider,
            settlement,
            da_store,
            prev_index,
            max_retries,
            cursor_path,
        }
    }

    /// Post `batch` and persist `cursor` once the post is confirmed. Retry
    /// transient failures with bounded backoff. A failure that reconciles
    /// on-chain as this batch having landed (for example, a duplicate send
    /// after a receipt timeout, or a CAS revert of the duplicate) counts as
    /// success. Any foreign advance of `lastBatchIndex` is fatal. The
    /// batcher is single-instance, and the CAS check exists to make that
    /// race loud.
    pub async fn post_confirmed(
        &mut self,
        batch: &PostedBatch,
        mut cursor: BatchCursor,
    ) -> Result<()> {
        cursor.last_batch_index = self.prev_index + 1;
        let mut attempt: u32 = 0;
        loop {
            let res = post_batch(
                &self.provider,
                self.settlement,
                self.prev_index,
                batch,
                &self.da_store,
            )
            .await;
            match res {
                Ok(next) => {
                    self.prev_index = next;
                    break;
                }
                Err(e) => {
                    // Before retrying, ask the chain whether the tx actually
                    // landed. A receipt timeout and a `StaleBatchIndex`
                    // revert of a duplicate send both look like local errors.
                    let truth = read_l1_truth(&self.provider, self.settlement).await;
                    match truth {
                        Ok(t) if t.last_batch_index == self.prev_index + 1 => {
                            if t.covered_through_block == batch.l2_block_end {
                                info!(
                                    batch_index = t.last_batch_index,
                                    "post reconciled on-chain as ours after send error"
                                );
                                self.prev_index += 1;
                                break;
                            }
                            bail!(
                                "lastBatchIndex advanced to {} covering block {} but our batch \
                                 ends at {} — a second batcher is writing; refusing to continue \
                                 (send error was: {e})",
                                t.last_batch_index,
                                t.covered_through_block,
                                batch.l2_block_end,
                            );
                        }
                        Ok(t) if t.last_batch_index > self.prev_index + 1 => bail!(
                            "lastBatchIndex jumped from {} to {} — a second batcher is writing; \
                             refusing to continue (send error was: {e})",
                            self.prev_index,
                            t.last_batch_index,
                        ),
                        Ok(_) => { /* not landed: a genuine transient failure */ }
                        Err(re) => warn!(error = %format!("{re:#}"), "reconcile read failed"),
                    }
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(e).with_context(|| {
                            format!(
                                "post batch (prev_index {}) after {attempt} attempts",
                                self.prev_index
                            )
                        });
                    }
                    counter!(live_metric_names::L1_POST_RETRIES).increment(1);
                    let backoff = Duration::from_secs(1 << attempt.min(4));
                    warn!(
                        attempt,
                        backoff_s = backoff.as_secs(),
                        error = %format!("{e:#}"),
                        "L1 post failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        cursor
            .store(&self.cursor_path)
            .context("persist cursor after confirmed post")?;
        counter!(metric_names::BATCHES_POSTED).increment(1);
        counter!(metric_names::BLOBS_POSTED).increment(batch.blobs.len() as u64);
        gauge!(live_metric_names::LAST_POSTED_BLOCK).set(batch.l2_block_end as f64);
        gauge!(live_metric_names::LAST_BATCH_INDEX).set(self.prev_index as f64);
        info!(
            batch_index = self.prev_index,
            l2_block_start = batch.l2_block_start,
            l2_block_end = batch.l2_block_end,
            blobs = batch.blobs.len(),
            "batch confirmed on L1"
        );
        Ok(())
    }
}

/// Feed-loop tunables.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    pub blocks_per_batch: usize,
    pub compress: bool,
    /// Post a partial group if the oldest pending block has waited this long.
    pub flush: Duration,
    /// Drop closed blocks at or below this number without posting. L1
    /// already covers them, from the startup reconcile.
    pub skip_through_block: u64,
}

/// The live feed loop. `ReaderToExec` records flow through the accumulator,
/// then the close policy, then to [`LiveSender`]. It runs as an async task.
/// The reader thread feeds it over a bounded tokio channel, until that
/// channel closes or a post fails and stops the loop. This is crash-only:
/// there is no graceful drain. The cursor is at-least-once, and a restart
/// re-observes records.
pub async fn run_feed<P: Provider>(
    mut rx: Receiver<ReaderToExec>,
    mut sender: LiveSender<P>,
    cfg: FeedConfig,
) -> Result<()> {
    let pack_cfg = BatcherConfig {
        blocks_per_batch: cfg.blocks_per_batch,
        compress: cfg.compress,
        ..Default::default()
    };
    let mut acc = BatchAccumulator::new();
    let mut pending: Vec<ClosedBlock> = Vec::new();
    let mut oldest_pending: Option<Instant> = None;

    loop {
        match tokio::time::timeout(cfg.flush, rx.recv()).await {
            Ok(Some(ReaderToExec::Tx {
                envelope, position, ..
            })) => acc.observe_tx(envelope, position),
            // Deposits are absent from DA by design. A reconstructor
            // re-derives them from L1 (this mirrors MultiArchiveReader
            // skipping DepositRefs offline). Skip the epoch marker for the
            // same reason: a reconstructor reads the origin from the block
            // boundary and re-derives that L1 block's deposits itself.
            Ok(Some(ReaderToExec::Deposit { .. } | ReaderToExec::Epoch { .. })) => {}
            // Remote-epoch records travel in DA. Unlike deposits, they are
            // not derivable again from this chain's L1 origin. So the
            // record, with its messages and calldata by value, is buffered
            // into the block it leads. It travels in the KAR1 v2 payload
            // for the reconstruction replay to run again.
            Ok(Some(ReaderToExec::RemoteEpoch { record, .. })) => acc.observe_remote_epoch(*record),
            // The per-message expansion of the record above. The messages
            // already travel by value inside the buffered record, so these
            // expanded dispatches add nothing new. Skip them here, the same
            // way the exec side expands them again from the record.
            Ok(Some(ReaderToExec::XChain { .. })) => {}
            Ok(Some(ReaderToExec::Boundary(b))) => {
                let closed = acc.observe_boundary(b);
                counter!(metric_names::BLOCKS_OBSERVED).increment(1);
                if closed.block_number <= cfg.skip_through_block {
                    counter!(live_metric_names::SKIPPED_POSTED_BLOCKS).increment(1);
                    continue;
                }
                pending.push(closed);
                oldest_pending.get_or_insert_with(Instant::now);
                gauge!(live_metric_names::PENDING_BLOCKS).set(pending.len() as f64);
                if pending.len() >= cfg.blocks_per_batch {
                    post_group(&pack_cfg, &mut pending, &mut oldest_pending, &mut sender).await?;
                }
            }
            // Flush timeout.
            Err(_) => {
                if !pending.is_empty() && oldest_pending.is_some_and(|t| t.elapsed() >= cfg.flush) {
                    post_group(&pack_cfg, &mut pending, &mut oldest_pending, &mut sender).await?;
                }
            }
            Ok(None) => bail!("tx_ordering reader channel closed; see reader thread error"),
        }
    }
}

/// Pack and post the pending group. This clears it.
async fn post_group<P: Provider>(
    pack_cfg: &BatcherConfig,
    pending: &mut Vec<ClosedBlock>,
    oldest: &mut Option<Instant>,
    sender: &mut LiveSender<P>,
) -> Result<()> {
    let group = std::mem::take(pending);
    *oldest = None;
    gauge!(live_metric_names::PENDING_BLOCKS).set(0.0);
    let last = group.last().expect("post_group called with pending blocks");
    let cursor = BatchCursor {
        next_index: last.end_tx_idx.as_index(),
        next_block: last.block_number + 1,
        // post_confirmed stamps last_batch_index.
        last_batch_index: 0,
    };
    let batch = pack_blocks(pack_cfg, &group)?;
    sender.post_confirmed(&batch, cursor).await
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
    pub shards: u8,
    pub cluster_egress_endpoint: Option<String>,
    pub replay_destination_endpoint: Option<String>,
    pub archive_control_response_endpoint: Option<String>,
    pub blocks_per_batch: usize,
    pub compress: bool,
    pub flush_ms: u64,
    pub l1_retries: u32,
}

/// Live service mode: the batcher as a third cluster-egress consumer.
/// Front-end wiring mirrors the validator: M tx_data subscriptions and
/// tx_deposits feed the join buffers, archive refetch runs on a join miss,
/// and the canonical ordering comes from the Aeron Cluster egress, with the
/// replay request seeded from the durable cursor. Runs until SIGTERM,
/// Ctrl-C, or a feed-loop failure that stops it.
pub async fn run(args: LiveArgs) -> Result<()> {
    use kardamom_engine::reader::{
        JoinBuffer, ReaderConfig, spawn_tx_data_reader, spawn_tx_ordering_reader,
    };
    use kardamom_log::aeron_live::AeronRuntime;
    use kardamom_log::config::LogConfig;

    // --- L1 side: provider, truth, cursor reconcile. -----------------------
    let (provider, da_store) = connect_l1(&args.rpc, &args.key, &args.da_store).await?;
    let settlement = args.settlement;
    let l1_truth = read_l1_truth(&provider, settlement).await?;
    let (cursor, skip_through_block) = reconcile(BatchCursor::load(&args.cursor_file)?, l1_truth)?;
    info!(
        %settlement,
        last_batch_index = l1_truth.last_batch_index,
        covered_through_block = l1_truth.covered_through_block,
        replay_from_index = cursor.next_index,
        replay_from_block = cursor.next_block,
        skip_through_block,
        "live batcher starting"
    );

    // --- Ordering source: the engine reader stack. -------------------------
    let raw = std::fs::read_to_string(&args.config).context("read batcher config")?;
    let mut file_cfg: BatcherFileConfig = toml::from_str(&raw).context("parse batcher config")?;
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = log_cfg.channels;
    let mut aeron_cfg = log_cfg.aeron;
    if let Some(dir) = args.aeron_dir.as_ref() {
        aeron_cfg.aeron_dir = dir.clone();
    }
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let tx_data_subs = bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
    let join_recovery = bin_support::archive_join_recovery(
        &channels,
        &aeron_cfg,
        args.aeron_dir.as_deref(),
        args.archive_control_response_endpoint.as_deref(),
        args.replay_destination_endpoint.as_deref(),
    );

    // A dedicated cluster runtime, exactly as in the executor and
    // validator. The cluster session must never contend with the tx_data
    // work on `rt`. The guard must outlive the feed loop.
    let (_cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
        args.aeron_dir.as_deref(),
        file_cfg.cluster.to_live(),
        kardamom_engine::reader::cluster::ReplayCursor::new(cursor.next_index, cursor.next_block),
    )?;
    // The kardamom_sealer_* re-export is the executor's job.
    let tx_ordering_sub = cluster_sub.suppress_sealer_metrics();
    info!("kardamom-batcher: tx_ordering via Aeron Cluster");

    let join_buffer = JoinBuffer::new();
    let mut reader_handles = Vec::new();
    for sub in tx_data_subs {
        reader_handles.push(spawn_tx_data_reader(sub, join_buffer.clone()));
    }
    // There is no tx_deposits reader. Deposits ride inside the epoch record
    // on the canonical stream, so there is nothing to join against.
    // The channel is bounded. The reader thread calls `blocking_send`. The
    // feed task calls `recv`.
    let (feed_tx, feed_rx) = tokio::sync::mpsc::channel(1 << 14);
    // The default 100 ms join timeout assumes IPC locality. On the
    // cluster's UDP multicast, a transient frame drop needs the archive
    // refetch to repair it, and refetch only engages after
    // `join_refetch_after` (10 s). Use the same bounded budget here as the
    // executor and validator, or the batcher dies before refetch can fire.
    let reader_cfg = ReaderConfig {
        join_timeout: bin_support::bounded_join_timeout(cursor.next_index > 0),
        ..ReaderConfig::default()
    };
    let ordering_handle = spawn_tx_ordering_reader(
        tx_ordering_sub,
        join_buffer,
        reader_cfg,
        feed_tx,
        kardamom_engine::TxIndex(cursor.next_index),
        join_recovery,
    );

    // --- Feed loop task. ---------------------------------------------------
    let sender = LiveSender::new(
        provider,
        settlement,
        da_store,
        l1_truth.last_batch_index,
        args.l1_retries,
        args.cursor_file,
    );
    let feed_cfg = FeedConfig {
        blocks_per_batch: args.blocks_per_batch,
        compress: args.compress,
        flush: Duration::from_millis(args.flush_ms),
        skip_through_block,
    };
    let mut feed = tokio::spawn(run_feed(feed_rx, sender, feed_cfg));
    let feed_result = tokio::select! {
        r = &mut feed => r.context("feed task panicked")?,
        () = bin_support::wait_for_shutdown() => {
            // Exit cleanly. The cursor is reconciled against L1 truth on
            // every restart, so tearing down mid-batch loses nothing.
            info!("shutdown signal received; stopping live batcher");
            return Ok(());
        }
    };

    // The feed loop returns only on failure (channel closed, or a post
    // that stopped it). Surface the reader threads' errors for context
    // before propagating them. The channel-closed case's root cause lives
    // there.
    if let Err(e) = &feed_result {
        warn!(error = %format!("{e:#}"), "feed loop exited");
        if ordering_handle.is_finished()
            && let Ok(Err(re)) = ordering_handle.join()
        {
            bail!("tx_ordering reader failed: {re:#} (feed loop: {e:#})");
        }
        for h in reader_handles {
            if h.is_finished()
                && let Ok(Err(re)) = h.join()
            {
                bail!("stream reader failed: {re:#} (feed loop: {e:#})");
            }
        }
    }
    feed_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip_and_missing() {
        let dir = std::env::temp_dir().join(format!("batcher-cursor-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cursor.json");
        assert_eq!(BatchCursor::load(&path).unwrap(), None);
        let c = BatchCursor {
            next_index: 42,
            next_block: 7,
            last_batch_index: 3,
        };
        c.store(&path).unwrap();
        assert_eq!(BatchCursor::load(&path).unwrap(), Some(c));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reconcile_matrix() {
        let l1_empty = L1Truth {
            last_batch_index: 0,
            covered_through_block: 0,
        };
        let l1_posted = L1Truth {
            last_batch_index: 5,
            covered_through_block: 120,
        };
        // Fresh start, empty chain: genesis, nothing skipped.
        assert_eq!(
            reconcile(None, l1_empty).unwrap(),
            (BatchCursor::genesis(), 0)
        );
        // Lost cursor on a posted chain: genesis replay, skip L1 coverage.
        assert_eq!(
            reconcile(None, l1_posted).unwrap(),
            (BatchCursor::genesis(), 120)
        );
        // Stale cursor (crash between post and cursor write): replay from
        // the cursor, skip through L1's covered block.
        let stale = BatchCursor {
            next_index: 900,
            next_block: 100,
            last_batch_index: 4,
        };
        assert_eq!(reconcile(Some(stale), l1_posted).unwrap(), (stale, 120));
        // Cursor ahead of L1 (chain regressed): fail-stop.
        let ahead = BatchCursor {
            next_index: 2000,
            next_block: 200,
            last_batch_index: 9,
        };
        let err = reconcile(Some(ahead), l1_posted).unwrap_err().to_string();
        assert!(err.contains("regressed"), "unexpected error: {err}");
    }
}
