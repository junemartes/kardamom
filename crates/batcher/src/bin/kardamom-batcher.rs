//! `kardamom-batcher` CLI. Two modes:
//!
//! **Live service** (`--live`, #39): tail the canonical ordering from the
//! Aeron Cluster egress (joining tx_data via the engine reader stack), pack
//! batches as boundaries arrive, and post each to L1 as it closes, resuming
//! across restarts from the durable cursor + on-chain `lastBatchIndex`. See
//! `docs/agents/batcher-live-l1-spec.md` and `kardamom_batcher::live`.
//!
//! **Offline** (default): pure orchestration —
//!   - Open the offline tx_ordering segment reader at `--channel-b-segment`.
//!   - Open per-sequencer tx_data segment readers via
//!     `--channel-a-archive sid=path,sid=path,...`.
//!   - Accumulate per-block batches as `BlockBoundaryStart` markers arrive.
//!   - In `--dry-run` (default) just report what would be posted. When an L1
//!     endpoint + signer + settlement address + DA store are supplied and
//!     `--dry-run=false`, post each batch as a real EIP-4844 blob transaction
//!     to `KardamomL2Settlement` and record the blobs in the DA store — the
//!     data-availability write path that the `kardamom-reconstruct` tool
//!     inverts.

use std::net::SocketAddr;
use std::path::PathBuf;

use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, bail};
use clap::Parser;
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender};
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::l1::post_batch;
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use kardamom_batcher::settlement::IKardamomL2Settlement;
use tracing::{info, warn};

/// Top-level config the batcher deserializes from `--config` in live mode.
/// Same `[cluster]` section shape as the executor's / validator's.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
struct BatcherFileConfig {
    cluster: kardamom_engine::reader::cluster::ClusterConfig,
}

#[derive(Parser, Debug)]
#[command(name = "kardamom-batcher", version)]
struct Cli {
    /// Live service mode (#39): tail the canonical ordering from the Aeron
    /// Cluster egress (joining tx_data), pack batches as boundaries arrive,
    /// and post each to L1 as it closes. Requires `--config`, `--dry-run=false`
    /// and the L1 flags; the segment-file flags are ignored.
    #[arg(long, default_value_t = false)]
    live: bool,

    /// Path to the tx_ordering Aeron Archive segment file (.rec) — the
    /// canonical orderer carrying `TxOrderingMessage` records (TxRef + boundary).
    /// Offline mode only.
    #[arg(long, alias = "segment", required_unless_present = "live")]
    channel_b_segment: Option<PathBuf>,

    /// Per-sequencer tx_data archive paths, in the form
    /// `sid=path,sid=path,...`.
    #[arg(long, default_value = "")]
    channel_a_archive: String,

    /// Optional explicit blocks-per-batch group size.
    #[arg(long, default_value_t = 1)]
    blocks_per_batch: usize,

    /// Disable zstd compression on the framed payload.
    #[arg(long, default_value_t = false)]
    no_compress: bool,

    /// Skip L1 broadcast; only inspect the archive. Live posting requires
    /// `--dry-run=false` plus `--l1-rpc`, `--l1-key`, `--settlement`, `--da-store`.
    ///
    /// A real boolean VALUE flag, not `SetTrue`: with clap's default bool
    /// action `--dry-run=false` is rejected outright ("unexpected value"), so
    /// the documented live invocation was unusable. Bare `--dry-run` still
    /// means true.
    #[arg(
        long,
        default_value_t = true,
        num_args(0..=1),
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    dry_run: bool,

    /// L1 JSON-RPC endpoint for live blob posting.
    #[arg(long, env = "KARDAMOM_L1_RPC")]
    l1_rpc: Option<String>,

    /// Batcher EOA private key (hex) — must equal the settlement's `l1Batcher`.
    #[arg(long, env = "KARDAMOM_L1_KEY")]
    l1_key: Option<String>,

    /// `KardamomL2Settlement` proxy address for live posting.
    #[arg(long, env = "KARDAMOM_SETTLEMENT")]
    settlement: Option<Address>,

    // --- Live-mode source (all ignored in offline mode) --------------------
    /// TOML config supplying the `[cluster]` section (live mode).
    #[arg(long, env = "KARDAMOM_BATCHER_CONFIG")]
    config: Option<PathBuf>,

    /// Log/channels config (tx_data / tx_deposits channel layout).
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,

    /// Aeron media-driver directory.
    #[arg(long, env = "KARDAMOM_AERON_DIR")]
    aeron_dir: Option<PathBuf>,

    /// Number of sender shards (tx_data channels to subscribe).
    #[arg(long, default_value_t = 1)]
    shards: u8,

    /// This node's cluster-egress endpoint `ip:port` (overrides the config's
    /// egress_channel, injected per node by the Nomad job).
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,

    /// UDP endpoint on this node where refetched tx_data / tx_deposits
    /// fragments land (join-miss recovery from the remote durability
    /// archives). Unset ⇒ refetch disabled; a lost envelope is then fatal
    /// after the join timeout.
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,

    /// UDP endpoint on this node for the refetch client's archive-control
    /// responses. Required alongside `--replay-destination-endpoint`.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    archive_control_response_endpoint: Option<String>,

    /// Durable cursor file: the ordering-stream position of the last
    /// confirmed L1 post (live mode; required there).
    #[arg(long, env = "KARDAMOM_BATCHER_CURSOR")]
    cursor_file: Option<PathBuf>,

    /// Post a partial group if the oldest pending block has waited this long.
    #[arg(long, default_value_t = 2_000)]
    flush_ms: u64,

    /// Bounded retries per L1 post before fail-stop (live mode).
    #[arg(long, default_value_t = 5)]
    l1_retries: u32,

    /// DA blob store directory: each posted blob is written here keyed by its
    /// versioned hash, so `kardamom-reconstruct` can fetch the bytes later.
    #[arg(long)]
    da_store: Option<PathBuf>,

    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9002")]
    metrics_addr: SocketAddr,

    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kardamom_engine::bin_support::init_tracing();
    let cli = Cli::parse();
    kardamom_obs::init_service!("batcher", cli.metrics_addr, &cli.host_id)?;

    if cli.live {
        return live_main(cli).await;
    }

    let b_segment = cli
        .channel_b_segment
        .clone()
        .expect("clap: --channel-b-segment required unless --live");
    let a_segments = MultiArchiveConfig::parse_a_spec(&cli.channel_a_archive)?;
    let multi_cfg = MultiArchiveConfig {
        b_segment,
        a_segments,
    };
    let reader = MultiArchiveReader::open(&multi_cfg)?;
    info!(
        a_archive_count = reader.a_archive_count(),
        "opened M-archive reader"
    );

    // Scan the archives into batches via the collecting MockSender.
    let mut batcher = Batcher::new(
        BatcherConfig {
            blocks_per_batch: cli.blocks_per_batch,
            compress: !cli.no_compress,
            ..Default::default()
        },
        MockSender::default(),
    );
    let mut tx_count: u64 = 0;
    let mut block_count: u64 = 0;
    let mut remote_epoch_count: u64 = 0;
    for rec in reader {
        match rec? {
            ResolvedRecord::Tx { position, env, .. } => {
                tx_count += 1;
                batcher.accumulator().observe_tx(env, position);
            }
            ResolvedRecord::RemoteEpoch { record, .. } => {
                remote_epoch_count += 1;
                batcher.accumulator().observe_remote_epoch(record);
            }
            ResolvedRecord::Boundary { marker, .. } => {
                block_count += 1;
                let closed = batcher.accumulator().observe_boundary(marker);
                batcher.on_closed_block(closed)?;
            }
        }
    }
    let batches = &batcher.sender().sent;
    info!(
        tx_count,
        block_count,
        remote_epoch_count,
        batches = batches.len(),
        "archive scan complete"
    );

    // Live post path.
    let live = !cli.dry_run;
    if live {
        let (rpc, key, settlement, da_dir) = match (
            cli.l1_rpc.as_ref(),
            cli.l1_key.as_ref(),
            cli.settlement,
            cli.da_store.as_ref(),
        ) {
            (Some(r), Some(k), Some(s), Some(d)) => (r, k, s, d),
            _ => bail!("--dry-run=false requires --l1-rpc, --l1-key, --settlement and --da-store"),
        };
        let signer: PrivateKeySigner = key.parse().context("parse --l1-key")?;
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(rpc)
            .await
            .with_context(|| format!("connect L1 RPC {rpc}"))?;
        let da_store = FsBlobStore::open(da_dir)?;

        // Start from the contract's current index (CAS replay guard).
        let contract = IKardamomL2Settlement::new(settlement, &provider);
        let mut prev_index = contract
            .lastBatchIndex()
            .call()
            .await
            .context("read lastBatchIndex")?;
        info!(%settlement, start_index = prev_index, "live L1 posting");

        for batch in batches {
            prev_index = post_batch(&provider, settlement, prev_index, batch, &da_store)
                .await
                .context("post batch to L1")?;
        }
        info!(
            posted = batches.len(),
            head_index = prev_index,
            "live posting complete"
        );
    } else {
        if cli.l1_rpc.is_some() || cli.settlement.is_some() {
            warn!("L1 args supplied but --dry-run is set; not broadcasting");
        }
        info!(
            sent = batches.len(),
            "dry-run: batches that would have been posted"
        );
    }
    Ok(())
}

/// Live service mode (#39): the batcher as a third cluster-egress consumer.
/// Front-end wiring mirrors the validator: M tx_data subscriptions +
/// tx_deposits feeding the join buffers, archive refetch on join miss, and
/// the canonical ordering from the Aeron Cluster egress with the replay
/// request seeded from the durable cursor. The feed loop and L1 sender live
/// in `kardamom_batcher::live`.
async fn live_main(cli: Cli) -> anyhow::Result<()> {
    use kardamom_batcher::live;
    use kardamom_engine::bin_support;
    use kardamom_engine::reader::{
        JoinBuffer, ReaderConfig, spawn_tx_data_reader, spawn_tx_ordering_reader,
    };
    use kardamom_log::aeron_live::AeronRuntime;
    use kardamom_log::config::LogConfig;

    if cli.dry_run {
        bail!(
            "--live requires --dry-run=false: a live batcher that does not post is not a DA service"
        );
    }
    let (rpc, key, settlement, da_dir) = match (
        cli.l1_rpc.as_ref(),
        cli.l1_key.as_ref(),
        cli.settlement,
        cli.da_store.as_ref(),
    ) {
        (Some(r), Some(k), Some(s), Some(d)) => (r, k, s, d),
        _ => bail!("--live requires --l1-rpc, --l1-key, --settlement and --da-store"),
    };
    let config_path = cli
        .config
        .as_ref()
        .context("--live requires --config (the [cluster] TOML)")?;
    let cursor_path = cli
        .cursor_file
        .clone()
        .context("--live requires --cursor-file")?;

    // --- L1 side: provider, truth, cursor reconcile. -----------------------
    let signer: PrivateKeySigner = key.parse().context("parse --l1-key")?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect(rpc)
        .await
        .with_context(|| format!("connect L1 RPC {rpc}"))?;
    let da_store = FsBlobStore::open(da_dir)?;
    let l1_truth = live::read_l1_truth(&provider, settlement).await?;
    let (cursor, skip_through_block) =
        live::reconcile(live::BatchCursor::load(&cursor_path)?, l1_truth)?;
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
    let raw = std::fs::read_to_string(config_path).context("read batcher config")?;
    let mut file_cfg: BatcherFileConfig = toml::from_str(&raw).context("parse batcher config")?;
    if let Some(ep) = cli.cluster_egress_endpoint.as_deref() {
        file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    let log_cfg = LogConfig::resolve(cli.log_config.as_deref()).context("resolve log config")?;
    let channels = log_cfg.channels;
    let mut aeron_cfg = log_cfg.aeron;
    if let Some(dir) = cli.aeron_dir.as_ref() {
        aeron_cfg.aeron_dir = dir.clone();
    }
    let rt = AeronRuntime::spawn(cli.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let a_subs = bin_support::open_tx_data_subs(&rt, &channels, cli.shards)?;
    let join_recovery = bin_support::archive_join_recovery(
        &channels,
        &aeron_cfg,
        cli.aeron_dir.as_deref(),
        cli.archive_control_response_endpoint.as_deref(),
        cli.replay_destination_endpoint.as_deref(),
    );

    // Dedicated cluster runtime, exactly as in the executor/validator: the
    // cluster session must never contend with the tx_data work on `rt`. The
    // guard must outlive the feed loop.
    let (_cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
        cli.aeron_dir.as_deref(),
        file_cfg.cluster.to_live(),
        kardamom_engine::reader::cluster::ReplayCursor::new(cursor.next_index, cursor.next_block),
    )?;
    // The kardamom_sealer_* re-export is the executor's job.
    let b_sub = cluster_sub.suppress_sealer_metrics();
    info!("kardamom-batcher: tx_ordering via Aeron Cluster");

    let join_buffer = JoinBuffer::new();
    let mut reader_handles = Vec::new();
    for a_sub in a_subs {
        reader_handles.push(spawn_tx_data_reader(a_sub, join_buffer.clone()));
    }
    // No tx_deposits reader: deposits ride inside the epoch record on the
    // canonical stream, so there is nothing to join against.
    let (feed_tx, feed_rx) = crossbeam_channel::bounded(1 << 14);
    // The default 100 ms join timeout is an IPC-locality assumption; on the
    // cluster's UDP multicast a transient frame drop takes the archive
    // refetch (which only engages after `join_refetch_after` = 10 s) to
    // repair. The executor/validator learned this in #84/#89 — same bounded
    // budget here, else the batcher dies before refetch can ever fire.
    let reader_cfg = ReaderConfig {
        join_timeout: bin_support::bounded_join_timeout(cursor.next_index > 0),
        ..ReaderConfig::default()
    };
    let ordering_handle = spawn_tx_ordering_reader(
        b_sub,
        join_buffer,
        reader_cfg,
        feed_tx,
        kardamom_engine::TxIndex(cursor.next_index),
        join_recovery,
    );

    // --- Feed loop on a blocking thread. -----------------------------------
    let sender = live::LiveSender::new(
        tokio::runtime::Handle::current(),
        provider,
        settlement,
        da_store,
        l1_truth.last_batch_index,
        cli.l1_retries,
        cursor_path,
    );
    let feed_cfg = live::FeedConfig {
        blocks_per_batch: cli.blocks_per_batch,
        compress: !cli.no_compress,
        flush: std::time::Duration::from_millis(cli.flush_ms),
        skip_through_block,
    };
    let mut feed = tokio::task::spawn_blocking(move || live::run_feed(feed_rx, sender, feed_cfg));
    let feed_result = tokio::select! {
        r = &mut feed => r.context("feed thread panicked")?,
        () = bin_support::wait_for_shutdown() => {
            // Exit cleanly: the cursor is reconciled against L1 truth on every
            // restart, so tearing down mid-batch loses nothing.
            info!("shutdown signal received; stopping live batcher");
            return Ok(());
        }
    };

    // The feed loop only returns on failure (channel closed or post
    // fail-stop). Surface the reader threads' errors for context before
    // propagating — the channel-closed case's root cause lives there.
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
