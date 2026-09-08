//! `kardamom-batcher` CLI. It has two modes:
//!
//! Live service (`--live`): tails the canonical ordering from the Aeron
//! Cluster egress (joining `tx_data` through the engine reader stack), packs
//! batches as boundaries arrive, and posts each one to L1 as it closes. It
//! resumes across restarts from the durable cursor and the on-chain
//! `lastBatchIndex`. See `docs/agents/batcher-live-l1-spec.md` and
//! `kardamom_batcher::live`. This binary only validates the flag set and
//! hands off to `live::run`.
//!
//! Offline (the default): pure orchestration.
//!   - Open the offline `tx_ordering` segment reader at `--channel-b-segment`.
//!   - Open per-sequencer `tx_data` segment readers with
//!     `--channel-a-archive sid=path,sid=path,...`.
//!   - Accumulate per-block batches as `BlockBoundaryStart` markers arrive.
//!   - With `--dry-run` (the default), just report what would be posted.
//!     When an L1 endpoint, signer, settlement address, and DA store are
//!     supplied, and `--dry-run=false`, post each batch as a real EIP-4844
//!     blob transaction to `KardamomL2Settlement`, and record the blobs in
//!     the DA store. This is the data-availability write path that the
//!     `kardamom-reconstruct` tool inverts.

use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

use alloy_primitives::Address;
use anyhow::{Context, Result, bail};
use clap::Parser;
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender, PostedBatch};
use kardamom_batcher::l1::post_batch;
use kardamom_batcher::live;
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "kardamom-batcher", version)]
struct Cli {
    /// Live service mode: tails the canonical ordering from the Aeron
    /// Cluster egress (joining `tx_data`), packs batches as boundaries
    /// arrive, and posts each one to L1 as it closes. Requires `--config`,
    /// `--dry-run=false`, and the L1 flags. Ignores the segment-file flags.
    #[arg(long, default_value_t = false)]
    live: bool,

    /// The path to the `tx_ordering` Aeron Archive segment file (.rec). This
    /// is the canonical orderer, carrying `TxOrderingMessage` records
    /// (`TxRef` and boundary). Offline mode only.
    #[arg(long, alias = "segment", required_unless_present = "live")]
    channel_b_segment: Option<PathBuf>,

    /// Per-sequencer `tx_data` archive paths, in the form
    /// `sid=path,sid=path,...`.
    #[arg(long, default_value = "")]
    channel_a_archive: String,

    /// Optional explicit blocks-per-batch group size. Must be nonzero: 0
    /// would make the "group is full" check vacuously true.
    #[arg(long, default_value = "1")]
    blocks_per_batch: NonZeroUsize,

    /// Disable zstd compression on the framed payload.
    #[arg(long, default_value_t = false)]
    no_compress: bool,

    /// Skip L1 broadcast; only inspect the archive. Live posting requires
    /// `--dry-run=false` plus `--l1-rpc`, `--l1-key`, `--settlement`, and
    /// `--da-store`.
    ///
    /// This is a real boolean value flag, not `SetTrue`. With clap's
    /// default bool action, `--dry-run=false` is rejected outright
    /// ("unexpected value"), which would make the documented live
    /// invocation unusable. A bare `--dry-run` still means true.
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

    /// The batcher EOA private key (hex). Must equal the settlement's
    /// `l1Batcher`.
    #[arg(long, env = "KARDAMOM_L1_KEY")]
    l1_key: Option<String>,

    /// `KardamomL2Settlement` proxy address for live posting.
    #[arg(long, env = "KARDAMOM_SETTLEMENT")]
    settlement: Option<Address>,

    // --- Live-mode source (all ignored in offline mode) --------------------
    /// TOML config supplying the `[cluster]` section (live mode).
    #[arg(long, env = "KARDAMOM_BATCHER_CONFIG")]
    config: Option<PathBuf>,

    /// Log/channels config (`tx_data` / `tx_deposits` channel layout).
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,

    /// Aeron media-driver directory.
    #[arg(long, env = "KARDAMOM_AERON_DIR")]
    aeron_dir: Option<PathBuf>,

    /// Number of sender shards (`tx_data` channels to subscribe). Must be
    /// nonzero: 0 opens no readers, so every join misses.
    #[arg(long, default_value = "1")]
    shards: NonZeroU8,

    /// This node's cluster-egress endpoint `ip:port`. It overrides the
    /// config's `egress_channel`, and the Nomad job injects it per node.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,

    /// The UDP endpoint on this node where refetched `tx_data` and
    /// `tx_deposits` fragments land (join-miss recovery from the remote
    /// durability archives). If unset, refetch is disabled, and a lost
    /// envelope is then fatal after the join timeout.
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,

    /// UDP endpoint on this node for the refetch client's archive-control
    /// responses. Required alongside `--replay-destination-endpoint`.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    archive_control_response_endpoint: Option<String>,

    /// The durable cursor file: the ordering-stream position of the last
    /// confirmed L1 post. Live mode only; required there.
    #[arg(long, env = "KARDAMOM_BATCHER_CURSOR")]
    cursor_file: Option<PathBuf>,

    /// Post a partial group if the oldest pending block has waited this
    /// long. Must be nonzero: 0 makes the flush timeout expire at once, a
    /// busy loop.
    #[arg(long, default_value = "2000")]
    flush_ms: NonZeroU64,

    /// Bounded retries per L1 post before fail-stop (live mode).
    #[arg(long, default_value_t = 5)]
    l1_retries: u32,

    /// The DA blob store directory. Each posted blob is written here, keyed
    /// by its versioned hash, so `kardamom-reconstruct` can fetch the bytes
    /// later.
    #[arg(long)]
    da_store: Option<PathBuf>,

    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9002")]
    metrics_addr: SocketAddr,

    /// A host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kardamom_engine::bin_support::init_tracing();
    let cli = Cli::parse();
    kardamom_obs::init_service!("batcher", cli.metrics_addr, &cli.host_id).await?;

    if cli.live {
        return live_main(cli).await;
    }

    let batcher = cli.scan_offline_archives()?;
    cli.post_or_dry_run(&batcher.sender().sent).await
}

impl Cli {
    /// The L1 flag tuple both post paths require. `mode` names the flag
    /// that asked for it, so the error message stays exact (`--live` or
    /// `--dry-run=false`).
    fn require_l1_flags(&self, mode: &str) -> Result<(&String, &String, Address, &PathBuf)> {
        match (
            self.l1_rpc.as_ref(),
            self.l1_key.as_ref(),
            self.settlement,
            self.da_store.as_ref(),
        ) {
            (Some(r), Some(k), Some(s), Some(d)) => Ok((r, k, s, d)),
            _ => bail!("{mode} requires --l1-rpc, --l1-key, --settlement and --da-store"),
        }
    }

    /// Open the offline `tx_ordering`/`tx_data` archives this CLI names,
    /// and drive the accumulator over every record. Returns the batcher
    /// holding the resulting batches, collected by its `MockSender`.
    fn scan_offline_archives(&self) -> anyhow::Result<Batcher<MockSender>> {
        let b_segment = self
            .channel_b_segment
            .clone()
            .expect("clap: --channel-b-segment required unless --live");
        let a_segments = MultiArchiveConfig::parse_a_spec(&self.channel_a_archive)?;
        let multi_cfg = MultiArchiveConfig {
            b_segment,
            a_segments,
        };
        let mut reader = MultiArchiveReader::open(&multi_cfg)?;
        info!(
            a_archive_count = reader.a_archive_count(),
            "opened M-archive reader"
        );

        let mut batcher = Batcher::new(
            BatcherConfig {
                blocks_per_batch: self.blocks_per_batch,
                compress: !self.no_compress,
                ..Default::default()
            },
            MockSender::default(),
        );
        let mut tx_count: u64 = 0;
        let mut block_count: u64 = 0;
        let mut remote_epoch_count: u64 = 0;
        reader.try_for_each(|rec| -> anyhow::Result<()> {
            let rec = rec?;
            match &rec {
                ResolvedRecord::Tx { .. } => tx_count += 1,
                ResolvedRecord::RemoteEpoch { .. } => remote_epoch_count += 1,
                ResolvedRecord::Boundary { .. } => block_count += 1,
            }
            if let Some(closed) = batcher.accumulator().observe(rec) {
                batcher.on_closed_block(closed)?;
            }
            Ok(())
        })?;
        info!(
            tx_count,
            block_count,
            remote_epoch_count,
            batches = batcher.sender().sent.len(),
            "archive scan complete"
        );
        Ok(batcher)
    }

    /// Post every batch in `sent_batches` to L1 in order, each one guarded
    /// by the previous call's returned index (a CAS replay guard). Returns
    /// the index after the last successful post.
    async fn post_all_batches<P: alloy_provider::Provider>(
        provider: &P,
        settlement: Address,
        mut prev_index: u64,
        sent_batches: &[PostedBatch],
        da_store: &kardamom_batcher::FsBlobStore,
    ) -> anyhow::Result<u64> {
        for batch in sent_batches {
            prev_index = post_batch(provider, settlement, prev_index, batch, da_store)
                .await
                .context("post batch to L1")?;
        }
        Ok(prev_index)
    }

    /// Broadcast `sent_batches` to L1 when `--dry-run=false` (with the L1
    /// flag tuple present), or just report what would have been posted.
    async fn post_or_dry_run(&self, sent_batches: &[PostedBatch]) -> anyhow::Result<()> {
        let live = !self.dry_run;
        if live {
            let (rpc, key, settlement, da_dir) = self.require_l1_flags("--dry-run=false")?;
            let (provider, da_store) = live::connect_l1(rpc, key, da_dir).await?;

            // Start from the contract's current index (CAS replay guard).
            let prev_index = live::read_last_batch_index(&provider, settlement).await?;
            info!(%settlement, start_index = prev_index, "live L1 posting");

            let head_index =
                Self::post_all_batches(&provider, settlement, prev_index, sent_batches, &da_store)
                    .await?;
            info!(
                posted = sent_batches.len(),
                head_index, "live posting complete"
            );
        } else {
            if self.l1_rpc.is_some() || self.settlement.is_some() {
                warn!("L1 args supplied but --dry-run is set; not broadcasting");
            }
            info!(
                sent = sent_batches.len(),
                "dry-run: batches that would have been posted"
            );
        }
        Ok(())
    }
}

/// Validate the live-mode flag set, then hand off to
/// [`kardamom_batcher::live::run`], the batcher as a third cluster-egress
/// consumer. Front-end wiring mirrors the validator; the feed loop and L1
/// sender live in `kardamom_batcher::live`.
async fn live_main(cli: Cli) -> anyhow::Result<()> {
    if cli.dry_run {
        bail!(
            "--live requires --dry-run=false: a live batcher that does not post is not a DA service"
        );
    }
    let (rpc, key, settlement, da_dir) = cli.require_l1_flags("--live")?;
    let config = cli
        .config
        .clone()
        .context("--live requires --config (the [cluster] TOML)")?;
    let cursor_file = cli
        .cursor_file
        .clone()
        .context("--live requires --cursor-file")?;

    live::run(live::LiveArgs {
        rpc: rpc.clone(),
        key: key.clone(),
        settlement,
        da_store: da_dir.clone(),
        config,
        cursor_file,
        log_config: cli.log_config.clone(),
        aeron_dir: cli.aeron_dir.clone(),
        shards: cli.shards,
        cluster_egress_endpoint: cli.cluster_egress_endpoint.clone(),
        replay_destination_endpoint: cli.replay_destination_endpoint.clone(),
        archive_control_response_endpoint: cli.archive_control_response_endpoint.clone(),
        blocks_per_batch: cli.blocks_per_batch,
        compress: !cli.no_compress,
        flush_ms: cli.flush_ms,
        l1_retries: cli.l1_retries,
    })
    .await
}
