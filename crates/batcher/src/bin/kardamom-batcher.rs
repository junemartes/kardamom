//! `kardamom-batcher` CLI.
//!
//! Pure orchestration:
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

#[derive(Parser, Debug)]
#[command(name = "kardamom-batcher", version)]
struct Cli {
    /// Path to the tx_ordering Aeron Archive segment file (.rec) — the
    /// canonical orderer carrying `TxOrderingMessage` records (TxRef + boundary).
    #[arg(long, alias = "segment")]
    channel_b_segment: PathBuf,

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
    #[arg(long, default_value_t = true)]
    dry_run: bool,

    /// L1 JSON-RPC endpoint for live blob posting.
    #[arg(long, env = "KARDAMOM_L1_RPC")]
    l1_rpc: Option<String>,

    /// Batcher EOA private key (hex) — must equal the settlement's `l1Batcher`.
    #[arg(long, env = "KARDAMOM_L1_KEY")]
    l1_key: Option<String>,

    /// `KardamomL2Settlement` proxy address for live posting.
    #[arg(long)]
    settlement: Option<Address>,

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
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    kardamom_obs::init(
        "batcher",
        cli.metrics_addr,
        &cli.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;

    let a_segments = MultiArchiveConfig::parse_a_spec(&cli.channel_a_archive)?;
    let multi_cfg = MultiArchiveConfig {
        b_segment: cli.channel_b_segment.clone(),
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
    for rec in reader {
        match rec? {
            ResolvedRecord::Tx { position, env, .. } => {
                tx_count += 1;
                batcher.accumulator().observe_tx(env, position);
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
