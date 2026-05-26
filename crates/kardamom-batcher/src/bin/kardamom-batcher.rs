//! `kardamom-batcher` CLI.
//!
//! Pure orchestration:
//!   - Open the offline channel-B segment reader at `--channel-b-segment`.
//!   - Open per-sequencer channel-A segment readers via
//!     `--channel-a-archive sid=path,sid=path,...`.
//!   - Build a [`Batcher`] with a `Sender` that talks to the settlement
//!     contract over `--rpc-url`.
//!   - Run until SIGTERM, advancing per-block batches as `BlockBoundaryStart`
//!     markers arrive in the canonical (channel-B) archive.
//!
//! v0 wires the parsing + smoke print path; the real `Sender` impl is a
//! follow-up task that wraps an `alloy_provider::Provider` + the
//! `BlobTransactionSidecar` machinery. The CLI here gates that work behind a
//! `--dry-run` flag so the binary stays usable for archive inspection from
//! day one.

use std::path::PathBuf;

use clap::Parser;
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender};
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use kardamom_leases::{Lease, LeaseConfig};
use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "kardamom-batcher", version)]
struct Cli {
    /// Path to the channel-B Aeron Archive segment file (.rec) — the
    /// canonical orderer carrying `ChannelBMessage` records (TxRef + boundary).
    #[arg(long, alias = "segment")]
    channel_b_segment: PathBuf,

    /// Per-sequencer channel-A archive paths, in the form
    /// `sid=path,sid=path,...`. Each sequencer that appears on channel B
    /// via a `TxRef` must have its A-archive listed here so the batcher can
    /// resolve the envelope bytes.
    #[arg(long, default_value = "")]
    channel_a_archive: String,

    /// Optional explicit blocks-per-batch group size.
    #[arg(long, default_value_t = 1)]
    blocks_per_batch: usize,

    /// Disable zstd compression on the framed payload.
    #[arg(long, default_value_t = false)]
    no_compress: bool,

    /// Skip L1 broadcast; only inspect the archive.
    #[arg(long, default_value_t = true)]
    dry_run: bool,

    /// Recorder host id for this batcher (used by the lease).
    #[arg(long, default_value_t = 0u8)]
    self_id: u8,

    /// Comma-separated list of all recorder host ids in the cluster.
    #[arg(long, value_delimiter = ',', default_value = "0")]
    all_ids: Vec<u8>,

    /// Bytes of stream lag that still count as "caught up" for lease purposes.
    #[arg(long, default_value_t = 16 * 1024 * 1024i64)]
    caught_up_window: i64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    if !cli.dry_run {
        warn!("live L1 broadcast path not yet wired; falling back to dry-run");
    }

    let a_segments = MultiArchiveConfig::parse_a_spec(&cli.channel_a_archive)?;
    let multi_cfg = MultiArchiveConfig {
        b_segment: cli.channel_b_segment.clone(),
        a_segments,
    };
    let reader = MultiArchiveReader::open(&multi_cfg)?;
    info!(
        a_archive_count = reader.a_archive_count(),
        "opened M-archive reader",
    );
    let mut batcher = Batcher::new(
        BatcherConfig {
            blocks_per_batch: cli.blocks_per_batch,
            compress: !cli.no_compress,
            ..Default::default()
        },
        MockSender::default(),
    );

    // Self-elect a lease so we exercise the production code path even in dry-run.
    let mut lease = Lease::new(LeaseConfig {
        self_id: cli.self_id,
        all_ids: cli.all_ids.clone(),
        caught_up_window: cli.caught_up_window,
    });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: cli.self_id,
        position: BPosition::ZERO,
    });
    lease.observe_quorum(QuorumWatermark {
        position: BPosition::ZERO,
    });

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
                batcher.on_closed_block(closed, &lease)?;
            }
        }
    }

    info!(tx_count, block_count, "archive scan complete");
    info!(
        sent = batcher.sender().sent.len(),
        "dry-run: batches that would have been posted"
    );
    Ok(())
}
