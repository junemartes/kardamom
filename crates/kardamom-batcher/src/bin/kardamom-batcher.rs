//! `kardamom-batcher` CLI.
//!
//! Pure orchestration:
//!   - Open the offline segment reader at `--archive-dir / --recording-id`.
//!   - Build a [`Batcher`] with a `Sender` that talks to the settlement
//!     contract over `--rpc-url`.
//!   - Run until SIGTERM, advancing per-block batches as `BlockBoundaryStart`
//!     markers arrive in the archive.
//!
//! v0 wires the parsing + smoke print path; the real `Sender` impl is a
//! follow-up task that wraps an `alloy_provider::Provider` + the
//! `BlobTransactionSidecar` machinery. The CLI here gates that work behind a
//! `--dry-run` flag so the binary stays usable for archive inspection from
//! day one.

use std::path::PathBuf;

use clap::Parser;
use kardamom_batcher::archive_reader::{SegmentReader, SegmentRecord};
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender};
use kardamom_leases::{Lease, LeaseConfig};
use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "kardamom-batcher", version)]
struct Cli {
    /// Path to one Aeron Archive segment file (.rec).
    #[arg(long)]
    segment: PathBuf,

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

    let mut reader = SegmentReader::open(&cli.segment)?;
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
    for rec in &mut reader {
        match rec? {
            SegmentRecord::Tx { position, env } => {
                tx_count += 1;
                batcher.accumulator().observe_tx(env, position);
            }
            SegmentRecord::Boundary { marker, .. } => {
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
