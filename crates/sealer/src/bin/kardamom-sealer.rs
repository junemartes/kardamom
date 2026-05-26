//! `kardamom-sealer`: block sealer process.
//!
//! Loads a TOML [`SealerConfig`], opens a tx_ordering publisher + a brief
//! tx_ordering subscriber (to bootstrap `block_number` from the tail), and
//! runs [`Sealer::run_forever`] until SIGTERM / Ctrl-C.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxOrderingPublisherHandle, TxOrderingSubscriberHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_sealer::clock::SystemClock;
use kardamom_sealer::emitter::{BoundaryPublisher, PublishError};
use kardamom_sealer::{Sealer, SealerConfig};
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sealer",
    version,
    about = "kardamom block sealer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SealerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Aeron Media Driver directory (`aeron.dir`). If unset, uses the
    /// default location embedded in the C client (typically
    /// `/dev/shm/aeron-<user>`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let cfg: SealerConfig = toml::from_str(&raw).context("parse config")?;
    cfg.validate().context("validate config")?;

    tracing::info!(
        host_id = cfg.host_id,
        tick_interval_ms = cfg.tick_interval_ms,
        "kardamom-sealer starting"
    );

    // Build a minimal ChannelsConfig pointing at the configured tx_ordering URI.
    // Other channels stay at their defaults — the sealer never touches them.
    let mut channels = LogConfig::default().channels;
    channels.tx_ordering_channel = cfg.channel_b_uri.clone();
    channels.tx_ordering_stream_id = cfg.channel_b_boundary_stream_id;

    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Bootstrap: drain the tx_ordering tail and pick `max(block_number) + 1`.
    let initial_block = bootstrap_block_number(&rt, &channels).await?;
    tracing::info!(
        initial_block,
        "tx_ordering bootstrap complete; this sealer resumes from block_number"
    );

    let publisher = TxOrderingPublisherHandle::open(&rt, &channels)
        .context("open TxOrderingPublisherHandle")?;
    let adapter = TxOrderingBoundaryAdapter::new(publisher);

    let sealer = Sealer::new(cfg.clone(), SystemClock, adapter, initial_block)
        .context("construct Sealer")?;

    // Race the sealer's run_forever against a shutdown signal.
    let run_handle = tokio::spawn(async move {
        if let Err(e) = sealer.run_forever().await {
            tracing::error!(error = %e, "sealer.run_forever exited with error");
        }
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-sealer: shutdown signal received; aborting run loop");
    run_handle.abort();
    let _ = run_handle.await;
    drop(rt);
    Ok(())
}

/// Subscribe to tx_ordering, drain whatever is currently in the stream, and
/// return `max(BoundaryStart::block_number) + 1`. If the stream is empty
/// (genesis) returns 1.
async fn bootstrap_block_number(rt: &AeronRuntime, channels: &ChannelsConfig) -> Result<u64> {
    let mut sub = TxOrderingSubscriberHandle::open(rt, channels)
        .context("open TxOrderingSubscriberHandle for bootstrap")?;
    // Quiesce window: drain whatever is buffered. We can't tell apart "no
    // history" from "history hasn't arrived yet"; cap at 2 s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut max_seen: Option<u64> = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some((_pos, TxOrderingMessage::BoundaryStart(b)))) => {
                max_seen = Some(max_seen.map_or(b.block_number, |m| m.max(b.block_number)));
            }
            Ok(Some(_)) => {} // TxRef — ignore for bootstrap
            Ok(None) => break,
            Err(_) => {} // timeout — keep polling until deadline
        }
    }
    Ok(max_seen.map_or(1, |n| n + 1))
}

/// `BoundaryPublisher` impl over a `TxOrderingPublisherHandle`. Tracks the
/// last published position so `current_tx_tail` returns a sensible value —
/// in the MDS topology the same stream carries both `TxRef` (from racing
/// sequencers) and `BoundaryStart` (from this sealer), so the "tail" we
/// know about is the position we last published into. Acceptable proxy for
/// `end_tx_idx` since downstream consumers reconstruct block membership
/// from in-stream order anyway.
struct TxOrderingBoundaryAdapter {
    pub_handle: TxOrderingPublisherHandle,
    last_pos: Arc<Mutex<BPosition>>,
}

impl TxOrderingBoundaryAdapter {
    fn new(pub_handle: TxOrderingPublisherHandle) -> Self {
        Self {
            pub_handle,
            last_pos: Arc::new(Mutex::new(BPosition::ZERO)),
        }
    }
}

impl BoundaryPublisher for TxOrderingBoundaryAdapter {
    fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError> {
        match self.pub_handle.publish_boundary(msg) {
            Ok(pos) => {
                *self.last_pos.lock().unwrap() = pos;
                Ok(pos)
            }
            Err(e) => Err(PublishError::Fatal(e.to_string())),
        }
    }

    fn current_tx_tail(&self) -> BPosition {
        *self.last_pos.lock().unwrap()
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl-C received");
    }
}
