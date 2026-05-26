//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the canonical tx_ordering publisher + a receipt-cache publisher for
//! duplicate notifications, and runs the sequencer main loop on a dedicated
//! blocking thread until SIGTERM / Ctrl-C.

use std::path::PathBuf;
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, PubHandle, TxDataSubscriberHandle, TxOrderingPublisherHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::duplicate::DuplicateNotification;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{ReceiptCachePublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{
    BPosition, Receipt, StateDatabase, StateError, TxEnvelope, TxOrderingMessage, TxRef,
};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sequencer",
    version,
    about = "kardamom sequencer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Override the partition index from the config.
    #[arg(long)]
    partition_index: Option<u32>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<u32>,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;

    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = m;
    }
    if let Some(id) = args.sequencer_id {
        cfg.sequencer_id = id;
    } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
        cfg.sequencer_id = cfg.partition_index as u8;
    }
    if let Some(c) = args.core_id {
        cfg.core_id = Some(c);
    }
    cfg.validate().context("validate config")?;

    tracing::info!(
        partition_index = cfg.partition_index,
        sequencer_id = cfg.sequencer_id,
        "kardamom-sequencer starting"
    );

    let channels: ChannelsConfig = LogConfig::default().channels;
    let rt = AeronRuntime::spawn_default().context("spawn AeronRuntime")?;

    let shard_id = cfg.sequencer_id;
    let tx_data_sub = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
        .context("open TxDataSubscriberHandle")?;
    let tx_ordering_pub = TxOrderingPublisherHandle::open(&rt, &channels)
        .context("open TxOrderingPublisherHandle")?;
    let receipt_cache_pub = rt
        .open_publication(
            &channels.receipt_cache_channel,
            channels.receipt_cache_stream_id,
        )
        .context("open receipt-cache publication")?;

    let shutdown = Shutdown::new();
    let shutdown_for_task = shutdown.clone();

    let state_db = Arc::new(EmptyStateDatabase);
    let cfg_clone = cfg.clone();

    // The sequencer main loop is sync (std::thread + std::thread::sleep
    // backoff). Hand it to spawn_blocking so the async runtime stays
    // responsive for shutdown handling.
    let join = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut sequencer = Sequencer::new(cfg_clone, state_db);
        let mut tx_data = LiveTxDataSub::new(tx_data_sub);
        let mut tx_ordering = LiveTxOrderingRefPub::new(tx_ordering_pub);
        let mut rc = LiveReceiptCachePub::new(receipt_cache_pub);
        sequencer.run(&mut tx_data, &mut tx_ordering, &mut rc, shutdown_for_task)
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    shutdown.signal();
    match join.await {
        Ok(Ok(())) => tracing::info!("sequencer main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer task panicked"),
    }
    drop(rt);
    Ok(())
}

// ---------------------------------------------------------------------------
// Adapters: log::aeron_live handles → sequencer trait surface.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    handle: TxDataSubscriberHandle,
}

impl LiveTxDataSub {
    fn new(handle: TxDataSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl TxDataSubscriber for LiveTxDataSub {
    fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        Ok(self.handle.try_recv())
    }
}

struct LiveTxOrderingRefPub {
    handle: TxOrderingPublisherHandle,
}

impl LiveTxOrderingRefPub {
    fn new(handle: TxOrderingPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxOrderingRefPublisher for LiveTxOrderingRefPub {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError> {
        match self.handle.publish(&TxOrderingMessage::TxRef(*r)) {
            Ok(_pos) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // The AeronRuntime publish loop retries on back-pressure
                // internally (up to 1024 attempts); a returned error means
                // we hit the cap. Surface as Backpressure so the sequencer
                // state machine rewinds and retries on the next pass.
                if msg.contains("back-pressure") {
                    Err(SequencerError::Backpressure)
                } else {
                    tracing::error!(error = %msg, "tx_ordering publish failed");
                    Err(SequencerError::Backpressure)
                }
            }
        }
    }
}

struct LiveReceiptCachePub {
    handle: PubHandle,
}

impl LiveReceiptCachePub {
    fn new(handle: PubHandle) -> Self {
        Self { handle }
    }
}

impl ReceiptCachePublisher for LiveReceiptCachePub {
    fn publish_duplicate(&mut self, notification: DuplicateNotification) {
        if let Err(e) = self.handle.publish(&notification) {
            tracing::warn!(error = %e, "receipt-cache publish_duplicate failed (non-fatal)");
        }
    }
}

// ---------------------------------------------------------------------------
// Empty StateDatabase: cache-miss hydration always seeds at nonce 0. Sound for
// fresh chains; on warm restarts the operator should wire a real read-only
// libmdbx snapshot here. Lives in the bin because it's a deployment choice.
// ---------------------------------------------------------------------------

struct EmptyStateDatabase;

#[derive(Debug)]
struct EmptyStateError;

impl std::fmt::Display for EmptyStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyStateDatabase: no state available")
    }
}

impl std::error::Error for EmptyStateError {}
impl StateError for EmptyStateError {}

impl StateDatabase for EmptyStateDatabase {
    type Error = EmptyStateError;

    fn basic(&self, _: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Ok(None)
    }
    fn storage(&self, _: Address, _: B256) -> Result<U256, Self::Error> {
        Ok(U256::ZERO)
    }
    fn code_by_hash(&self, _: B256) -> Result<Bytes, Self::Error> {
        Ok(Bytes::new())
    }
    fn get_receipt(&self, _: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(None)
    }
    fn get_tx_position(&self, _: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(None)
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
