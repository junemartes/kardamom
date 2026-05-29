//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the canonical tx_ordering publisher + a tx_errors publisher for
//! rejection signals, and runs the sequencer main loop on a dedicated
//! blocking thread until SIGTERM / Ctrl-C.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxOrderingPublisherHandle,
};
use kardamom_log::bootstrap::{BootstrapPolicy, BootstrappedSubscriberHandle};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_log::recorder::Recorder;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::deposit::{DepositSubscriber, process_deposit};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{
    BPosition, Deposit, DepositRef, Receipt, StateDatabase, StateError, TxEnvelope, TxError,
    TxOrderingMessage, TxRef,
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
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
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

    let log_cfg = LogConfig::default();
    let channels: ChannelsConfig = log_cfg.channels.clone();
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Spawn a Recorder for tx_ordering on a dedicated thread.
    // AeronArchive is !Send + !Sync, so it must stay on its own OS thread.
    // SOURCE_LOCATION_LOCAL: sequencer is co-located with the tx_ordering publisher.
    let archive_dir = log_cfg.aeron.archive_dir.clone();
    let ch_for_rec = channels.clone();
    let recorder_id = cfg.sequencer_id; // use sequencer_id as recorder_id (unique per host)
    let _tx_ordering_recorder_thread = std::thread::Builder::new()
        .name("kardamom-rec-b".into())
        .spawn(move || {
            let archive = match kardamom_log::connect_archive_client() {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "sequencer: archive connect failed; tx_ordering not recorded");
                    return;
                }
            };
            match Recorder::start_b(archive, &ch_for_rec, recorder_id, archive_dir) {
                Ok(rec) => {
                    tracing::info!(recording_id = rec.recording_id(), "sequencer: tx_ordering recording started");
                    // Hold the Recorder alive until the process exits.
                    loop {
                        std::thread::park();
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "sequencer: start_b for tx_ordering failed");
                }
            }
        })
        .expect("spawn tx_ordering recorder thread");

    let shard_id = cfg.sequencer_id;
    let mut tx_data_sub =
        TxDataSubscriberHandle::open_bootstrapped(&rt, &channels, shard_id, BootstrapPolicy::default_tx_data())
            .context("open TxDataSubscriberHandle (bootstrapped)")?;
    let tx_ordering_pub = TxOrderingPublisherHandle::open(&rt, &channels)
        .context("open TxOrderingPublisherHandle")?;
    let mut tx_deposits_sub =
        TxDepositsSubscriberHandle::open_bootstrapped(&rt, &channels, BootstrapPolicy::default_tx_deposits())
            .context("open TxDepositsSubscriberHandle (bootstrapped)")?;
    let tx_errors_pub =
        TxErrorsPublisherHandle::open(&rt, &channels).context("open TxErrorsPublisherHandle")?;

    tracing::info!("sequencer waiting for tx_data + tx_deposits bootstrap");
    tx_data_sub
        .wait_caught_up()
        .await
        .map_err(|e| anyhow::anyhow!("tx_data bootstrap failed: {e}"))?;
    tx_deposits_sub
        .wait_caught_up()
        .await
        .map_err(|e| anyhow::anyhow!("tx_deposits bootstrap failed: {e}"))?;
    tracing::info!("sequencer bootstrap complete, accepting txs");

    let shutdown = Shutdown::new();
    let shutdown_for_main = shutdown.clone();
    let shutdown_for_deposits = shutdown.clone();

    let state_db = Arc::new(EmptyStateDatabase);
    let cfg_clone = cfg.clone();

    // The tx_ordering publisher carries both `TxRef` (this loop) and
    // `DepositRef` (the deposit pump below). Cloning the handle is sound
    // — the underlying Aeron publication is a single multi-publisher
    // stream and the SDK serialises offers internally.
    let tx_ordering_pub_for_deposits = tx_ordering_pub.clone();

    // The sequencer main loop is sync (std::thread + std::thread::sleep
    // backoff). Hand it to spawn_blocking so the async runtime stays
    // responsive for shutdown handling.
    let join_main = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut sequencer = Sequencer::new(cfg_clone, state_db);
        let mut tx_data = LiveTxDataSub::new(tx_data_sub);
        let mut tx_ordering = LiveTxOrderingRefPub::new(tx_ordering_pub);
        let mut tx_errors = LiveTxErrorPub::new(tx_errors_pub);
        sequencer.run(
            &mut tx_data,
            &mut tx_ordering,
            &mut tx_errors,
            shutdown_for_main,
        )
    });

    // Independent pump for tx_deposits → DepositRef on tx_ordering. The
    // deposit path is not nonce-gated; it's a simple poll → publish loop
    // that runs alongside the canonical TxData → TxRef path.
    let join_deposits = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut deposit_sub = LiveDepositSub::new(tx_deposits_sub);
        let mut tx_ordering = LiveTxOrderingRefPub::new(tx_ordering_pub_for_deposits);
        let mut backoff_us = 1u64;
        loop {
            if shutdown_for_deposits.is_signaled() {
                return Ok(());
            }
            match process_deposit(&mut deposit_sub, &mut tx_ordering) {
                Ok(true) => backoff_us = 1,
                Ok(false) => {
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = backoff_us.saturating_mul(2).min(100);
                }
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    shutdown.signal();
    match join_main.await {
        Ok(Ok(())) => tracing::info!("sequencer main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer task panicked"),
    }
    match join_deposits.await {
        Ok(Ok(())) => tracing::info!("sequencer deposit pump returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer deposit pump returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer deposit task panicked"),
    }
    drop(rt);
    Ok(())
}

// ---------------------------------------------------------------------------
// Adapters: log::aeron_live handles → sequencer trait surface.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    handle: BootstrappedSubscriberHandle<TxEnvelope>,
}

impl LiveTxDataSub {
    fn new(handle: BootstrappedSubscriberHandle<TxEnvelope>) -> Self {
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

struct LiveDepositSub {
    handle: BootstrappedSubscriberHandle<Deposit>,
}

impl LiveDepositSub {
    fn new(handle: BootstrappedSubscriberHandle<Deposit>) -> Self {
        Self { handle }
    }
}

impl DepositSubscriber for LiveDepositSub {
    fn poll(&mut self) -> Result<Option<(BPosition, Deposit)>, SequencerError> {
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
        publish_ordering(&self.handle, TxOrderingMessage::TxRef(*r))
    }

    fn try_publish_deposit_ref(&mut self, r: &DepositRef) -> Result<(), SequencerError> {
        publish_ordering(&self.handle, TxOrderingMessage::DepositRef(*r))
    }
}

fn publish_ordering(
    handle: &TxOrderingPublisherHandle,
    msg: TxOrderingMessage,
) -> Result<(), SequencerError> {
    match handle.publish(&msg) {
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

/// Live `TxErrorPublisher` wrapping a `TxErrorsPublisherHandle`. Sequencer-
/// emitted rejections (duplicate / past-nonce today) are published on the
/// `tx_errors` Aeron channel; ingress consumes them to release parked
/// clients early. Publish failures are logged and dropped — the canonical
/// state has already advanced (or the tx was rejected), so there is nothing
/// to roll back.
struct LiveTxErrorPub {
    handle: TxErrorsPublisherHandle,
}

impl LiveTxErrorPub {
    fn new(handle: TxErrorsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxErrorPublisher for LiveTxErrorPub {
    fn publish_error(&mut self, e: TxError) {
        if let Err(err) = self.handle.publish(&e) {
            tracing::warn!(error = %err, "tx_errors publish failed (dropped)");
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
