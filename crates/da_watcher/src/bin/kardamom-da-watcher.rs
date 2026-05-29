//! kardamom-da-watcher: L1 deposit monitor CLI.
//!
//! Parses an `--l1-rpc <URL>` + `--lockbox <ADDRESS>` (plus an optional
//! `--poll-interval <DURATION>`, default 12s), constructs an
//! [`da_watcher::RpcL1Source`] over an alloy HTTP provider, spawns the
//! watcher loop, and waits for ctrl-c. Each observed deposit is republished
//! onto the `tx_deposits` Aeron channel via the live
//! [`LiveTxDepositsPublisher`] adapter (wraps `kardamom_log::TxDepositsPublisherHandle`).

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use anyhow::Context;
use clap::Parser;
use tokio::signal;

use kardamom_da_watcher::{
    DaWatcherConfig, DepositPublisher, PublishError, RpcL1Source, spawn as spawn_watcher,
};
use kardamom_log::aeron_live::{AeronRuntime, TxDepositsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::recorder::Recorder;
use kardamom_types::{BPosition, Deposit};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "L1 deposit monitor — tails finalized L1 blocks and publishes Deposits onto tx_deposits"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoint (e.g. `http://127.0.0.1:8545`).
    #[arg(long)]
    l1_rpc: String,
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    #[arg(long)]
    lockbox: String,
    /// Polling cadence in seconds (default 12).
    #[arg(long, default_value_t = 12)]
    poll_interval_secs: u64,
    /// Aeron Media Driver directory (`aeron.dir`). When omitted, falls
    /// back to the Aeron client's default lookup (`AERON_DIR` env / OS
    /// default). The local-e2e `just` recipe always passes this explicitly.
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let lockbox = Address::from_str(&args.lockbox)
        .map_err(|e| anyhow::anyhow!("--lockbox is not a valid address: {e}"))?;
    let cfg = DaWatcherConfig {
        lockbox,
        poll_interval: Duration::from_secs(args.poll_interval_secs),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let log_cfg = LogConfig::default();
    let channels = log_cfg.channels.clone();
    let aeron_rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Spawn a Recorder for tx_deposits on a dedicated thread.
    // AeronArchive is !Send + !Sync, so it must stay on its own OS thread.
    // SOURCE_LOCATION_LOCAL: da-watcher is co-located with the tx_deposits publisher.
    let archive_dir = log_cfg.aeron.archive_dir.clone();
    let ch_for_rec = channels.clone();
    let _tx_deposits_recorder_thread = std::thread::Builder::new()
        .name("kardamom-rec-c".into())
        .spawn(move || {
            let archive = match kardamom_log::connect_archive_client() {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "da-watcher: archive connect failed; tx_deposits not recorded");
                    return;
                }
            };
            // recorder_id 0: da-watcher runs as a singleton; there is exactly
            // one tx_deposits recorder per deployment.
            match Recorder::start_c(archive, &ch_for_rec, 0, archive_dir) {
                Ok(rec) => {
                    tracing::info!(recording_id = rec.recording_id(), "da-watcher: tx_deposits recording started");
                    // Hold the Recorder alive until the process exits.
                    loop {
                        std::thread::park();
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "da-watcher: start_c for tx_deposits failed");
                }
            }
        })
        .expect("spawn tx_deposits recorder thread");

    let tx_deposits_pub = TxDepositsPublisherHandle::open(&aeron_rt, &channels)
        .context("open TxDepositsPublisherHandle")?;

    rt.block_on(async move {
        let provider = ProviderBuilder::new()
            .connect(&args.l1_rpc)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to L1 RPC {}: {e}", args.l1_rpc))?;
        let source = RpcL1Source::new(provider);
        let publisher = LiveTxDepositsPublisher::new(tx_deposits_pub);

        tracing::info!(
            l1_rpc = %args.l1_rpc,
            ?lockbox,
            poll_interval = ?cfg.poll_interval,
            "kardamom-da-watcher starting; publishing deposits onto tx_deposits"
        );

        let handle = spawn_watcher(publisher, source, cfg);
        // Wait for ctrl-c, then ask the watcher to exit at the next tick
        // boundary. Drop on the shutdown channel is also enough to signal,
        // but explicit send() gives a clearer log line.
        signal::ctrl_c()
            .await
            .map_err(|e| anyhow::anyhow!("ctrl-c handler failed: {e}"))?;
        let _ = handle.shutdown.send(());
        handle
            .task
            .await
            .map_err(|e| anyhow::anyhow!("watcher task panicked: {e}"))?;
        Ok::<(), anyhow::Error>(())
    })?;
    drop(aeron_rt);
    Ok(())
}

/// Live [`DepositPublisher`] backed by an Aeron `tx_deposits` publication.
/// Each observed deposit is republished onto the canonical `tx_deposits`
/// stream so the downstream sequencer can derive `DepositRef`s onto
/// `tx_ordering`.
struct LiveTxDepositsPublisher {
    handle: TxDepositsPublisherHandle,
}

impl LiveTxDepositsPublisher {
    fn new(handle: TxDepositsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl DepositPublisher for LiveTxDepositsPublisher {
    fn publish(&self, deposit: &Deposit) -> Result<BPosition, PublishError> {
        match self.handle.publish(deposit) {
            Ok(pos) => Ok(pos),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("back-pressure") {
                    Err(PublishError::Backpressure)
                } else {
                    Err(PublishError::Transport(msg))
                }
            }
        }
    }
}
