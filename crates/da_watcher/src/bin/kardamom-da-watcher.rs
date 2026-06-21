//! kardamom-da-watcher: L1 deposit monitor CLI.
//!
//! Parses an `--l1-rpc <URL>` + `--lockbox <ADDRESS>` (plus an optional
//! `--poll-interval <DURATION>`, default 12s), constructs an
//! [`da_watcher::RpcL1Source`] over an alloy HTTP provider, spawns the
//! watcher loop, and waits for ctrl-c. Each observed deposit is republished
//! onto the `tx_deposits` Aeron channel via the live
//! [`LiveTxDepositsPublisher`] adapter (wraps `kardamom_log::TxDepositsPublisherHandle`).

use std::net::SocketAddr;
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
use kardamom_log::recorder::{Recorder, RecorderKind, connect_archive};
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
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults (preserves local/e2e
    /// behaviour); multi-host deployments point this at the rendered UDP
    /// channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`). When omitted, falls
    /// back to the Aeron client's default lookup (`AERON_DIR` env / OS
    /// default). The local-e2e `just` recipe always passes this explicitly.
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Record the tx_deposits publication to the Aeron Archive so the executor
    /// can replay deposit envelopes on crash recovery (`kardamom_log::replay`).
    /// Off by default; the cluster sets this where the archive runs.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9005")]
    metrics_addr: SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
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

    kardamom_obs::init(
        "da-watcher",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )
    .context("init prometheus exporter")?;
    kardamom_da_watcher::metrics::describe();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let resolved = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = resolved.channels;
    let aeron_cfg = resolved.aeron;
    let aeron_rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };
    let tx_deposits_pub = TxDepositsPublisherHandle::open(&aeron_rt, &channels)
        .context("open TxDepositsPublisherHandle")?;

    // Archive recorder for tx_deposits, co-located with the publisher here, so
    // the executor can replay deposit envelopes on crash recovery.
    let recorder_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let recorder_handle = if args.archive_durability {
        let aeron_dir = args.aeron_dir.clone();
        let channels = channels.clone();
        let stop = recorder_stop.clone();
        Some(
            std::thread::Builder::new()
                .name("da-watcher-tx-deposits-recorder".into())
                .spawn(move || {
                    if let Err(e) =
                        run_tx_deposits_recorder(aeron_dir.as_deref(), &channels, &aeron_cfg, stop)
                    {
                        tracing::error!(error = %e, "tx_deposits recorder exited with error");
                    }
                })
                .expect("spawn tx_deposits recorder thread"),
        )
    } else {
        None
    };

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
    recorder_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(h) = recorder_handle {
        let _ = h.join();
    }
    drop(aeron_rt);
    Ok(())
}

/// Connect a thread-confined archive session and record the tx_deposits
/// publication until `stop` is set. The recording runs in the
/// ArchivingMediaDriver; this thread keeps the session connected (and re-adopts
/// an existing recording on restart).
fn run_tx_deposits_recorder(
    aeron_dir: Option<&std::path::Path>,
    channels: &kardamom_log::config::ChannelsConfig,
    aeron_cfg: &kardamom_log::config::AeronConfig,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let session = connect_archive(aeron_dir, aeron_cfg).context("connect archive")?;
    let mut should_stop = || stop.load(std::sync::atomic::Ordering::SeqCst);
    let recorder = match Recorder::start_stream(
        session.archive,
        &channels.tx_deposits_channel,
        channels.tx_deposits_stream_id,
        RecorderKind::TxDeposits,
        &mut should_stop,
    )
    .context("start tx_deposits recording")?
    {
        Some(r) => r,
        None => return Ok(()),
    };
    tracing::info!(
        recording_id = recorder.recording_id(),
        "da-watcher: recording tx_deposits"
    );
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(500));
    }
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
