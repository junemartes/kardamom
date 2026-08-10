//! kardamom-da-watcher: L1 deposit monitor CLI.
//!
//! Parses an `--l1-rpc <URL>` + `--lockbox <ADDRESS>` (plus an optional
//! `--poll-interval <DURATION>`, default 12s), constructs an
//! [`da_watcher::RpcL1Source`] over an alloy HTTP provider, spawns the
//! watcher loop, and waits for ctrl-c. Each finalized L1 block is republished
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

use kardamom_da_watcher::{
    DaWatcherConfig, EpochPublisher, PublishError, RpcL1Source, spawn as spawn_watcher,
};
use kardamom_log::aeron_live::{AeronRuntime, TxDepositsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::recorder::{RecorderKind, record_stream_until_stopped};
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_types::{BPosition, EpochRecord};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "L1 epoch monitor — tails finalized L1 blocks and publishes one EpochRecord each onto tx_deposits"
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
    kardamom_obs::bin::init_tracing();

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
    let aeron_rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let tx_deposits_pub = TxDepositsPublisherHandle::open(&aeron_rt, &channels)
        .context("open TxDepositsPublisherHandle")?;

    // Archive recorder for tx_deposits, co-located with the publisher here, so
    // the executor can replay deposit envelopes on crash recovery.
    //
    // BARRIER: the watcher loop must not publish a single deposit before the
    // recording is confirmed active — recovery replays from record 0 and needs
    // every envelope, so a birth-of-stream gap permanently breaks executor
    // crash recovery. The recorder reports its startup outcome on `ready`; we
    // block on it below (the tx_deposits publication is already open, so the
    // recording materialises promptly) and treat failure as fatal: the
    // operator asked for --archive-durability, so running without it would be
    // a silent lie.
    let recorder_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let recorder_handle = if args.archive_durability {
        let aeron_dir = args.aeron_dir.clone();
        let channels = channels.clone();
        let stop = recorder_stop.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<i64, String>>();
        let handle = std::thread::Builder::new()
            .name("da-watcher-tx-deposits-recorder".into())
            .spawn(move || {
                // Shared recorder-thread body (kardamom_log::recorder): connect
                // a thread-confined archive session, record tx_deposits, report
                // the startup outcome on `ready`, hold until `stop`.
                if let Err(e) = record_stream_until_stopped(
                    aeron_dir.as_deref(),
                    &aeron_cfg,
                    &channels.tx_deposits_channel,
                    channels.tx_deposits_stream_id,
                    RecorderKind::TxDeposits,
                    &stop,
                    |outcome| {
                        if let Ok(recording_id) = &outcome {
                            tracing::info!(
                                recording_id = *recording_id,
                                "da-watcher: recording tx_deposits"
                            );
                        }
                        let _ = ready_tx.send(outcome);
                    },
                ) {
                    tracing::error!(error = %e, "tx_deposits recorder exited with error");
                }
            })
            .expect("spawn tx_deposits recorder thread");
        // Generous budget: normally one catalog-poll tick (~500ms); the
        // timeout only bounds a wedged/unreachable archive.
        match ready_rx.recv_timeout(Duration::from_secs(60)) {
            Ok(Ok(recording_id)) => {
                tracing::info!(recording_id, "tx_deposits recording confirmed active");
            }
            Ok(Err(e)) => anyhow::bail!(
                "archive durability requested but the tx_deposits recorder failed to start: {e}"
            ),
            Err(e) => anyhow::bail!(
                "archive durability requested but the tx_deposits recording did not become \
                 active within 60s: {e}"
            ),
        }
        Some(handle)
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
            "kardamom-da-watcher starting; publishing epochs onto tx_deposits"
        );

        let handle = spawn_watcher(publisher, source, cfg);
        // Wait for SIGTERM (orchestrator stop) or Ctrl-C, then ask the
        // watcher to exit at the next tick boundary. Drop on the shutdown
        // channel is also enough to signal, but explicit send() gives a
        // clearer log line.
        wait_for_shutdown().await;
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

/// Live [`EpochPublisher`] backed by an Aeron `tx_deposits` publication.
/// One epoch per finalized L1 block is published onto `tx_deposits`; the
/// downstream sequencer forwards each verbatim onto `tx_ordering` as an
/// origin-advancing record.
struct LiveTxDepositsPublisher {
    handle: TxDepositsPublisherHandle,
}

impl LiveTxDepositsPublisher {
    fn new(handle: TxDepositsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl EpochPublisher for LiveTxDepositsPublisher {
    fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError> {
        match self.handle.publish(epoch) {
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
