//! kardamom-da-watcher: L1 deposit monitor CLI.
//!
//! It parses `--l1-rpc <URL>` and `--lockbox <ADDRESS>`, plus an optional
//! `--poll-interval <DURATION>` (default 12s). It builds an
//! [`da_watcher::RpcL1Source`] over an alloy HTTP provider, spawns the
//! watcher loop, and waits for ctrl-c. Each finalized L1 block is
//! republished on the `tx_deposits` Aeron channel through the live
//! [`LiveTxDepositsPublisher`] adapter, which wraps
//! `kardamom_log::TxDepositsPublisherHandle`.

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
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "L1 epoch monitor — tails finalized L1 blocks and publishes one EpochRecord each onto tx_deposits"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoint, for example `http://127.0.0.1:8545`.
    #[arg(long)]
    l1_rpc: String,
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    #[arg(long)]
    lockbox: String,
    /// Polling cadence in seconds (default 12).
    #[arg(long, default_value_t = 12)]
    poll_interval_secs: u64,
    /// Optional `LogConfig` TOML that supplies the Aeron `[channels]`
    /// config. If unset, this uses built-in single-host IPC defaults,
    /// which keep local and e2e behavior unchanged. A multi-host
    /// deployment points this at the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`). If omitted, this falls
    /// back to the Aeron client's default lookup (the `AERON_DIR`
    /// environment variable, or the OS default). The local-e2e `just`
    /// recipe always passes this explicitly.
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Record the tx_deposits publication to the Aeron Archive, so the
    /// executor can replay deposit envelopes on crash recovery
    /// (`kardamom_log::replay`). This is off by default. The cluster
    /// enables it where the archive runs.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9005")]
    metrics_addr: SocketAddr,
    /// Host identifier. It is stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
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
    .await
    .context("init prometheus exporter")?;
    kardamom_da_watcher::metrics::describe();

    let resolved = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = resolved.channels;
    let aeron_cfg = resolved.aeron;
    let aeron_rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let tx_deposits_pub = TxDepositsPublisherHandle::open(&aeron_rt, &channels)
        .context("open TxDepositsPublisherHandle")?;

    // Archive recorder for tx_deposits, placed here with the publisher, so
    // the executor can replay deposit envelopes on crash recovery. The
    // thread stays a std thread. It holds an Aeron archive session, which
    // is `!Send`. The seam to the async shell uses a `CancellationToken`
    // for stop and a `oneshot` channel for readiness.
    //
    // The watcher loop must not publish a single deposit before the
    // recording is confirmed active. Recovery replays from record 0 and
    // needs every envelope, so a gap at the start of the stream would
    // permanently break executor crash recovery. The recorder reports its
    // startup outcome on `ready`. The code below waits on it (the
    // tx_deposits publication is already open, so the recording starts
    // quickly) and treats failure as fatal. The operator asked for
    // --archive-durability, so running without it would be a silent lie.
    let stop = CancellationToken::new();
    let recorder_handle = if args.archive_durability {
        let aeron_dir = args.aeron_dir.clone();
        let channels = channels.clone();
        let stop = stop.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<Result<i64, String>>();
        let handle = std::thread::Builder::new()
            .name("da-watcher-tx-deposits-recorder".into())
            .spawn(move || {
                // Shared recorder-thread body (kardamom_log::recorder):
                // connect a thread-confined archive session, record
                // tx_deposits, report the startup outcome on `ready`, and
                // hold until `stop`.
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
        // This budget is generous: normally one catalog-poll tick is about
        // 500ms. The timeout only bounds a stuck or unreachable archive.
        match tokio::time::timeout(Duration::from_secs(60), ready_rx).await {
            Ok(Ok(Ok(recording_id))) => {
                tracing::info!(recording_id, "tx_deposits recording confirmed active");
            }
            Ok(Ok(Err(e))) => anyhow::bail!(
                "archive durability requested but the tx_deposits recorder failed to start: {e}"
            ),
            Ok(Err(_)) => anyhow::bail!(
                "archive durability requested but the tx_deposits recorder thread exited before \
                 reporting readiness"
            ),
            Err(_) => anyhow::bail!(
                "archive durability requested but the tx_deposits recording did not become \
                 active within 60s"
            ),
        }
        Some(handle)
    } else {
        None
    };

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
    // Wait for SIGTERM (an orchestrator stop) or Ctrl-C, then ask the
    // watcher to exit at the next tick boundary. Dropping the shutdown
    // channel would also signal this, but an explicit send() gives a
    // clearer log line.
    wait_for_shutdown().await;
    let _ = handle.shutdown.send(());
    handle
        .task
        .await
        .map_err(|e| anyhow::anyhow!("watcher task panicked: {e}"))?;

    stop.cancel();
    if let Some(h) = recorder_handle {
        // The recorder thread polls the stop flag; joining it blocks, so
        // move the join off the runtime workers.
        let _ = tokio::task::spawn_blocking(move || h.join()).await;
    }
    drop(aeron_rt);
    Ok(())
}

/// Live [`EpochPublisher`] backed by an Aeron `tx_deposits` publication.
/// It publishes one epoch per finalized L1 block on `tx_deposits`. The
/// downstream sequencer forwards each one, unchanged, onto `tx_ordering` as
/// an origin-advancing record.
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
