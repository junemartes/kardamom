//! kardamom-da-watcher: externally-sourced-transaction monitor CLI.
//!
//! Runs up to two independent origin watchers in one process:
//!
//! * **L1 deposits** (`--l1-rpc` + `--lockbox`, optional `--poll-interval`):
//!   an [`da_watcher::RpcL1Source`] over an alloy HTTP provider; each
//!   finalized L1 block becomes one `EpochRecord` on the `tx_deposits` Aeron
//!   channel via [`LiveTxDepositsPublisher`].
//! * **Interop** (`--interop-feed-url` + `--interop-peer-chain-id` +
//!   `--self-chain-id`): a WebSocket outbox feed from ONE peer Kardamom
//!   chain; each origin block that carried messages becomes one
//!   `RemoteEpochRecord` on `tx_remote_epochs` via
//!   [`LiveRemoteEpochsPublisher`].
//!
//! Either may run alone or both together — they share nothing but the Aeron
//! runtime, because a stalled peer pairing must not hold up L1 deposits (and
//! vice versa). At least one must be configured.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use anyhow::Context;
use clap::Parser;

use kardamom_da_watcher::interop::{
    InteropWatcherConfig, RemoteEpochPublisher, WsRemoteChainSource, spawn as spawn_interop_watcher,
};
use kardamom_da_watcher::{
    DaWatcherConfig, EpochPublisher, PublishError, RpcL1Source, WatcherHandle,
    spawn as spawn_watcher,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDepositsPublisherHandle, TxRemoteEpochsPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::recorder::{Recorder, RecorderKind, connect_archive};
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "origin monitor — tails finalized L1 blocks onto tx_deposits and/or a peer chain's outbox feed onto tx_remote_epochs"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoint (e.g. `http://127.0.0.1:8545`). Enables the
    /// L1 deposit path; requires `--lockbox`.
    #[arg(long)]
    l1_rpc: Option<String>,
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    #[arg(long)]
    lockbox: Option<String>,
    /// Polling cadence in seconds (default 12).
    #[arg(long, default_value_t = 12)]
    poll_interval_secs: u64,
    /// Peer Kardamom chain to source cross-chain messages FROM. Enables the
    /// interop path; requires `--interop-feed-url` and `--self-chain-id`.
    #[arg(long)]
    interop_peer_chain_id: Option<u64>,
    /// The peer validator's outbox-feed WebSocket endpoint
    /// (e.g. `ws://127.0.0.1:9944`).
    #[arg(long)]
    interop_feed_url: Option<String>,
    /// OUR chain id. The feed filters on it and `derive_remote_epoch` REJECTS
    /// (never drops) a message addressed elsewhere, so a wrong value here
    /// fail-stops the pair rather than executing another chain's traffic.
    #[arg(long)]
    self_chain_id: Option<u64>,
    /// Cursor seed: the first per-pair seq not yet canonicalised. 0 is correct
    /// only for a pair that has never run — a restart must be given the
    /// destination chain's recorded position, or the first derivation fault
    /// will be this flag.
    #[arg(long, default_value_t = 0)]
    interop_start_seq: u64,
    /// Pause before retrying after a feed transport/decode failure. Not a poll
    /// interval: the feed is stream-driven.
    #[arg(long, default_value_t = 2)]
    interop_retry_interval_secs: u64,
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

/// The L1 deposit path, resolved. Present only when both `--l1-rpc` and
/// `--lockbox` were given.
struct L1Path {
    rpc: String,
    cfg: DaWatcherConfig,
}

/// The interop path, resolved. Present only when the full peer triple
/// (`--interop-peer-chain-id`, `--interop-feed-url`, `--self-chain-id`) was
/// given.
struct InteropPath {
    peer_chain_id: u64,
    feed_url: String,
    cfg: InteropWatcherConfig,
}

/// Split the flags into the two independent origin paths.
///
/// A HALF-specified path is a hard error, not a silent skip: a process that
/// quietly ran only the other origin would look healthy while one origin never
/// advanced, and the resulting seq hole is exactly what a destination verifier
/// halts on much later.
fn resolve_paths(args: &Args) -> anyhow::Result<(Option<L1Path>, Option<InteropPath>)> {
    let l1 = match (&args.l1_rpc, &args.lockbox) {
        (Some(rpc), Some(lockbox)) => {
            let lockbox = Address::from_str(lockbox)
                .map_err(|e| anyhow::anyhow!("--lockbox is not a valid address: {e}"))?;
            Some(L1Path {
                rpc: rpc.clone(),
                cfg: DaWatcherConfig {
                    lockbox,
                    poll_interval: Duration::from_secs(args.poll_interval_secs),
                },
            })
        }
        (None, None) => None,
        _ => anyhow::bail!("--l1-rpc and --lockbox must be given together"),
    };

    let interop = match (
        args.interop_peer_chain_id,
        &args.interop_feed_url,
        args.self_chain_id,
    ) {
        (Some(peer_chain_id), Some(feed_url), Some(self_chain_id)) => {
            if peer_chain_id == self_chain_id {
                anyhow::bail!(
                    "--interop-peer-chain-id equals --self-chain-id ({self_chain_id}): a chain \
                     cannot be its own remote origin"
                );
            }
            Some(InteropPath {
                peer_chain_id,
                feed_url: feed_url.clone(),
                cfg: InteropWatcherConfig {
                    self_chain_id,
                    start_seq: args.interop_start_seq,
                    retry_interval: Duration::from_secs(args.interop_retry_interval_secs),
                },
            })
        }
        (None, None, _) => None,
        _ => anyhow::bail!(
            "--interop-peer-chain-id, --interop-feed-url and --self-chain-id must be given together"
        ),
    };

    if l1.is_none() && interop.is_none() {
        anyhow::bail!(
            "nothing to watch: give --l1-rpc + --lockbox, or the interop triple \
             (--interop-peer-chain-id + --interop-feed-url + --self-chain-id), or both"
        );
    }
    Ok((l1, interop))
}

fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();

    let args = Args::parse();
    let (l1, interop) = resolve_paths(&args)?;

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
    let tx_deposits_pub = l1
        .is_some()
        .then(|| TxDepositsPublisherHandle::open(&aeron_rt, &channels))
        .transpose()
        .context("open TxDepositsPublisherHandle")?;
    let tx_remote_epochs_pub = interop
        .is_some()
        .then(|| TxRemoteEpochsPublisherHandle::open(&aeron_rt, &channels))
        .transpose()
        .context("open TxRemoteEpochsPublisherHandle")?;

    // --archive-durability records tx_deposits specifically; with no L1 path
    // there is no such publication, and starting a recording on a stream this
    // process never writes would report durability it is not providing.
    if args.archive_durability && tx_deposits_pub.is_none() {
        anyhow::bail!("--archive-durability records tx_deposits and requires the L1 path");
    }

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
                if let Err(e) = run_tx_deposits_recorder(
                    aeron_dir.as_deref(),
                    &channels,
                    &aeron_cfg,
                    stop,
                    ready_tx,
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
        let mut watchers: Vec<(&'static str, WatcherHandle)> = Vec::new();

        if let (Some(l1), Some(tx_deposits_pub)) = (l1, tx_deposits_pub) {
            let provider = ProviderBuilder::new()
                .connect(&l1.rpc)
                .await
                .map_err(|e| anyhow::anyhow!("failed to connect to L1 RPC {}: {e}", l1.rpc))?;
            tracing::info!(
                l1_rpc = %l1.rpc,
                lockbox = ?l1.cfg.lockbox,
                poll_interval = ?l1.cfg.poll_interval,
                "kardamom-da-watcher: publishing L1 epochs onto tx_deposits"
            );
            watchers.push((
                "l1",
                spawn_watcher(
                    LiveTxDepositsPublisher::new(tx_deposits_pub),
                    RpcL1Source::new(provider),
                    l1.cfg,
                ),
            ));
        }

        if let (Some(interop), Some(tx_remote_epochs_pub)) = (interop, tx_remote_epochs_pub) {
            tracing::info!(
                feed_url = %interop.feed_url,
                origin = interop.peer_chain_id,
                self_chain_id = interop.cfg.self_chain_id,
                start_seq = interop.cfg.start_seq,
                "kardamom-da-watcher: publishing remote epochs onto tx_remote_epochs"
            );
            let source = WsRemoteChainSource::new(
                interop.peer_chain_id,
                interop.cfg.self_chain_id,
                interop.feed_url,
            );
            watchers.push((
                "interop",
                spawn_interop_watcher(
                    LiveRemoteEpochsPublisher::new(tx_remote_epochs_pub),
                    source,
                    interop.cfg,
                ),
            ));
        }

        // Wait for SIGTERM (orchestrator stop) or Ctrl-C, then ask each
        // watcher to exit at the next tick boundary. Drop on the shutdown
        // channel is also enough to signal, but explicit send() gives a
        // clearer log line. A watcher that already fail-stopped on its own
        // (an interop derivation fault) is simply already finished.
        wait_for_shutdown().await;
        for (name, handle) in watchers {
            let _ = handle.shutdown.send(());
            handle
                .task
                .await
                .map_err(|e| anyhow::anyhow!("{name} watcher task panicked: {e}"))?;
        }
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
/// publication until `stop` is set, reporting the startup outcome on `ready`
/// (the F13.2 barrier the binary blocks on before publishing any deposit).
/// The recording runs in the ArchivingMediaDriver; this thread keeps the
/// session connected (and re-adopts an existing recording on restart).
fn run_tx_deposits_recorder(
    aeron_dir: Option<&std::path::Path>,
    channels: &kardamom_log::config::ChannelsConfig,
    aeron_cfg: &kardamom_log::config::AeronConfig,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ready: std::sync::mpsc::Sender<Result<i64, String>>,
) -> anyhow::Result<()> {
    let session = match connect_archive(aeron_dir, aeron_cfg) {
        Ok(s) => s,
        Err(e) => {
            let _ = ready.send(Err(format!("connect archive: {e}")));
            return Err(e).context("connect archive");
        }
    };
    let mut should_stop = || stop.load(std::sync::atomic::Ordering::SeqCst);
    let recorder = match Recorder::start_stream(
        session.archive,
        &channels.tx_deposits_channel,
        channels.tx_deposits_stream_id,
        RecorderKind::TxDeposits,
        &mut should_stop,
    ) {
        Ok(Some(r)) => r,
        Ok(None) => {
            // Stopped before the recording materialised (shutdown during
            // startup); report it so the waiting barrier doesn't hang.
            let _ = ready.send(Err("stopped before the recording materialised".into()));
            return Ok(());
        }
        Err(e) => {
            let _ = ready.send(Err(format!("start tx_deposits recording: {e}")));
            return Err(e).context("start tx_deposits recording");
        }
    };
    tracing::info!(
        recording_id = recorder.recording_id(),
        "da-watcher: recording tx_deposits"
    );
    let _ = ready.send(Ok(recorder.recording_id()));
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(500));
    }
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
                // Aeron's own token, via `offer_code_str` — matching on
                // prose ("back-pressure") never fires, so every stall used to
                // report as Transport.
                if msg.contains("BACK_PRESSURED") {
                    Err(PublishError::Backpressure)
                } else {
                    Err(PublishError::Transport(msg))
                }
            }
        }
    }
}

/// Live [`RemoteEpochPublisher`] backed by an Aeron `tx_remote_epochs`
/// publication — [`LiveTxDepositsPublisher`] for the interop path. One record
/// per peer-origin block that carried messages; the sequencer relays each
/// verbatim onto `tx_ordering` as a remote-origin-advancing record.
struct LiveRemoteEpochsPublisher {
    handle: TxRemoteEpochsPublisherHandle,
}

impl LiveRemoteEpochsPublisher {
    fn new(handle: TxRemoteEpochsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl RemoteEpochPublisher for LiveRemoteEpochsPublisher {
    /// A failed offer MUST be reported as failed. Both non-`Closed` variants
    /// are non-fatal — the watcher holds its cursor and re-derives the same
    /// batch, which is safe only because re-derivation is byte-identical, so
    /// cluster dedup on `canonical_id` absorbs a record that actually landed.
    /// The reverse error is unrecoverable: a publisher that reported a failed
    /// publish as complete would advance the cursor past a record that never
    /// existed, leaving a permanent hole in the pair's dense seq that the
    /// destination halts on and no retry can fill.
    fn publish(&self, record: &RemoteEpochRecord) -> Result<BPosition, PublishError> {
        match self.handle.publish(record) {
            Ok(pos) => Ok(pos),
            Err(e) => {
                let msg = e.to_string();
                // Aeron's own token, via `offer_code_str` — matching on
                // prose ("back-pressure") never fires, so every stall used to
                // report as Transport.
                if msg.contains("BACK_PRESSURED") {
                    Err(PublishError::Backpressure)
                } else {
                    Err(PublishError::Transport(msg))
                }
            }
        }
    }
}
