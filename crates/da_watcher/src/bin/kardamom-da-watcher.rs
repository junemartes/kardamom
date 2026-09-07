//! kardamom-da-watcher: externally-sourced-transaction monitor CLI.
//!
//! It runs up to two independent origin watchers in one process:
//!
//! * L1 deposits (`--l1-rpc` and `--lockbox`, plus an optional
//!   `--poll-interval`): an [`da_watcher::RpcL1Source`] over an alloy HTTP
//!   provider. Each finalized L1 block becomes one `EpochRecord` on the
//!   `tx_deposits` Aeron channel, through [`LiveTxDepositsPublisher`].
//! * Interop (`--interop-feed-url`, `--interop-peer-chain-id`, and
//!   `--self-chain-id`): a WebSocket outbox feed from one peer Kardamom
//!   chain. Each origin block that carried messages becomes one
//!   `RemoteEpochRecord` on `tx_remote_epochs`, through
//!   [`LiveRemoteEpochsPublisher`].
//!
//! Either path can run alone, or both together. They share nothing but the
//! Aeron runtime: a stalled peer pairing must not hold up L1 deposits, and
//! the reverse must also hold. At least one path must be set.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use anyhow::Context;
use clap::Parser;

use kardamom_da_watcher::interop::{
    CursorFile, InteropWatcherConfig, RemoteEpochPublisher, WsRemoteChainSource,
    spawn as spawn_interop_watcher,
};
use kardamom_da_watcher::{
    DaWatcherConfig, EpochPublisher, PublishError, RpcL1Source, WatcherHandle,
    spawn as spawn_watcher,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDepositsPublisherHandle, TxRemoteEpochsPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::recorder::{RecorderKind, record_stream_until_stopped};
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "origin monitor — tails finalized L1 blocks onto tx_deposits and/or a peer chain's outbox feed onto tx_remote_epochs"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoint, for example `http://127.0.0.1:8545`. Enables
    /// the L1 deposit path. It requires `--lockbox`.
    #[arg(long)]
    l1_rpc: Option<String>,
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    #[arg(long)]
    lockbox: Option<String>,
    /// Polling cadence in seconds (default 12).
    #[arg(long, default_value_t = 12)]
    poll_interval_secs: u64,
    /// Peer Kardamom chain to source cross-chain messages from. Enables the
    /// interop path. It requires `--interop-feed-url` and `--self-chain-id`.
    #[arg(long)]
    interop_peer_chain_id: Option<u64>,
    /// The peer validator's outbox-feed WebSocket endpoint, for example
    /// `ws://127.0.0.1:9944`.
    #[arg(long)]
    interop_feed_url: Option<String>,
    /// Our chain id. The feed filters on it. `derive_remote_epoch` rejects,
    /// and never drops, a message addressed elsewhere. A wrong value here
    /// fail-stops the pair instead of executing another chain's traffic.
    #[arg(long)]
    self_chain_id: Option<u64>,
    /// Durable per-pair cursor file, required with the interop triple. The
    /// watcher saves its resume position here, atomically, after each
    /// successful publish. On restart the file overrides
    /// `--interop-start-seq`. A file that exists but does not parse is a
    /// hard error, never a silent seq 0.
    #[arg(long)]
    interop_cursor_file: Option<PathBuf>,
    /// Cursor seed used only when `--interop-cursor-file` does not exist yet
    /// (first boot of the pair). 0 is correct only for a pair that has never
    /// run. Once the file exists, it is authoritative, and this flag is
    /// ignored.
    #[arg(long, default_value_t = 0)]
    interop_start_seq: u64,
    /// Pause before a retry after a feed transport or decode failure. Not a
    /// poll interval: the feed is stream-driven.
    #[arg(long, default_value_t = 2)]
    interop_retry_interval_secs: u64,
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

/// The L1 deposit path, resolved. Present only when both `--l1-rpc` and
/// `--lockbox` were given.
struct L1Path {
    rpc: String,
    cfg: DaWatcherConfig,
}

/// The interop path, resolved. Present only when the full peer triple
/// (`--interop-peer-chain-id`, `--interop-feed-url`, `--self-chain-id`) plus
/// `--interop-cursor-file` were given.
struct InteropPath {
    peer_chain_id: u64,
    feed_url: String,
    cursor_file: CursorFile,
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
            // The cursor file is REQUIRED, not optional-with-a-default: a
            // watcher whose resume position lives only in a CLI flag replays
            // (or worse, skips) on every restart, and the skip direction is a
            // permanent lane hole.
            let Some(path) = &args.interop_cursor_file else {
                anyhow::bail!(
                    "the interop path requires --interop-cursor-file (the durable resume \
                     position; --interop-start-seq only seeds the very first boot)"
                );
            };
            // `open` takes the cursor's file lock. A second watcher on the
            // same file stops here with `CursorError::Locked`.
            let cursor_file =
                CursorFile::open(path.clone()).context("open --interop-cursor-file")?;
            // A corrupt file must stop the process HERE, before anything is
            // derived — see `CursorFile::load` for why it is never treated
            // as 0.
            let start_seq = match cursor_file.load().context("load --interop-cursor-file")? {
                Some(persisted) => {
                    if args.interop_start_seq != 0 && args.interop_start_seq != persisted {
                        tracing::info!(
                            persisted,
                            flag = args.interop_start_seq,
                            "cursor file exists; ignoring --interop-start-seq"
                        );
                    }
                    persisted
                }
                None => args.interop_start_seq,
            };
            Some(InteropPath {
                peer_chain_id,
                feed_url: feed_url.clone(),
                cursor_file,
                cfg: InteropWatcherConfig {
                    self_chain_id,
                    start_seq,
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
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
    .await
    .context("init prometheus exporter")?;
    kardamom_da_watcher::metrics::describe();

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
            cursor_file = %interop.cursor_file.path().display(),
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
                Some(interop.cursor_file),
            ),
        ));
    }

    // Wait for SIGTERM (an orchestrator stop) or Ctrl-C, then ask each
    // watcher to exit at the next tick boundary. Dropping the shutdown
    // channel would also signal this, but an explicit send() gives a
    // clearer log line.
    //
    // Also wait on the watcher tasks themselves. A watcher that finishes
    // without being asked has fail-stopped: an interop derivation fault, or
    // a closed publisher. While at least one other watcher still runs, the
    // process stays up. The fault domain is the pair, and killing the L1
    // deposit path because a peer feed served a gap would widen it. But
    // once the last watcher has fail-stopped, nothing is left to watch.
    // Staying up as a healthy-looking husk would hide the halt from the
    // orchestrator, so the process exits nonzero. This lets the supervisor,
    // or an e2e harness, see the fail-stop as a process outcome, not just a
    // log line.
    let all_fail_stopped = tokio::select! {
        _ = wait_for_shutdown() => false,
        () = async {
            loop {
                if watchers.iter().all(|(_, h)| h.task.is_finished()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        } => true,
    };
    for (name, handle) in watchers {
        if handle.task.is_finished() {
            tracing::error!(watcher = name, "watcher exited without a shutdown request");
        }
        let _ = handle.shutdown.send(());
        handle
            .task
            .await
            .map_err(|e| anyhow::anyhow!("{name} watcher task panicked: {e}"))?;
    }
    if all_fail_stopped {
        anyhow::bail!(
            "every configured watcher fail-stopped (no shutdown was requested); \
             exiting nonzero so the halt is a process outcome"
        );
    }

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
