//! kardamom-da-watcher: externally-sourced-transaction monitor CLI.
//!
//! It runs up to two independent origin watchers in one process:
//!
//! * L1 deposits (`--l1-rpc` and `--lockbox`, plus an optional
//!   `--poll-interval`): an `da_watcher::RpcL1Source` over an alloy HTTP
//!   provider. Each finalized L1 block becomes one `EpochRecord` on the
//!   `tx_deposits` Aeron channel, through
//!   [`publishers::LiveTxDepositsPublisher`].
//! * Interop (`--interop-feed-url`, `--interop-peer-chain-id`, and
//!   `--self-chain-id`): a WebSocket outbox feed from one peer Kardamom
//!   chain. Each origin block that carried messages becomes one
//!   `RemoteEpochRecord` on `tx_remote_epochs`, through
//!   [`publishers::LiveRemoteEpochsPublisher`]. At startup the cursor file
//!   is reconciled with the destination's `Inbox.nextSeq` through
//!   `--interop-dest-rpc` (see `interop::reconcile`).
//!
//! Either path can run alone, or both together. They share nothing but the
//! Aeron runtime: a stalled peer pairing must not hold up L1 deposits, and
//! the reverse must also hold. At least one path must be set.

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::Context;
use clap::Parser;

use kardamom_da_watcher::DaWatcherConfig;
use kardamom_da_watcher::interop::{
    CursorFile, CursorReconcile, InteropWatcherConfig, ReconcileRetry, RpcDestinationReader,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDepositsPublisherHandle, TxRemoteEpochsPublisherHandle,
};
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_log::recorder::{RecorderKind, RecorderThreads, record_stream_until_stopped};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[path = "kardamom-da-watcher/publishers.rs"]
mod publishers;
#[path = "kardamom-da-watcher/watchers.rs"]
mod watchers;

use watchers::Watchers;

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
    /// Polling cadence in seconds (default 12). Must be nonzero: 0 reaches
    /// `tokio::time::interval`, which panics on a zero period.
    #[arg(long, default_value = "12")]
    poll_interval_secs: NonZeroU64,
    /// Peer Kardamom chain to source cross-chain messages from. Enables the
    /// interop path. It requires `--interop-feed-url` and `--self-chain-id`.
    /// Chain id 0 is not valid.
    #[arg(long)]
    interop_peer_chain_id: Option<NonZeroU64>,
    /// The peer validator's outbox-feed WebSocket endpoint, for example
    /// `ws://127.0.0.1:9944`.
    #[arg(long)]
    interop_feed_url: Option<String>,
    /// Our chain id. The feed filters on it. `derive_remote_epoch` rejects,
    /// and never drops, a message addressed elsewhere. A wrong value here
    /// fail-stops the pair instead of executing another chain's traffic.
    /// Chain id 0 is not valid.
    #[arg(long)]
    self_chain_id: Option<NonZeroU64>,
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
    /// poll interval: the feed is stream-driven. Must be nonzero, or the
    /// retry path becomes a tight reconnect loop.
    #[arg(long, default_value = "2")]
    interop_retry_interval_secs: NonZeroU64,
    /// Exit the process nonzero when the interop pair fail-stops (a
    /// derivation fault or a feed lag), even while the L1 path still runs.
    /// Default true. With `--interop-fault-exits=false` the L1 path keeps
    /// the process up and the halt shows only in the log and the
    /// `kardamom_da_watcher_remote_tick_total{outcome="fault"}` metric.
    #[arg(
        long,
        env = "KARDAMOM_INTEROP_FAULT_EXITS",
        default_value_t = true,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    interop_fault_exits: bool,
    /// JSON-RPC endpoint of OUR chain (the destination), for example the
    /// validator's `--serve-feed` address as `ws://host:port`. At startup
    /// the watcher reads `Inbox.nextSeq[origin]` there with
    /// `eth_getStorageAt` and reconciles the cursor file with it: a stale
    /// cursor advances, an equal cursor is fine, and a cursor AHEAD of the
    /// chain refuses to start. Required with the interop triple unless
    /// `--interop-skip-cursor-reconcile` is set.
    #[arg(long)]
    interop_dest_rpc: Option<String>,
    /// Skip the startup cursor reconcile. For tests only: a cursor that is
    /// ahead of the destination is a permanent lane hole.
    #[arg(long, default_value_t = false)]
    interop_skip_cursor_reconcile: bool,
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
    /// Record the `tx_deposits` publication to the Aeron Archive, so the
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
    host_id: kardamom_obs::HostId,
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
    /// How the startup cursor reconcile reaches the destination.
    cursor_reconcile: CursorReconcile,
}

impl InteropPath {
    /// Reconcile the resume position with the destination BEFORE anything
    /// is derived. A cursor ahead of the chain is fatal here, so the
    /// process exits before it can publish a hole. [`CursorReconcile::Skip`]
    /// is a no-op: the cursor file is trusted as is.
    async fn reconcile(&mut self) -> anyhow::Result<()> {
        let CursorReconcile::Rpc(url) = &self.cursor_reconcile else {
            return Ok(());
        };
        let reader = RpcDestinationReader::connect(url)
            .await
            .with_context(|| format!("connect --interop-dest-rpc {url}"))?;
        let reconciled = ReconcileRetry::default()
            .reconcile(&reader, self.peer_chain_id, self.cfg.start_seq)
            .await
            .context("reconcile the interop cursor with the destination")?;
        if reconciled != self.cfg.start_seq {
            // The file is behind the chain. Persisting the chain's cursor
            // is safe: every seq below it was delivered.
            self.cursor_file
                .persist(reconciled)
                .context("persist the reconciled cursor")?;
            self.cfg.start_seq = reconciled;
        }
        Ok(())
    }
}

/// Split the flags into the two independent origin paths.
///
/// A HALF-specified path is a hard error, not a silent skip: a process that
/// quietly ran only the other origin would look healthy while one origin never
/// advanced, and the resulting seq hole is exactly what a destination verifier
/// halts on much later.
fn resolve_paths(args: &Args) -> anyhow::Result<(Option<L1Path>, Option<InteropPath>)> {
    let l1 = args.l1_path()?;
    let interop = args.interop_path()?;
    if l1.is_none() && interop.is_none() {
        anyhow::bail!(
            "nothing to watch: give --l1-rpc + --lockbox, or the interop triple \
             (--interop-peer-chain-id + --interop-feed-url + --self-chain-id), or both"
        );
    }
    Ok((l1, interop))
}

impl Args {
    /// Resolve the L1 deposit path: `Some` only when both `--l1-rpc` and
    /// `--lockbox` were given.
    fn l1_path(&self) -> anyhow::Result<Option<L1Path>> {
        match (&self.l1_rpc, &self.lockbox) {
            (Some(rpc), Some(lockbox)) => {
                let lockbox = Address::from_str(lockbox)
                    .map_err(|e| anyhow::anyhow!("--lockbox is not a valid address: {e}"))?;
                Ok(Some(L1Path {
                    rpc: rpc.clone(),
                    cfg: DaWatcherConfig {
                        lockbox,
                        poll_interval: Duration::from_secs(self.poll_interval_secs.get()),
                    },
                }))
            }
            (None, None) => Ok(None),
            _ => anyhow::bail!("--l1-rpc and --lockbox must be given together"),
        }
    }

    /// Resolve the interop path: `Some` only when the full peer triple
    /// (`--interop-peer-chain-id`, `--interop-feed-url`,
    /// `--self-chain-id`) was given.
    fn interop_path(&self) -> anyhow::Result<Option<InteropPath>> {
        let (peer_chain_id, feed_url, self_chain_id) = match (
            self.interop_peer_chain_id,
            &self.interop_feed_url,
            self.self_chain_id,
        ) {
            (Some(peer_chain_id), Some(feed_url), Some(self_chain_id)) => {
                (peer_chain_id, feed_url, self_chain_id)
            }
            (None, None, _) => return Ok(None),
            _ => anyhow::bail!(
                "--interop-peer-chain-id, --interop-feed-url and --self-chain-id must be given \
                 together"
            ),
        };
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
        let Some(path) = &self.interop_cursor_file else {
            anyhow::bail!(
                "the interop path requires --interop-cursor-file (the durable resume position; \
                 --interop-start-seq only seeds the very first boot)"
            );
        };
        // `open` takes the cursor's file lock. A second watcher on the
        // same file stops here with `CursorError::Locked`.
        let cursor_file = CursorFile::open(path.clone()).context("open --interop-cursor-file")?;
        let start_seq = self.interop_start_seq(&cursor_file)?;
        let cursor_reconcile = CursorReconcile::parse(
            self.interop_dest_rpc.clone(),
            self.interop_skip_cursor_reconcile,
        )
        .context("--interop-dest-rpc / --interop-skip-cursor-reconcile")?;
        if matches!(cursor_reconcile, CursorReconcile::Skip) {
            tracing::warn!(
                "--interop-skip-cursor-reconcile: the cursor file is trusted as is; a cursor \
                 ahead of the destination is a permanent lane hole"
            );
        }
        Ok(Some(InteropPath {
            peer_chain_id: peer_chain_id.get(),
            feed_url: feed_url.clone(),
            cursor_file,
            cfg: InteropWatcherConfig {
                self_chain_id: self_chain_id.get(),
                start_seq,
                retry_interval: Duration::from_secs(self.interop_retry_interval_secs.get()),
            },
            cursor_reconcile,
        }))
    }

    /// The resume position to start from: the persisted cursor if the
    /// file already has one (a corrupt file stops the process here,
    /// before anything is derived — see [`CursorFile::load`] for why it
    /// is never treated as 0), or `--interop-start-seq` on first boot.
    fn interop_start_seq(&self, cursor_file: &CursorFile) -> anyhow::Result<u64> {
        match cursor_file.load().context("load --interop-cursor-file")? {
            Some(persisted) => Ok(self.resolve_persisted_start_seq(persisted)),
            None => Ok(self.interop_start_seq),
        }
    }

    /// Log when `--interop-start-seq` conflicts with a persisted cursor.
    /// Return the value to use: the persisted cursor always wins.
    fn resolve_persisted_start_seq(&self, persisted: u64) -> u64 {
        if self.interop_start_seq != 0 && self.interop_start_seq != persisted {
            tracing::info!(
                persisted,
                flag = self.interop_start_seq,
                "cursor file exists; ignoring --interop-start-seq"
            );
        }
        persisted
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();

    let args = Args::parse();
    let (l1, interop) = resolve_paths(&args)?;

    kardamom_obs::init(
        "da-watcher",
        args.metrics_addr,
        args.host_id.as_ref(),
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )
    .await
    .context("init prometheus exporter")?;
    kardamom_da_watcher::metrics::describe();

    let resolved = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    serve(args, l1, interop, resolved.channels, resolved.aeron).await
}

/// Run both origin watchers to completion. The Aeron runtime is scoped to
/// this function, so it drops as soon as the watchers and the recorder
/// thread have stopped, with no explicit `drop` needed.
async fn serve(
    args: Args,
    l1: Option<L1Path>,
    interop: Option<InteropPath>,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
) -> anyhow::Result<()> {
    let aeron_rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let service = DaWatcherService {
        aeron_rt,
        channels,
        aeron_cfg,
        aeron_dir: args.aeron_dir.clone(),
    };
    let (tx_deposits_pub, tx_remote_epochs_pub) =
        service.open_publishers(l1.is_some(), interop.is_some())?;

    // --archive-durability records tx_deposits specifically; with no L1 path
    // there is no such publication, and starting a recording on a stream this
    // process never writes would report durability it is not providing.
    if args.archive_durability && tx_deposits_pub.is_none() {
        anyhow::bail!("--archive-durability records tx_deposits and requires the L1 path");
    }

    let recorders = if args.archive_durability {
        service.start_deposits_recorder().await?
    } else {
        RecorderThreads::new()
    };

    let watchers = Watchers::spawn(
        l1,
        interop,
        tx_deposits_pub,
        tx_remote_epochs_pub,
        args.interop_fault_exits,
    )
    .await?;
    // A panicked watcher task or an all-fail-stopped exit both return `Err`
    // here. That return drops `recorders` in place, which stops and joins
    // the recorder thread. The thread is past startup, so it wakes at
    // once from its park on the stop token.
    watchers.await_shutdown_or_fail_stop().await?;

    // Joining the recorder thread blocks, so move the join off the
    // runtime workers.
    let _ = tokio::task::spawn_blocking(move || recorders.join()).await;
    Ok(())
}

/// The setup state `serve` builds once and both `open_publishers` and
/// `start_deposits_recorder` read from: the Aeron runtime and directory,
/// and the resolved channels/Aeron config. The watcher lifecycle needs
/// none of this; it lives on `Watchers`.
struct DaWatcherService {
    aeron_rt: AeronRuntime,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    aeron_dir: Option<PathBuf>,
}

impl DaWatcherService {
    /// Open the publications each configured path needs. `tx_deposits`
    /// opens only when the L1 path is set, `tx_remote_epochs` only when
    /// interop is; opening either unconditionally would advertise a
    /// publication this process may never write to.
    fn open_publishers(
        &self,
        want_l1: bool,
        want_interop: bool,
    ) -> anyhow::Result<(
        Option<TxDepositsPublisherHandle>,
        Option<TxRemoteEpochsPublisherHandle>,
    )> {
        let tx_deposits_pub = want_l1
            .then(|| TxDepositsPublisherHandle::open(&self.aeron_rt, &self.channels))
            .transpose()
            .context("open TxDepositsPublisherHandle")?;
        let tx_remote_epochs_pub = want_interop
            .then(|| TxRemoteEpochsPublisherHandle::open(&self.aeron_rt, &self.channels))
            .transpose()
            .context("open TxRemoteEpochsPublisherHandle")?;
        Ok((tx_deposits_pub, tx_remote_epochs_pub))
    }

    /// Start the `tx_deposits` archive recorder and wait for it to confirm
    /// an active recording before returning.
    ///
    /// The thread stays a std thread: it holds an Aeron archive session,
    /// which is `!Send`. The seam to the async shell is the stop token
    /// inside the returned [`RecorderThreads`], and a `oneshot` channel
    /// for readiness. Dropping the returned value stops the thread.
    ///
    /// The watcher loop must not publish a single deposit before the
    /// recording is confirmed active. Recovery replays from record 0 and
    /// needs every envelope, so a gap at the start of the stream would
    /// permanently break executor crash recovery. The operator asked for
    /// `--archive-durability`, so returning before the recording is live
    /// would run without it while claiming otherwise.
    async fn start_deposits_recorder(&self) -> anyhow::Result<RecorderThreads> {
        let aeron_dir = self.aeron_dir.clone();
        let aeron_cfg = self.aeron_cfg.clone();
        let channels = self.channels.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<Result<i64, String>>();
        let mut recorders = RecorderThreads::new();
        recorders
            .spawn("da-watcher-tx-deposits-recorder".into(), move |stop| {
                Self::run_tx_deposits_recorder(
                    aeron_dir.as_deref(),
                    &aeron_cfg,
                    &channels,
                    &stop,
                    ready_tx,
                );
            })
            .context("spawn tx_deposits recorder thread")?;
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
        Ok(recorders)
    }

    /// Run the shared recorder-thread body (`kardamom_log::recorder`):
    /// connect a thread-confined archive session, record `tx_deposits`,
    /// report the startup outcome on `ready_tx`, and hold until `stop`.
    fn run_tx_deposits_recorder(
        aeron_dir: Option<&Path>,
        aeron_cfg: &AeronConfig,
        channels: &ChannelsConfig,
        stop: &CancellationToken,
        ready_tx: oneshot::Sender<Result<i64, String>>,
    ) {
        if let Err(e) = record_stream_until_stopped(
            aeron_dir,
            aeron_cfg,
            &channels.tx_deposits_channel,
            channels.tx_deposits_stream_id,
            RecorderKind::TxDeposits,
            stop,
            move |outcome| Self::report_deposit_recording_ready(outcome, ready_tx),
        ) {
            tracing::error!(error = %e, "tx_deposits recorder exited with error");
        }
    }

    /// Log a successful recording start, then report the startup
    /// outcome on `ready_tx`.
    fn report_deposit_recording_ready(
        outcome: Result<i64, String>,
        ready_tx: oneshot::Sender<Result<i64, String>>,
    ) {
        if let Ok(recording_id) = &outcome {
            tracing::info!(
                recording_id = *recording_id,
                "da-watcher: recording tx_deposits"
            );
        }
        let _ = ready_tx.send(outcome);
    }
}
