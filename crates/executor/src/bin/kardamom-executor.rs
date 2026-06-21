//! `kardamom-executor`: standalone executor service process.
//!
//! Opens M tx_data subscribers + 1 tx_ordering subscriber + 1 tx_receipts
//! publisher via the log layer's Aeron runtime, wires them into the
//! executor's reader/exec/commit thread topology, and runs until SIGTERM /
//! Ctrl-C. The state backend is the libmdbx-backed `kardamom-state` writer,
//! opened at `--state-dir`: chain state is committed durably per block and
//! persists across restarts. Genesis is seeded once into a fresh env.
//!
//! Crash-recovery resume (replaying the canonical stream from the persisted
//! cursor) is Phase 2; until then a non-empty state DB is refused at startup so
//! we never double-apply already-committed blocks.

use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_executor::{
    CMessage, DepositSubscription, Executor, ExecutorConfig, ExecutorError, MdbxSnapshotSource,
    MdbxWriterQueue, MdbxWriterSignal, ResumePoint, TxDataSubscription, TxOrderingSubscription,
    TxReceiptsPublication,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxOrderingSubscriberHandle,
    TxReceiptsPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::replay;
use kardamom_state::{Durability, StateEnvBuilder, StateWriter, read_recovery_point, seed_genesis};
use kardamom_types::{
    AccountChange, BPosition, CodeEntry, Deposit, TxDataLoc, TxEnvelope, TxOrderingMessage,
};

/// CLI mirror of [`kardamom_state::Durability`] (clap renders the variants as
/// `durable` / `safe-no-sync`).
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum StateDurabilityArg {
    Durable,
    SafeNoSync,
}

impl From<StateDurabilityArg> for Durability {
    fn from(a: StateDurabilityArg) -> Self {
        match a {
            StateDurabilityArg::Durable => Durability::Durable,
            StateDurabilityArg::SafeNoSync => Durability::SafeNoSync,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-executor",
    version,
    about = "kardamom executor process"
)]
struct Args {
    /// Path to the TOML config file (schema: `ExecutorConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults (preserves local/e2e
    /// behaviour); multi-host deployments point this at the rendered UDP
    /// channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// This replica's index, used as the per-replica tx_receipts MDS endpoint
    /// selector (`channels.tx_receipts_endpoint(recorder_id)`). In the cluster
    /// this is wired from `${NOMAD_ALLOC_INDEX}` (the executor job is
    /// count-based with `distinct_hosts`), matching the co-located recorder's
    /// id. Only consulted when `tx_receipts_mds_enabled()`; the legacy shared
    /// single-channel path ignores it.
    #[arg(long, env = "KARDAMOM_RECORDER_ID", default_value_t = 0)]
    recorder_id: u32,
    /// Number of tx_data shards to subscribe to (defaults to 8 — matches
    /// the default `partition_count` in the sequencer).
    #[arg(long, default_value_t = 8)]
    shards: u8,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    chain_id: u64,
    /// Deprecated / ignored: the executor's startup block is now read from the
    /// persisted state cursor (`last_committed_block`, 0 for a fresh genesis
    /// DB). Kept so existing invocations don't error; the value is not used.
    #[arg(long, default_value_t = 1)]
    initial_block: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`). The
    /// chain id is taken from this file (must match `--chain-id` if both
    /// are set), and every `[[alloc]]` entry seeds the in-memory state
    /// DB with the listed balance / nonce / code so revm has account
    /// state to debit on the first transaction from each sender.
    #[arg(long)]
    chain: Option<PathBuf>,
    /// Directory for the libmdbx state database. The Nomad executor job mounts
    /// a persistent volume here so chain state survives restarts.
    #[arg(
        long,
        env = "KARDAMOM_STATE_DIR",
        default_value = "/opt/kardamom/state"
    )]
    state_dir: PathBuf,
    /// State durability mode. `durable` fdatasyncs on every block commit (the
    /// production default); `safe-no-sync` skips the fsync (tests / ephemeral
    /// runs only — unsafe on real hosts).
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    state_durability: StateDurabilityArg,
    /// UDP endpoint (`host:port`) the archive replay-merge binds to receive
    /// **replayed** tx_ordering fragments on crash recovery. Required only when
    /// resuming a non-empty state DB over MDC; ignored on a fresh start.
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) the archive replay-merge binds to receive the
    /// **live** tx_ordering stream once replay catches up. Required only when
    /// resuming a non-empty state DB over MDC; ignored on a fresh start.
    #[arg(long, env = "KARDAMOM_LIVE_DESTINATION")]
    live_destination_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9004")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "executor",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    kardamom_executor::metrics::describe();
    // Validate the config file exists so misconfigured deployments fail
    // fast. v0 doesn't yet drive ExecutorConfig from the TOML — operators
    // tune via the CLI flags above.
    let _raw =
        std::fs::read_to_string(&args.config).context("read executor config (presence check)")?;

    tracing::info!(
        shards = args.shards,
        chain_id = args.chain_id,
        "kardamom-executor starting"
    );

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = log_cfg.channels;
    // Archive-replay recovery (below) connects its own archive client; it needs
    // the archive control channels + media-driver dir from the AeronConfig. Use
    // the CLI `--aeron-dir` when given so it joins the same driver as the runtime.
    let mut aeron_cfg = log_cfg.aeron;
    if let Some(dir) = args.aeron_dir.as_ref() {
        aeron_cfg.aeron_dir = dir.clone();
    }
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // SEPARATE Aeron runtime/thread for the tx_receipts PUBLICATION. The
    // executor's single Aeron thread otherwise services both the tx_ordering
    // SUBSCRIPTION poll AND the per-tx receipt/boundary publishes; under
    // sustained load the publish work delays the tx_ordering poll past Aeron's
    // flow-control Status-Message deadline, the sealer drops this subscriber
    // from the tx_ordering MDC, its image dies, and the executor freezes
    // (reader stops, exec blocks reading). Isolating the publisher onto its own
    // thread keeps the subscription poll timely no matter the receipt load.
    let rt_pub = match args.aeron_dir.as_ref() {
        Some(dir) => {
            AeronRuntime::spawn_with_dir(dir).context("spawn receipts AeronRuntime with dir")?
        }
        None => AeronRuntime::spawn_default().context("spawn receipts AeronRuntime")?,
    };

    // --- State backend + crash-recovery decision (before the subscriptions,
    // because the tx_ordering subscription branches on whether we are resuming).
    // Load genesis (its chain_id is adopted when present).
    let genesis = match args.chain.as_ref() {
        Some(path) => Some(load_genesis(path)?),
        None => None,
    };
    let chain_id = genesis
        .as_ref()
        .map(|g| g.chain_id)
        .unwrap_or(args.chain_id);
    if let Some(g) = &genesis
        && args.chain_id != 1
        && args.chain_id != g.chain_id
    {
        anyhow::bail!(
            "--chain-id {} conflicts with genesis chain_id {}",
            args.chain_id,
            g.chain_id
        );
    }

    // Open the libmdbx state env and read the durable cursor.
    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;
    // Crash-recovery resume. A non-empty state DB means we restarted mid-chain:
    // re-read tx_ordering from the archive (replay-merge) and skip-count past the
    // durable cursor (see `ResumePoint` + the exec thread's skip logic). This
    // requires the UDP-MDC transport (archive replay-merge does not work over the
    // single-host IPC default), so over IPC a non-empty DB is still refused.
    let resume = if recovery.last_committed_block > 0 {
        if !channels.tx_ordering_mdc_enabled() {
            anyhow::bail!(
                "state DB at {} is at block {}, but tx_ordering MDC is disabled — archive \
                 replay-merge recovery needs the UDP-MDC transport. Start against a fresh \
                 --state-dir, or run with an MDC log-config.",
                args.state_dir.display(),
                recovery.last_committed_block,
            );
        }
        let rp = ResumePoint {
            block: recovery.last_committed_block,
            record_count: recovery.last_fsynced_b_position.as_index(),
        };
        tracing::info!(
            resume_block = rp.block,
            resume_record_count = rp.record_count,
            "resuming from persisted state cursor via tx_ordering archive replay"
        );
        Some(rp)
    } else {
        None
    };

    // On crash recovery (`resume.is_some()`) every inbound stream is read from
    // the archive replay-merge instead of live, so the executor re-sees the full
    // history (tx_ordering refs + tx_data/tx_deposits envelopes) and can
    // skip-count past its durable cursor AND re-execute any txs sealed while it
    // was down. The replay endpoints are shared across streams (the media driver
    // demuxes by stream id); only tx_ordering (MDC) also needs a live endpoint.
    let recovery_endpoints = if resume.is_some() {
        let replay_dst = args.replay_destination_endpoint.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "crash recovery needs --replay-destination-endpoint (host:port) for archive \
                 replay-merge"
            )
        })?;
        let live_dst = args.live_destination_endpoint.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "crash recovery needs --live-destination-endpoint (host:port) for archive \
                 replay-merge"
            )
        })?;
        Some((replay_dst, live_dst))
    } else {
        None
    };

    // M tx_data subscriptions, one per shard. We bridge each handle's
    // async `recv()` to a synchronous `next()` (what the executor's reader
    // thread expects) through a `std::sync::mpsc::channel`, with a
    // dedicated tokio pump task per shard.
    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(args.shards as usize);
    for shard_id in 0..args.shards {
        // Live on a fresh start; archive replay-merge on crash recovery. Both
        // paths yield (TxDataLoc, TxEnvelope): the live path reads the publisher
        // session from the fragment header, and replay recovers the SAME session
        // (frame-faithful), so the reader's session-keyed join works either way.
        let (tx, rx) = sync_mpsc::channel::<(TxDataLoc, TxEnvelope)>();
        if let Some((replay_dst, _)) = recovery_endpoints.as_ref() {
            let mut replay =
                replay::open_tx_data_replay(&channels, &aeron_cfg, shard_id, replay_dst.clone())
                    .with_context(|| format!("open tx_data replay-merge shard={shard_id}"))?;
            tokio::spawn(async move {
                while let Some(item) = replay.recv().await {
                    if tx.send(item).is_err() {
                        break;
                    }
                }
            });
        } else {
            let mut handle = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
                .with_context(|| format!("open TxDataSubscriberHandle shard={shard_id}"))?;
            tokio::spawn(async move {
                while let Some(item) = handle.recv().await {
                    if tx.send(item).is_err() {
                        break;
                    }
                }
            });
        }
        a_subs.push(Box::new(LiveTxDataSub {
            sequencer_id: shard_id,
            rx,
        }));
    }

    // 1 tx_ordering subscription, async→sync bridged. On a fresh start this is
    // the live MDC/IPC subscriber; on crash recovery it is the archive
    // replay-merge subscriber (replays the recorded stream from the start, then
    // follows live) so the exec thread can skip-count past its durable cursor.
    let (b_tx, b_rx) = sync_mpsc::channel::<(BPosition, TxOrderingMessage)>();
    if let Some((replay_dst, live_dst)) = recovery_endpoints.as_ref() {
        let mut replay = replay::open_tx_ordering_replay(
            &channels,
            &aeron_cfg,
            replay_dst.clone(),
            live_dst.clone(),
        )
        .context("open tx_ordering archive replay-merge subscriber")?;
        tokio::spawn(async move {
            while let Some(item) = replay.recv().await {
                if b_tx.send(item).is_err() {
                    break;
                }
            }
        });
    } else {
        let mut tx_ordering_handle = TxOrderingSubscriberHandle::open(&rt, &channels)
            .context("open TxOrderingSubscriberHandle")?;
        tokio::spawn(async move {
            while let Some(item) = tx_ordering_handle.recv().await {
                if b_tx.send(item).is_err() {
                    break;
                }
            }
        });
    }
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(LiveTxOrderingSub { rx: b_rx });

    // 1 tx_deposits subscription. The executor's deposit reader thread
    // joins `(deposit_position, Deposit)` envelopes against the
    // `DepositRef`s observed on tx_ordering; `source_hash` is the dedup
    // key. Live on a fresh start; archive replay-merge on recovery.
    let (d_tx, d_rx) = sync_mpsc::channel::<(BPosition, Deposit)>();
    if let Some((replay_dst, _)) = recovery_endpoints.as_ref() {
        let mut replay = replay::open_tx_deposits_replay(&channels, &aeron_cfg, replay_dst.clone())
            .context("open tx_deposits archive replay-merge subscriber")?;
        tokio::spawn(async move {
            while let Some(item) = replay.recv().await {
                if d_tx.send(item).is_err() {
                    break;
                }
            }
        });
    } else {
        let mut tx_deposits_handle = TxDepositsSubscriberHandle::open(&rt, &channels)
            .context("open TxDepositsSubscriberHandle")?;
        tokio::spawn(async move {
            while let Some(item) = tx_deposits_handle.recv().await {
                if d_tx.send(item).is_err() {
                    break;
                }
            }
        });
    }
    let dep_sub: Box<dyn DepositSubscription> = Box::new(LiveTxDepositsSub { rx: d_rx });

    // tx_receipts publication. With MDS (fan-in) enabled, this replica
    // publishes both the receipt stream and the boundary side-stream to its
    // OWN per-replica unicast endpoint (selected by --recorder-id); ingress
    // aggregates every replica's endpoint onto one multi-destination
    // subscription. Without MDS (the IPC default), fall back to the shared
    // single-channel path so single-host/local behaviour is unchanged. Either
    // way the commit thread's must-deliver retry drives the same
    // publish_receipt/publish_boundary surface.
    let c_handle = if channels.tx_receipts_mds_enabled() {
        tracing::info!(
            replica_idx = args.recorder_id,
            endpoint = channels.tx_receipts_endpoint(args.recorder_id).as_deref(),
            "tx_receipts MDS publish (per-replica endpoint)"
        );
        TxReceiptsPublisherHandle::open_mds(&rt_pub, &channels, args.recorder_id)
            .context("open TxReceiptsPublisherHandle (MDS)")?
    } else {
        TxReceiptsPublisherHandle::open(&rt_pub, &channels)
            .context("open TxReceiptsPublisherHandle")?
    };
    let c_pub = LiveTxReceiptsPub { handle: c_handle };

    // Seed genesis once into a fresh env (no-op if already seeded, e.g. on
    // recovery). Must run before StateWriter::spawn so the writer's initial
    // published snapshot already reflects genesis.
    let (genesis_accounts, genesis_code) = build_genesis_alloc(genesis.as_ref());
    let seeded = seed_genesis(&env, &genesis_accounts, &genesis_code)
        .context("seed genesis into state env")?;
    tracing::info!(
        state_dir = %args.state_dir.display(),
        durability = ?args.state_durability,
        genesis_accounts = genesis_accounts.len(),
        seeded,
        "state env opened"
    );

    // Spawn the writer; build the three executor adapters from its handle. The
    // snapshot-swap channel feeds reads (snapshot source + commit signal); the
    // delta channel feeds writes.
    let writer = StateWriter::spawn(env).context("spawn state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let sw_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
    let sw_queue = MdbxWriterQueue::new(writer.delta_tx.clone());

    let mut cfg = ExecutorConfig {
        chain_id,
        ..ExecutorConfig::default()
    };
    // During recovery the tx_ordering and per-shard tx_data streams replay
    // independently from the archive and catch up at different rates, so the
    // tight live join timeout (100 ms) would fire spuriously when a TxRef
    // arrives before its replayed envelope. Relax it generously while resuming;
    // a genuine missing-envelope still aborts, just after a longer grace.
    if resume.is_some() {
        cfg.reader.join_timeout = std::time::Duration::from_secs(30);
    }
    // Resume at the persisted cursor — 0 for a fresh genesis DB.
    let initial_block = recovery.last_committed_block;

    // The executor's main loop is sync (std::thread spawns underneath).
    // Run it inside spawn_blocking so the runtime stays responsive for
    // shutdown handling.
    let join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            Some(dep_sub),
            c_pub,
            snapshots,
            sw_signal,
            sw_queue,
            initial_block,
            // On crash recovery `resume` carries the persisted cursor; the exec
            // thread skip-counts the replayed prefix past it. `None` on a fresh
            // start. The tx_ordering subscription above is the matching archive
            // replay-merge when this is `Some`.
            resume,
        )
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-executor: shutdown signal received; dropping runtime");
    // Dropping the AeronRuntime closes every subscription, which causes
    // the tokio pump tasks to return None, which closes the sync mpsc
    // channels, which surfaces TxDataClosed / TxOrderingClosed to the
    // executor — clean shutdown.
    drop(rt);
    match join.await {
        Ok(Ok(())) => tracing::info!("executor main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "executor main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "executor task panicked"),
    }
    // Stop the state writer thread (closes the delta channel, joins, surfaces
    // its final result). The executor task has finished, so its adapter clones
    // of the delta sender are already dropped.
    if let Err(e) = writer.shutdown() {
        tracing::error!(error = %e, "state writer shutdown returned an error");
    }
    Ok(())
}

/// Load a kardamom genesis TOML and run its semantic validation
/// (chain_id != 0, no duplicate alloc addresses).
fn load_genesis(path: &std::path::Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

/// Build the genesis allocation set (accounts + code) from a `Genesis`, ready
/// for [`seed_genesis`]. Every `AllocEntry` becomes one `AccountChange` with the
/// declared balance/nonce and the keccak256 hash of its code (if any); the code
/// bytes become a `CodeEntry` retrievable via `code_by_hash`. Returns empty vecs
/// when no genesis was supplied.
fn build_genesis_alloc(
    genesis: Option<&kardamom_types::Genesis>,
) -> (Vec<AccountChange>, Vec<CodeEntry>) {
    use alloy_primitives::{B256, keccak256};
    let mut accounts = Vec::new();
    let mut code = Vec::new();
    let Some(g) = genesis else {
        return (accounts, code);
    };
    for entry in &g.alloc {
        let nonce = entry.nonce.unwrap_or(0);
        let code_hash = entry
            .code
            .as_ref()
            .map(|c| keccak256(c.as_ref()))
            .unwrap_or(B256::ZERO);
        tracing::info!(
            address = ?entry.address,
            balance = %entry.balance,
            nonce,
            has_code = entry.code.is_some(),
            "seeding genesis account"
        );
        accounts.push(AccountChange {
            address: entry.address,
            nonce,
            balance: entry.balance,
            code_hash,
        });
        if let Some(c) = entry.code.as_ref() {
            // `AllocEntry.code` is `alloy_primitives::Bytes`; `CodeEntry.code`
            // is `bytes::Bytes`. They wrap the same buffer (`c.0`).
            code.push(CodeEntry {
                code_hash,
                code: c.0.clone(),
            });
        }
    }
    (accounts, code)
}

// ---------------------------------------------------------------------------
// Adapters: async log handles → sync executor traits.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    sequencer_id: u8,
    rx: sync_mpsc::Receiver<(TxDataLoc, TxEnvelope)>,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

struct LiveTxOrderingSub {
    rx: sync_mpsc::Receiver<(BPosition, TxOrderingMessage)>,
}

impl TxOrderingSubscription for LiveTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}

struct LiveTxDepositsSub {
    rx: sync_mpsc::Receiver<(BPosition, Deposit)>,
}

impl DepositSubscription for LiveTxDepositsSub {
    fn next(&mut self) -> Result<(BPosition, Deposit), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::DepositsClosed)
    }
}

struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self
                .handle
                .publish_receipt(&r)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_receipt: {e}"))),
            // BEST-EFFORT: a block-boundary marker. Ingress acks on the receipt /
            // durable watermark, not on this — and blocking the commit thread
            // here (e.g. at startup before ingress's MDS destinations attach)
            // would freeze ALL state progress. Fire-and-forget so empty blocks
            // never stall the executor; a dropped boundary is harmless.
            CMessage::BlockBoundary(b) => self
                .handle
                .publish_boundary_best_effort(&b)
                .map_err(|e| ExecutorError::State(format!("publish_boundary: {e}"))),
        }
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
