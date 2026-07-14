//! `kardamom-validator`: monolithic validator node.
//!
//! Follows the sequencer by subscribing to the same canonical streams the
//! executor reads (`tx_data` × M, `tx_ordering` from the Aeron Cluster (Raft)
//! egress, `tx_deposits`), re-executes
//! every block through the shared `kardamom-engine` pipeline, and commits to its
//! own libmdbx state via the **trie-aware** writer — advancing a canonical
//! Ethereum MPT state root per block. It additionally subscribes to the
//! executor's `tx_receipts` and per-block `tx_bal` (BAL) streams and cross-checks
//! its independent re-execution against them, fail-stopping on any proven
//! divergence. No HA; off the hot path.
//!
//! Milestone 1: re-execute from genesis (or resume via the same archive
//! replay-merge the executor uses) + produce roots + cross-check. It publishes
//! nothing.

use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::reader::cluster::ClusterConfig;
use kardamom_engine::{
    DepositSubscription, Executor, ExecutorConfig, ExecutorError, MdbxSnapshotSource,
    MdbxWriterQueue, MdbxWriterSignal, ResumePoint, TxDataSubscription, TxOrderingSubscription,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_log::replay;
use kardamom_state::{
    Durability, StateEnvBuilder, StateWriter, TrieMode, read_recovery_point, seed_genesis,
};
use kardamom_types::{
    AccountChange, BPosition, BlockDelta, CodeEntry, Deposit, TxDataLoc, TxEnvelope,
};
use kardamom_validator::{
    BalBuffer, Divergence, ReceiptBuffer, ValidatorReceiptSink, ValidatorWriterQueue, metrics,
};

/// Top-level config the `kardamom-validator` binary deserializes from
/// `--config`. Same `[cluster]` section shape as the executor's.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
struct ValidatorFileConfig {
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS comes from
    /// the cluster egress — there is no non-cluster path.
    cluster: ClusterConfig,
}

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
    name = "kardamom-validator",
    version,
    about = "kardamom validator node"
)]
struct Args {
    /// Path to the TOML config file (presence-checked; tuning is via flags).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Number of tx_data shards to subscribe to.
    #[arg(long, default_value_t = 8)]
    shards: u8,
    /// Number of executor replicas whose tx_receipts endpoints to attach when
    /// tx_receipts MDS is enabled. Falls back to `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<u32>,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    chain_id: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`).
    #[arg(long)]
    chain: Option<PathBuf>,
    /// Directory for the libmdbx state database (the validator keeps its own).
    #[arg(
        long,
        env = "KARDAMOM_STATE_DIR",
        default_value = "/opt/kardamom/validator-state"
    )]
    state_dir: PathBuf,
    /// State durability mode.
    #[arg(long, value_enum, default_value_t = StateDurabilityArg::Durable)]
    state_durability: StateDurabilityArg,
    /// Enable the state-trie shadow-check: every N blocks, recompute the world
    /// state root by full rebuild and fail-stop on mismatch with the incremental
    /// walker (a canary against trie bugs). Absent ⇒ incremental only; `1` ⇒
    /// every block. Costs a full rebuild on the sampled blocks.
    #[arg(long, env = "KARDAMOM_TRIE_SHADOW_CHECK")]
    trie_shadow_check: Option<u64>,
    /// UDP endpoint the archive replay-merge binds to receive replayed
    /// tx_data / tx_deposits fragments on crash recovery (resume only —
    /// tx_ordering recovery is the cluster client's replay, not the archive).
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,
    /// This node's cluster-egress endpoint `ip:port`. Overrides/sets the
    /// [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9006")]
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
        "validator",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    kardamom_engine::metrics::describe();
    metrics::describe();
    // The TOML supplies the `[cluster]` section (the canonical tx_ordering
    // stream is ALWAYS the Aeron Cluster egress); all other runtime tuning
    // still comes from the CLI flags above.
    let raw = std::fs::read_to_string(&args.config).context("read validator config")?;
    let mut file_cfg: ValidatorFileConfig =
        toml::from_str(&raw).context("parse validator config")?;

    // Per-node cluster egress endpoint: the cluster client's egress_channel is
    // this node's reachable address, so it's injected by the deploy rather than
    // baked into the static config file.
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }

    tracing::info!(
        shards = args.shards,
        chain_id = args.chain_id,
        "kardamom-validator starting"
    );

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = log_cfg.channels;
    let mut aeron_cfg = log_cfg.aeron;
    if let Some(dir) = args.aeron_dir.as_ref() {
        aeron_cfg.aeron_dir = dir.clone();
    }
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // --- State backend + crash-recovery decision (mirrors the executor). ---
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

    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;
    let resume = if recovery.last_committed_block > 0 {
        let rp = ResumePoint {
            block: recovery.last_committed_block,
            record_count: recovery.last_fsynced_b_position.as_index(),
        };
        tracing::info!(
            resume_block = rp.block,
            "validator resuming from persisted cursor"
        );
        Some(rp)
    } else {
        None
    };
    let recovery_endpoints = if resume.is_some() {
        let replay_dst = args
            .replay_destination_endpoint
            .clone()
            .context("crash recovery needs --replay-destination-endpoint")?;
        Some(replay_dst)
    } else {
        None
    };

    // M tx_data subscriptions (async→sync bridged), identical to the executor.
    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(args.shards as usize);
    for shard_id in 0..args.shards {
        let (tx, rx) = sync_mpsc::channel::<(TxDataLoc, TxEnvelope)>();
        if let Some(replay_dst) = recovery_endpoints.as_ref() {
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

    // 1 tx_ordering subscription — ALWAYS the Aeron Cluster (Raft) egress,
    // exactly as in the executor. The cluster has already deduped + totally
    // ordered the stream and exposes a blocking `next()`, so no async→sync
    // bridge is needed. Leader failover / reconnect — including crash-recovery
    // replay of the canonical stream — is handled inside the cluster client.
    // The cluster-session guard (`LiveCluster`) + its dedicated Aeron runtime
    // must outlive the validator loop, so bind the guard in the outer scope;
    // it is dropped only after the `join` await below.
    let cluster_rt = match args.aeron_dir.as_ref() {
        Some(dir) => {
            AeronRuntime::spawn_with_dir(dir).context("spawn cluster AeronRuntime with dir")?
        }
        None => AeronRuntime::spawn_default().context("spawn cluster AeronRuntime")?,
    };
    // Replay cursor: resume from the persisted state cursor (fresh validators
    // start at genesis and receive the full retained canonical stream). The
    // replay request is re-sent on every session establishment, so a validator
    // whose session dies mid-chaos catches back up instead of fail-stopping on
    // an unrecoverable gap.
    let cluster_cursor = match &resume {
        Some(rp) => {
            kardamom_engine::reader::cluster::ReplayCursor::new(rp.record_count, rp.block + 1)
        }
        None => kardamom_engine::reader::cluster::ReplayCursor::genesis(),
    };
    let (cluster_guard, cluster_sub) =
        kardamom_engine::reader::cluster::cluster_tx_ordering_subscription(
            cluster_rt,
            file_cfg.cluster.to_live(),
            cluster_cursor,
        )
        .context("connect cluster tx_ordering subscription")?;
    tracing::info!("kardamom-validator: tx_ordering via Aeron Cluster");
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(cluster_sub);

    // tx_deposits subscription.
    let (d_tx, d_rx) = sync_mpsc::channel::<(BPosition, Deposit)>();
    if let Some(replay_dst) = recovery_endpoints.as_ref() {
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
        let mut handle = TxDepositsSubscriberHandle::open(&rt, &channels)
            .context("open TxDepositsSubscriberHandle")?;
        tokio::spawn(async move {
            while let Some(item) = handle.recv().await {
                if d_tx.send(item).is_err() {
                    break;
                }
            }
        });
    }
    let dep_sub: Box<dyn DepositSubscription> = Box::new(LiveTxDepositsSub { rx: d_rx });

    // --- Verification streams: tx_bal (BAL) + tx_receipts. ---
    let divergence = Divergence::new();
    let bals = BalBuffer::new();
    let receipts = ReceiptBuffer::new();

    // tx_bal: per-block BlockDelta (BAL). Simple (multicast/IPC) subscription.
    {
        let mut bal_rx = rt
            .open_subscription::<BlockDelta>(&channels.tx_bal_channel, channels.tx_bal_stream_id)
            .context("open tx_bal subscription")?;
        let bals = bals.clone();
        tokio::spawn(async move {
            while let Some((_pos, delta)) = bal_rx.recv().await {
                bals.insert(delta);
            }
        });
    }

    // tx_receipts: the executor's published receipts (MDS fan-in in the cluster).
    {
        let executor_count = args
            .executor_count
            .unwrap_or(channels.tx_receipts_executor_count);
        let mut handle = open_tx_receipts(&rt, &channels, executor_count)?;
        let receipts = receipts.clone();
        tokio::spawn(async move {
            while let Some((_pos, r)) = handle.recv().await {
                receipts.insert(r);
            }
        });
    }

    // Seed genesis once into a fresh env.
    let (genesis_accounts, genesis_code) = build_genesis_alloc(genesis.as_ref());
    let seeded = seed_genesis(&env, &genesis_accounts, &genesis_code)
        .context("seed genesis into validator state env")?;
    tracing::info!(
        state_dir = %args.state_dir.display(),
        genesis_accounts = genesis_accounts.len(),
        seeded,
        "validator state env opened"
    );

    // Trie-aware writer: each block commit advances the MPT state root.
    let trie_mode = match args.trie_shadow_check {
        Some(every_n) => TrieMode::ShadowCheck { every_n },
        None => TrieMode::Incremental,
    };
    let writer =
        StateWriter::spawn_with_trie(env, trie_mode).context("spawn trie-aware state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let sw_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
    let sw_queue = ValidatorWriterQueue::new(
        MdbxWriterQueue::new(writer.delta_tx.clone()),
        bals.clone(),
        divergence.clone(),
    );
    let c_pub = ValidatorReceiptSink::new(receipts.clone(), divergence.clone());

    // Background poller: expose committed-block + state-root height as metrics.
    {
        let snap_rx = writer.snapshot_rx.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            loop {
                if let Some(snap) = snap_rx.current() {
                    let block = snap.block_number();
                    if block != last {
                        last = block;
                        metrics::set_committed_block(block);
                        if let Ok(Some(root)) = snap.state_root() {
                            tracing::debug!(block, state_root = %root, "validator committed block");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }

    let mut cfg = ExecutorConfig {
        chain_id,
        ..ExecutorConfig::default()
    };
    if resume.is_some() {
        cfg.reader.join_timeout = Duration::from_secs(30);
    }
    let initial_block = recovery.last_committed_block;

    let mut join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
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
            resume,
        )
    });

    // Exit on WHICHEVER comes first: an operator shutdown signal, or the
    // engine loop finishing on its own (a divergence fail-stop or a stream
    // error). Waiting only for SIGTERM would leave a halted validator lingering
    // "alive" — metrics up, chain frozen — hiding the very fail-stop signal
    // the divergence machinery exists to surface.
    let engine_result = tokio::select! {
        _ = wait_for_shutdown() => {
            tracing::info!("kardamom-validator: shutdown signal received; dropping runtime");
            None
        }
        res = &mut join => Some(res),
    };
    drop(rt);
    drop(cluster_guard);
    let mut diverged = divergence.is_halted();
    let joined = match engine_result {
        Some(r) => r,
        None => join.await,
    };
    match joined {
        Ok(Ok(())) => tracing::info!("validator main loop returned cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "validator main loop returned an error");
            diverged = true;
        }
        Err(e) => tracing::error!(error = %e, "validator task panicked"),
    }
    if let Err(e) = writer.shutdown() {
        tracing::error!(error = %e, "state writer shutdown returned an error");
    }
    if diverged {
        if let Some(reason) = divergence.reason() {
            tracing::error!(reason = %reason, "validator halted on divergence");
        }
        std::process::exit(2);
    }
    Ok(())
}

/// Open the tx_receipts subscription: MDS fan-in (attach each executor replica's
/// endpoint) when configured, else the simple shared-channel subscription.
fn open_tx_receipts(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    executor_count: u32,
) -> Result<TxReceiptsSubscriberHandle> {
    if channels.tx_receipts_mds_enabled() {
        let sub =
            TxReceiptsSubscriberHandle::open_mds(rt, channels).context("open tx_receipts (MDS)")?;
        for i in 0..executor_count {
            if let Some(uri) = channels.tx_receipts_endpoint(i) {
                sub.add_destination(&uri)
                    .with_context(|| format!("attach tx_receipts endpoint {i}"))?;
            }
        }
        Ok(sub)
    } else {
        TxReceiptsSubscriberHandle::open(rt, channels).context("open tx_receipts")
    }
}

fn load_genesis(path: &std::path::Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

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
        accounts.push(AccountChange {
            address: entry.address,
            nonce,
            balance: entry.balance,
            code_hash,
        });
        if let Some(c) = entry.code.as_ref() {
            code.push(CodeEntry {
                code_hash,
                code: c.0.clone(),
            });
        }
    }
    (accounts, code)
}

// --- Adapters: async log handles → sync engine traits (as in the executor). ---

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

struct LiveTxDepositsSub {
    rx: sync_mpsc::Receiver<(BPosition, Deposit)>,
}
impl DepositSubscription for LiveTxDepositsSub {
    fn next(&mut self) -> Result<(BPosition, Deposit), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::DepositsClosed)
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
                tracing::error!(error = %e, "failed to install SIGTERM handler; Ctrl-C only");
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
    }
}
