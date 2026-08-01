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
//! nothing on the L2 streams.
//!
//! **L1 output attestation** (optional): when `--l1-rpc-url`,
//! `--output-oracle` and `--attester-key` are ALL given, a background attester
//! collects each committed block's `MessagePassed` withdrawal leaves, builds
//! the per-output withdrawals root, and posts one output per
//! `--attester-post-interval` blocks to the L1 `WithdrawalOutputOracle`. The
//! key must be the oracle's permissioned `attester`. Without the three flags
//! the validator performs no automatic attestation (previous behavior).

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support::{self, StateDurabilityArg};
use kardamom_engine::reader::cluster::ClusterConfig;
use kardamom_engine::{
    DepositSubscription, Executor, ExecutorConfig, ExecutorError, MdbxSnapshotSource,
    MdbxWriterQueue, MdbxWriterSignal, ResumePoint, StateWriterQueue, TxDataSubscription,
    TxOrderingSubscription,
};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsSubscriberHandle};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_state::{StateEnvBuilder, StateWriter, TrieMode, read_recovery_point, seed_genesis};
use kardamom_types::BlockDelta;
use kardamom_validator::attester::{self, AttesterConfig};
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
    /// UDP endpoint (`host:port`) on this node where **refetched** tx_data /
    /// tx_deposits fragments land (join-miss recovery from the remote
    /// durability archives; see the executor's flag of the same name). Unset
    /// ⇒ refetch disabled; a lost envelope is then fatal after the join
    /// timeout. (tx_ordering recovery is the cluster client's replay, not
    /// this path.)
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control RESPONSES. Required alongside
    /// `--replay-destination-endpoint` for refetch to engage.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    archive_control_response_endpoint: Option<String>,
    /// This node's cluster-egress endpoint `ip:port`. Overrides/sets the
    /// [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener. (9007: 9006 is the
    /// ingress default — running both locally with defaults must not race for
    /// one socket; see docs/observability.md.)
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9007")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,

    // --- L1 output attestation (all three required to enable) -------------
    /// L1 JSON-RPC endpoint the attester posts withdrawal outputs to.
    #[arg(long, env = "KARDAMOM_L1_RPC_URL")]
    l1_rpc_url: Option<String>,
    /// Address of the deployed `WithdrawalOutputOracle` proxy.
    #[arg(long, env = "KARDAMOM_OUTPUT_ORACLE")]
    output_oracle: Option<alloy_primitives::Address>,
    /// Attester private key: raw hex, or `env:VAR` to read it from the
    /// environment (the deployer's key convention). Must be the oracle's
    /// permissioned `attester`.
    #[arg(long, env = "KARDAMOM_ATTESTER_KEY")]
    attester_key: Option<String>,
    /// Post one L1 output per this many L2 blocks.
    #[arg(long, env = "KARDAMOM_ATTESTER_POST_INTERVAL", default_value_t = 1)]
    attester_post_interval: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    bin_support::init_tracing();
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
    let (genesis, chain_id) = bin_support::resolve_genesis(args.chain.as_deref(), args.chain_id)?;

    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;
    let resume = if recovery.last_committed_block > 0 {
        let rp = ResumePoint {
            block: recovery.last_committed_block,
            record_count: recovery.last_fsynced_b_position.as_index(),
            l2_timestamp: recovery.last_committed_l2_timestamp,
        };
        tracing::info!(
            resume_block = rp.block,
            "validator resuming from persisted cursor"
        );
        Some(rp)
    } else {
        None
    };

    // M tx_data subscriptions + tx_deposits (async→sync bridged), identical to
    // the executor: ALWAYS live, with the down-window/lapse gap recovered
    // in-band by the reader's join-miss refetch against the remote durability
    // archives (the resume-gated replay-merge this replaces pointed at the
    // LOCAL archive, which records neither stream).
    let a_subs: Vec<Box<dyn TxDataSubscription>> =
        bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
    let dep_sub: Box<dyn DepositSubscription> = bin_support::open_tx_deposits_sub(&rt, &channels)?;
    let join_recovery = bin_support::archive_join_recovery(
        &channels,
        &aeron_cfg,
        args.aeron_dir.as_deref(),
        args.archive_control_response_endpoint.as_deref(),
        args.replay_destination_endpoint.as_deref(),
    );

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
    // The kardamom_sealer_* re-export is the EXECUTOR's job — a validator
    // emitting a second (lagging) copy of the series would break sum()-style
    // queries and contradict the documented observation point.
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(cluster_sub.suppress_sealer_metrics());

    // --- Verification streams: tx_bal (BAL) + tx_receipts. ---
    let divergence = Divergence::new();
    let bals = BalBuffer::new();
    let receipts = ReceiptBuffer::new();

    // tx_bal: per-block BlockDelta (BAL). Simple (multicast/IPC) subscription.
    {
        // BalFrame (spec: bal-attribution-parallel-validation): V1 carries
        // the merged delta alone; V2 adds the EIP-7928 access list. The
        // write-set cross-check consumes the merged section either way —
        // attribution drives the parallel engine in phase 3.
        let mut bal_rx = rt
            .open_subscription::<kardamom_types::BalFrame>(
                &channels.tx_bal_channel,
                channels.tx_bal_stream_id,
            )
            .context("open tx_bal subscription")?;
        let bals = bals.clone();
        tokio::spawn(async move {
            while let Some((_pos, frame)) = bal_rx.recv().await {
                if let kardamom_types::BalFrame::V2 {
                    bal_rlp,
                    granularity,
                    ..
                } = &frame
                {
                    tracing::debug!(
                        bal_bytes = bal_rlp.len(),
                        granularity,
                        "BAL frame with access attribution"
                    );
                }
                bals.insert(frame.delta().clone());
            }
        });
    }

    // tx_receipts: the executor's published receipts (MDS fan-in in the cluster).
    //
    // `into_receiver()` is load-bearing for shutdown: the handle carries an
    // `AeronRuntime` clone (for MDS destination churn), and moving that clone
    // into this pump task would deadlock process exit — the runtime shuts
    // down only when its last clone drops, that shutdown is what ends
    // `recv()`, and this task would be holding the clone that prevents it.
    // The symptom was a validator that ignored SIGTERM entirely (`drop(rt)`
    // became a no-op, so the engine's tx_data subscriptions never closed and
    // the join below never returned) while the executor — which publishes
    // receipts rather than subscribing — shut down fine. MDS destinations are
    // attached inside `open_tx_receipts`, so nothing needs the clone after
    // this point.
    {
        let executor_count = args
            .executor_count
            .unwrap_or(channels.tx_receipts_executor_count);
        let mut rx = open_tx_receipts(&rt, &channels, executor_count)?.into_receiver();
        let receipts = receipts.clone();
        tokio::spawn(async move {
            while let Some((_pos, r)) = rx.recv().await {
                receipts.insert(r);
            }
        });
    }

    // Seed genesis once into a fresh env.
    let (genesis_accounts, genesis_code) = bin_support::build_genesis_alloc(genesis.as_ref());
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

    // L1 output attester: enabled only when the three flags are all present.
    // (Runs inside this tokio runtime; the task lives as long as a handle
    // clone does — `attester_handle` is held below for the process lifetime.)
    let attester_handle = match (
        args.l1_rpc_url.clone(),
        args.output_oracle,
        args.attester_key.as_deref(),
    ) {
        (Some(url), Some(oracle), Some(key)) => {
            let (handle, _task) = attester::spawn_attester(AttesterConfig {
                l1_rpc_url: url,
                oracle,
                private_key: resolve_attester_key(key)?,
                post_interval_blocks: args.attester_post_interval,
            })
            .context("spawn attester")?;
            tracing::info!(
                oracle = %oracle,
                post_interval_blocks = args.attester_post_interval,
                "L1 output attester enabled"
            );
            Some(handle)
        }
        (None, None, None) => None, // milestone-1 default: no automatic attestation
        _ => anyhow::bail!(
            "attestation needs --l1-rpc-url, --output-oracle and --attester-key together \
             (got a partial set)"
        ),
    };
    let sw_queue: Box<dyn StateWriterQueue> = Box::new(sw_queue);

    // Tee each block's withdrawal leaves into the attester from the RECEIPT
    // stream (a plain sink when attestation is disabled).
    //
    // NOT from the BlockDelta: the engine finalizes every delta with an empty
    // receipts vec (receipts travel on tx_receipts), so the previous
    // `AttestingWriterQueue` wiring collected nothing — every posted output
    // carried `leaves=0`, no withdrawal was ever attested, and none could be
    // finalized on L1. Caught end-to-end by the chain-semantics suite's S2.
    let c_pub: Box<dyn kardamom_engine::TxReceiptsPublication> = {
        let sink = ValidatorReceiptSink::new(receipts.clone(), divergence.clone());
        match &attester_handle {
            Some(h) => Box::new(attester::AttestingReceiptSink::new(sink, h.clone())),
            None => Box::new(sink),
        }
    };

    // Background poller: expose committed-block + state-root height as
    // metrics, and feed each block's observed MPT root to the attester.
    // `validator_state_root_block` is set only when the committed snapshot
    // actually yielded a root — an independent measurement, not a mirror of
    // the committed-block gauge.
    {
        let snap_rx = writer.snapshot_rx.clone();
        let attester_handle = attester_handle.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            loop {
                if let Some(snap) = snap_rx.current() {
                    let block = snap.block_number();
                    if block != last {
                        last = block;
                        metrics::set_committed_block(block);
                        match snap.state_root() {
                            Ok(Some(root)) => {
                                metrics::set_state_root_block(block);
                                tracing::debug!(block, state_root = %root, "validator committed block");
                                if let Some(h) = attester_handle.as_ref() {
                                    h.submit_root(block, root);
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(block, error = %e, "state_root read failed")
                            }
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
    // ALWAYS bound the tx_data join wait — a verifier that loses an envelope
    // must fail LOUDLY into the supervisor-restart + archive-replay recovery
    // loop, not hang forever mid-join. (Divergence fail-stops stay
    // distinguishable by their 'halted on divergence' log line.) See
    // `bounded_join_timeout` for why fresh > resume.
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(resume.is_some());
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
            // No BAL capture: the validator VERIFIES BALs, never publishes them.
            None,
            // Join-miss archive refetch (None on single-host/IPC runs).
            join_recovery,
        )
    });

    // Exit on WHICHEVER comes first: an operator shutdown signal, or the
    // engine loop finishing on its own (a divergence fail-stop or a stream
    // error). Waiting only for SIGTERM would leave a halted validator
    // lingering "alive" — metrics up, chain frozen — hiding the very
    // fail-stop signal the divergence machinery exists to surface.
    let engine_result = tokio::select! {
        _ = bin_support::wait_for_shutdown() => {
            tracing::info!("kardamom-validator: shutdown signal received; dropping runtime");
            None
        }
        res = &mut join => Some(res),
    };
    drop(rt);
    drop(cluster_guard);
    let joined = match engine_result {
        Some(r) => r,
        None => join.await,
    };
    let mut engine_error = false;
    match joined {
        Ok(Ok(())) => tracing::info!("validator main loop returned cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "validator main loop returned an error");
            engine_error = true;
        }
        Err(e) => {
            tracing::error!(error = %e, "validator task panicked");
            engine_error = true;
        }
    }
    if let Err(e) = writer.shutdown() {
        tracing::error!(error = %e, "state writer shutdown returned an error");
    }
    // Exit 2 is RESERVED for a proven divergence (the latch records before the
    // engine surfaces `Divergence`) — the page-the-humans signal. Any other
    // engine failure (a stream error, a replay-window overrun needing resync)
    // is an availability problem, not an integrity one, and must not
    // impersonate it: exit 1 and let the orchestrator restart.
    if divergence.is_halted() {
        if let Some(reason) = divergence.reason() {
            tracing::error!(reason = %reason, "validator halted on divergence");
        }
        std::process::exit(2);
    }
    if engine_error {
        tracing::error!(
            "validator halted on an engine error (NOT a proven divergence); if the \
             cluster refused replay (resync required), rebuild state via \
             kardamom-reconstruct or restore a checkpoint"
        );
        std::process::exit(1);
    }
    Ok(())
}

/// Resolve the attester key flag: raw hex, or `env:VAR` (the deployer's key
/// convention) read from the environment.
fn resolve_attester_key(key: &str) -> Result<String> {
    match key.strip_prefix("env:") {
        Some(var) => {
            std::env::var(var).with_context(|| format!("read attester key from env var {var}"))
        }
        None => Ok(key.to_string()),
    }
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
