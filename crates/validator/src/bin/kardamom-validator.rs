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
    Executor, ExecutorConfig, ExecutorError, MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal,
    ResumePoint, StateWriterQueue, TxDataSubscription, TxOrderingSubscription,
};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_state::{StateEnvBuilder, StateWriter, TrieMode, read_recovery_point, seed_genesis};
use kardamom_validator::attester::{self, AttesterConfig};
use kardamom_validator::epoch_verify;
use kardamom_validator::{
    BalBuffer, Divergence, ReceiptBuffer, ValidatorReceiptSink, ValidatorWriterQueue, metrics,
};

/// Marker file in the state dir: a checkpoint was adopted and the trie
/// bootstrap has not yet committed. See the adoption block in `main`.
const ADOPTION_MARKER: &str = ".adopted-needs-trie-bootstrap";

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
    /// Local checkpoint staging dir for the replay-unavailable fallback
    /// (#143): peer checkpoints are fetched here and adopted on the next
    /// start. The validator never CREATES checkpoints (its state is derived);
    /// this is an adoption-only dir.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    checkpoint_dir: Option<PathBuf>,
    /// Executor checkpoint-serve addresses (`host:port`, comma-separated) to
    /// fetch from when the cluster refuses replay (cursor below the retention
    /// floor). Blocks through an adopted checkpoint are UNVERIFIED by this
    /// validator — the same trust class as #78 catch-up; the trustless
    /// alternative is rebuild-from-L1 (kardamom-reconstruct).
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    checkpoint_peers: Vec<String>,
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
    /// Address of the deployed `ETHLockbox` proxy. With `--l1-rpc-url`, turns
    /// on EPOCH VERIFICATION: every epoch on the canonical stream is
    /// re-derived from L1 and a mismatch is a divergence (phase 1 of
    /// docs/agents/l1-origin-deposit-derivation-spec.md). Without it the
    /// validator still checks the origin SEQUENCE (rules 1-2, which need no
    /// L1) but cannot check an epoch's contents.
    #[arg(long, env = "KARDAMOM_LOCKBOX")]
    lockbox: Option<alloy_primitives::Address>,
    /// Attester private key: raw hex, or `env:VAR` to read it from the
    /// environment (the deployer's key convention). Must be the oracle's
    /// permissioned `attester`.
    #[arg(long, env = "KARDAMOM_ATTESTER_KEY")]
    attester_key: Option<String>,
    /// Post one L1 output per this many L2 blocks.
    /// Re-execute each block as seeded PARALLEL batches driven by the
    /// EIP-7928 BAL (spec: bal-attribution-parallel-validation). Falls back
    /// to sequential per block when claims are unavailable or the block
    /// contains deposits, so liveness never depends on the BAL.
    #[arg(long, env = "KARDAMOM_PARALLEL_VALIDATION", default_value_t = false)]
    parallel_validation: bool,
    /// Transactions per parallel batch (scheduling granularity — independent
    /// of the BAL's attribution granularity).
    #[arg(long, env = "KARDAMOM_VALIDATION_BATCH_SIZE", default_value_t = 8)]
    validation_batch_size: usize,

    #[arg(long, env = "KARDAMOM_ATTESTER_POST_INTERVAL", default_value_t = 1)]
    attester_post_interval: u64,

    // --- Interop serving surfaces (egress spec E1) ------------------------
    /// Enable the interop feed server on this address (`host:port`; port 0
    /// picks one). OFF by default. When set, the validator extracts outbox
    /// messages from its re-executed receipts (cross-checked against BAL
    /// claims) and serves `kardamom_subscribeOutbox` +
    /// `kardamom_subscribeAttestations` over WS — the public-validator role.
    #[arg(long, env = "KARDAMOM_SERVE_FEED")]
    serve_feed: Option<std::net::SocketAddr>,
    /// How many blocks of outbox messages / attestations the feed retains.
    /// A subscriber whose cursor falls below the window is told `Lagged`;
    /// deeper backfill is a v2 concern (the data is in DA).
    #[arg(long, env = "KARDAMOM_FEED_RETENTION_BLOCKS", default_value_t = 1024)]
    feed_retention_blocks: u64,
    /// File the feed server's actually-bound address is written to
    /// (`host:port` + newline), for harnesses that pass port 0.
    #[arg(long, env = "KARDAMOM_SERVE_FEED_ADDR_FILE")]
    serve_feed_addr_file: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    bin_support::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("validator", args.metrics_addr, &args.host_id)?;
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
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

    // --- State backend + crash-recovery decision (mirrors the executor). ---
    let (genesis, chain_id) = bin_support::resolve_genesis(args.chain.as_deref(), args.chain_id)?;

    // Checkpoint adoption, cold-start half (#143): a fresh validator joining
    // a chain that outgrew the cluster retention window can NOT re-execute
    // from genesis (REPLAY_FROM(genesis) is refused), so adopt the newest
    // staged/peer checkpoint BEFORE opening the env — startup then resumes
    // from its cursor and only the tail replays. Blocks through the adopted
    // checkpoint are UNVERIFIED by this validator (trust class of #78
    // catch-up); the trustless alternative is rebuild-from-L1.
    let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        let fresh = !kardamom_state::checkpoint::has_state_db(&args.state_dir)
            .context("probe validator state dir")?;
        if fresh {
            let restored = bin_support::restore_or_fetch_checkpoint(
                ckpt_dir,
                &args.state_dir,
                &args.checkpoint_peers,
                expected_genesis,
            )?;
            if let Some((block, ckpt_path)) = restored {
                // Adoption is recorded EXPLICITLY: executor checkpoints carry
                // the genesis-seeded mirror + trie (built for every env by
                // seed_genesis, then never updated by the trie-off writer),
                // so a "trie present?" probe passes on an image whose trie is
                // frozen at genesis — and the incremental walker would extend
                // that stale base into silently wrong roots the shadow-check
                // cannot catch (it rebuilds from the SAME stale mirror).
                // Caught by the validator-join chaos case's non-vacuity grep.
                // The marker survives a crash between restore and bootstrap;
                // the bootstrap below is idempotent.
                std::fs::write(args.state_dir.join(ADOPTION_MARKER), b"")
                    .context("write adoption marker")?;
                tracing::info!(
                    restored_block = block,
                    checkpoint = %ckpt_path.display(),
                    "adopted state from checkpoint (UNVERIFIED through this block); \
                     will re-execute + verify the tail from here"
                );
            }
        }
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
    // Fresh validators start at genesis and receive the full retained
    // canonical stream. The replay request is re-sent on every session
    // establishment, so a validator whose session dies mid-chaos catches
    // back up instead of fail-stopping on an unrecoverable gap.
    let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
        args.aeron_dir.as_deref(),
        file_cfg.cluster.to_live(),
        bin_support::cluster_replay_cursor(resume.as_ref()),
    )?;
    tracing::info!("kardamom-validator: tx_ordering via Aeron Cluster");
    // The kardamom_sealer_* re-export is the EXECUTOR's job — a validator
    // emitting a second (lagging) copy of the series would break sum()-style
    // queries and contradict the documented observation point.
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(cluster_sub.suppress_sealer_metrics());

    // --- Verification streams: tx_bal (BAL) + tx_receipts. ---
    let divergence = Divergence::new();
    let bals = BalBuffer::new();
    let claims = kardamom_validator::ClaimBuffer::new();
    let receipts = ReceiptBuffer::new();

    // Interop serving surfaces (egress spec E1), OFF by default. The
    // extractor gets its OWN claim buffer: the engine's is consumed by the
    // whole-block strategy (and never drained in streaming mode), so the
    // BAL pump feeds both, sharing the decoded index.
    struct InteropServe {
        addr: std::net::SocketAddr,
        store: std::sync::Arc<kardamom_validator::interop::FeedStore>,
        attestations: std::sync::Arc<kardamom_validator::interop::AttestationStore>,
        claims: std::sync::Arc<kardamom_validator::ClaimBuffer>,
    }
    let interop_serve = args.serve_feed.map(|addr| InteropServe {
        addr,
        store: std::sync::Arc::new(kardamom_validator::interop::FeedStore::new(
            chain_id,
            args.feed_retention_blocks,
        )),
        attestations: std::sync::Arc::new(kardamom_validator::interop::AttestationStore::new(
            args.feed_retention_blocks,
        )),
        claims: kardamom_validator::ClaimBuffer::new(),
    });

    // tx_bal: per-block BlockDelta (BAL). Simple (multicast/IPC) subscription,
    // wrapped in a SILENCE WATCHDOG (#144): a multicast image that never
    // joins (or silently dies) starves verification while everything else
    // works — observed as `validator_blocks_verified_total == 0` for a whole
    // run. Executors publish one BAL per committed block (empty blocks
    // included), so 60s of silence on a progressing chain is a dead
    // subscription, not an idle one: drop it and reopen. On a genuinely idle
    // cluster the reopen is a harmless no-op churn.
    //
    // The pump holds an AeronRuntime clone (needed to reopen), which is the
    // documented SIGTERM-deadlock trap (see the tx_receipts comment below):
    // the runtime only shuts down when its last clone drops. Hence the 5s
    // wake tick + `bal_pump_stop`, set BEFORE the main path drops `rt`, so
    // this task releases its clone within one tick and graceful shutdown
    // completes.
    let bal_pump_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        const BAL_TICK: Duration = Duration::from_secs(5);
        const BAL_SILENCE_REOPEN: Duration = Duration::from_secs(60);
        // BalFrame (spec: bal-attribution-parallel-validation): the merged
        // delta plus the EIP-7928 access list; the write-set cross-check
        // consumes the merged section, attribution drives the parallel
        // engine.
        let mut bal_rx = rt
            .open_subscription::<kardamom_types::BalFrame>(
                &channels.tx_bal_channel,
                channels.tx_bal_stream_id,
            )
            .context("open tx_bal subscription")?;
        let bals = bals.clone();
        let claims_sub = claims.clone();
        let extract_claims = interop_serve.as_ref().map(|s| s.claims.clone());
        let bal_rt = rt.clone();
        let bal_channel = channels.tx_bal_channel.clone();
        let bal_stream_id = channels.tx_bal_stream_id;
        let stop = bal_pump_stop.clone();
        tokio::spawn(async move {
            let mut silent_for = Duration::ZERO;
            loop {
                if stop.load(std::sync::atomic::Ordering::SeqCst) {
                    return; // release the runtime clone promptly on shutdown
                }
                let frame = match tokio::time::timeout(BAL_TICK, bal_rx.recv()).await {
                    Ok(Some((_pos, frame))) => {
                        silent_for = Duration::ZERO;
                        frame
                    }
                    Ok(None) => return, // runtime shutting down
                    Err(_) => {
                        silent_for += BAL_TICK;
                        if silent_for >= BAL_SILENCE_REOPEN {
                            silent_for = Duration::ZERO;
                            metrics::counter_bal_sub_reopen();
                            tracing::warn!(
                                silence_s = BAL_SILENCE_REOPEN.as_secs(),
                                "tx_bal silent — reopening the subscription \
                                 (never-joined or dead multicast image, #144)"
                            );
                            match bal_rt.open_subscription::<kardamom_types::BalFrame>(
                                &bal_channel,
                                bal_stream_id,
                            ) {
                                Ok(rx) => bal_rx = rx,
                                Err(e) => tracing::warn!(
                                    error = %e,
                                    "tx_bal reopen failed; retrying after the next window"
                                ),
                            }
                        }
                        continue;
                    }
                };
                {
                    let kardamom_types::BalFrame {
                        bal_rlp,
                        granularity,
                        delta,
                    } = &frame;
                    // Decode the access list into seed-lookup form for the
                    // parallel engine. A decode failure degrades to
                    // sequential re-execution (the merged cross-check below
                    // is unaffected), never to a verification gap.
                    let mut slice: &[u8] = bal_rlp;
                    match <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(
                        &mut slice,
                    ) {
                        // Empty lists are skipped: empty blocks never take
                        // claims (the parallel path short-circuits before
                        // its take), so inserting them would grow the
                        // buffer for the whole idle period — the cursor
                        // only advances on takes. Quantized frames carry
                        // their granularity so verification coarsens to
                        // the chunk with batches aligned to it — the
                        // validator's ladder view always follows the wire.
                        Ok(bal) if !bal.is_empty() => {
                            let idx = std::sync::Arc::new(
                                kardamom_validator::parallel::ClaimIndex::from_alloy(&bal),
                            );
                            claims_sub.insert_arc(delta.block_number, *granularity, idx.clone());
                            if let Some(ec) = extract_claims.as_ref() {
                                ec.insert_arc(delta.block_number, *granularity, idx);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(
                            block = delta.block_number,
                            error = %e,
                            granularity,
                            "BAL access-list decode failed; block validates sequentially"
                        ),
                    }
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
        let mut rx = TxReceiptsSubscriberHandle::open_auto(&rt, &channels, executor_count)
            .context("open tx_receipts")?
            .into_receiver();
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
    // Adopted executor checkpoints carry a trie FROZEN AT GENESIS (seeded
    // into every env, never updated by the trie-off writer) — so adoption is
    // signaled by the explicit marker, not a presence probe, and the
    // bootstrap rebuilds the mirror + trie from the plain state tables
    // wholesale. Idempotent and crash-safe (one RW txn; the marker is
    // removed only after commit). `has_trie` remains as a belt-and-braces
    // net for a truly mirror-less image (e.g. an operator-copied dir).
    let adoption_marker = args.state_dir.join(ADOPTION_MARKER);
    if adoption_marker.exists() || !kardamom_state::has_trie(&env).context("probe state trie")? {
        tracing::info!("adopted state image — bootstrapping hashed mirror + trie");
        let started = std::time::Instant::now();
        let root = kardamom_state::bootstrap_trie_from_state(&env)
            .context("bootstrap trie from adopted state")?;
        tracing::info!(
            state_root = %root,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "trie bootstrap complete"
        );
        if adoption_marker.exists() {
            std::fs::remove_file(&adoption_marker).context("clear adoption marker")?;
        }
    }
    let writer =
        StateWriter::spawn_with_trie(env, trie_mode).context("spawn trie-aware state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let sw_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
    let sw_queue = ValidatorWriterQueue::new(
        MdbxWriterQueue::new(writer.delta_tx.clone()),
        bals.clone(),
        divergence.clone(),
    )
    // Blocks at or below the recovery resume point were verified before
    // the restart; replay re-execution against already-applied state
    // yields empty deltas that CANNOT match the BAL — comparing them
    // produced false-divergence restart cascades.
    .with_verify_floor(recovery.last_committed_block);

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
    // Flight ring: recent block inputs for the receipt-divergence dump.
    // Always on — the receipt cross-check runs on the sequential path too,
    // and the F3-era wsh mismatch is exactly the class it captures.
    let flight = kardamom_validator::flight::FlightRing::new();
    let c_pub: Box<dyn kardamom_engine::TxReceiptsPublication> = {
        let sink = ValidatorReceiptSink::new(receipts.clone(), divergence.clone())
            .with_flight(flight.clone());
        let mut chain: Box<dyn kardamom_engine::TxReceiptsPublication> = match &attester_handle {
            Some(h) => Box::new(attester::AttestingReceiptSink::new(sink, h.clone())),
            None => Box::new(sink),
        };
        // Outbox extraction sits OUTERMOST: it buffers before forwarding but
        // flushes to the feed store only after the inner chain (the receipt
        // cross-check) accepted the boundary — a diverging block is never
        // served.
        if let Some(s) = interop_serve.as_ref() {
            chain = Box::new(kardamom_validator::interop::ExtractingReceiptSink::new(
                chain,
                chain_id,
                s.claims.clone(),
                s.store.clone(),
                divergence.clone(),
            ));
        }
        chain
    };

    // Background poller: expose committed-block + state-root height as
    // metrics, and feed each block's observed MPT root to the attester.
    // `validator_state_root_block` is set only when the committed snapshot
    // actually yielded a root — an independent measurement, not a mirror of
    // the committed-block gauge.
    {
        let snap_rx = writer.snapshot_rx.clone();
        let attester_handle = attester_handle.clone();
        let attestation_store = interop_serve.as_ref().map(|s| s.attestations.clone());
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
                                // Interop attestation stream (E1, unsigned):
                                // the same observed roots, retained + served.
                                if let Some(s) = attestation_store.as_ref() {
                                    s.push(block, root);
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

    // Start the feed server (E1 serving surfaces). The handle's drop stops
    // the server, so it is held for the process lifetime; a divergence halt
    // exits the process and the sockets die with it — a validator whose
    // verification halts must stop serving.
    let _feed_server = match interop_serve.as_ref() {
        Some(s) => {
            let (local, handle) = kardamom_validator::interop::start_feed_server(
                s.addr,
                kardamom_validator::interop::FeedServerState {
                    chain_id,
                    validator_id: args.host_id.clone(),
                    store: s.store.clone(),
                    attestations: s.attestations.clone(),
                },
            )
            .await
            .context("start interop feed server")?;
            if let Some(path) = args.serve_feed_addr_file.as_ref() {
                std::fs::write(path, format!("{local}\n")).context("write feed addr file")?;
            }
            tracing::info!(
                addr = %local,
                retention_blocks = args.feed_retention_blocks,
                "interop feed server enabled (kardamom_subscribeOutbox, \
                 kardamom_subscribeAttestations)"
            );
            Some(handle)
        }
        None => None,
    };

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

    // Parallel validation strategy (opt-in): seeded batches driven by the
    // BAL. `None` keeps the engine's streaming per-tx path byte-for-byte.
    let block_exec = if args.parallel_validation {
        tracing::info!(
            batch_size = args.validation_batch_size,
            "parallel validation ENABLED (seeded BAL batches)"
        );
        Some(kardamom_validator::parallel::parallel_block_exec(
            claims.clone(),
            args.validation_batch_size,
            Some(flight.clone()),
        ))
    } else {
        None
    };

    // Epoch verification (phase 1). Sequence rules 1-2 are local and always
    // enforced once an epoch appears; the CONTENT check needs L1, so it is
    // only wired when both the RPC URL and the lockbox address are given.
    let epoch_observer: Option<Box<dyn kardamom_engine::EpochObserver>> =
        match (args.l1_rpc_url.as_deref(), args.lockbox) {
            (Some(url), Some(lockbox)) => {
                let provider = alloy_provider::ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_http(url.parse().context("parse --l1-rpc-url")?);
                let source = std::sync::Arc::new(kardamom_da_watcher::RpcL1Source::new(provider));
                tracing::info!(
                    %lockbox,
                    "epoch verification enabled: epochs are re-derived from L1"
                );
                Some(Box::new(epoch_verify::EpochVerifier::spawn(
                    source,
                    lockbox,
                    divergence.clone(),
                    &tokio::runtime::Handle::current(),
                )))
            }
            _ => {
                tracing::info!(
                    "epoch CONTENT verification disabled (needs --l1-rpc-url and \
                     --lockbox); origin sequence rules still apply"
                );
                None
            }
        };

    // Remote-epoch verification (interop): inline pair-sequence checks on
    // every RemoteEpochRecord, always on — they need no transport, exactly
    // like the L1 origin sequence rules. Content-vs-origin verification (§10
    // postures) is a later phase; see `interop::verify`'s module docs.
    let remote_epoch_observer: Option<Box<dyn kardamom_engine::RemoteEpochObserver>> =
        Some(Box::new(
            kardamom_validator::interop::RemoteEpochVerifier::new(divergence.clone()),
        ));

    let mut join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            c_pub,
            snapshots,
            sw_signal,
            sw_queue,
            initial_block,
            resume,
            // No BAL capture: the validator VERIFIES BALs, never publishes them.
            None,
            // Join-miss archive refetch (None on single-host/IPC runs).
            // Whole-block exec strategy (validator parallel path).
            block_exec,
            join_recovery,
            epoch_observer,
            remote_epoch_observer,
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
    bal_pump_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(rt);
    drop(cluster_guard);
    let joined = match engine_result {
        Some(r) => r,
        None => join.await,
    };
    let mut engine_error: Option<Option<ExecutorError>> = None;
    match joined {
        Ok(Ok(())) => tracing::info!("validator main loop returned cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "validator main loop returned an error");
            engine_error = Some(Some(e));
        }
        Err(e) => {
            tracing::error!(error = %e, "validator task panicked");
            engine_error = Some(None);
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
    if let Some(cause) = engine_error {
        // Replay-window overrun: repair BEFORE exiting, exactly like the
        // executor's recovery-D path (#94) — fetch a peer checkpoint at/above
        // the retention floor and park the stale DB, so the next restart takes
        // the ordinary fresh-start restore path instead of a deterministic
        // crash loop re-requesting the same refused REPLAY_FROM (#143).
        // Adoption trust class: the adopted state is unverified BY THIS
        // validator through the checkpoint block — the same accepted tradeoff
        // as #78's BAL catch-up; the divergence latch only ever covers blocks
        // this validator actually verified. The trustless alternative remains
        // kardamom-reconstruct (rebuild-from-L1) into --state-dir.
        if let Some(outcome) = bin_support::replay_unavailable_fallback(
            cause.as_ref(),
            args.checkpoint_dir.as_deref(),
            &args.checkpoint_peers,
            &args.state_dir,
            expected_genesis,
            true,
        )? {
            metrics::resync_counter(outcome).increment(1);
        }
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
