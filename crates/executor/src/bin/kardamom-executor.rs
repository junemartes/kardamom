//! `kardamom-executor`: standalone executor service process.
//!
//! Opens M tx_data subscribers + 1 tx_ordering subscriber + 1 tx_receipts
//! publisher via the log layer's Aeron runtime, wires them into the
//! executor's reader/exec/commit thread topology, and runs until SIGTERM /
//! Ctrl-C. The state backend is the libmdbx-backed `kardamom-state` writer,
//! opened at `--state-dir`: chain state is committed durably per block and
//! persists across restarts. Genesis is seeded once into a fresh env.
//!
//! Crash-recovery resume: startup reads the persisted state cursor
//! (`last_committed_block` / `last_committed_end_tx_position`) and replays the
//! canonical stream from the Aeron archives via a replay-merge, skip-counting
//! past the cursor so already-committed blocks are never double-applied.
//!
//! The role-agnostic scaffolding (durability arg, genesis loading, the
//! async→sync stream bridges, tracing/shutdown helpers) is shared with the
//! validator binary via `kardamom_engine::bin_support`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support::{self, StateDurabilityArg};
use kardamom_executor::{
    CMessage, Executor, ExecutorConfig, ExecutorError, ExecutorFileConfig, MdbxSnapshotSource,
    MdbxWriterQueue, MdbxWriterSignal, ResumePoint, TxDataSubscription, TxOrderingSubscription,
    TxReceiptsPublication,
};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_state::checkpoint::{create_checkpoint, prune_checkpoints};
use kardamom_state::{StateEnvBuilder, StateWriter, read_recovery_point, seed_genesis};

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
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Overrides/sets
    /// the [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
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
    /// UDP endpoint (`host:port`) on this node where **refetched** tx_data /
    /// tx_deposits fragments land. A canonical ref whose envelope never
    /// arrived on the live multicast (image lapse, blackout, restart
    /// down-window) is recovered in-band: the reader replays the missing range
    /// from the remote durability archives (`tx_data_archive_endpoints` /
    /// `tx_deposits_archive_endpoints` in channels.toml) onto this endpoint.
    /// Unset ⇒ refetch disabled (single-host/IPC runs); a lost envelope is
    /// then fatal after the join timeout. (tx_ordering crash recovery is
    /// handled by the Aeron Cluster client's REPLAY_FROM, not this path.)
    #[arg(long, env = "KARDAMOM_REPLAY_DESTINATION")]
    replay_destination_endpoint: Option<String>,
    /// UDP endpoint (`host:port`) on this node for the refetch client's
    /// archive-control RESPONSES (the control connection to a remote archive
    /// is UDP in both directions). Required alongside
    /// `--replay-destination-endpoint` for refetch to engage.
    #[arg(long, env = "KARDAMOM_ARCHIVE_CONTROL_RESPONSE")]
    archive_control_response_endpoint: Option<String>,
    /// Directory for periodic state checkpoints (fast cold-start recovery). When
    /// set, a wiped/empty `state_dir` is restored from the newest checkpoint here
    /// before startup (replaying only the tail instead of re-syncing from
    /// genesis), and — if `checkpoint_interval_secs > 0` — new checkpoints are
    /// written here as the chain advances. A peer's checkpoint dir is a valid
    /// restore source (executor replicas are deterministic at the same block).
    #[arg(long, env = "KARDAMOM_CHECKPOINT_DIR")]
    checkpoint_dir: Option<PathBuf>,
    /// Interval, in seconds, between periodic state checkpoints. 0 disables
    /// checkpoint creation (restore-only). Ignored unless `checkpoint_dir` is set.
    #[arg(long, default_value_t = 0)]
    checkpoint_interval_secs: u64,
    /// Number of recent checkpoints to retain (older ones are pruned).
    #[arg(long, default_value_t = 3)]
    checkpoint_keep: u64,
    /// TCP address on which to serve this node's newest checkpoint to peer
    /// executors (`GET /checkpoint/latest`). Replicas are deterministic state
    /// machines, so any replica's checkpoint is a valid restore source for
    /// another. Requires `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_SERVE_ADDR")]
    checkpoint_serve_addr: Option<std::net::SocketAddr>,
    /// Comma-separated peer checkpoint servers (`host:port`) to fetch a
    /// checkpoint from when local state can't reach the chain: a fresh/wiped
    /// node whose genesis replay aged out of the cluster retention window, or
    /// a resuming node whose cursor did (`REPLAY_UNAVAILABLE`). Requires
    /// `--checkpoint-dir`.
    #[arg(long, env = "KARDAMOM_CHECKPOINT_PEERS", value_delimiter = ',')]
    checkpoint_peers: Vec<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9004")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    bin_support::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("executor", args.metrics_addr, &args.host_id)?;
    kardamom_executor::metrics::describe();
    // The TOML supplies the optional `[cluster]` section (default disabled);
    // all other runtime tuning still comes from the CLI flags above. An empty
    // / comment-only file (the existing deployment shape) deserializes to a
    // disabled cluster, so behaviour is unchanged unless `[cluster]` is set.
    let raw = std::fs::read_to_string(&args.config).context("read executor config")?;
    let mut file_cfg: ExecutorFileConfig = toml::from_str(&raw).context("parse executor config")?;

    // Per-node cluster egress endpoint: the cluster client's egress_channel is
    // this node's reachable address (the node IP differs per replica), so it's
    // injected by the Nomad job rather than baked into the static config file.
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }

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
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

    // SEPARATE Aeron runtime/thread for the tx_receipts PUBLICATION. The
    // executor's single Aeron thread otherwise services both the tx_ordering
    // SUBSCRIPTION poll AND the per-tx receipt/boundary publishes; under
    // sustained load the publish work delays the tx_ordering poll past Aeron's
    // flow-control Status-Message deadline, the sealer drops this subscriber
    // from the tx_ordering MDC, its image dies, and the executor freezes
    // (reader stops, exec blocks reading). Isolating the publisher onto its own
    // thread keeps the subscription poll timely no matter the receipt load.
    let rt_pub =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn receipts AeronRuntime")?;

    // --- State backend + crash-recovery decision (before the subscriptions,
    // because the tx_ordering subscription branches on whether we are resuming).
    // Load genesis (its chain_id is adopted when present).
    let (genesis, chain_id) = bin_support::resolve_genesis(args.chain.as_deref(), args.chain_id)?;

    // Fast cold-start recovery: if the state dir is empty (a fresh/wiped node)
    // and a checkpoint is available, restore the newest one BEFORE opening the
    // env. Startup then sees a populated DB and resumes from the checkpoint's
    // block — replaying only the tail instead of re-syncing from genesis.
    let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
    if let Some(ckpt_dir) = args.checkpoint_dir.as_ref() {
        // Serve this node's checkpoints to peers (the other side of the peer
        // fetch below). Best-effort infrastructure, but a bad bind address is
        // a deploy bug — fail startup loudly.
        if let Some(addr) = args.checkpoint_serve_addr {
            kardamom_state::serve_checkpoints(addr, ckpt_dir.clone())
                .context("bind checkpoint serve address")?;
        }
        // Fresh iff the state dir has no mdbx data file — checked WITHOUT opening
        // the env (opening would itself create the data file and defeat restore).
        // (Observed motivation for the ladder's quarantine rung: a copy that
        // raced the writer's prune had no MANIFEST; every restart refused it
        // and the fleet sat at 2/3.)
        let fresh = !kardamom_state::checkpoint::has_state_db(&args.state_dir)
            .context("probe state dir")?;
        if fresh {
            let restored = bin_support::restore_or_fetch_checkpoint(
                ckpt_dir,
                &args.state_dir,
                &args.checkpoint_peers,
                expected_genesis,
            )?;
            match restored {
                Some((block, path)) => {
                    tracing::info!(
                        restored_block = block,
                        checkpoint = %path.display(),
                        "restored state from checkpoint; will replay tail from here"
                    );
                }
                None => tracing::info!(
                    checkpoint_dir = %ckpt_dir.display(),
                    "no checkpoint available locally or from peers; fresh start will \
                     replay from genesis (refused if the chain outgrew the cluster \
                     retention window — then a peer checkpoint or rebuild-from-L1 is required)"
                ),
            }
        }
    }

    // Open the libmdbx state env and read the durable cursor.
    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;

    // Periodic checkpointing (fast recovery for OTHER nodes, and for this node
    // on a future wipe). compact_to runs against an online RO snapshot, so it
    // never blocks the writer. Guarded by an interval; prunes to `checkpoint_keep`.
    if let (Some(ckpt_dir), true) = (
        args.checkpoint_dir.clone(),
        args.checkpoint_interval_secs > 0,
    ) {
        let ckpt_env = env.clone();
        let interval = std::time::Duration::from_secs(args.checkpoint_interval_secs);
        let keep = args.checkpoint_keep;
        std::thread::Builder::new()
            .name("kardamom-checkpointer".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    match create_checkpoint(&ckpt_env, &ckpt_dir) {
                        Ok(info) => {
                            if info.block > keep
                                && let Err(e) = prune_checkpoints(&ckpt_dir, info.block - keep + 1)
                            {
                                tracing::warn!(error = %e, "checkpoint prune failed");
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "checkpoint creation failed"),
                    }
                }
            })
            .context("spawn checkpointer thread")?;
    }
    // Crash-recovery resume. A non-empty state DB means we restarted mid-chain:
    // the cluster client replays the canonical stream FROM this cursor and the
    // reader/exec threads seed their absolute counters from it (see
    // `ResumePoint`).
    let resume = if recovery.last_committed_block > 0 {
        let rp = ResumePoint {
            block: recovery.last_committed_block,
            record_count: recovery.last_fsynced_b_position.as_index(),
            l2_timestamp: recovery.last_committed_l2_timestamp,
        };
        tracing::info!(
            resume_block = rp.block,
            resume_record_count = rp.record_count,
            "resuming from persisted state cursor via cluster canonical replay"
        );
        Some(rp)
    } else {
        None
    };

    // M tx_data subscriptions + tx_deposits, async→sync bridged (shared with
    // the validator binary — see `bin_support`). ALWAYS live: the down-window
    // /-lapse gap is recovered in-band by the reader's join-miss refetch
    // against the remote durability archives (the resume-gated replay-merge
    // this replaces pointed at the consumer's LOCAL archive, which records
    // neither stream — a resuming process had no tx_data source at all).
    let a_subs: Vec<Box<dyn TxDataSubscription>> =
        bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
    let join_recovery = bin_support::archive_join_recovery(
        &channels,
        &aeron_cfg,
        args.aeron_dir.as_deref(),
        args.archive_control_response_endpoint.as_deref(),
        args.replay_destination_endpoint.as_deref(),
    );

    // 1 tx_ordering subscription — ALWAYS the Aeron Cluster (Raft) egress. The
    // cluster has already deduped + totally ordered the stream and exposes a
    // blocking `next()`, so NO async→sync bridge is needed. Leader failover /
    // reconnect — including crash-recovery replay of the canonical stream — is
    // handled inside the cluster client, so the reader never sees an image
    // rotation; the executor's skip-count + `DedupWindow` provide idempotency
    // across any reconnect overlap. (The single-sealer restart BoundaryMisaligned
    // can't occur: the cluster continues the committed count/block across leader
    // failover.) The cluster-session guard (`LiveCluster`) must outlive the
    // executor loop, so bind the guard in the outer scope; it is dropped only
    // after the `join` await below.
    let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
        args.aeron_dir.as_deref(),
        file_cfg.cluster.to_live(),
        bin_support::cluster_replay_cursor(resume.as_ref()),
    )?;
    tracing::info!("kardamom-executor: tx_ordering via Aeron Cluster");
    // The executor is the blessed emitter of the kardamom_sealer_* re-export
    // (default-on in the shared subscription; the validator suppresses it).
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(cluster_sub);

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
    let (genesis_accounts, genesis_code) = bin_support::build_genesis_alloc(genesis.as_ref());
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
    // BAL publication: tee each block's BlockDelta onto tx_bal so validators can
    // cross-check their re-execution. Published on the isolated publication
    // runtime (rt_pub), like receipts, so it never stalls the subscription poll.
    let bal_pub = rt_pub
        .open_publication(&channels.tx_bal_channel, channels.tx_bal_stream_id)
        .context("open tx_bal publication")?;
    // EIP-7928 BAL publisher (spec:
    // docs/agents/bal-attribution-parallel-validation-spec.md): the exec
    // thread hands off each block's captured Bal + receipts-free delta; this
    // thread encodes and delivers with ack + bounded retry, retaining recent
    // frames for validator catch-up. Emission is NOT best-effort — parallel
    // validation makes BAL availability a validator liveness property.
    // Bounded depth: a wedged publisher back-pressures exec rather than
    // dropping state transitions.
    let (bal_tx, bal_rx) = crossbeam_channel::bounded(8);
    let _bal_publisher = std::thread::Builder::new()
        .name("bal-publisher".into())
        .spawn(move || kardamom_executor::bal::run_bal_publisher(bal_rx, bal_pub))
        .context("spawn BAL publisher")?;
    // The legacy writer-queue tee is superseded by the publisher thread.
    let sw_queue = MdbxWriterQueue::new(writer.delta_tx.clone());

    // `verify_record_identity` stays OFF here by decision, not omission:
    // with the validator checking every record (3a.1), a forged envelope
    // halts verification with proof, and sequencer-side rejection would buy
    // defense-in-depth at an ecrecover per tx on the hot path. See the
    // field's doc for the full trade.
    let mut cfg = ExecutorConfig {
        chain_id,
        ..ExecutorConfig::default()
    };
    // ALWAYS bound the tx_data join wait: a replica whose multicast tx_data
    // image races a sequencer restart (new publisher session) can lose an
    // envelope, and an unbounded join wait then FREEZES that replica silently
    // while its peers advance (observed under the hard-sequencer chaos case:
    // one executor frozen at +0 blocks while the chain advanced 195). Failing
    // loudly hands recovery to the designed loop: nomad restarts the task and
    // crash recovery replays the tx_data gap from the archive. See
    // `bounded_join_timeout` for why the fresh-start bound exceeds resume's.
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(resume.is_some());
    // Resume at the persisted cursor — 0 for a fresh genesis DB.
    let initial_block = recovery.last_committed_block;

    // The executor's main loop is sync (std::thread spawns underneath).
    // Run it inside spawn_blocking so the runtime stays responsive for
    // shutdown handling.
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
            // On crash recovery `resume` carries the persisted cursor; the
            // cluster source replays from it and the reader/exec counters seed
            // from it. `None` on a fresh start.
            resume,
            // EIP-7928 capture handoff.
            Some(bal_tx),
            // Join-miss archive refetch (None on single-host/IPC runs).
            // Whole-block exec strategy (validator parallel path).
            None,
            join_recovery,
            // The executor trusts the ordered stream (phase 2 would
            // give it its own L1 dependency); only the validator verifies.
            None,
        )
    });

    // Exit on WHICHEVER comes first: an operator shutdown signal, or the
    // engine loop finishing on its own (a fatal stream/join error). Waiting
    // only for SIGTERM left an errored executor lingering "alive" — metrics
    // up, pipeline dead — instead of exiting so the orchestrator restarts it
    // into the crash-recovery path.
    let engine_result = tokio::select! {
        _ = bin_support::wait_for_shutdown() => {
            tracing::info!("kardamom-executor: shutdown signal received; dropping runtime");
            None
        }
        res = &mut join => Some(res),
    };
    // Dropping the AeronRuntime closes every subscription, which causes
    // the tokio pump tasks to return None, which closes the sync mpsc
    // channels, which surfaces TxDataClosed / TxOrderingClosed to the
    // executor — clean shutdown.
    drop(rt);
    // In cluster mode, the tx_ordering reader blocks on cluster egress
    // `recv()`, which only returns `None` once the session thread drops its
    // sender — i.e. when the `LiveCluster` guard is dropped. Drop it here so the
    // reader sees `TxOrderingClosed` and the executor loop can exit cleanly.
    drop(cluster_guard);
    let joined = match engine_result {
        Some(r) => r,
        None => join.await,
    };
    let mut engine_error: Option<ExecutorError> = None;
    match joined {
        Ok(Ok(())) => tracing::info!("executor main loop returned cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "executor main loop returned an error");
            engine_error = Some(e);
        }
        Err(e) => tracing::error!(error = %e, "executor task panicked"),
    }
    // Stop the state writer thread (closes the delta channel, joins, surfaces
    // its final result). The executor task has finished, so its adapter clones
    // of the delta sender are already dropped.
    if let Err(e) = writer.shutdown() {
        tracing::error!(error = %e, "state writer shutdown returned an error");
    }
    // Replay-window overrun: repair BEFORE exiting so the restart resumes
    // from a fetched peer checkpoint instead of crash-looping on the same
    // refused REPLAY_FROM — see `bin_support::replay_unavailable_fallback`.
    if let Some(outcome) = bin_support::replay_unavailable_fallback(
        engine_error.as_ref(),
        args.checkpoint_dir.as_deref(),
        &args.checkpoint_peers,
        &args.state_dir,
        expected_genesis,
        false,
    )? {
        metrics::counter!(kardamom_executor::metrics::RESYNC_TOTAL, "outcome" => outcome)
            .increment(1);
    }
    // Non-zero exits so the orchestrator can tell a failed recovery / dead
    // pipeline from a clean shutdown (F13.4): exit status is the restart
    // signal the whole "fail loudly, resume from the cursor" loop keys on.
    if let Some(e) = engine_error {
        anyhow::bail!("executor pipeline failed: {e}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Role-specific adapter: tx_receipts publication.
// ---------------------------------------------------------------------------

struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    /// One `Vec<Receipt>` wire frame per batch: one encode + one blocking
    /// ack round trip through the Aeron thread instead of one per receipt.
    /// All-or-nothing per frame, so a transient failure reports 0 published
    /// and the commit thread's must-deliver loop retries the whole batch
    /// (harmless duplicates — tx_receipts is AT-LEAST-ONCE, consumers dedup
    /// on `tx_idx`).
    fn publish_receipts(
        &mut self,
        receipts: &[kardamom_types::Receipt],
    ) -> (usize, Option<ExecutorError>) {
        match self.handle.publish_receipts(&receipts.to_vec()) {
            Ok(_) => (receipts.len(), None),
            Err(e) => (
                0,
                Some(ExecutorError::State(format!("publish_receipts: {e}"))),
            ),
        }
    }

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
