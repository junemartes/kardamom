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
//!
//! Structure: CLI schema in [`args`]; state recovery (checkpoint
//! restore/serve, env open, resume decision) in [`state`]; role adapters
//! (wiring, receipts publication, the opt-in Block-STM strategy) in
//! [`wiring`]. This file is the driver: open streams, wire, run, tear down.

mod args;
mod state;
mod wiring;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support;
use kardamom_engine::{
    Executor, ExecutorConfig, ExecutorError, Inbound, MdbxSnapshotSource, MdbxWriterQueue,
    MdbxWriterSignal, Outbound, RoleHooks,
};
use kardamom_executor::ExecutorFileConfig;
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_state::{StateWriter, seed_genesis};

use args::Args;
use wiring::ExecutorWiring;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    bin_support::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("executor", args.metrics_addr, &args.host_id).await?;
    kardamom_engine::metrics::describe();
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
    let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
    // Cancelled on the way out; stops the periodic checkpointer task.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let state::PreparedState { env, start } =
        state::prepare_state(&args, expected_genesis, shutdown.clone())?;

    // M tx_data subscriptions + tx_deposits, async→sync bridged (shared with
    // the validator binary — see `bin_support`). ALWAYS live: the down-window
    // /-lapse gap is recovered in-band by the reader's join-miss refetch
    // against the remote durability archives (the resume-gated replay-merge
    // this replaces pointed at the consumer's LOCAL archive, which records
    // neither stream — a resuming process had no tx_data source at all).
    let tx_data_subs = bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
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
        bin_support::cluster_replay_cursor(&start),
    )?;
    tracing::info!("kardamom-executor: tx_ordering via Aeron Cluster");
    // The executor is the blessed emitter of the kardamom_sealer_* re-export
    // (default-on in the shared subscription; the validator suppresses it).
    let tx_ordering_sub = cluster_sub;

    let tx_receipts_pub = wiring::open_tx_receipts_pub(&rt_pub, &channels, &args)?;

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
    let mut writer = StateWriter::spawn(env).context("spawn state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let writer_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
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
    let writer_queue = MdbxWriterQueue::new(writer.delta_tx.clone());
    // P1 footprint shadow (spec: block-stm-executor §P1): behind
    // KARDAMOM_FOOTPRINT_SHADOW=1, the exec thread hands each block's tx
    // captures to a grading thread (measurement only — execution stays
    // sequential). None when the env flag is unset: zero cost.
    let footprint_shadow = kardamom_engine::shadow::spawn_from_env();

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
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(start.is_resume());

    let block_exec = wiring::build_block_exec(&args);

    // The executor's main loop is sync (std::thread spawns underneath).
    // Run it inside spawn_blocking so the runtime stays responsive for
    // shutdown handling.
    let mut join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::run::<ExecutorWiring>(
            cfg,
            Inbound {
                tx_data: tx_data_subs,
                tx_ordering: tx_ordering_sub,
                // Join-miss archive refetch (None on single-host/IPC runs).
                join_recovery,
            },
            Outbound {
                tx_receipts: tx_receipts_pub,
                snapshots,
                writer_signal,
                writer_queue,
            },
            // The persisted cursor (GENESIS-valued on a fresh DB): the
            // cluster source replays from it and the reader/exec counters
            // seed from it.
            start,
            RoleHooks {
                // EIP-7928 capture handoff.
                bal_capture: Some(bal_tx),
                // Footprint-shadow capture handoff (Some only under the flag).
                footprint_shadow,
                // Whole-block Block-STM strategy under --parallel-execution;
                // None keeps the streaming per-tx path. No epoch check
                // either way: the executor trusts the ordered stream
                // (phase 2 would give it its own L1 dependency).
                block_exec,
                epoch_observer: None,
            },
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
    // Dropping the AeronRuntime closes every subscription's sender, so the
    // reader threads' `blocking_recv` returns None, which surfaces
    // TxDataClosed to the executor — clean shutdown.
    drop(rt);
    shutdown.cancel();
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
        metrics::counter!(kardamom_engine::metrics::RESYNC_TOTAL, "outcome" => outcome)
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
