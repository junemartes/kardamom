//! `kardamom-executor`: standalone executor service process.
//!
//! Opens one `tx_data` subscriber per lane, one `tx_ordering` subscriber,
//! and one `tx_receipts` publisher through the log layer's Aeron runtime. It wires
//! them into the executor's reader, exec, and commit thread topology, and
//! runs until SIGTERM or Ctrl-C. The state backend is the libmdbx-backed
//! `kardamom-state` writer, opened at `--state-dir`: chain state commits
//! durably per block and persists across restarts. Genesis seeds once
//! into a fresh env.
//!
//! Crash-recovery resume: startup reads the persisted state cursor
//! (`last_committed_block` and `last_committed_end_tx_position`) and
//! replays the canonical stream from the Aeron archives through a
//! replay-merge. It skip-counts past the cursor, so already-committed
//! blocks are never applied twice.
//!
//! The role-agnostic scaffolding (durability arg, genesis loading, the
//! async-to-sync stream bridges, tracing and shutdown helpers) is shared
//! with the validator binary through `kardamom_engine::bin_support`.
//!
//! Structure: the CLI schema is in [`args`]. State recovery (checkpoint
//! restore and serve, env open, resume decision) is in [`state`]. Role
//! adapters (wiring, receipts publication, the opt-in Block-STM strategy)
//! are in [`wiring`]. This file is the driver: open streams, wire, run,
//! tear down.

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
use kardamom_log::discovery::StreamPlane;
use kardamom_state::{StateWriter, seed_genesis};

use args::Args;
use wiring::ExecutorWiring;

/// The state writer and the adapters built from its handle, plus the
/// BAL and footprint-shadow capture handoffs.
struct WriterAdapters {
    writer: kardamom_state::WriterHandle,
    snapshots: MdbxSnapshotSource,
    writer_signal: MdbxWriterSignal,
    writer_queue: MdbxWriterQueue,
    bal_tx: crossbeam_channel::Sender<kardamom_engine::actor::BalHandoff>,
    footprint_shadow: Option<crossbeam_channel::Sender<kardamom_engine::shadow::ShadowBlock>>,
    /// Kept alive so the BAL publisher thread keeps running; not read.
    _bal_publisher: std::thread::JoinHandle<()>,
    /// The nonce query listener, when `--nonce-query-addr` is set. Kept
    /// alive so its accept task keeps running; not read.
    _nonce_query: Option<kardamom_state::NonceQueryServer>,
}

/// Seed genesis into `env` if not already seeded, spawn the state
/// writer and its adapters, open the `tx_bal` publication, and start the
/// BAL publisher thread.
async fn spawn_writer_and_bal(
    args: &Args,
    env: kardamom_state::StateEnv,
    genesis: Option<&kardamom_types::Genesis>,
    rt_pub: &AeronRuntime,
    plane: &mut StreamPlane,
) -> Result<WriterAdapters> {
    // Seed genesis once into a fresh env (a no-op if already seeded, for
    // example on recovery). This must run before `StateWriter::spawn`, so
    // the writer's initial published snapshot already reflects genesis.
    let (genesis_accounts, genesis_code) = bin_support::build_genesis_alloc(genesis);
    let seeded = seed_genesis(&env, &genesis_accounts, &genesis_code)
        .context("seed genesis into state env")?;
    tracing::info!(
        state_dir = %args.state_dir.display(),
        durability = ?args.state_durability,
        genesis_accounts = genesis_accounts.len(),
        seeded,
        "state env opened"
    );

    // The nonce query endpoint reads the committed state through its own
    // short-lived snapshots. It must exist before the writer takes `env`.
    let nonce_query = args
        .nonce_query_addr
        .map(|addr| kardamom_state::serve_nonce_queries(addr, env.clone()))
        .transpose()
        .context("bind nonce query address")?;

    // Spawn the writer, and build the three executor adapters from its
    // handle. The snapshot-swap channel feeds reads (the snapshot source
    // and commit signal). The delta channel feeds writes.
    let writer = StateWriter::spawn(env).context("spawn state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let writer_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
    let writer_queue = MdbxWriterQueue::new(writer.delta_tx.clone());

    // BAL publication: tee each block's `BlockDelta` onto tx_bal, so
    // validators can cross-check their re-execution. This publishes on
    // the isolated publication runtime (`rt_pub`), like receipts, so it
    // never stalls the subscription poll.
    let bal_pub = plane
        .tx_bal_publisher(rt_pub)
        .await
        .context("open tx_bal publication")?;
    // EIP-7928 BAL publisher. The exec thread hands off each block's
    // captured Bal and receipts-free delta. This thread encodes and
    // delivers it with an ack and bounded retry, retaining recent
    // frames for validator catch-up. Emission is not best-effort:
    // parallel validation makes BAL availability a validator liveness
    // property. The channel has a bounded depth, so a wedged publisher
    // back-pressures exec instead of dropping state transitions.
    let (bal_tx, bal_rx) = crossbeam_channel::bounded(8);
    let bal_publisher = kardamom_executor::bal::BalPublisher::new(bal_rx, bal_pub)
        .spawn()
        .context("spawn BAL publisher")?;

    // Footprint shadow. Behind `KARDAMOM_FOOTPRINT_SHADOW=1`, the exec
    // thread hands each block's tx captures to a grading thread
    // (measurement only; execution stays sequential). It is `None`
    // when the env flag is unset, for zero cost.
    let footprint_shadow = kardamom_engine::shadow::Shadow::spawn_from_env();

    Ok(WriterAdapters {
        writer,
        snapshots,
        writer_signal,
        writer_queue,
        bal_tx,
        footprint_shadow,
        _bal_publisher: bal_publisher,
        _nonce_query: nonce_query,
    })
}

/// Load the executor's TOML config, and apply the per-node cluster
/// egress endpoint override.
///
/// The TOML supplies the optional `[cluster]` section (disabled by
/// default). All other runtime tuning still comes from the CLI flags.
/// An empty or comment-only file (the current deployment shape)
/// deserializes to a disabled cluster, so behavior stays the same
/// unless `[cluster]` is set.
///
/// The cluster client's `egress_channel` is this node's reachable
/// address (the node IP differs per replica), so the Nomad job injects
/// it as `--cluster-egress-endpoint`, instead of baking it into the
/// static config file.
fn load_file_config(args: &Args) -> Result<ExecutorFileConfig> {
    let raw = std::fs::read_to_string(&args.config).context("read executor config")?;
    let mut file_cfg: ExecutorFileConfig = toml::from_str(&raw).context("parse executor config")?;
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        file_cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    Ok(file_cfg)
}

/// The resolved log config, the stream plane, and the two Aeron
/// runtimes: everything [`main`] opens before it touches state.
struct Transport {
    aeron_cfg: kardamom_log::config::AeronConfig,
    plane: StreamPlane,
    rt: AeronRuntime,
    rt_pub: AeronRuntime,
}

/// Resolve the log config and open the stream plane and the runtimes.
///
/// The archive-replay recovery connects its own archive client. It needs
/// the archive control channels and media-driver dir from the
/// `AeronConfig`, so the CLI `--aeron-dir` overrides it when given, and
/// recovery joins the same driver as the runtime.
///
/// `rt_pub` is a separate Aeron runtime and thread for the `tx_receipts`
/// publication. One Aeron thread would otherwise service both the
/// `tx_ordering` subscription poll and the per-tx receipt and boundary
/// publishes. Under sustained load, the publish work delays the
/// `tx_ordering` poll past Aeron's flow-control Status-Message deadline.
/// Then the sealer drops this subscriber, its image dies, and the
/// executor freezes (the reader stops, and exec blocks on reading).
/// Isolating the publisher on its own thread keeps the subscription poll
/// timely no matter the receipt load.
fn open_transport(args: &Args) -> Result<Transport> {
    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let plane = StreamPlane::from_config(&log_cfg, &format!("executor-{}", args.recorder_id))
        .context("build the stream plane")?;
    let mut aeron_cfg = log_cfg.aeron;
    if let Some(dir) = args.aeron_dir.as_ref() {
        aeron_cfg.aeron_dir.clone_from(dir);
    }
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let rt_pub =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn receipts AeronRuntime")?;
    Ok(Transport {
        aeron_cfg,
        plane,
        rt,
        rt_pub,
    })
}

/// The one `tx_ordering` subscription, always the Aeron Cluster (Raft)
/// egress. The cluster has already deduped and totally ordered the
/// stream, and exposes a blocking `next()`, so no async-to-sync bridge is
/// needed. Leader failover and reconnect, including crash-recovery replay
/// of the canonical stream, are handled inside the cluster client, so the
/// reader never sees an image rotation. The executor's skip-count and
/// `DedupWindow` give idempotency across any reconnect overlap. The
/// cluster-session guard (`LiveCluster`) must outlive the executor loop,
/// so [`bin_support::LiveStreams`] holds it. It drops only after the
/// shutdown wait ends.
///
/// The member ingress endpoints come from the catalog when discovery
/// lists them, else from the static `[cluster]` section. The executor is
/// the chosen emitter of the `kardamom_sealer_*` re-export, on by default
/// in the shared subscription.
async fn connect_cluster(
    args: &Args,
    file_cfg: &ExecutorFileConfig,
    plane: &StreamPlane,
    start: &kardamom_engine::ResumePoint,
) -> Result<(
    kardamom_cluster_adapter::LiveCluster,
    bin_support::LiveTxOrderingSub,
)> {
    let mut cluster_cfg = file_cfg.cluster.to_live();
    if let Some(endpoints) = plane.cluster_ingress_endpoints().await? {
        cluster_cfg.ingress_endpoints = endpoints;
    }
    let connected = bin_support::connect_cluster_ordering(
        args.aeron_dir.as_deref(),
        cluster_cfg,
        bin_support::cluster_replay_cursor(start),
    )?;
    tracing::info!("kardamom-executor: tx_ordering via Aeron Cluster");
    Ok(connected)
}

/// What one process sets up once: tracing, the metrics exporter, the
/// parsed configs, the checkpoint server, and the shutdown signal. Every
/// revolution of the pipeline starts from this.
struct Boot {
    args: Args,
    file_cfg: ExecutorFileConfig,
    /// Serves this node's checkpoints to peers for the process lifetime;
    /// held only for its bound socket. `None` without a checkpoint dir
    /// or serve address.
    _checkpoints: Option<kardamom_state::CheckpointServer>,
    /// Cancelled on the operator's shutdown signal. A revolution checks
    /// it before it starts, so a signal that lands during the repair
    /// between two revolutions ends the process instead of being lost.
    stop: tokio_util::sync::CancellationToken,
}

impl Boot {
    async fn init(args: Args) -> Result<Self> {
        bin_support::init_tracing();
        kardamom_obs::init_service!("executor", args.metrics_addr, &args.host_id).await?;
        kardamom_engine::metrics::describe();
        let file_cfg = load_file_config(&args)?;
        tracing::info!(
            lanes = kardamom_types::shard_map::LANE_COUNT,
            chain_id = args.chain_id,
            "kardamom-executor starting"
        );
        // Serve this node's checkpoints to peers (the other side of the
        // peer fetch in `state`). This is best-effort infrastructure. But
        // a bad bind address is a deploy bug, so fail startup loudly.
        let checkpoints = match (args.checkpoint_dir.as_ref(), args.checkpoint_serve_addr) {
            (Some(dir), Some(addr)) => Some(
                kardamom_state::serve_checkpoints(addr, dir.clone())
                    .context("bind checkpoint serve address")?,
            ),
            _ => None,
        };
        let stop = tokio_util::sync::CancellationToken::new();
        let signal = stop.clone();
        tokio::spawn(async move {
            bin_support::wait_for_shutdown().await;
            signal.cancel();
        });
        Ok(Self {
            args,
            file_cfg,
            _checkpoints: checkpoints,
            stop,
        })
    }
}

/// What the process does after one revolution of the pipeline.
enum Verdict {
    /// The engine returned cleanly: the operator asked for shutdown.
    Done,
    /// A peer checkpoint is staged and the stale state parked: run the
    /// pipeline again, which restores it.
    Revolve,
    /// The pipeline failed and the process cannot repair it.
    Failed(ExecutorError),
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let boot = Boot::init(Args::parse()).await?;
    // Each turn runs the whole pipeline once. A refused replay stages a
    // peer checkpoint and comes back here for the next turn, which
    // restores it in-process, with no orchestrator restart in between
    // (issue #298); every other end leaves the loop.
    let mut revolutions = 0u32;
    while turn(&boot, &mut revolutions).await?.is_continue() {}
    Ok(())
}

/// One turn of the process loop: a revolution, then its verdict.
/// `Continue` means run again; `Break` means the process is done. A
/// failure the process cannot repair returns `Err`, so the process exits
/// non-zero: the orchestrator tells a dead pipeline from a clean shutdown
/// by the exit status, which the whole "fail loudly, resume from the
/// cursor" loop relies on.
async fn turn(boot: &Boot, revolutions: &mut u32) -> Result<std::ops::ControlFlow<()>> {
    match Box::pin(run_once(boot)).await? {
        Verdict::Done => return Ok(std::ops::ControlFlow::Break(())),
        Verdict::Failed(e) => anyhow::bail!("executor pipeline failed: {e}"),
        Verdict::Revolve => (),
    }
    if boot.stop.is_cancelled() {
        tracing::info!("shutdown signal received during the resync repair; not revolving");
        return Ok(std::ops::ControlFlow::Break(()));
    }
    *revolutions += 1;
    tracing::info!(
        revolutions,
        "resync: the pipeline starts again in-process and restores the staged peer checkpoint"
    );
    Ok(std::ops::ControlFlow::Continue(()))
}

/// One revolution: open the transport and the state, run the engine to
/// its end, repair a refused replay, and return the verdict. Every
/// handle, the nonce query server's clone of the state env included, is
/// gone before the repair parks the state.
async fn run_once(boot: &Boot) -> Result<Verdict> {
    let args = &boot.args;
    let file_cfg = &boot.file_cfg;
    let Transport {
        aeron_cfg,
        mut plane,
        rt,
        rt_pub,
    } = open_transport(args)?;

    // --- State backend and crash-recovery decision. This runs before the
    // subscriptions, because the tx_ordering subscription branches on
    // whether the node is resuming.
    // Load genesis (its chain_id is adopted when present).
    let (genesis, chain_id) = bin_support::resolve_genesis(args.chain.as_deref(), args.chain_id)?;
    let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
    // The guard in `LiveStreams` cancels this token on the way out; that
    // stops the periodic checkpointer task.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let state::PreparedState { env, start } =
        state::prepare_state(args, expected_genesis, shutdown.clone())?;

    // M tx_data subscriptions, plus tx_deposits, bridged from async to
    // sync (shared with the validator binary; see `bin_support`). These
    // stay live always: the reader's join-miss refetch recovers any
    // down-window or lapse gap in-band, against the remote durability
    // archives.
    let tx_data_subs = bin_support::open_tx_data_subs(&rt, &mut plane)?;
    let join_recovery = bin_support::archive_join_recovery(
        &mut plane,
        &aeron_cfg,
        args.aeron_dir.as_deref(),
        args.archive_control_response_endpoint.as_deref(),
        args.replay_destination_endpoint.as_deref(),
    );

    let (cluster_guard, tx_ordering_sub) = connect_cluster(args, file_cfg, &plane, &start).await?;

    let tx_receipts_pub = wiring::open_tx_receipts_pub(&rt_pub, &mut plane, args).await?;

    let WriterAdapters {
        mut writer,
        snapshots,
        writer_signal,
        writer_queue,
        bal_tx,
        footprint_shadow,
        _bal_publisher,
        _nonce_query: nonce_query,
    } = spawn_writer_and_bal(args, env, genesis.as_ref(), &rt_pub, &mut plane).await?;

    // `verify_record_identity` stays off here by decision, not omission.
    // With the validator checking every record, a forged envelope
    // halts verification with proof. Sequencer-side rejection would only
    // buy defense-in-depth, at the cost of an ecrecover per tx on the hot
    // path. See the field's doc for the full trade-off.
    let mut cfg = ExecutorConfig {
        chain_id,
        ..ExecutorConfig::default()
    };
    // Always bound the tx_data join wait. A replica whose multicast
    // tx_data image races a sequencer restart (a new publisher session)
    // can lose an envelope, and an unbounded join wait then freezes that
    // replica silently while its peers advance. Failing loudly hands
    // recovery to the designed loop: Nomad restarts the task, and crash
    // recovery replays the tx_data gap from the archive. See
    // `bounded_join_timeout` for why the fresh-start bound exceeds
    // resume's.
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(start.is_resume());
    cfg.reader.voter_id = args.void_voter_id;

    let block_exec = wiring::build_block_exec(args);

    // The executor's main loop is sync (std::thread spawns underneath).
    // Run it inside spawn_blocking so the runtime stays responsive for
    // shutdown handling.
    let join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::<ExecutorWiring>::new(
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
            // The persisted cursor. It has a genesis value on a fresh
            // DB. The cluster source replays from it, and the reader
            // and exec counters seed from it.
            start,
            RoleHooks {
                // EIP-7928 capture handoff.
                bal_capture: Some(bal_tx),
                // Footprint-shadow capture handoff (Some only under the flag).
                footprint_shadow,
                // Whole-block Block-STM strategy under `--parallel-execution`.
                // `None` keeps the streaming per-tx path. Either way, there
                // is no epoch check: the executor trusts the ordered stream.
                block_exec,
                epoch_observer: None,
                // No remote-epoch check either: that seam is wired by the
                // destination validator only.
                remote_epoch_observer: None,
            },
        )
        .run()
    });

    let engine_error = wait_for_engine(rt, cluster_guard, shutdown, join).await;
    // The plane's registrations deregister once the engine has stopped.
    plane.shutdown().await;
    // Stop the state writer thread (this closes the delta channel, joins
    // it, and surfaces its final result). The executor task has finished,
    // so its adapter clones of the delta sender are already dropped.
    if let Err(e) = writer.shutdown() {
        tracing::error!(error = %e, "state writer shutdown returned an error");
    }
    // End the nonce query server before the repair: it holds a clone of
    // the state env, and the repair parks that env.
    stop_nonce_query(nonce_query).await;
    // Replay-window overrun: repair between revolutions, so the next one
    // restores a fetched peer checkpoint instead of re-requesting the
    // same refused `REPLAY_FROM`. See
    // `bin_support::replay_unavailable_fallback`.
    let repaired = bin_support::replay_unavailable_fallback(
        engine_error.as_ref(),
        args.checkpoint_dir.as_deref(),
        &args.checkpoint_peers,
        &args.state_dir,
        expected_genesis,
        false,
    )?;
    if let Some(outcome) = repaired {
        metrics::counter!(kardamom_engine::metrics::RESYNC_TOTAL, "outcome" => outcome)
            .increment(1);
    }
    Ok(verdict(engine_error, repaired))
}

/// The verdict for one engine result and the repair's result. `repaired`
/// is the resync outcome label the fallback returned, or `None` when the
/// error was not a refused replay, or the fallback is not configured.
fn verdict(engine_error: Option<ExecutorError>, repaired: Option<&str>) -> Verdict {
    match (engine_error, repaired) {
        (None, _) => Verdict::Done,
        (Some(_), Some("peer-checkpoint")) => Verdict::Revolve,
        (Some(e), _) => Verdict::Failed(e),
    }
}

/// End the nonce query server, when one runs, and wait until its task is
/// gone: an abort only schedules the cancel, and the port is free only
/// once the task has dropped its listener. The next revolution binds the
/// same address.
async fn stop_nonce_query(server: Option<kardamom_state::NonceQueryServer>) {
    let Some(mut server) = server else {
        return;
    };
    server.task.abort();
    let _ = (&mut server.task).await;
}

/// Wait for whichever comes first: an operator shutdown signal, or the
/// engine loop finishing on its own. Then end the streams, in
/// [`bin_support::LiveStreams`] field order, and log the outcome. Returns
/// the engine error when the loop returned one. A panic leaves no error
/// value.
async fn wait_for_engine(
    rt: AeronRuntime,
    cluster_guard: kardamom_cluster_adapter::LiveCluster,
    shutdown: tokio_util::sync::CancellationToken,
    join: tokio::task::JoinHandle<Result<(), ExecutorError>>,
) -> Option<ExecutorError> {
    let joined = bin_support::EngineShutdown {
        bin_name: "kardamom-executor",
        join,
        streams: bin_support::LiveStreams {
            stop: shutdown.drop_guard(),
            rt,
            cluster_guard,
        },
    }
    .wait()
    .await;
    match joined {
        Ok(Ok(())) => {
            tracing::info!("executor main loop returned cleanly");
            None
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "executor main loop returned an error");
            Some(e)
        }
        Err(e) => {
            tracing::error!(error = %e, "executor task panicked");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_revolves_only_on_a_staged_checkpoint() {
        let refused = || ExecutorError::ClusterReplayUnavailable {
            from_index: 10,
            oldest_index: 500,
            oldest_block: 7,
        };
        assert!(matches!(
            verdict(Some(refused()), Some("peer-checkpoint")),
            Verdict::Revolve
        ));
        assert!(matches!(
            verdict(Some(refused()), Some("unrecoverable")),
            Verdict::Failed(_)
        ));
        assert!(matches!(verdict(Some(refused()), None), Verdict::Failed(_)));
        assert!(matches!(verdict(None, None), Verdict::Done));
    }
}
