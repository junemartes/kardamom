//! `kardamom-validator`: monolithic validator node.
//!
//! It follows the sequencer by subscribing to the same canonical streams
//! the executor reads (`tx_data` x M, `tx_ordering` from the Aeron
//! Cluster (Raft) egress, `tx_deposits`). It re-executes every block
//! through the shared `kardamom-engine` pipeline, and commits to its own
//! libmdbx state through the trie-aware writer, advancing a canonical
//! Ethereum MPT state root per block. It also subscribes to the
//! executor's `tx_receipts` and per-block `tx_bal` (BAL) streams, checks
//! its independent re-execution against them, and stops on any proven
//! divergence. It has no HA and runs off the hot path.
//!
//! Milestone 1: re-execute from genesis, or resume through the same
//! archive replay-merge the executor uses, produce roots, and check
//! them. It publishes nothing on the L2 streams.
//!
//! L1 output attestation (optional): when `--l1-rpc-url`,
//! `--output-oracle`, and `--attester-key` are all given, a background
//! attester collects each committed block's `MessagePassed` withdrawal
//! leaves, builds the per-output withdrawals root, and posts one output
//! per `--attester-post-interval` blocks to the L1
//! `WithdrawalOutputOracle`. The key must be the oracle's permissioned
//! `attester`. Without all three flags, the validator does no automatic
//! attestation.
//!
//! Structure: CLI and file config in [`args`]; the checkpoint-trust
//! lifecycle (adoption marker, trie bootstrap, resync fallback) in
//! [`adoption`]; the verification-stream pump tasks in [`pumps`].

mod adoption;
mod args;
mod pumps;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_engine::bin_support;
use kardamom_engine::{
    EngineWiring, Executor, ExecutorConfig, ExecutorError, Inbound, MdbxSnapshotSource,
    MdbxWriterQueue, MdbxWriterSignal, Outbound, ResumePoint, RoleHooks, TxReceiptsPublication,
};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_state::{StateEnvBuilder, StateWriter, TrieMode, read_recovery_point, seed_genesis};
use kardamom_validator::attester::{self, AttesterConfig};
use kardamom_validator::epoch_verify;
use kardamom_validator::{
    BalBuffer, Divergence, ReceiptBuffer, ValidatorReceiptSink, ValidatorWriterQueue, metrics,
};

use args::{Args, ValidatorFileConfig, resolve_attester_key};

/// The validator role's port types. Only the receipts sink stays boxed,
/// since it is genuinely chosen at runtime (the optional attester tee
/// wraps the plain sink). The epoch check is the L1-re-deriving
/// [`epoch_verify::EpochVerifier`].
struct ValidatorWiring;

impl EngineWiring for ValidatorWiring {
    type TxData = bin_support::LiveTxDataSub;
    type TxOrdering = bin_support::LiveTxOrderingSub;
    type TxReceipts = Box<dyn TxReceiptsPublication>;
    type Snapshots = MdbxSnapshotSource;
    type WriterSignal = MdbxWriterSignal;
    type WriterQueue = ValidatorWriterQueue<MdbxWriterQueue>;
    type Epoch = epoch_verify::EpochVerifier;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    bin_support::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("validator", args.metrics_addr, &args.host_id)?;
    kardamom_engine::metrics::describe();
    metrics::describe();
    // The TOML supplies the `[cluster]` section. The canonical tx_ordering
    // stream is always the Aeron Cluster egress; all other runtime tuning
    // still comes from the CLI flags above.
    let raw = std::fs::read_to_string(&args.config).context("read validator config")?;
    let mut file_cfg: ValidatorFileConfig =
        toml::from_str(&raw).context("parse validator config")?;

    // Per-node cluster egress endpoint. The cluster client's
    // egress_channel is this node's reachable address, so the deploy
    // injects it rather than baking it into the static config file.
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

    // --- State backend and crash-recovery decision (mirrors the executor). ---
    let (genesis, chain_id) = bin_support::resolve_genesis(args.chain.as_deref(), args.chain_id)?;

    // Cold-start checkpoint adoption: see `adoption` for the trust
    // lifecycle (marker, trie bootstrap, resync fallback).
    let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
    adoption::adopt_checkpoint_if_fresh(
        args.checkpoint_dir.as_deref(),
        &args.state_dir,
        &args.checkpoint_peers,
        expected_genesis,
    )?;

    let env = StateEnvBuilder::new(&args.state_dir)
        .durability(args.state_durability.into())
        .open()
        .with_context(|| format!("open state env at {}", args.state_dir.display()))?;
    let recovery = read_recovery_point(&env).context("read state recovery point")?;
    let start = ResumePoint {
        block: recovery.last_committed_block,
        record_count: recovery.last_fsynced_b_position.as_index(),
        l2_timestamp: recovery.last_committed_l2_timestamp,
    };
    if start.is_resume() {
        tracing::info!(
            resume_block = start.block,
            "validator resuming from persisted cursor"
        );
    }

    // M tx_data subscriptions plus tx_deposits (bridged async to sync),
    // identical to the executor: always live, with the down-window or
    // lapse gap recovered in-band by the reader's join-miss refetch
    // against the remote durability archives.
    let tx_data_subs = bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
    let join_recovery = bin_support::archive_join_recovery(
        &channels,
        &aeron_cfg,
        args.aeron_dir.as_deref(),
        args.archive_control_response_endpoint.as_deref(),
        args.replay_destination_endpoint.as_deref(),
    );

    // One tx_ordering subscription, always the Aeron Cluster (Raft)
    // egress, exactly as in the executor. The cluster has already
    // deduplicated and totally ordered the stream and exposes a
    // blocking `next()`, so no async-to-sync bridge is needed. Leader
    // failover and reconnect, including crash-recovery replay of the
    // canonical stream, is handled inside the cluster client. The
    // cluster-session guard (`LiveCluster`) and its dedicated Aeron
    // runtime must outlive the validator loop, so bind the guard in the
    // outer scope; it is dropped only after the `join` await below.
    // Fresh validators start at genesis and receive the full retained
    // canonical stream. The replay request is re-sent on every session
    // start, so a validator whose session dies mid-chaos catches back up
    // instead of stopping on an unrecoverable gap.
    let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
        args.aeron_dir.as_deref(),
        file_cfg.cluster.to_live(),
        bin_support::cluster_replay_cursor(&start),
    )?;
    tracing::info!("kardamom-validator: tx_ordering via Aeron Cluster");
    // The kardamom_sealer_* re-export is the executor's job. A validator
    // emitting a second, lagging copy of the series would break
    // sum()-style queries and contradict the documented observation point.
    let tx_ordering_sub = cluster_sub.suppress_sealer_metrics();

    // --- Verification streams: tx_bal (BAL) and tx_receipts (see `pumps`). ---
    let divergence = Divergence::new();
    let bals = BalBuffer::new();
    let claims = kardamom_validator::ClaimBuffer::new();
    let receipts = ReceiptBuffer::new();

    let bal_pump_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    pumps::spawn_bal_pump(
        &rt,
        &channels,
        bals.clone(),
        claims.clone(),
        bal_pump_stop.clone(),
    )?;
    pumps::spawn_receipts_pump(&rt, &channels, args.executor_count, receipts.clone())?;

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
    // Adoption, half two: rebuild the mirror and trie when the adoption
    // marker, or a truly trie-less image, says to. This must run before
    // the trie-aware writer spawns.
    adoption::bootstrap_trie_if_adopted(&args.state_dir, &env)?;
    let writer =
        StateWriter::spawn_with_trie(env, trie_mode).context("spawn trie-aware state writer")?;
    let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
    let writer_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
    let writer_queue = ValidatorWriterQueue::new(
        MdbxWriterQueue::new(writer.delta_tx.clone()),
        bals.clone(),
        divergence.clone(),
    )
    // Blocks at or below the recovery resume point were verified before
    // the restart. Replay re-execution against already-applied state
    // gives empty deltas that cannot match the BAL. Comparing them
    // caused false-divergence restart cascades.
    .with_verify_floor(recovery.last_committed_block);

    // L1 output attester: enabled only when all three flags are present.
    // It runs inside this tokio runtime. The task lives as long as a
    // handle clone does; `attester_handle` is held below for the process
    // lifetime.
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
        (None, None, None) => None, // Milestone-1 default: no automatic attestation.
        _ => anyhow::bail!(
            "attestation needs --l1-rpc-url, --output-oracle and --attester-key together \
             (got a partial set)"
        ),
    };

    // Tee each block's withdrawal leaves into the attester from the
    // receipt stream (a plain sink when attestation is disabled).
    //
    // This does not read from the BlockDelta. The engine finalizes every
    // delta with an empty receipts vector, since receipts travel on
    // tx_receipts, so reading from the delta would collect nothing: every
    // posted output would carry `leaves=0`, no withdrawal would be
    // attested, and none could be finalized on L1.
    //
    // Flight ring: recent block inputs for the receipt-divergence dump.
    // Always on, since the receipt check runs on the sequential path too.
    let flight = kardamom_validator::flight::FlightRing::new();
    let tx_receipts_pub: Box<dyn TxReceiptsPublication> = {
        let sink = ValidatorReceiptSink::new(receipts.clone(), divergence.clone())
            .with_flight(flight.clone());
        match &attester_handle {
            Some(h) => Box::new(attester::AttestingReceiptSink::new(sink, h.clone())),
            None => Box::new(sink),
        }
    };

    pumps::spawn_commit_poller(writer.snapshot_rx.clone(), attester_handle.clone());

    let mut cfg = ExecutorConfig {
        chain_id,
        // A validator always re-derives record identity. The
        // stream's sender and tx_hash are proxy claims, and verification
        // that trusts them re-executes the very theft it exists to catch.
        // The resulting RecordIdentity halt is classified as integrity
        // (exit 2) below.
        verify_record_identity: true,
        ..ExecutorConfig::default()
    };
    // Always bound the tx_data join wait. A verifier that loses an
    // envelope must fail loudly into the supervisor-restart and
    // archive-replay recovery loop, not hang forever mid-join. Divergence
    // stops stay distinguishable by their "halted on divergence" log
    // line. See `bounded_join_timeout` for why fresh differs from resume.
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(start.is_resume());

    // Parallel validation strategy, opt-in: seeded batches driven by the
    // BAL. `None` keeps the engine's streaming per-tx path byte-for-byte.
    let block_exec = if args.parallel_validation {
        // 0 means auto; hard cap 40 per the mdbx reader-slot budget
        // (geometry::MAX_READERS = 64, shared with exec, RPC, and compaction).
        let workers = match args.validation_workers {
            0 => std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4),
            n => n.min(40),
        };
        tracing::info!(
            batch_size = args.validation_batch_size,
            workers,
            "parallel validation ENABLED (seeded BAL batches on the shared pool)"
        );
        Some(kardamom_validator::parallel::parallel_block_exec(
            claims.clone(),
            args.validation_batch_size,
            workers,
            Some(flight.clone()),
        ))
    } else if args.prove_batches.is_some() {
        // The spool feeds from the flight ring, which only the
        // whole-block path fills. Run the sequential whole-block
        // strategy, which has the same semantics as streaming, since it
        // delegates to the shared driver.
        Some(kardamom_validator::prover::sequential_block_exec(
            flight.clone(),
        ))
    } else {
        None
    };
    if let Some(dir) = args.prove_batches.clone() {
        tracing::info!(spool = %dir.display(), "prover spool ENABLED (one frame per block)");
        kardamom_validator::prover::spawn_prover_spool(
            dir,
            chain_id,
            writer.snapshot_rx.clone(),
            flight.clone(),
        );
    }

    // Epoch verification (phase 1). Sequence rules 1-2 are local and
    // always enforced once an epoch appears. The content check needs L1,
    // so it is wired only when both the RPC URL and the lockbox address
    // are given.
    let epoch_observer: Option<epoch_verify::EpochVerifier> =
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
                Some(epoch_verify::EpochVerifier::spawn(
                    source,
                    lockbox,
                    divergence.clone(),
                    &tokio::runtime::Handle::current(),
                ))
            }
            _ => {
                tracing::info!(
                    "epoch CONTENT verification disabled (needs --l1-rpc-url and \
                     --lockbox); origin sequence rules still apply"
                );
                None
            }
        };

    let mut join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::run::<ValidatorWiring>(
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
            start,
            RoleHooks {
                // No BAL capture: the validator verifies BALs, never
                // publishes them.
                bal_capture: None,
                // No footprint shadow either: measurement runs on the
                // executor role.
                footprint_shadow: None,
                // Whole-block exec strategy (the parallel-validation path).
                block_exec,
                epoch_observer,
            },
        )
    });

    // Exit on whichever comes first: an operator shutdown signal, or the
    // engine loop finishing on its own (a divergence stop or a stream
    // error). Waiting only for SIGTERM would leave a halted validator
    // looking "alive", with metrics up and the chain frozen, hiding the
    // very stop signal the divergence machinery exists to surface.
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
            // A forged record identity is proof, not an outage. Latch it,
            // so the exit-2 path below fires instead of the restart loop.
            kardamom_validator::latch_integrity_failure(&divergence, &e);
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
    // If this line is present in the log, the clean-shutdown path ran to
    // the writer stop. If it is absent before exit, the process died
    // uncleanly, and the mdbx env is left unsteady: read-only consumers
    // will see WANNA_RECOVERY.
    tracing::info!("validator shutdown: state writer stopped");
    // Exit 2 is reserved for a proven divergence (the latch records
    // before the engine surfaces `Divergence`), the page-the-humans
    // signal. Any other engine failure (a stream error, a replay-window
    // overrun needing resync) is an availability problem, not an
    // integrity one, and must not look like one: exit 1 and let the
    // orchestrator restart.
    if divergence.is_halted() {
        if let Some(reason) = divergence.reason() {
            tracing::error!(reason = %reason, "validator halted on divergence");
        }
        std::process::exit(2);
    }
    if let Some(cause) = engine_error {
        adoption::resync_after_engine_error(
            cause.as_ref(),
            args.checkpoint_dir.as_deref(),
            &args.checkpoint_peers,
            &args.state_dir,
            expected_genesis,
        )?;
        tracing::error!(
            "validator halted on an engine error (NOT a proven divergence); if the \
             cluster refused replay (resync required), rebuild state via \
             kardamom-reconstruct or restore a checkpoint"
        );
        std::process::exit(1);
    }
    Ok(())
}
