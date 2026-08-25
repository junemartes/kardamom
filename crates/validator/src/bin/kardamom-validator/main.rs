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
//!
//! Structure: CLI/file config in [`args`]; the checkpoint-trust lifecycle
//! (adoption marker / trie bootstrap / resync fallback) in [`adoption`]; the
//! verification-stream pump tasks in [`pumps`].

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

/// The validator role's port types. Only the receipts sink stays boxed —
/// it is genuinely chosen at runtime (the optional attester tee wraps the
/// plain sink); the epoch check is the L1-re-deriving
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

    // Cold-start checkpoint adoption (#143) — see `adoption` for the trust
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

    // M tx_data subscriptions + tx_deposits (async→sync bridged), identical to
    // the executor: ALWAYS live, with the down-window/lapse gap recovered
    // in-band by the reader's join-miss refetch against the remote durability
    // archives (the resume-gated replay-merge this replaces pointed at the
    // LOCAL archive, which records neither stream).
    let tx_data_subs = bin_support::open_tx_data_subs(&rt, &channels, args.shards)?;
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
        bin_support::cluster_replay_cursor(&start),
    )?;
    tracing::info!("kardamom-validator: tx_ordering via Aeron Cluster");
    // The kardamom_sealer_* re-export is the EXECUTOR's job — a validator
    // emitting a second (lagging) copy of the series would break sum()-style
    // queries and contradict the documented observation point.
    let tx_ordering_sub = cluster_sub.suppress_sealer_metrics();

    // --- Verification streams: tx_bal (BAL) + tx_receipts (see `pumps`). ---
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
    // Adoption half two: rebuild the mirror + trie when the adoption marker
    // (or a truly trie-less image) says so — BEFORE the trie-aware writer
    // spawns.
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
        // 3a.1: a validator ALWAYS re-derives record identity. The stream's
        // sender/tx_hash are proxy claims, and verification that trusts them
        // re-executes the very theft it exists to catch; the resulting
        // RecordIdentity halt is classified as integrity (exit 2) below.
        verify_record_identity: true,
        ..ExecutorConfig::default()
    };
    // ALWAYS bound the tx_data join wait — a verifier that loses an envelope
    // must fail LOUDLY into the supervisor-restart + archive-replay recovery
    // loop, not hang forever mid-join. (Divergence fail-stops stay
    // distinguishable by their 'halted on divergence' log line.) See
    // `bounded_join_timeout` for why fresh > resume.
    cfg.reader.join_timeout = bin_support::bounded_join_timeout(start.is_resume());

    // Parallel validation strategy (opt-in): seeded batches driven by the
    // BAL. `None` keeps the engine's streaming per-tx path byte-for-byte.
    let block_exec = if args.parallel_validation {
        // 0 = auto; hard cap 40 per the mdbx reader-slot budget
        // (geometry::MAX_READERS = 64, shared with exec/RPC/compaction).
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
        // The spool feeds from the flight ring, which only the whole-block
        // path fills: run the SEQUENTIAL whole-block strategy (identical
        // semantics to streaming — it delegates to the shared driver).
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

    // Epoch verification (phase 1). Sequence rules 1-2 are local and always
    // enforced once an epoch appears; the CONTENT check needs L1, so it is
    // only wired when both the RPC URL and the lockbox address are given.
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
                // No BAL capture: the validator VERIFIES BALs, never
                // publishes them.
                bal_capture: None,
                // No footprint shadow either: P1 measures on the EXECUTOR
                // role.
                footprint_shadow: None,
                // Whole-block exec strategy (the parallel-validation path).
                block_exec,
                epoch_observer,
            },
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
            // A forged record identity is proof, not an outage: latch it so
            // the exit-2 path below fires instead of the restart loop.
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
    // Present in the log ⇒ the clean-shutdown path ran to the writer stop;
    // absent before exit ⇒ the process died uncleanly (mdbx env left
    // unsteady — read-only consumers will see WANNA_RECOVERY).
    tracing::info!("validator shutdown: state writer stopped");
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
