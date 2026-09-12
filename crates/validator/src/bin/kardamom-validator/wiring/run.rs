//! The receipts sink, the engine loop, and shutdown: everything after the
//! optional attester spawns.

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use kardamom_cluster_adapter::LiveCluster;
use kardamom_engine::bin_support;
use kardamom_engine::{
    Either, EngineWiring, ExecPorts, Executor, ExecutorError, Outbound, RoleHooks,
};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_state::StateEnv;
use kardamom_validator::flight::FlightRing;
use kardamom_validator::{ClaimBuffer, Divergence, epoch_verify};

use super::pipeline::{Attested, WriterPorts, Written};
use super::startup::{InteropServe, Opened, OpenedState, Startup, Streamed, StreamsState};
use crate::args::{Args, WorkerCount};

impl Attested {
    /// Build the `tx_receipts` publication chain: the receipt cross-check,
    /// optionally teed to the attester, optionally wrapped by outbox
    /// extraction. Outbox extraction sits outermost: it buffers before
    /// forwarding but flushes to the feed store only after the inner
    /// chain (the receipt cross-check) accepted the boundary, so a
    /// diverging block is never served.
    ///
    /// This does not read leaves from the `BlockDelta`. The engine
    /// finalizes every delta with an empty receipts vector, since
    /// receipts travel on `tx_receipts`, so reading from the delta would
    /// collect nothing.
    ///
    /// Also starts the commit poller, which feeds the attester and the
    /// interop attestation feed from committed-block snapshots.
    #[must_use]
    pub(crate) fn build_sink(self) -> Ready {
        let streams = &self.written.streamed.streams;
        let sink = kardamom_validator::ValidatorReceiptSink::new(
            streams.receipts.clone(),
            streams.divergence.clone(),
        )
        .with_flight(streams.flight.clone());
        let tee: ReceiptsTee = match self.attester_handle.as_ref() {
            Some(h) => Either::Left(kardamom_validator::attester::AttestingReceiptSink::new(
                sink,
                h.clone(),
            )),
            None => Either::Right(sink),
        };
        let tx_receipts_pub: TxReceiptsChain = match streams.interop_serve.as_ref() {
            Some(s) => Either::Left(kardamom_validator::interop::ExtractingReceiptSink::new(
                tee,
                self.written.streamed.opened.state.chain_id.get(),
                s.claims.clone(),
                s.store.clone(),
                streams.divergence.clone(),
            )),
            None => Either::Right(tee),
        };

        crate::pumps::CommitPoller::new(
            &self.written.writer.writer.snapshot_rx,
            self.attester_handle.clone(),
            streams
                .interop_serve
                .as_ref()
                .map(|s| s.attestations.clone()),
            streams.pump_shutdown.clone(),
        )
        .spawn();

        Ready {
            attested: self,
            tx_receipts_pub,
        }
    }
}

/// The attester tee, chosen at startup: teed to the attester, or the
/// plain cross-check sink alone.
type ReceiptsTee = Either<
    kardamom_validator::attester::AttestingReceiptSink<kardamom_validator::ValidatorReceiptSink>,
    kardamom_validator::ValidatorReceiptSink,
>;

/// The full `tx_receipts` publication chain, chosen at startup: outbox
/// extraction over the tee, or the tee alone. A value, not a boxed trait
/// object, even though the shape depends on two independent startup
/// choices (the attester tee and outbox extraction).
type TxReceiptsChain =
    Either<kardamom_validator::interop::ExtractingReceiptSink<ReceiptsTee>, ReceiptsTee>;

/// [`Attested`], plus the `tx_receipts` sink [`Attested::build_sink`]
/// builds: every port the executor needs, fully wired and ready to run.
pub(crate) struct Ready {
    attested: Attested,
    tx_receipts_pub: TxReceiptsChain,
}

/// The validator role's port types. The epoch check is the L1-re-deriving
/// [`epoch_verify::EpochVerifier`].
///
/// `pub(super)` so [`super::startup::Opened::open_streams`] can name it
/// for [`bin_support::open_inbound`].
pub(super) struct ValidatorWiring;

/// The validator's whole-block execution strategy, chosen at startup:
/// seeded parallel batches (`--parallel-validation`), or the prover
/// spool's sequential path (`--prove-batches` alone). `None` (neither
/// flag) keeps the engine's streaming per-tx path byte-for-byte; see
/// [`build_block_exec`].
type ValidatorBlockExec = Either<
    kardamom_validator::parallel::ParallelBlockExec,
    kardamom_validator::prover::SequentialBlockExec,
>;

impl ExecPorts for ValidatorWiring {
    type Snapshots = kardamom_engine::MdbxSnapshotSource;
    type WriterSignal = kardamom_engine::MdbxWriterSignal;
    type WriterQueue = kardamom_validator::ValidatorWriterQueue<kardamom_engine::MdbxWriterQueue>;
    type Epoch = epoch_verify::EpochVerifier;
    type RemoteEpoch = kardamom_validator::interop::RemoteEpochVerifier;
    type BlockExec = ValidatorBlockExec;
}

impl EngineWiring for ValidatorWiring {
    type TxData = bin_support::LiveTxDataSub;
    type TxOrdering = bin_support::LiveTxOrderingSub;
    type TxReceipts = TxReceiptsChain;
}

impl Ready {
    /// The executor config and the three role-specific `RoleHooks` ports:
    /// reads only `self`'s own fields, so it takes no argument list.
    ///
    /// Infallible: the `--l1-rpc-url` parse that could once fail here now
    /// happens once, at the CLI boundary (see [`build_epoch_observer`]).
    fn run_ports(&self) -> RunPorts {
        let args = &self.attested.written.streamed.opened.base.args;
        let chain_id = self.attested.written.streamed.opened.state.chain_id;
        let resuming = self
            .attested
            .written
            .streamed
            .opened
            .state
            .start
            .is_resume();
        let claims = self.attested.written.streamed.streams.claims.clone();
        let snap_rx = self.attested.written.writer.writer.snapshot_rx.clone();
        let flight = self.attested.written.streamed.streams.flight.clone();
        let divergence = self.attested.written.streamed.streams.divergence.clone();

        let mut cfg = kardamom_engine::ExecutorConfig {
            chain_id,
            // A validator always re-derives record identity. The
            // stream's sender and tx_hash are proxy claims, and
            // verification that trusts them re-executes the very theft
            // it exists to catch. The resulting RecordIdentity halt is
            // classified as integrity (exit 2) by the caller.
            verify_record_identity: true,
            ..kardamom_engine::ExecutorConfig::default()
        };
        // Always bound the tx_data join wait. A verifier that loses an
        // envelope must fail loudly into the supervisor-restart and
        // archive-replay recovery loop, not hang forever mid-join.
        // Divergence stops stay distinguishable by their "halted on
        // divergence" log line. See `bounded_join_timeout` for why fresh
        // differs from resume.
        cfg.reader.join_timeout = bin_support::bounded_join_timeout(resuming);

        // Parallel validation strategy, opt-in: seeded batches driven by
        // the BAL. `None` keeps the engine's streaming per-tx path
        // byte-for-byte.
        let block_exec = build_block_exec(args, claims, flight.clone());
        if let Some(dir) = args.prove_batches.clone() {
            tracing::info!(spool = %dir.display(), "prover spool ENABLED (one frame per block)");
            kardamom_validator::prover::ProverSpool::new(dir, chain_id.get(), &snap_rx, flight)
                .spawn();
        }

        // Epoch verification. Sequence rules 1-2 are local and always
        // enforced once an epoch appears. The content check needs L1, so
        // it is wired only when both the RPC URL and the lockbox address
        // are given.
        let epoch_observer =
            build_epoch_observer(args, divergence.clone(), &tokio::runtime::Handle::current());

        // Remote-epoch verification (interop): inline pair-sequence
        // checks on every RemoteEpochRecord, always on — they need no
        // transport, exactly like the L1 origin sequence rules.
        // Content-vs-origin verification is not checked here; see
        // `interop::verify`'s module docs.
        let remote_epoch_observer = Some(kardamom_validator::interop::RemoteEpochVerifier::new(
            divergence,
        ));

        RunPorts {
            cfg,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
        }
    }

    /// Start the interop feed server (if configured), build the executor
    /// config and role hooks, run the engine loop, and shut everything
    /// down in order on exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the interop feed server fails to bind, if
    /// `--l1-rpc-url` fails to parse, or if the final replay-window-
    /// overrun resync fails.
    pub(crate) async fn run(self) -> Result<()> {
        let args = &self.attested.written.streamed.opened.base.args;
        let chain_id = self.attested.written.streamed.opened.state.chain_id;
        let interop_serve = self
            .attested
            .written
            .streamed
            .streams
            .interop_serve
            .as_ref();
        // `StateEnv` is `Arc`-backed and clones cheaply. `env` is never
        // moved out of `OpenedState` (`spawn_writer` only clones it for
        // the writer), so it is still reachable here for the serving
        // surface's `eth_getStorageAt`.
        let state_env_for_rpc = self.attested.written.streamed.opened.state.env.clone();
        let feed_resume_block = self
            .attested
            .written
            .streamed
            .opened
            .state
            .feed_resume_block()?;

        // Start the feed server (the interop serving surfaces). Dropping
        // the handle stops the server, so the caller holds it for the
        // process lifetime.
        let _feed_server = start_interop_serving(
            args,
            chain_id.get(),
            interop_serve,
            state_env_for_rpc,
            feed_resume_block,
        )
        .await?;

        let ports = self.run_ports();
        self.execute(ports).await
    }

    /// Run the engine loop to completion, then shut everything down in
    /// order: the executor task first, then the pumps, cluster guard, and
    /// writer, in the sequence [`Shutdown::wait`] enforces. Two
    /// sequential calls, [`Self::run_engine`] then
    /// [`Running::shutdown_in_order`], keep that order exactly — nothing
    /// here interleaves them.
    async fn execute(self, ports: RunPorts) -> Result<()> {
        self.run_engine(ports).shutdown_in_order().await
    }

    /// Spawn the engine loop on a blocking task, and gather everything
    /// [`Running::shutdown_in_order`] needs once it finishes: the join
    /// handle, every value [`Shutdown::wait`]'s drop order depends on,
    /// and what `finish` needs to report the result.
    fn run_engine(self, ports: RunPorts) -> Running {
        let RunPorts {
            cfg,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
        } = ports;

        let Self {
            attested:
                Attested {
                    written:
                        Written {
                            streamed:
                                Streamed {
                                    opened:
                                        Opened {
                                            base: Startup { args, rt, .. },
                                            state:
                                                OpenedState {
                                                    expected_genesis,
                                                    start,
                                                    ..
                                                },
                                        },
                                    streams:
                                        StreamsState {
                                            inbound,
                                            cluster_guard,
                                            divergence,
                                            pump_shutdown,
                                            ..
                                        },
                                },
                            writer:
                                WriterPorts {
                                    writer,
                                    snapshots,
                                    writer_signal,
                                    writer_queue,
                                },
                        },
                    ..
                },
            tx_receipts_pub,
        } = self;

        let join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
            Executor::<ValidatorWiring>::new(
                cfg,
                inbound,
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
                    // Whole-block exec strategy (the parallel-validation
                    // path).
                    block_exec,
                    epoch_observer,
                    remote_epoch_observer,
                },
            )
            .run()
        });

        Running {
            join,
            pump_shutdown,
            rt,
            cluster_guard,
            writer,
            divergence,
            args,
            expected_genesis,
        }
    }
}

/// Everything [`Running::shutdown_in_order`] needs once
/// [`Ready::run_engine`] has spawned the engine loop: the join handle,
/// every value [`Shutdown::wait`]'s drop order depends on, and what
/// `finish` needs to report the result.
struct Running {
    join: tokio::task::JoinHandle<Result<(), ExecutorError>>,
    pump_shutdown: tokio_util::sync::CancellationToken,
    rt: AeronRuntime,
    cluster_guard: LiveCluster,
    writer: kardamom_state::WriterHandle,
    divergence: Arc<Divergence>,
    args: Args,
    expected_genesis: Option<alloy_primitives::B256>,
}

impl Running {
    /// Wait for the engine loop to finish, or a shutdown signal,
    /// whichever comes first, shutting down in the order
    /// [`Shutdown::wait`] enforces, then report the result.
    async fn shutdown_in_order(self) -> Result<()> {
        let engine_error = Shutdown {
            join: self.join,
            pumps: self.pump_shutdown,
            rt: self.rt,
            cluster_guard: self.cluster_guard,
            writer: self.writer,
            divergence: self.divergence.clone(),
        }
        .wait()
        .await;
        finish(
            &engine_error,
            &self.divergence,
            &self.args,
            self.expected_genesis,
        )
    }
}

/// Start the interop feed server when `interop_serve` is configured.
/// Dropping the returned handle stops the server, so the caller holds it
/// for the process lifetime; a divergence halt exits the process and the
/// sockets die with it — a validator whose verification halts must stop
/// serving.
async fn start_interop_serving(
    args: &Args,
    chain_id: u64,
    interop_serve: Option<&InteropServe>,
    state_env: StateEnv,
    resume_block: Option<u64>,
) -> Result<Option<jsonrpsee::server::ServerHandle>> {
    let Some(s) = interop_serve else {
        return Ok(None);
    };
    let (local, handle) = kardamom_validator::interop::start_feed_server(
        s.addr,
        kardamom_validator::interop::FeedServerState {
            chain_id,
            validator_id: args.host_id.clone(),
            store: s.store.clone(),
            attestations: s.attestations.clone(),
            limits: kardamom_validator::interop::FeedServerLimits {
                max_subscriptions: args.feed_max_subscriptions,
                max_subscriptions_per_dest: args.feed_max_subscriptions_per_dest,
            },
            state_env: Some(state_env),
        },
    )
    .await
    .context("start interop feed server")?;
    if let Some(path) = args.serve_feed_addr_file.as_ref() {
        std::fs::write(path, format!("{local}\n")).context("write feed addr file")?;
    }
    tracing::info!(
        addr = %local,
        retention_blocks = %args.feed_retention_blocks,
        resume_block = ?resume_block,
        max_subscriptions = args.feed_max_subscriptions,
        max_subscriptions_per_dest = args.feed_max_subscriptions_per_dest,
        "interop feed server enabled (kardamom_subscribeOutbox, \
         kardamom_subscribeAttestations, eth_getStorageAt)"
    );
    Ok(Some(handle))
}

/// The auto-worker cap: half of `geometry::MAX_READERS` (64), leaving
/// headroom for exec, RPC, and compaction reader slots.
const AUTO_WORKER_CAP: NonZeroUsize = NonZeroUsize::new(8).expect("compile-time constant");
/// The hard worker cap for an explicit `--validation-workers` count, per
/// the mdbx reader-slot budget (`geometry::MAX_READERS` = 64, shared with
/// exec, RPC, and compaction).
const MAX_WORKERS: NonZeroUsize = NonZeroUsize::new(40).expect("compile-time constant");
/// The worker count used when `std::thread::available_parallelism` cannot
/// be read.
const FALLBACK_WORKERS: NonZeroUsize = NonZeroUsize::new(4).expect("compile-time constant");

/// Build the whole-block execution strategy: seeded parallel batches when
/// `--parallel-validation` is on, the prover's sequential wrapper when
/// `--prove-batches` is set (so the flight ring sees every block, since
/// only the whole-block path fills it), or `None` to keep the engine's
/// streaming per-tx path byte-for-byte.
fn build_block_exec(
    args: &Args,
    claims: Arc<ClaimBuffer>,
    flight: Arc<FlightRing>,
) -> Option<ValidatorBlockExec> {
    if args.parallel_validation {
        // Hard cap per the mdbx reader-slot budget.
        let workers = match args.validation_workers {
            WorkerCount::Auto => std::thread::available_parallelism()
                .map_or(FALLBACK_WORKERS, |n| n.min(AUTO_WORKER_CAP)),
            WorkerCount::Fixed(n) => n.min(MAX_WORKERS),
        };
        tracing::info!(
            batch_size = args.validation_batch_size.get().get(),
            workers = workers.get(),
            "parallel validation ENABLED (seeded BAL batches on the shared pool)"
        );
        Some(Either::Left(
            kardamom_validator::parallel::parallel_block_exec(
                claims,
                args.validation_batch_size,
                workers,
                Some(flight),
            ),
        ))
    } else if args.prove_batches.is_some() {
        Some(Either::Right(
            kardamom_validator::prover::sequential_block_exec(flight),
        ))
    } else {
        None
    }
}

/// The executor config and the three role-specific `RoleHooks` ports:
/// the whole-block exec strategy, the epoch observer, and the
/// remote-epoch observer.
struct RunPorts {
    cfg: kardamom_engine::ExecutorConfig,
    block_exec: Option<ValidatorBlockExec>,
    epoch_observer: Option<epoch_verify::EpochVerifier>,
    remote_epoch_observer: Option<kardamom_validator::interop::RemoteEpochVerifier>,
}

/// Build the epoch observer: epochs are re-derived from L1 when both
/// `--l1-rpc-url` and `--lockbox` are given. Without both, only the local
/// origin sequence rules apply; there is no content check.
///
/// Infallible now that `--l1-rpc-url` is parsed into a `reqwest::Url` at
/// the CLI boundary: this used to also parse the flag's raw string, which
/// could fail; that parse (and this function's `Result`) is gone with it.
fn build_epoch_observer(
    args: &Args,
    divergence: Arc<Divergence>,
    rt: &tokio::runtime::Handle,
) -> Option<epoch_verify::EpochVerifier> {
    let (Some(l1_rpc_url), Some(lockbox)) = (args.l1_rpc_url.clone(), args.lockbox) else {
        tracing::info!(
            "epoch CONTENT verification disabled (needs --l1-rpc-url and \
             --lockbox); origin sequence rules still apply"
        );
        return None;
    };
    let provider = alloy_provider::ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(l1_rpc_url);
    let source = Arc::new(kardamom_da_watcher::RpcL1Source::new(provider));
    tracing::info!(
        %lockbox,
        "epoch verification enabled: epochs are re-derived from L1"
    );
    Some(epoch_verify::EpochVerifier::spawn(
        source, lockbox, divergence, rt,
    ))
}

/// The six values [`Ready::run`] must hold onto until shutdown, gathered
/// so `wait` reads no argument list of its own.
struct Shutdown {
    join: tokio::task::JoinHandle<Result<(), ExecutorError>>,
    /// The pump-stop token (named `pumps`, not `pump_shutdown`, so the
    /// field name does not repeat this struct's own name).
    pumps: tokio_util::sync::CancellationToken,
    rt: AeronRuntime,
    cluster_guard: LiveCluster,
    writer: kardamom_state::WriterHandle,
    divergence: Arc<Divergence>,
}

impl Shutdown {
    /// Wait for the engine loop to finish, or a shutdown signal, whichever
    /// comes first; then shut everything down in order and classify the
    /// result.
    async fn wait(self) -> EngineOutcome {
        let Self {
            join,
            pumps,
            rt,
            cluster_guard,
            mut writer,
            divergence,
        } = self;
        // Exit on whichever comes first: an operator shutdown signal, or
        // the engine loop finishing on its own (a divergence stop or a
        // stream error). Waiting only for SIGTERM would leave a halted
        // validator looking "alive", with metrics up and the chain
        // frozen, hiding the very stop signal the divergence machinery
        // exists to surface.
        //
        // Order matters: cancel the pumps first so the tx_bal pump
        // releases its AeronRuntime clone, then release the runtime and
        // the cluster session.
        let joined = bin_support::EngineShutdown {
            bin_name: "kardamom-validator",
            join,
            streams: bin_support::LiveStreams { rt, cluster_guard },
            before_drop: || {
                pumps.cancel();
            },
        }
        .wait()
        .await;
        let engine_error = classify_engine_result(joined, &divergence);
        if let Err(e) = writer.shutdown() {
            tracing::error!(error = %e, "state writer shutdown returned an error");
        }
        // If this line is present in the log, the clean-shutdown path ran
        // to the writer stop. If it is absent before exit, the process
        // died uncleanly, and the mdbx env is left unsteady: read-only
        // consumers will see WANNA_RECOVERY.
        tracing::info!("validator shutdown: state writer stopped");
        engine_error
    }
}

/// The joined engine loop's outcome: a clean return, an engine-level
/// failure (not necessarily a divergence), or a task panic (no error
/// value survives a panic).
pub(crate) enum EngineOutcome {
    Clean,
    Failed(ExecutorError),
    Panicked,
}

/// Classify the joined engine loop's outcome. Latches a forged record
/// identity as a divergence, since it is proof, not an outage.
fn classify_engine_result(
    joined: std::result::Result<std::result::Result<(), ExecutorError>, tokio::task::JoinError>,
    divergence: &Divergence,
) -> EngineOutcome {
    match joined {
        Ok(Ok(())) => {
            tracing::info!("validator main loop returned cleanly");
            EngineOutcome::Clean
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "validator main loop returned an error");
            // A forged record identity is proof, not an outage. Latch it,
            // so the exit-2 path fires instead of the restart loop.
            kardamom_validator::latch_integrity_failure(divergence, &e);
            EngineOutcome::Failed(e)
        }
        Err(e) => {
            tracing::error!(error = %e, "validator task panicked");
            EngineOutcome::Panicked
        }
    }
}

/// The exit path: a proven divergence exits 2 (page the humans); any
/// other engine failure repairs a replay-window overrun if needed, then
/// exits 1 (an availability problem, for the orchestrator to restart).
/// Returns cleanly only for [`EngineOutcome::Clean`].
fn finish(
    engine_error: &EngineOutcome,
    divergence: &Divergence,
    args: &Args,
    expected_genesis: Option<alloy_primitives::B256>,
) -> Result<()> {
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
    let cause = match engine_error {
        EngineOutcome::Clean => return Ok(()),
        EngineOutcome::Failed(e) => Some(e),
        EngineOutcome::Panicked => None,
    };
    crate::adoption::resync_after_engine_error(
        cause,
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
