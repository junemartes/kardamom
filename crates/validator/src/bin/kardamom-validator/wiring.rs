//! Startup and shutdown wiring for the validator binary.
//!
//! `main` is exactly the chain
//! `Startup::init(args).await?.open_state()?.open_streams()?.spawn_pumps()?
//! .spawn_writer()?.spawn_attester()?.build_sink().run().await`. Each step
//! is a method that reads only its own fields and returns the next phase;
//! there is no argument list to keep in sync with the step before it. Each
//! phase type nests the one before it as a single field, plus a small
//! struct of only what that step adds (`Opened { base: Startup, state:
//! OpenedState }`, and so on): no field is declared more than once, and no
//! step's struct lists a field it does not itself add. A few fields that
//! only matter for one step (`file_cfg`, `aeron_cfg`, `channels`, `genesis`,
//! `recovery`) simply ride along unused afterward, in exchange for never
//! re-declaring the fields that do carry all the way to [`Ready::run`].
//! `StateEnv` clones cheaply (it is `Arc`-backed), so [`Streamed::spawn_writer`]
//! never has to break a wrapper open just to move it.

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use kardamom_cluster_adapter::LiveCluster;
use kardamom_engine::bin_support;
use kardamom_engine::{
    EngineWiring, Executor, ExecutorError, Inbound, MdbxSnapshotSource, MdbxWriterQueue,
    MdbxWriterSignal, Outbound, ResumePoint, RoleHooks, TxReceiptsPublication,
};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_state::{StateEnv, StateEnvBuilder, StateWriter, read_recovery_point};
use kardamom_validator::attester::{self, AttesterConfig, AttesterHandle};
use kardamom_validator::flight::FlightRing;
use kardamom_validator::{
    BalBuffer, ClaimBuffer, Divergence, ReceiptBuffer, ValidatorReceiptSink, ValidatorWriterQueue,
    epoch_verify,
};

use crate::adoption;
use crate::args::{Args, ValidatorFileConfig, resolve_attester_key};

/// Started tracing and metrics, the resolved file config, and the main
/// Aeron runtime. The first phase: nothing has opened state or streams yet.
pub(crate) struct Startup {
    args: Args,
    file_cfg: ValidatorFileConfig,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    rt: AeronRuntime,
}

impl Startup {
    /// Start tracing and metrics, load the file config (merging the
    /// per-node cluster egress endpoint override), resolve the log
    /// config, and spawn the main Aeron runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if metrics init, config read/parse, log config
    /// resolve, or the Aeron runtime spawn fails.
    pub(crate) async fn init(args: Args) -> Result<Self> {
        bin_support::init_tracing();
        kardamom_obs::init_service!("validator", args.metrics_addr, &args.host_id).await?;
        kardamom_engine::metrics::describe();
        kardamom_validator::metrics::describe();

        // The TOML supplies the `[cluster]` section. The canonical
        // tx_ordering stream is always the Aeron Cluster egress; all other
        // runtime tuning still comes from the CLI flags.
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
            shards = args.shards.get(),
            chain_id = args.chain_id.get(),
            "kardamom-validator starting"
        );

        let log_cfg =
            LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
        let channels = log_cfg.channels;
        let mut aeron_cfg = log_cfg.aeron;
        if let Some(dir) = args.aeron_dir.as_ref() {
            aeron_cfg.aeron_dir.clone_from(dir);
        }
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

        Ok(Self {
            args,
            file_cfg,
            channels,
            aeron_cfg,
            rt,
        })
    }

    /// Resolve genesis and chain id, adopt a checkpoint if this is a fresh
    /// node joining past the cluster's retention window, open the state
    /// env, and compute the crash-recovery resume point.
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint adoption, opening the state env, or
    /// reading the recovery point fails.
    pub(crate) fn open_state(self) -> Result<Opened> {
        let (genesis, chain_id) =
            bin_support::resolve_genesis(self.args.chain.as_deref(), self.args.chain_id.get())?;

        // Cold-start checkpoint adoption: see `adoption` for the trust
        // lifecycle (marker, trie bootstrap, resync fallback).
        let expected_genesis = bin_support::expected_genesis_digest(genesis.as_ref());
        adoption::adopt_checkpoint_if_fresh(
            self.args.checkpoint_dir.as_deref(),
            &self.args.state_dir,
            &self.args.checkpoint_peers,
            expected_genesis,
        )?;

        let env = StateEnvBuilder::new(&self.args.state_dir)
            .durability(self.args.state_durability.into())
            .open()
            .with_context(|| format!("open state env at {}", self.args.state_dir.display()))?;
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

        Ok(Opened {
            base: self,
            state: OpenedState {
                genesis,
                chain_id,
                expected_genesis,
                env,
                recovery,
                start,
            },
        })
    }
}

/// Genesis, chain id, the open state env, and the crash-recovery resume
/// point: everything [`Startup::open_state`] adds.
pub(crate) struct OpenedState {
    genesis: Option<kardamom_types::Genesis>,
    chain_id: u64,
    expected_genesis: Option<alloy_primitives::B256>,
    env: StateEnv,
    recovery: kardamom_state::recovery::RecoveryPoint,
    start: ResumePoint,
}

/// [`Startup`], plus everything [`Startup::open_state`] resolved.
pub(crate) struct Opened {
    base: Startup,
    state: OpenedState,
}

impl Opened {
    /// Open every subscription the validator reads: the M `tx_data`
    /// streams plus `tx_deposits` (identical to the executor), the one
    /// cluster (Raft) `tx_ordering` egress, and the verification and
    /// interop side buffers those streams feed.
    ///
    /// The cluster-session guard (`LiveCluster`) and its dedicated Aeron
    /// runtime must outlive the validator loop; both are carried forward
    /// as fields so they drop only at [`Ready::run`]'s shutdown, in the
    /// order [`stop_streams`] documents. Fresh validators start at genesis
    /// and receive the full retained canonical stream. The replay request
    /// is re-sent on every session start, so a validator whose session
    /// dies mid-chaos catches back up instead of stopping on an
    /// unrecoverable gap.
    ///
    /// # Errors
    ///
    /// Returns an error if any subscription fails to open or the cluster
    /// session fails to connect.
    pub(crate) fn open_streams(self) -> Result<Streamed> {
        let args = &self.base.args;
        let tx_data_subs =
            bin_support::open_tx_data_subs(&self.base.rt, &self.base.channels, args.shards.get())?;
        let join_recovery = bin_support::archive_join_recovery(
            &self.base.channels,
            &self.base.aeron_cfg,
            args.aeron_dir.as_deref(),
            args.archive_control_response_endpoint.as_deref(),
            args.replay_destination_endpoint.as_deref(),
        );

        let (cluster_guard, cluster_sub) = bin_support::connect_cluster_ordering(
            args.aeron_dir.as_deref(),
            self.base.file_cfg.cluster.to_live(),
            bin_support::cluster_replay_cursor(&self.state.start),
        )?;
        tracing::info!("kardamom-validator: tx_ordering via Aeron Cluster");
        // The kardamom_sealer_* re-export is the executor's job. A
        // validator emitting a second, lagging copy of the series would
        // break sum()-style queries and contradict the documented
        // observation point.
        let tx_ordering_sub = cluster_sub.suppress_sealer_metrics();

        // --- Verification streams: tx_bal (BAL) and tx_receipts. ---
        let divergence = Divergence::new();
        let bals = BalBuffer::new();
        let claims = ClaimBuffer::new();
        let receipts = ReceiptBuffer::new();

        // Interop serving surfaces (the outbox and attestation feeds), off
        // by default. After a restart the extractor first sees block
        // `start.block + 1`. The store learns each lane's floor from that
        // point, so a stale subscriber gets `Lagged` instead of a silent
        // seq hole.
        let feed_resume_block = self
            .state
            .start
            .is_resume()
            .then(|| self.state.start.block + 1);
        let interop_serve = open_interop_serve(args, self.state.chain_id, feed_resume_block);

        // One token stops every pump. It is cancelled BEFORE `rt` drops
        // (see `pumps::spawn_bal_pump` for the runtime-clone deadlock it
        // prevents).
        let pump_shutdown = tokio_util::sync::CancellationToken::new();

        // Flight ring: recent block inputs for the receipt-divergence
        // dump. Always on, since the receipt check runs on the sequential
        // path too.
        let flight = FlightRing::new();

        Ok(Streamed {
            opened: self,
            streams: StreamsState {
                tx_data_subs,
                join_recovery,
                cluster_guard,
                tx_ordering_sub,
                divergence,
                bals,
                claims,
                receipts,
                interop_serve,
                pump_shutdown,
                flight,
            },
        })
    }
}

/// The interop serving surfaces (outbox and attestation feeds), on when
/// `--serve-feed` is given. The extractor gets its own claim buffer: the
/// engine's is consumed by the whole-block strategy (and never drained in
/// streaming mode), so the BAL pump feeds both, sharing the decoded index.
pub(crate) struct InteropServe {
    pub(crate) addr: std::net::SocketAddr,
    pub(crate) store: Arc<kardamom_validator::interop::FeedStore>,
    pub(crate) attestations: Arc<kardamom_validator::interop::AttestationStore>,
    pub(crate) claims: Arc<ClaimBuffer>,
}

fn open_interop_serve(
    args: &Args,
    chain_id: u64,
    resume_block: Option<u64>,
) -> Option<InteropServe> {
    args.serve_feed.map(|addr| InteropServe {
        addr,
        store: Arc::new(
            kardamom_validator::interop::FeedStore::new(chain_id, args.feed_retention_blocks)
                .with_resume_block(resume_block),
        ),
        attestations: Arc::new(kardamom_validator::interop::AttestationStore::new(
            args.feed_retention_blocks,
        )),
        claims: ClaimBuffer::new(),
    })
}

/// Every open subscription and side buffer: the M `tx_data` streams, the
/// cluster `tx_ordering` egress (with its session guard), and the
/// verification and interop buffers they feed. Everything
/// [`Opened::open_streams`] adds.
pub(crate) struct StreamsState {
    tx_data_subs: Vec<bin_support::LiveTxDataSub>,
    join_recovery: Option<kardamom_engine::reader::JoinRecoveryFactory>,
    cluster_guard: LiveCluster,
    tx_ordering_sub: bin_support::LiveTxOrderingSub,
    divergence: Arc<Divergence>,
    bals: Arc<BalBuffer>,
    claims: Arc<ClaimBuffer>,
    receipts: Arc<ReceiptBuffer>,
    interop_serve: Option<InteropServe>,
    pump_shutdown: tokio_util::sync::CancellationToken,
    flight: Arc<FlightRing>,
}

/// [`Opened`], plus everything [`Opened::open_streams`] resolved.
pub(crate) struct Streamed {
    opened: Opened,
    streams: StreamsState,
}

impl Streamed {
    /// Spawn the BAL and receipts verification pumps. Both run for the
    /// process lifetime, stopped only by `pump_shutdown` at shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if either subscription fails to open.
    pub(crate) fn spawn_pumps(self) -> Result<Self> {
        crate::pumps::spawn_bal_pump(
            &self.opened.base.rt,
            &self.opened.base.channels,
            self.streams.bals.clone(),
            self.streams.claims.clone(),
            self.streams
                .interop_serve
                .as_ref()
                .map(|s| s.claims.clone()),
            self.streams.pump_shutdown.clone(),
        )?;
        crate::pumps::spawn_receipts_pump(
            &self.opened.base.rt,
            &self.opened.base.channels,
            self.opened.base.args.executor_count,
            self.streams.receipts.clone(),
            self.streams.pump_shutdown.clone(),
        )?;
        Ok(self)
    }

    /// Seed genesis into a fresh env, then spawn the trie-aware writer:
    /// each block commit advances the MPT state root. Runs after
    /// [`crate::adoption::bootstrap_trie_if_adopted`], against the open
    /// env, before the writer spawns.
    ///
    /// # Errors
    ///
    /// Returns an error if seeding genesis, bootstrapping the trie, or
    /// spawning the writer fails.
    pub(crate) fn spawn_writer(self) -> Result<Written> {
        let args = &self.opened.base.args;
        let (genesis_accounts, genesis_code) =
            bin_support::build_genesis_alloc(self.opened.state.genesis.as_ref());
        let seeded =
            kardamom_state::seed_genesis(&self.opened.state.env, &genesis_accounts, &genesis_code)
                .context("seed genesis into validator state env")?;
        tracing::info!(
            state_dir = %args.state_dir.display(),
            genesis_accounts = genesis_accounts.len(),
            seeded,
            "validator state env opened"
        );

        let trie_mode = match args.trie_shadow_check {
            Some(every_n) => kardamom_state::TrieMode::ShadowCheck {
                every_n: every_n.get(),
            },
            None => kardamom_state::TrieMode::Incremental,
        };
        // Adoption, half two: rebuild the mirror and trie when the
        // adoption marker, or a truly trie-less image, says to. This must
        // run before the trie-aware writer spawns.
        adoption::bootstrap_trie_if_adopted(&args.state_dir, &self.opened.state.env)?;
        // `StateEnv` is `Arc`-backed and clones cheaply; cloning it out
        // here, instead of moving the field, keeps `self.opened` whole so
        // it can nest into `Written` as one field below.
        let writer = StateWriter::spawn_with_trie(self.opened.state.env.clone(), trie_mode)
            .context("spawn trie-aware state writer")?;
        let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
        let writer_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
        let writer_queue = ValidatorWriterQueue::new(
            MdbxWriterQueue::new(writer.delta_tx.clone()),
            self.streams.bals.clone(),
            self.streams.divergence.clone(),
        )
        // Blocks at or below the recovery resume point were verified
        // before the restart. Replay re-execution against already-applied
        // state gives empty deltas that cannot match the BAL. Comparing
        // them would report a false divergence on every restart.
        .with_verify_floor(self.opened.state.recovery.last_committed_block);

        Ok(Written {
            streamed: self,
            writer: WriterPorts {
                writer,
                snapshots,
                writer_signal,
                writer_queue,
            },
        })
    }
}

/// The trie-aware writer and its ports: everything [`Streamed::spawn_writer`]
/// adds.
pub(crate) struct WriterPorts {
    writer: kardamom_state::WriterHandle,
    snapshots: MdbxSnapshotSource,
    writer_signal: MdbxWriterSignal,
    writer_queue: ValidatorWriterQueue<MdbxWriterQueue>,
}

/// [`Streamed`], plus the trie-aware writer [`Streamed::spawn_writer`]
/// spawned.
pub(crate) struct Written {
    streamed: Streamed,
    writer: WriterPorts,
}

impl Written {
    /// Enable the L1 output attester when all three flags
    /// (`--l1-rpc-url`, `--output-oracle`, `--attester-key`) are present.
    /// Without all three, the validator does no automatic attestation. Any
    /// other combination is a configuration error. The task lives as long
    /// as a handle clone does; the handle is held for the process
    /// lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error if a partial set of the three flags is given, or
    /// if the attester fails to spawn (a bad key or a bad L1 RPC URL).
    pub(crate) fn spawn_attester(self) -> Result<Attested> {
        let args = &self.streamed.opened.base.args;
        let attester_handle = match (
            args.l1_rpc_url.clone(),
            args.output_oracle,
            args.attester_key.as_deref(),
        ) {
            (Some(url), Some(oracle), Some(key)) => {
                let attester::SpawnedAttester {
                    handle,
                    task: _task,
                } = attester::spawn_attester(&AttesterConfig {
                    l1_rpc_url: url.parse().context("parse --l1-rpc-url")?,
                    oracle,
                    signer: resolve_attester_key(key)?,
                    post_interval_blocks: args.attester_post_interval,
                });
                tracing::info!(
                    oracle = %oracle,
                    post_interval_blocks = args.attester_post_interval.get(),
                    "L1 output attester enabled"
                );
                Some(handle)
            }
            (None, None, None) => None, // Default: no automatic attestation.
            _ => anyhow::bail!(
                "attestation needs --l1-rpc-url, --output-oracle and \
                 --attester-key together (got a partial set)"
            ),
        };

        Ok(Attested {
            written: self,
            attester_handle,
        })
    }
}

/// [`Written`], plus the resolved (possibly absent) attester handle
/// [`Written::spawn_attester`] adds.
pub(crate) struct Attested {
    written: Written,
    attester_handle: Option<AttesterHandle>,
}

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
        let sink = ValidatorReceiptSink::new(streams.receipts.clone(), streams.divergence.clone())
            .with_flight(streams.flight.clone());
        let mut tx_receipts_pub: Box<dyn TxReceiptsPublication> =
            match self.attester_handle.as_ref() {
                Some(h) => Box::new(attester::AttestingReceiptSink::new(sink, h.clone())),
                None => Box::new(sink),
            };
        if let Some(s) = streams.interop_serve.as_ref() {
            tx_receipts_pub = Box::new(kardamom_validator::interop::ExtractingReceiptSink::new(
                tx_receipts_pub,
                self.written.streamed.opened.state.chain_id,
                s.claims.clone(),
                s.store.clone(),
                streams.divergence.clone(),
            ));
        }

        crate::pumps::spawn_commit_poller(
            self.written.writer.writer.snapshot_rx.clone(),
            self.attester_handle.clone(),
            streams
                .interop_serve
                .as_ref()
                .map(|s| s.attestations.clone()),
            streams.pump_shutdown.clone(),
        );

        Ready {
            attested: self,
            tx_receipts_pub,
        }
    }
}

/// [`Attested`], plus the `tx_receipts` sink [`Attested::build_sink`]
/// builds: every port the executor needs, fully wired and ready to run.
pub(crate) struct Ready {
    attested: Attested,
    tx_receipts_pub: Box<dyn TxReceiptsPublication>,
}

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

impl Ready {
    /// The executor config and the three role-specific `RoleHooks` ports:
    /// reads only `self`'s own fields, so it takes no argument list.
    ///
    /// # Errors
    ///
    /// Returns an error if `--l1-rpc-url` fails to parse for epoch
    /// verification.
    fn run_ports(&self) -> Result<RunPorts> {
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
            kardamom_validator::prover::spawn_prover_spool(dir, chain_id, snap_rx, flight);
        }

        // Epoch verification. Sequence rules 1-2 are local and always
        // enforced once an epoch appears. The content check needs L1, so
        // it is wired only when both the RPC URL and the lockbox address
        // are given.
        let epoch_observer =
            build_epoch_observer(args, divergence.clone(), &tokio::runtime::Handle::current())?;

        // Remote-epoch verification (interop): inline pair-sequence
        // checks on every RemoteEpochRecord, always on — they need no
        // transport, exactly like the L1 origin sequence rules.
        // Content-vs-origin verification is not checked here; see
        // `interop::verify`'s module docs.
        let remote_epoch_observer: Option<Box<dyn kardamom_engine::RemoteEpochObserver>> =
            Some(Box::new(
                kardamom_validator::interop::RemoteEpochVerifier::new(divergence),
            ));

        Ok(RunPorts {
            cfg,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
        })
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
            .start
            .is_resume()
            .then(|| self.attested.written.streamed.opened.state.start.block + 1);

        // Start the feed server (the interop serving surfaces). Dropping
        // the handle stops the server, so it is held for the process
        // lifetime.
        let _feed_server = start_interop_serving(
            args,
            chain_id,
            interop_serve,
            state_env_for_rpc,
            feed_resume_block,
        )
        .await?;

        let RunPorts {
            cfg,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
        } = self.run_ports()?;

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
                                            tx_data_subs,
                                            join_recovery,
                                            cluster_guard,
                                            tx_ordering_sub,
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
            Executor::run::<ValidatorWiring>(
                cfg,
                Inbound {
                    tx_data: tx_data_subs,
                    tx_ordering: tx_ordering_sub,
                    // Join-miss archive refetch (None on single-host/IPC
                    // runs).
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
                    // Whole-block exec strategy (the parallel-validation
                    // path).
                    block_exec,
                    epoch_observer,
                    remote_epoch_observer,
                },
            )
        });

        let engine_error = Shutdown {
            join,
            pumps: pump_shutdown,
            rt,
            cluster_guard,
            writer,
            divergence: divergence.clone(),
        }
        .wait()
        .await;
        finish(&engine_error, &divergence, &args, expected_genesis)
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
        retention_blocks = args.feed_retention_blocks,
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
) -> Option<kardamom_engine::actor::BlockExec<kardamom_state::StateSnapshot>> {
    if args.parallel_validation {
        // 0 means auto; hard cap per the mdbx reader-slot budget.
        let workers = match NonZeroUsize::new(args.validation_workers) {
            None => std::thread::available_parallelism()
                .map_or(FALLBACK_WORKERS, |n| n.min(AUTO_WORKER_CAP)),
            Some(n) => n.min(MAX_WORKERS),
        };
        tracing::info!(
            batch_size = args.validation_batch_size.get(),
            workers = workers.get(),
            "parallel validation ENABLED (seeded BAL batches on the shared pool)"
        );
        Some(kardamom_validator::parallel::parallel_block_exec(
            claims,
            args.validation_batch_size,
            workers,
            Some(flight),
        ))
    } else if args.prove_batches.is_some() {
        Some(kardamom_validator::prover::sequential_block_exec(flight))
    } else {
        None
    }
}

/// The executor config and the three role-specific `RoleHooks` ports:
/// the whole-block exec strategy, the epoch observer, and the
/// remote-epoch observer.
struct RunPorts {
    cfg: kardamom_engine::ExecutorConfig,
    block_exec: Option<kardamom_engine::actor::BlockExec<kardamom_state::StateSnapshot>>,
    epoch_observer: Option<epoch_verify::EpochVerifier>,
    remote_epoch_observer: Option<Box<dyn kardamom_engine::RemoteEpochObserver>>,
}

/// Build the epoch observer: epochs are re-derived from L1 when both
/// `--l1-rpc-url` and `--lockbox` are given. Without both, only the local
/// origin sequence rules apply; there is no content check.
fn build_epoch_observer(
    args: &Args,
    divergence: Arc<Divergence>,
    rt: &tokio::runtime::Handle,
) -> Result<Option<epoch_verify::EpochVerifier>> {
    let (Some(url), Some(lockbox)) = (args.l1_rpc_url.as_deref(), args.lockbox) else {
        tracing::info!(
            "epoch CONTENT verification disabled (needs --l1-rpc-url and \
             --lockbox); origin sequence rules still apply"
        );
        return Ok(None);
    };
    let provider = alloy_provider::ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(url.parse().context("parse --l1-rpc-url")?);
    let source = Arc::new(kardamom_da_watcher::RpcL1Source::new(provider));
    tracing::info!(
        %lockbox,
        "epoch verification enabled: epochs are re-derived from L1"
    );
    Ok(Some(epoch_verify::EpochVerifier::spawn(
        source, lockbox, divergence, rt,
    )))
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
    async fn wait(mut self) -> EngineOutcome {
        // Exit on whichever comes first: an operator shutdown signal, or
        // the engine loop finishing on its own (a divergence stop or a
        // stream error). Waiting only for SIGTERM would leave a halted
        // validator looking "alive", with metrics up and the chain
        // frozen, hiding the very stop signal the divergence machinery
        // exists to surface.
        let engine_result = tokio::select! {
            () = bin_support::wait_for_shutdown() => {
                tracing::info!("kardamom-validator: shutdown signal received; dropping runtime");
                None
            }
            res = &mut self.join => Some(res),
        };
        // Order matters: cancel the pumps first so the tx_bal pump
        // releases its AeronRuntime clone, then release the runtime and
        // the cluster session.
        self.pumps.cancel();
        stop_streams(self.cluster_guard, self.rt);
        let joined = match engine_result {
            Some(r) => r,
            None => self.join.await,
        };
        let engine_error = classify_engine_result(joined, &self.divergence);
        if let Err(e) = self.writer.shutdown() {
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

/// Release the cluster session and the Aeron runtime. `rt`, bound last,
/// drops first at this function's scope end, then `cluster_guard`: the
/// order the caller's cancelled pumps expect.
fn stop_streams(cluster_guard: LiveCluster, rt: AeronRuntime) {
    let _held_cluster_guard = cluster_guard;
    let _held_rt = rt;
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
    adoption::resync_after_engine_error(
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
