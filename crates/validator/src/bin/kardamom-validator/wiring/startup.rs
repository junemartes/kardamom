//! The opening chain: tracing/config, the state env and crash recovery,
//! then every subscription the validator reads.

use std::sync::Arc;

use anyhow::{Context, Result};
use kardamom_cluster_adapter::LiveCluster;
use kardamom_engine::ResumePoint;
use kardamom_engine::bin_support;
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_log::discovery::StreamPlane;
use kardamom_state::{StateEnv, StateEnvBuilder, read_recovery_point};
use kardamom_validator::flight::FlightRing;
use kardamom_validator::{BalBuffer, ClaimBuffer, Divergence, ReceiptBuffer};

use crate::args::{Args, ValidatorFileConfig};

/// Started tracing and metrics, the resolved file config, and the main
/// Aeron runtime. The first phase: nothing has opened state or streams yet.
pub(crate) struct Startup {
    pub(super) args: Args,
    pub(super) file_cfg: ValidatorFileConfig,
    pub(super) channels: ChannelsConfig,
    pub(super) aeron_cfg: AeronConfig,
    pub(super) rt: AeronRuntime,
    /// The stream plane the verification subscriptions open through.
    pub(super) plane: StreamPlane,
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
            lanes = kardamom_types::shard_map::LANE_COUNT,
            chain_id = args.chain_id.get(),
            "kardamom-validator starting"
        );

        let log_cfg =
            LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
        let plane =
            StreamPlane::from_config(&log_cfg, "validator").context("build the stream plane")?;
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
            plane,
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
        crate::adoption::adopt_checkpoint_if_fresh(
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
    pub(super) genesis: Option<kardamom_types::Genesis>,
    pub(super) chain_id: std::num::NonZeroU64,
    pub(super) expected_genesis: Option<alloy_primitives::B256>,
    pub(super) env: StateEnv,
    pub(super) recovery: kardamom_state::recovery::RecoveryPoint,
    pub(super) start: ResumePoint,
}

impl OpenedState {
    /// The interop feed's resume floor: the block after this run's start
    /// point, on a resumed run; `None` on a fresh genesis start.
    pub(super) fn feed_resume_block(&self) -> Result<Option<u64>> {
        if !self.start.is_resume() {
            return Ok(None);
        }
        let block = self
            .start
            .block
            .checked_add(1)
            .context("resume block overflow computing the interop feed floor")?;
        Ok(Some(block))
    }
}

/// [`Startup`], plus everything [`Startup::open_state`] resolved.
pub(crate) struct Opened {
    pub(super) base: Startup,
    pub(super) state: OpenedState,
}

impl Opened {
    /// Open every subscription the validator reads: the M `tx_data`
    /// streams plus `tx_deposits` (identical to the executor), the one
    /// cluster (Raft) `tx_ordering` egress, and the verification and
    /// interop side buffers those streams feed.
    ///
    /// The cluster-session guard (`LiveCluster`) and its dedicated Aeron
    /// runtime must outlive the validator loop; both are carried forward
    /// as fields so they drop only at [`run::Ready::run`]'s shutdown, in
    /// the order [`kardamom_engine::bin_support::EngineShutdown::wait`]
    /// documents. Fresh validators
    /// start at genesis and receive the full retained canonical stream.
    /// The replay request is re-sent on every session start, so a
    /// validator whose session dies mid-chaos catches back up instead of
    /// stopping on an unrecoverable gap.
    ///
    /// # Errors
    ///
    /// Returns an error if any subscription fails to open, the cluster
    /// session fails to connect, or the resume block is `u64::MAX` (so
    /// naming the next block would overflow).
    pub(crate) fn open_streams(self) -> Result<Streamed> {
        let args = &self.base.args;
        // The kardamom_sealer_* re-export is the executor's job. A
        // validator emitting a second, lagging copy of the series would
        // break sum()-style queries and contradict the documented
        // observation point, so `open_inbound` suppresses it here.
        let (inbound, cluster_guard) =
            bin_support::open_inbound::<super::run::ValidatorWiring>(bin_support::InboundConfig {
                rt: &self.base.rt,
                channels: &self.base.channels,
                aeron_cfg: &self.base.aeron_cfg,
                aeron_dir: args.aeron_dir.as_deref(),
                archive_control_response_endpoint: args
                    .archive_control_response_endpoint
                    .as_deref(),
                replay_destination_endpoint: args.replay_destination_endpoint.as_deref(),
                cluster_cfg: self.base.file_cfg.cluster.to_live(),
                cursor: bin_support::cluster_replay_cursor(&self.state.start),
                bin_name: "kardamom-validator",
                suppress_sealer_metrics: true,
            })?;

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
        let feed_resume_block = self.state.feed_resume_block()?;
        let interop_serve = self.open_interop_serve(feed_resume_block);

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
                inbound,
                cluster_guard,
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

impl Opened {
    /// The interop serving surfaces (outbox and attestation feeds), when
    /// `--serve-feed` is given. `resume_block` seeds the feed store's
    /// floor for a resumed run; `None` starts it at genesis.
    fn open_interop_serve(&self, resume_block: Option<u64>) -> Option<InteropServe> {
        let args = &self.base.args;
        args.serve_feed.map(|addr| InteropServe {
            addr,
            store: Arc::new(
                kardamom_validator::interop::FeedStore::new(
                    self.state.chain_id.get(),
                    args.feed_retention_blocks,
                )
                .with_resume_block(resume_block),
            ),
            attestations: Arc::new(kardamom_validator::interop::AttestationStore::new(
                args.feed_retention_blocks,
            )),
            claims: ClaimBuffer::new(),
        })
    }
}

/// Every open subscription and side buffer: the M `tx_data` streams, the
/// cluster `tx_ordering` egress (with its session guard), and the
/// verification and interop buffers they feed. Everything
/// [`Opened::open_streams`] adds.
pub(crate) struct StreamsState {
    pub(super) inbound: kardamom_engine::Inbound<super::run::ValidatorWiring>,
    pub(super) cluster_guard: LiveCluster,
    pub(super) divergence: Arc<Divergence>,
    pub(super) bals: Arc<BalBuffer>,
    pub(super) claims: Arc<ClaimBuffer>,
    pub(super) receipts: Arc<ReceiptBuffer>,
    pub(super) interop_serve: Option<InteropServe>,
    pub(super) pump_shutdown: tokio_util::sync::CancellationToken,
    pub(super) flight: Arc<FlightRing>,
}

/// [`Opened`], plus everything [`Opened::open_streams`] resolved.
pub(crate) struct Streamed {
    pub(super) opened: Opened,
    pub(super) streams: StreamsState,
}
