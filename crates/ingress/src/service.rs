//! The ingress service: the proxy with its Aeron side, as one process
//! runs it.
//!
//! [`IngressService`] owns everything the start needs: the parsed
//! [`IngressSettings`], the Aeron directory, the resolved log config, and
//! the parsed TOML file config. [`IngressService::run`] opens Aeron, the
//! `tx_data` recorders, the optional cluster watermark watcher, and the
//! proxy itself. It returns a [`RunningIngress`] that owns everything the
//! stop needs.
//!
//! The `kardamom-ingress` binary runs one service. A sequencer process can
//! run one inside itself.

mod recorders;
mod settings;
mod watermark;

use std::num::{NonZeroU8, NonZeroU32};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_cache::{ExecutorQueryConfig, LiveAccountsConfig};
use kardamom_log::aeron_live::{
    AeronRuntime, ServiceEventsPublisherHandle, TxStatusPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_log::recorder::RecorderThreads;
use kardamom_obs::events::{Beacon, Identity};
use kardamom_types::shard_map::{LANE_COUNT, ShardMap, validate_shard_count};

use crate::aeron_adapters::{LiveIngressPublication, LiveIngressSubscription};
use crate::cluster::cluster_watermark_observer;
use crate::config::{AdmissionAck, IngressConfig, IngressFileConfig};
use crate::proxy::{IngressHandle, IngressProxy};
use crate::stage_latency::{StageClock, StageClocks, StageStamps};
use recorders::{
    spawn_discovered_tx_data_recorder, spawn_tx_data_recorders, wait_for_discovered_recorder,
    wait_for_recorders,
};
pub use settings::IngressSettings;

/// What [`IngressService::spawn_recorders`] returns: the threads, the
/// per-lane readiness receivers of the static recorders, and the one
/// receiver of the discovered recorder.
type SpawnedRecorders = (
    RecorderThreads,
    Vec<recorders::RecorderReady>,
    Option<tokio::sync::oneshot::Receiver<kardamom_log::discovery::RecorderProgress>>,
);

/// The lane plane as the non-zero count the publisher and recorder
/// openers take: every lane opens, whatever the active shard count.
const LANE_PLANE: NonZeroU8 = NonZeroU8::new(LANE_COUNT).unwrap();

/// Everything opened on the way to a running proxy: the Aeron runtime,
/// the `tx_data` archive recorder threads, and the `tx_data` publication
/// and `tx_receipts` subscription built on top of it.
struct OpenedAeron {
    rt: AeronRuntime,
    plane: StreamPlane,
    recorders: RecorderThreads,
    publication: LiveIngressPublication,
    subscription: LiveIngressSubscription,
}

/// Owns everything needed to open the ingress runtime. The process builds
/// one of these, then calls [`Self::run`].
pub struct IngressService {
    /// The flags of the service.
    pub settings: IngressSettings,
    /// Aeron Media Driver directory (`aeron.dir`). `None` uses the
    /// default directory of the Aeron client.
    pub aeron_dir: Option<PathBuf>,
    /// The resolved Aeron and channels config.
    pub log_cfg: LogConfig,
    /// The `[cluster]` and `[cache]` sections. The
    /// `--cluster-egress-endpoint` flag replaces the egress channel. An
    /// empty egress channel starts no cluster egress tap.
    pub file_cfg: IngressFileConfig,
}

impl IngressService {
    /// Builds the runtime `IngressConfig` from the settings. The v0
    /// deployment keeps the binary protocol off; an operator who wants it
    /// enabled can set the binds in a follow-up that drives the config
    /// from TOML.
    fn build_config(&self) -> Result<IngressConfig> {
        let args = &self.settings;
        validate_shard_count(u32::from(args.shards.get())).context("--shards")?;
        if args.verdicts && args.admission_checks {
            anyhow::bail!(
                "--verdicts needs --admission-checks false: the v2 sequencers check each sender"
            );
        }
        let mut cfg = IngressConfig {
            jsonrpc_bind: args.jsonrpc_bind,
            partition_count_m: NonZeroU32::from(args.shards),
            shard_map: self.load_shard_map()?,
            ingress_id: args.ingress_id,
            ack_policy: args.ack_policy.clone().into(),
            rpc_max_connections: args.rpc_max_connections,
            chain_id: args.chain_id,
            pending_receipt_timeout: Duration::from_millis(args.pending_receipt_timeout_ms),
            live_accounts: self.live_accounts_config(),
            admission_checks: args.admission_checks,
            executor_query: ExecutorQueryConfig {
                endpoints: args.executor_query_endpoints.clone(),
                ..ExecutorQueryConfig::default()
            },
            receipt_query: ExecutorQueryConfig {
                endpoints: args.receipt_query_endpoints.clone(),
                ..ExecutorQueryConfig::default()
            },
            cache: self.file_cfg.cache.clone(),
            // The mirror ids are the executor indexes. One when no count
            // is known: the Redis reader then polls `head:0` only.
            mirror_count: self.executor_count().unwrap_or(NonZeroU32::MIN),
            inclusion_horizon_blocks: args.inclusion_horizon_blocks,
            max_tx_bytes: args.max_tx_bytes,
            admission_ack: args.verdicts.then(|| AdmissionAck {
                timeout: Duration::from_millis(args.ack_timeout_ms),
                twins: args.twins,
                ..AdmissionAck::default()
            }),
            ..IngressConfig::default()
        };
        cfg.binary_tcp_bind = None;
        cfg.binary_uds_path = None;
        Ok(cfg)
    }

    /// The executor count: the CLI or env `--executor-count`, else the
    /// log config's `tx_receipts_executor_count`, else unknown.
    fn executor_count(&self) -> Option<NonZeroU32> {
        self.settings
            .executor_count
            .or(self.log_cfg.channels.tx_receipts_executor_count)
    }

    /// The local layer config: the defaults, with the two CLI overrides.
    fn live_accounts_config(&self) -> LiveAccountsConfig {
        let defaults = LiveAccountsConfig::default();
        LiveAccountsConfig {
            capacity: self
                .settings
                .live_accounts_capacity
                .unwrap_or(defaults.capacity),
            ttl_ms: self
                .settings
                .live_accounts_ttl_ms
                .unwrap_or(defaults.ttl_ms),
        }
    }

    /// Read and validate the `--shard-map` file, when one is given.
    fn load_shard_map(&self) -> Result<Option<ShardMap>> {
        let Some(path) = self.settings.shard_map.as_deref() else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read shard map {}", path.display()))?;
        let map: ShardMap =
            toml::from_str(&text).with_context(|| format!("parse shard map {}", path.display()))?;
        Ok(Some(map))
    }

    /// The recorder threads, when archive durability is on. With
    /// discovery, one thread follows every `tx_data` publisher the
    /// catalog lists. Without it, one static thread per lane records the
    /// shared channel. Returns the threads and the readiness receivers of
    /// the path taken: per lane for the static recorders, or one for the
    /// discovered recorder.
    fn spawn_recorders(&self, plane: &mut StreamPlane) -> Result<SpawnedRecorders> {
        if !self.settings.archive_durability {
            return Ok((RecorderThreads::new(), Vec::new(), None));
        }
        let discovered = spawn_discovered_tx_data_recorder(
            self.aeron_dir.as_deref(),
            &self.log_cfg.aeron,
            plane,
            LANE_PLANE,
        )?;
        if let Some((recorders, ready)) = discovered {
            return Ok((recorders, Vec::new(), Some(ready)));
        }
        let (recorders, ready) = spawn_tx_data_recorders(
            self.aeron_dir.as_deref(),
            &self.log_cfg.channels,
            &self.log_cfg.aeron,
            LANE_PLANE,
        )?;
        Ok((recorders, ready, None))
    }

    /// Opens the Aeron runtime, the `tx_data` archive recorders, and the
    /// `tx_data` publication and `tx_receipts` subscription.
    ///
    /// These are archive recorders for `tx_data`, one per lane, co-located
    /// with the publishers. An idle lane records an empty stream. They
    /// make the full transaction envelopes durable, so the executor can
    /// replay them on crash recovery. Without them, only the canonical
    /// order survives a restart, not the bytes needed to re-execute.
    ///
    /// Each recorder reports its startup outcome on its own `oneshot`. This
    /// blocks on all of them, after the `tx_data` publications open and before
    /// it returns, so no transaction can be accepted before its shard's
    /// recording is active. Recovery replays from record 0 and needs every
    /// envelope, so a gap at the start of the stream would permanently break
    /// executor crash recovery. A recorder startup failure is fatal: the
    /// operator asked for `--archive-durability`, so serving without it
    /// would be a silent lie.
    async fn open_aeron_side(
        &self,
        live_cfg: &LiveAccountsConfig,
        stamps: StageStamps,
        executed: StageClock,
    ) -> Result<OpenedAeron> {
        let args = &self.settings;
        let rt = AeronRuntime::spawn(self.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        let mut plane =
            StreamPlane::from_config(&self.log_cfg, &format!("ingress-{}", args.ingress_id))
                .context("build the stream plane")?;

        let (recorders, recorder_ready, discovered_ready) = self.spawn_recorders(&mut plane)?;

        // tx_receipts MDS membership: the CLI or env `--executor-count`,
        // else the log-config field, else `None` (no known executor
        // count). The proxy reads this only when MDS is enabled.
        let executor_count = self.executor_count();

        let publication = LiveIngressPublication::open(&rt, &mut plane, LANE_PLANE, stamps)
            .await
            .context("open IngressPublication")?;

        // This is the recorder barrier: with the tx_data publications now
        // open, every lane's recording can start.
        if let Some(ready) = discovered_ready {
            wait_for_discovered_recorder(ready)
                .await
                .context("archive durability requested but the tx_data recorder failed to start")?;
        } else if args.archive_durability {
            wait_for_recorders(recorder_ready)
                .await
                .context("archive durability requested but tx_data recorders failed to start")?;
        }
        let verdicts = args
            .verdicts
            .then(|| u8::try_from(args.ingress_id))
            .transpose()
            .context("--verdicts needs an --ingress-id of at most 255")?;
        let subscription = LiveIngressSubscription::open(
            &rt,
            &mut plane,
            args.recorder_id,
            executor_count,
            live_cfg,
            verdicts,
            executed,
        )
        .context("open IngressSubscription")?;

        Ok(OpenedAeron {
            rt,
            plane,
            recorders,
            publication,
            subscription,
        })
    }

    /// Connects to the cluster as a client and folds its egress progress
    /// into the proxy's on-quorum watermark bus. No standalone sealer
    /// publishes the durable watermark in the cluster-only topology, so this
    /// is the source of it: a record or boundary on egress is a
    /// Raft-quorum-durability signal. The same session publishes the
    /// `Sealed` status of every relayed transaction on `status`.
    ///
    /// Returns the running thread's handle. Dropping it stops the thread
    /// and ends the cluster session.
    fn spawn_cluster_watermark(
        &self,
        subscription: &LiveIngressSubscription,
        ingress_endpoints: Option<String>,
        status: TxStatusPublisherHandle,
        seal_clock: StageClock,
    ) -> Result<watermark::ClusterWatermark> {
        let mut live = self.file_cfg.cluster.to_live();
        if let Some(endpoints) = ingress_endpoints {
            live.ingress_endpoints = endpoints;
        }
        // This is a dedicated cluster runtime, with its own Aeron thread and
        // the same aeron dir, so the cluster session never contends with the
        // tx_data publish and receipts work.
        let cluster_rt =
            AeronRuntime::spawn(self.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
        let (guard, observer) =
            cluster_watermark_observer(cluster_rt, live).context("connect cluster watermark")?;
        // A dedicated std thread runs a blocking egress poll, since the
        // observer holds the `!Send` cluster client. It sends the durable
        // count to the bus. The thread stops when the returned handle
        // drops, or when the observer ends. The bus is a tokio `broadcast`
        // channel, so the send never blocks. A send with no live receiver
        // is not an error here.
        let running = watermark::ClusterWatermarkPump::new(
            observer,
            seal_clock,
            subscription.watermark_sender(),
            subscription.cluster_status_sender(),
            status,
        )
        .spawn(guard)
        .context("spawn cluster watermark thread")?;
        tracing::info!(
            "kardamom-ingress: on-quorum watermark and tx_status Sealed via Aeron Cluster egress"
        );
        Ok(running)
    }

    /// Publish this ingress's lifecycle and the sealer's, as this ingress
    /// observes it, on the `events` stream.
    async fn spawn_beacons(
        plane: &mut StreamPlane,
        rt: &AeronRuntime,
        proxy: &IngressProxy<LiveIngressPublication, LiveIngressSubscription>,
    ) -> Result<()> {
        let events: ServiceEventsPublisherHandle =
            plane.publisher(rt).await.context("open events")?;
        events.spawn_process_beacon();
        let sealer = Beacon::new(
            Identity {
                service: crate::chain::SEALER.to_string(),
                instance: crate::chain::SEALER_INSTANCE.to_string(),
            },
            proxy.sealer().subscribe(),
        );
        events.spawn_beacon(sealer);
        Ok(())
    }

    /// Open the status publisher and start the egress tap on `opened`.
    async fn spawn_egress_tap(
        &self,
        opened: &mut OpenedAeron,
        seal_clock: StageClock,
    ) -> Result<watermark::ClusterWatermark> {
        let members = opened.plane.cluster_ingress_endpoints().await?;
        let status = opened
            .plane
            .publisher::<TxStatusPublisherHandle>(&opened.rt)
            .await
            .context("open TxStatusPublisherHandle")?;
        self.spawn_cluster_watermark(&opened.subscription, members, status, seal_clock)
    }

    /// Whether the CLI or the config file names a cluster egress channel.
    /// The CLI endpoint is in the config from the start of [`Self::run`].
    fn has_egress_channel(&self) -> bool {
        !self.file_cfg.cluster.egress_channel.is_empty()
    }

    /// Builds the config, opens Aeron, starts the cluster egress tap when
    /// an egress channel is configured, and starts the proxy's listeners. Returns a [`RunningIngress`] that
    /// owns everything needed to shut back down.
    ///
    /// # Errors
    ///
    /// Returns the first failure of the start: an invalid setting, an
    /// Aeron open, a recorder start, the egress tap under a quorum policy,
    /// or a listener bind.
    pub async fn run(mut self) -> Result<RunningIngress> {
        self.file_cfg
            .cluster
            .set_egress_endpoint(self.settings.cluster_egress_endpoint.as_deref());
        let cfg = self.build_config()?;
        let map_version = cfg.shard_map.as_ref().map(ShardMap::version);
        metrics::gauge!(crate::metrics::SHARD_MAP_VERSION).set(f64::from(map_version.unwrap_or(0)));
        tracing::info!(
            jsonrpc_bind = %cfg.jsonrpc_bind,
            shards = cfg.partition_count_m.get(),
            lanes = LANE_COUNT,
            shard_map_version = ?map_version,
            active_lanes = cfg.shard_map.as_ref().map(ShardMap::active_lanes),
            ingress_id = cfg.ingress_id,
            ack_policy = ?cfg.ack_policy,
            "kardamom-ingress starting"
        );

        // The publication stamps each transaction. The egress tap records
        // its seal latency, and the receipt pump its execute latency.
        // Without a tap nothing reads the sealed stamps: the full or closed
        // channel drops them.
        let (
            stamps,
            StageClocks {
                sealed: seal_clock,
                executed,
            },
        ) = StageClock::pair();
        let mut opened = self
            .open_aeron_side(&cfg.live_accounts, stamps, executed)
            .await?;
        // The egress tap is the `Sealed` publisher of the status stream,
        // and under a quorum policy also the source of the durable
        // watermark. It runs whenever an egress channel is configured. A
        // quorum policy needs it, so a failure to connect is fatal there;
        // under any other policy the ingress serves without it, so a
        // cluster that is down at start never keeps the front door shut.
        let cluster_watermark = if cfg.ack_policy.requires_quorum() {
            Some(self.spawn_egress_tap(&mut opened, seal_clock).await?)
        } else if self.has_egress_channel() {
            self.spawn_egress_tap(&mut opened, seal_clock)
                .await
                .inspect_err(|e| {
                    tracing::warn!(error = %e, "kardamom-ingress: cluster egress tap not started; tx_status Sealed is not published");
                })
                .ok()
        } else {
            tracing::warn!(
                "kardamom-ingress: no cluster egress channel; tx_status Sealed is not published"
            );
            None
        };

        let drain_timeout = cfg.pending_receipt_timeout;
        let proxy = IngressProxy::new(cfg, opened.publication, opened.subscription);
        Self::spawn_beacons(&mut opened.plane, &opened.rt, &proxy).await?;
        let drainer = proxy.clone();
        let handle = proxy.start().await.context("IngressProxy::start")?;
        tracing::info!(jsonrpc_addr = %handle.jsonrpc_addr, "JSON-RPC listening");

        Ok(RunningIngress {
            handle,
            drainer,
            drain_timeout,
            plane: opened.plane,
            _recorders: opened.recorders,
            _rt: opened.rt,
            _cluster_watermark: cluster_watermark,
        })
    }
}

/// A started ingress: the JSON-RPC handle, the recorder threads, the
/// Aeron runtime, and the optional cluster watermark thread.
/// [`Self::stop`] drains, and then lets `self` drop. Field order is drop
/// order: the recorders stop and join first, then the Aeron runtime ends,
/// then the watermark thread's stop token cancels and its cluster session
/// ends.
pub struct RunningIngress {
    handle: IngressHandle,
    /// A proxy clone for the graceful drain at shutdown.
    drainer: IngressProxy<LiveIngressPublication, LiveIngressSubscription>,
    /// How long the drain waits for parked submits: the park bound.
    drain_timeout: Duration,
    /// The stream plane. [`Self::stop`] ends it before `self` drops, so
    /// the discovery tasks and registrations end before the runtime.
    plane: StreamPlane,
    /// Held only for its `Drop` side effect (stopping and joining the
    /// recorder threads); never read. The join is short: a recorder past
    /// startup wakes at once from its park on the stop token.
    _recorders: RecorderThreads,
    /// Held only for its `Drop` side effect (closing the Aeron runtime);
    /// the leading underscore marks it never read, not that it is unused.
    /// Dropping it is the point.
    _rt: AeronRuntime,
    /// Held only for its `Drop` side effect; never read.
    _cluster_watermark: Option<watermark::ClusterWatermark>,
}

impl RunningIngress {
    /// Drains the parked submits and stops the JSON-RPC server. `self` is
    /// consumed here, so the recorders, the runtime, and the watermark
    /// thread end at the end of this call, in field declaration order.
    pub async fn stop(self) {
        // The graceful drain: refuse new submits, let the parked ones finish
        // within the park bound, then stop the server. The kill timeout of
        // the Nomad job covers this wait.
        self.drainer.begin_drain();
        let still_parked = self.drainer.drain(self.drain_timeout).await;
        tracing::info!(still_parked, "kardamom-ingress: drain finished");
        self.handle.jsonrpc_handle.stop().ok();
        self.handle.jsonrpc_handle.stopped().await;
        self.plane.shutdown().await;
    }
}
