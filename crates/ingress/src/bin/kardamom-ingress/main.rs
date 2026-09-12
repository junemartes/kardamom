//! `kardamom-ingress`: the standalone proxy (ingress) service process.
//!
//! This opens one `tx_data` publisher per lane, a receipt-cache publisher, and
//! subscribers for the receipts, quorum-watermark, fsync-watermark,
//! receipt-cache, and block-boundary streams. It wires them into an
//! [`IngressProxy`] and starts its JSON-RPC server, plus optional TCP and
//! UDS binary protocol listeners. It idles on SIGTERM or Ctrl-C.
//!
//! The live Aeron adapters behind the proxy's channel traits live in
//! `kardamom_ingress::aeron_adapters`. The `tx_data` archive-recorder
//! threads, and their ready barrier, live in [`recorders`].
//!
//! [`IngressService`] owns everything `main` needs to open the runtime:
//! the parsed args, the resolved log config, and the parsed TOML file
//! config. [`IngressService::run`] opens Aeron, the recorders, the
//! optional cluster watermark watcher, and the proxy itself, and returns
//! a [`RunningIngress`] that owns everything needed to shut back down.

mod recorders;

use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_cluster_adapter::LiveCluster;
use kardamom_ingress::aeron_adapters::{LiveIngressPublication, LiveIngressSubscription};
use kardamom_ingress::cluster::cluster_watermark_observer;
use kardamom_ingress::config::{IngressConfig, IngressFileConfig};
use kardamom_ingress::proxy::{IngressHandle, IngressProxy};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_types::QuorumWatermark;
use tokio_util::sync::CancellationToken;

use kardamom_types::shard_map::{LANE_COUNT, ShardMap, validate_shard_count};
use recorders::{spawn_tx_data_recorders, wait_for_recorders};

/// The lane plane as the non-zero count the publisher and recorder
/// openers take: every lane opens, whatever the active shard count.
const LANE_PLANE: NonZeroU8 = NonZeroU8::new(LANE_COUNT).unwrap();

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-ingress",
    version,
    about = "kardamom ingress (proxy) process"
)]
struct Args {
    /// Path to the TOML config file (schema: `IngressConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional path to a `LogConfig` TOML file. It supplies the Aeron
    /// `[channels]` config, and the `[aeron]` and `[quorum]` config. When
    /// unset, the built-in single-host IPC defaults apply, which keeps
    /// local and e2e behavior the same. A multi-host deployment points
    /// this at the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// JSON-RPC bind address. Defaults to 127.0.0.1:8545.
    #[arg(long, default_value = "127.0.0.1:8545")]
    jsonrpc_bind: SocketAddr,
    /// Recorder id for the local fsync watermark subscription. Used when
    /// `ack_policy` includes a local-fsync gate. Defaults to 0.
    #[arg(long, default_value_t = 0)]
    recorder_id: u8,
    /// Number of executor replicas to attach to the `tx_receipts` MDS
    /// (fan-in) subscription at startup. Used only when MDS is enabled,
    /// with `tx_receipts_control_channel` set. When unset on the CLI,
    /// this falls back to `channels.tx_receipts_executor_count` from the
    /// log config.
    ///
    /// Static-membership fallback: ingress attaches replicas `0..N` once
    /// at startup. Membership is static for the life of the process.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<NonZeroU32>,
    /// The active shard count (M). The ingress opens every lane of the
    /// lane plane, and the identity map routes senders to the first M.
    /// So M must be 1, 2, 4, or 8. Defaults to 8. `--shard-map` replaces
    /// the identity map. Zero is rejected at parse time.
    #[arg(long, default_value = "8")]
    shards: NonZeroU8,
    /// A TOML file with the versioned vslot-to-lane map: `version = N`
    /// and `table = [256 lanes]`. It replaces the identity map. A resize
    /// re-renders this file and restarts the ingress. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.2.
    #[arg(long, env = "KARDAMOM_SHARD_MAP")]
    shard_map: Option<PathBuf>,
    /// Records each per-lane `tx_data` publication to the Aeron Archive,
    /// so the executor can replay full transaction envelopes on crash
    /// recovery, through `kardamom_log::replay`. Off by default, since
    /// single-host IPC has no archive. The cluster sets this on the node
    /// where the `ArchivingMediaDriver` runs.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Durability gate before acking a submit. Mirrors `AckPolicy`:
    ///   - `on-offer`: releases as soon as the receipt arrives. Lowest
    ///     latency, weakest guarantee.
    ///   - `on-local-fsync`: waits for this node's recorder fsync
    ///     watermark.
    ///   - `on-quorum`: waits for Q of N recorders to fsync.
    ///   - `on-local-fsync-and-quorum`: waits for both.
    ///
    /// Defaults to `on-offer`, because no process in the deployed
    /// topology runs a `QuorumAggregator` yet: nothing publishes the
    /// quorum watermark, so a quorum-gated policy would park every submit
    /// forever.
    #[arg(long, default_value = "on-offer")]
    ack_policy: AckPolicyArg,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9006")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: kardamom_obs::HostId,
    /// The stable identity of this ingress replica. An active/active
    /// deployment runs N replicas. This id namespaces `correlation_id`,
    /// so `(replica, sequence)` stays unique, and it is stamped as a
    /// metric label. Defaults to 0.
    #[arg(long, env = "KARDAMOM_INGRESS_ID", default_value_t = 0)]
    ingress_id: u16,
    /// This node's cluster-egress endpoint, `ip:port`, in cluster mode.
    /// Overrides `[cluster] egress_channel` as
    /// `aeron:udp?endpoint=<ip:port>`, the cluster client's per-node
    /// response channel. The Nomad job injects this per node. The proxy
    /// reads it only when the ack policy requires the quorum gate.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Max concurrent JSON-RPC connections. A submission parks its
    /// connection until the receipt arrives, so this value must
    /// comfortably exceed the offered rate times the receipt latency. See
    /// `IngressConfig::rpc_max_connections`.
    #[arg(
        long = "rpc-max-connections",
        env = "KARDAMOM_RPC_MAX_CONNECTIONS",
        default_value_t = 8192
    )]
    rpc_max_connections: u32,
    /// L2 chain id returned by `eth_chainId`. This is purely informational
    /// for clients, since the ingress recovers senders from the tx's own
    /// EIP-155 signature. But tooling that queries `eth_chainId` before
    /// signing needs this value to match the executor's `--chain-id`.
    /// EIP-155 forbids chain id 0, so this is rejected at parse time.
    #[arg(long, env = "KARDAMOM_CHAIN_ID", default_value = "1")]
    chain_id: NonZeroU64,
    /// Max time, in milliseconds, that a submit parks waiting for its
    /// receipt and ack gate before the client gets a `-32000` timeout.
    /// This bounds every `eth_sendRawTransaction` call. A nonce-gap tx
    /// that never becomes executable shows up as exactly this timeout.
    #[arg(
        long = "pending-receipt-timeout-ms",
        env = "KARDAMOM_PENDING_RECEIPT_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pending_receipt_timeout_ms: u64,
}

#[derive(Clone, Debug, clap::ValueEnum)]
#[allow(clippy::enum_variant_names)] // Mirrors `types::AckPolicy` one to one.
enum AckPolicyArg {
    OnOffer,
    OnLocalFsync,
    OnQuorum,
    OnLocalFsyncAndQuorum,
}

impl From<AckPolicyArg> for kardamom_types::AckPolicy {
    fn from(a: AckPolicyArg) -> Self {
        match a {
            AckPolicyArg::OnOffer => Self::OnOffer,
            AckPolicyArg::OnLocalFsync => Self::OnLocalFsync,
            AckPolicyArg::OnQuorum => Self::OnQuorum,
            AckPolicyArg::OnLocalFsyncAndQuorum => Self::OnLocalFsyncAndQuorum,
        }
    }
}

/// Everything opened on the way to a running proxy: the Aeron runtime,
/// the `tx_data` archive recorder threads, and the `tx_data` publication
/// and `tx_receipts` subscription built on top of it.
struct OpenedAeron {
    rt: AeronRuntime,
    plane: StreamPlane,
    recorder_handles: Vec<std::thread::JoinHandle<()>>,
    publication: LiveIngressPublication,
    subscription: LiveIngressSubscription,
}

/// Owns everything needed to open the ingress runtime: the parsed CLI
/// args, the resolved Aeron/channels config, and the parsed cluster TOML
/// file config. `main` builds one of these, then calls [`Self::run`].
struct IngressService {
    args: Args,
    log_cfg: LogConfig,
    file_cfg: IngressFileConfig,
    stop: CancellationToken,
}

impl IngressService {
    fn new(args: Args, log_cfg: LogConfig, file_cfg: IngressFileConfig) -> Self {
        Self {
            args,
            log_cfg,
            file_cfg,
            stop: CancellationToken::new(),
        }
    }

    /// Builds the runtime `IngressConfig` from the CLI args. The v0
    /// deployment keeps the binary protocol off; an operator who wants it
    /// enabled can set the binds in a follow-up that drives the config
    /// from TOML.
    fn build_config(&self) -> Result<IngressConfig> {
        let args = &self.args;
        validate_shard_count(u32::from(args.shards.get())).context("--shards")?;
        let mut cfg = IngressConfig {
            jsonrpc_bind: args.jsonrpc_bind,
            partition_count_m: NonZeroU32::from(args.shards),
            shard_map: self.load_shard_map()?,
            ingress_id: args.ingress_id,
            ack_policy: args.ack_policy.clone().into(),
            rpc_max_connections: args.rpc_max_connections,
            chain_id: args.chain_id,
            pending_receipt_timeout: Duration::from_millis(args.pending_receipt_timeout_ms),
            ..IngressConfig::default()
        };
        cfg.binary_tcp_bind = None;
        cfg.binary_uds_path = None;
        Ok(cfg)
    }

    /// Read and validate the `--shard-map` file, when one is given.
    fn load_shard_map(&self) -> Result<Option<ShardMap>> {
        let Some(path) = self.args.shard_map.as_deref() else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read shard map {}", path.display()))?;
        let map: ShardMap =
            toml::from_str(&text).with_context(|| format!("parse shard map {}", path.display()))?;
        Ok(Some(map))
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
    async fn open_aeron_side(&self) -> Result<OpenedAeron> {
        let args = &self.args;
        let channels = &self.log_cfg.channels;
        let aeron_cfg = &self.log_cfg.aeron;
        let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
        let mut plane =
            StreamPlane::from_config(&self.log_cfg, &format!("ingress-{}", args.ingress_id))
                .context("build the stream plane")?;

        let (recorder_handles, recorder_ready) = if args.archive_durability {
            spawn_tx_data_recorders(
                args.aeron_dir.as_deref(),
                channels,
                aeron_cfg,
                LANE_PLANE,
                &self.stop,
            )
        } else {
            (Vec::new(), Vec::new())
        };

        // tx_receipts MDS membership: prefer the CLI or env
        // `--executor-count`, and fall back to the log-config field, or
        // `None` (no known executor count) when neither is set. The
        // proxy reads this only when MDS is enabled.
        let executor_count = args.executor_count.or(channels.tx_receipts_executor_count);

        let publication = LiveIngressPublication::open(&rt, channels, LANE_PLANE)
            .context("open IngressPublication")?;

        // This is the recorder barrier: with the tx_data publications now
        // open, every lane's recording can start.
        if args.archive_durability {
            wait_for_recorders(recorder_ready)
                .await
                .context("archive durability requested but tx_data recorders failed to start")?;
        }
        let subscription =
            LiveIngressSubscription::open(&rt, &mut plane, args.recorder_id, executor_count)
                .context("open IngressSubscription")?;

        Ok(OpenedAeron {
            rt,
            plane,
            recorder_handles,
            publication,
            subscription,
        })
    }

    /// Connects to the cluster as a client and folds its egress progress
    /// into the proxy's on-quorum watermark bus. No standalone sealer
    /// publishes the durable watermark in the cluster-only topology, so this
    /// is the source of it: a record or boundary on egress is a
    /// Raft-quorum-durability signal.
    ///
    /// Returns the `LiveCluster` guard; the caller must keep it alive for as
    /// long as the returned watermark thread runs.
    fn spawn_cluster_watermark(
        &self,
        subscription: &LiveIngressSubscription,
    ) -> Result<LiveCluster> {
        let args = &self.args;
        let mut live = self.file_cfg.cluster.to_live();
        if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
            live.egress_channel = format!("aeron:udp?endpoint={ep}");
        }
        // This is a dedicated cluster runtime, with its own Aeron thread and
        // the same aeron dir, so the cluster session never contends with the
        // tx_data publish and receipts work.
        let cluster_rt =
            AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
        let (guard, mut observer) =
            cluster_watermark_observer(cluster_rt, live).context("connect cluster watermark")?;
        // A dedicated std thread runs a blocking egress poll, since the
        // observer holds the `!Send` cluster client. It sends the durable
        // count to the bus. The thread stops on the shutdown token, or when
        // the observer ends. The bus is a tokio `broadcast` channel, so the
        // send never blocks. A send with no live receiver is not an error
        // here.
        let wm_tx = subscription.watermark_sender();
        let wm_stop = self.stop.clone();
        std::thread::Builder::new()
            .name("cluster-watermark".into())
            .spawn(move || {
                while !wm_stop.is_cancelled() {
                    let Some(position) = observer.next_position() else {
                        break;
                    };
                    let _ = wm_tx.send(QuorumWatermark { position });
                }
            })
            .context("spawn cluster watermark thread")?;
        tracing::info!("kardamom-ingress: on-quorum watermark via Aeron Cluster egress");
        Ok(guard)
    }

    /// Builds the config, opens Aeron, optionally starts the cluster
    /// watermark watcher, and starts the proxy's listeners. Returns a
    /// [`RunningIngress`] that owns everything needed to shut back down.
    async fn run(self) -> Result<RunningIngress> {
        let cfg = self.build_config()?;
        let map_version = cfg.shard_map.as_ref().map(ShardMap::version);
        metrics::gauge!(kardamom_ingress::metrics::SHARD_MAP_VERSION)
            .set(f64::from(map_version.unwrap_or(0)));
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

        let opened = self.open_aeron_side().await?;
        let cluster_guard = if cfg.ack_policy.requires_quorum() {
            Some(self.spawn_cluster_watermark(&opened.subscription)?)
        } else {
            None
        };

        let drain_timeout = cfg.pending_receipt_timeout;
        let proxy = IngressProxy::new(cfg, opened.publication, opened.subscription);
        let drainer = proxy.clone();
        let handle = proxy.start().await.context("IngressProxy::start")?;
        tracing::info!(jsonrpc_addr = %handle.jsonrpc_addr, "JSON-RPC listening");

        Ok(RunningIngress {
            handle,
            drainer,
            drain_timeout,
            stop: self.stop,
            plane: opened.plane,
            recorder_handles: opened.recorder_handles,
            // Declared before `_cluster_guard`: struct fields drop in
            // declaration order, so `_rt` drops before `_cluster_guard`
            // when `RunningIngress::shutdown` consumes `self`. This
            // matches the original `main`'s explicit `drop(rt)` before
            // `_cluster_guard` went out of scope.
            _rt: opened.rt,
            _cluster_guard: cluster_guard,
        })
    }
}

/// A started ingress process: the JSON-RPC handle, the shutdown token,
/// the recorder thread handles, the Aeron runtime, and the optional
/// cluster watermark guard. [`Self::shutdown`] waits for the shutdown
/// signal and tears everything down in the right order.
struct RunningIngress {
    handle: IngressHandle,
    /// A proxy clone for the graceful drain at shutdown.
    drainer: IngressProxy<LiveIngressPublication, LiveIngressSubscription>,
    /// How long the drain waits for parked submits: the park bound.
    drain_timeout: Duration,
    stop: CancellationToken,
    /// The stream plane: shut down before `_rt` drops, so the discovery
    /// tasks and registrations end first.
    plane: StreamPlane,
    recorder_handles: Vec<std::thread::JoinHandle<()>>,
    /// See the field-order comment in [`IngressService::run`]: this must
    /// stay declared before `_cluster_guard`. Held only for its `Drop`
    /// side effect (closing the Aeron runtime); the leading underscore
    /// marks it never read, not that it is unused — dropping it is the
    /// point.
    _rt: AeronRuntime,
    /// Held only for its `Drop` side effect; never read.
    _cluster_guard: Option<LiveCluster>,
}

impl RunningIngress {
    /// Waits for the shutdown signal, drains the parked submits, stops the
    /// JSON-RPC server, cancels the recorder stop token, and joins the
    /// recorder threads. `self` is consumed here, so `_rt` and
    /// `_cluster_guard` drop at the end of this call, in field declaration
    /// order: `_rt` first, then `_cluster_guard`.
    async fn shutdown(self) {
        wait_for_shutdown().await;
        tracing::info!("kardamom-ingress: shutdown signal received");
        // The graceful drain: refuse new submits, let the parked ones finish
        // within the park bound, then stop the server. The Nomad job's
        // kill_timeout covers this wait.
        self.drainer.begin_drain();
        let still_parked = self.drainer.drain(self.drain_timeout).await;
        tracing::info!(still_parked, "kardamom-ingress: drain finished");
        self.handle.jsonrpc_handle.stop().ok();
        self.handle.jsonrpc_handle.stopped().await;
        self.stop.cancel();
        for h in self.recorder_handles {
            let _ = h.join();
        }
        self.plane.shutdown().await;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("ingress", args.metrics_addr, args.host_id.as_ref()).await?;
    kardamom_ingress::metrics::describe();

    // Runtime tunables come from defaults and CLI flags. The TOML file
    // supplies only the optional `[cluster]` section, the Aeron Cluster
    // client connection that the on-quorum watermark observer uses.
    let raw = std::fs::read_to_string(&args.config).context("read ingress config")?;
    let file_cfg: IngressFileConfig = toml::from_str(&raw).context("parse ingress config")?;
    let resolved = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;

    let running = IngressService::new(args, resolved, file_cfg).run().await?;
    running.shutdown().await;
    Ok(())
}
