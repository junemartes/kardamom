//! `kardamom-ingress`: standalone proxy (ingress) service process.
//!
//! Opens M tx_data publishers + a receipt-cache publisher + subscribers for
//! receipts / quorum watermark / fsync watermark / receipt-cache /
//! block-boundary streams. Wires them into an [`IngressProxy`] and starts
//! its JSON-RPC server (plus optional TCP/UDS binary protocol listeners).
//! Idles on SIGTERM / Ctrl-C.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use kardamom_ingress::channels::{IngressPublication, IngressSubscription};
use kardamom_ingress::cluster::cluster_watermark_observer;
use kardamom_ingress::config::{IngressConfig, IngressFileConfig};
use kardamom_ingress::error::IngressError;
use kardamom_ingress::proxy::IngressProxy;
use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, TxDataPublisherHandle, TxErrorsSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_log::recorder::{Recorder, RecorderKind, connect_archive};
use kardamom_types::{
    BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope, TxError,
};
use tokio::sync::broadcast;

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
    /// Optional path to a `LogConfig` TOML supplying the Aeron `[channels]`
    /// (and `[aeron]`/`[quorum]`) config. When unset, the built-in single-host
    /// IPC defaults are used (preserving local/e2e behaviour). Multi-host
    /// deployments point this at the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// JSON-RPC bind address. Defaults to 127.0.0.1:8545.
    #[arg(long, default_value = "127.0.0.1:8545")]
    jsonrpc_bind: SocketAddr,
    /// Recorder id for the local fsync watermark subscription (used when
    /// `ack_policy` includes a local-fsync gate). Defaults to 0.
    #[arg(long, default_value_t = 0)]
    recorder_id: u8,
    /// Number of executor replicas to attach to the tx_receipts MDS
    /// (fan-in) subscription at startup. Only used when MDS is enabled
    /// (`tx_receipts_control_channel` set). When unset on the CLI it falls
    /// back to `channels.tx_receipts_executor_count` from the log config.
    ///
    /// STATIC-MEMBERSHIP FALLBACK: ingress attaches replicas `0..N` once at
    /// startup. The real design watches Consul for `executor-receipts` service
    /// membership and add/removes destinations dynamically — see the
    /// TODO(consul-watch) on `ChannelsConfig::tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<u32>,
    /// Number of tx_data shards (M). Defaults to 8.
    #[arg(long, default_value_t = 8)]
    shards: u32,
    /// Record each per-shard tx_data publication to the Aeron Archive so the
    /// executor can replay full transaction envelopes on crash recovery
    /// (`kardamom_log::replay`). Off by default (single-host IPC has no
    /// archive); the cluster sets this where the ArchivingMediaDriver runs.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Durability gate before acking a submit. Mirrors `AckPolicy`:
    ///   - `on-offer`: release as soon as the receipt arrives (lowest
    ///     latency, weakest guarantee).
    ///   - `on-local-fsync`: wait for this node's recorder fsync watermark.
    ///   - `on-quorum`: wait for Q-of-N recorders to fsync.
    ///   - `on-local-fsync-and-quorum`: both.
    ///
    /// Defaults to `on-offer` because no process in the deployed topology
    /// runs a `QuorumAggregator` yet — nothing publishes the quorum
    /// watermark, so the quorum-gated policies would park every submit
    /// indefinitely. Flip the default back to `on-quorum` (the design's
    /// production default) once the aggregator is wired in.
    #[arg(long, default_value = "on-offer")]
    ack_policy: AckPolicyArg,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9006")]
    metrics_addr: SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
    /// Stable identity of this ingress replica (active/active deployments run N
    /// of them). Namespaces `correlation_id` so `(replica, sequence)` is
    /// globally unique, and is stamped as a metric label. Default 0.
    #[arg(long, env = "KARDAMOM_INGRESS_ID", default_value_t = 0)]
    ingress_id: u16,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Overrides
    /// the `[cluster] egress_channel` as `aeron:udp?endpoint=<ip:port>` — the
    /// cluster client's per-node response channel. Injected per node by the
    /// Nomad job. Only consulted when the ack policy requires the quorum gate.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Max concurrent JSON-RPC connections. Submissions park their connection
    /// until the receipt arrives, so this must comfortably exceed offered
    /// rate × receipt latency; see `IngressConfig::rpc_max_connections`.
    #[arg(
        long = "rpc-max-connections",
        env = "KARDAMOM_RPC_MAX_CONNECTIONS",
        default_value_t = 8192
    )]
    rpc_max_connections: u32,
    /// L2 chain id returned by `eth_chainId`. Purely informational to clients
    /// (the ingress recovers senders from the tx's own EIP-155 signature), but
    /// tooling that queries `eth_chainId` before signing needs it to match the
    /// executor's `--chain-id`.
    #[arg(long, env = "KARDAMOM_CHAIN_ID", default_value_t = 1)]
    chain_id: u64,
    /// Max time (milliseconds) a submit parks waiting for its receipt (and ack
    /// gate) before the client gets a `-32000` timeout. The park bounds every
    /// `eth_sendRawTransaction` — a nonce-gap tx that never becomes executable
    /// surfaces as exactly this timeout.
    #[arg(
        long = "pending-receipt-timeout-ms",
        env = "KARDAMOM_PENDING_RECEIPT_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    pending_receipt_timeout_ms: u64,
}

#[derive(Clone, Debug, clap::ValueEnum)]
#[allow(clippy::enum_variant_names)] // mirrors `types::AckPolicy` 1:1
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "ingress",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    kardamom_ingress::metrics::describe();
    // v0 config loading: runtime tunables come from defaults + CLI flags; the
    // TOML supplies the optional `[cluster]` section (the Aeron Cluster client
    // connection used by the on-quorum watermark observer below). A future
    // revision will derive Deserialize for the full IngressConfig.
    let raw = std::fs::read_to_string(&args.config).context("read ingress config")?;
    let file_cfg: IngressFileConfig = toml::from_str(&raw).context("parse ingress config")?;

    let mut cfg = IngressConfig {
        jsonrpc_bind: args.jsonrpc_bind,
        partition_count_m: args.shards,
        ingress_id: args.ingress_id,
        ack_policy: args.ack_policy.into(),
        rpc_max_connections: args.rpc_max_connections,
        chain_id: args.chain_id,
        pending_receipt_timeout: Duration::from_millis(args.pending_receipt_timeout_ms),
        ..IngressConfig::default()
    };
    // Wipe the binary-protocol binds for the v0 deployment; operators that
    // want them enabled set them in a follow-up that drives the config from
    // TOML.
    cfg.binary_tcp_bind = None;
    cfg.binary_uds_path = None;

    tracing::info!(
        jsonrpc_bind = %cfg.jsonrpc_bind,
        shards = cfg.partition_count_m,
        ingress_id = cfg.ingress_id,
        ack_policy = ?cfg.ack_policy,
        "kardamom-ingress starting"
    );

    let resolved = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let channels = resolved.channels;
    let aeron_cfg = resolved.aeron;
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Archive recorders for tx_data (one per shard), co-located with the
    // publishers here. They make the full transaction envelopes durable so the
    // executor can replay them on crash recovery; without them only the
    // canonical order survives a restart, not the bytes to re-execute.
    //
    // Each recorder reports its startup outcome on `recorder_ready_rx`; main
    // BLOCKS on all of them (after the tx_data publications are open, before
    // serving RPC) so no transaction can be accepted before its shard's
    // recording is active — recovery replays from record 0 and needs every
    // envelope, so a birth-of-stream gap would permanently break executor
    // crash recovery. A recorder startup failure is fatal: the operator asked
    // for --archive-durability, so serving without it would be a silent lie.
    let recorder_stop = Arc::new(kardamom_log::shutdown::Gate::new());
    let (recorder_ready_tx, recorder_ready_rx) =
        std::sync::mpsc::channel::<(u8, Result<i64, String>)>();
    let recorder_handles = if args.archive_durability {
        spawn_tx_data_recorders(
            args.aeron_dir.clone(),
            channels.clone(),
            aeron_cfg.clone(),
            args.shards as u8,
            recorder_stop.clone(),
            recorder_ready_tx,
        )
    } else {
        Vec::new()
    };

    // tx_receipts MDS membership: prefer the CLI/env `--executor-count`, else
    // the log-config field. Only consulted when MDS is enabled.
    let executor_count = args
        .executor_count
        .unwrap_or(channels.tx_receipts_executor_count);

    let publication = LiveIngressPublication::open(&rt, &channels, args.shards as u8)
        .context("open IngressPublication")?;

    // Recorder barrier (see the recorder-spawn comment above): with the
    // tx_data publications now open, every shard's recording can materialise.
    // Wait for all of them to be confirmed active (or fail startup) BEFORE the
    // JSON-RPC server accepts its first transaction.
    if args.archive_durability {
        wait_for_recorders(&recorder_ready_rx, args.shards as u8)
            .context("archive durability requested but tx_data recorders failed to start")?;
    }
    let subscription =
        LiveIngressSubscription::open(&rt, &channels, args.recorder_id, executor_count)
            .context("open IngressSubscription")?;

    // Cluster watermark → on-quorum ack gate. In the cluster-only topology there
    // is no standalone sealer publishing the durable watermark; the ingress
    // instead connects to the Aeron Cluster (Raft) as a client and folds its
    // egress progress into a monotonic durable count (a record/boundary on
    // egress is a Raft-quorum-durability signal), feeding the proxy's watermark
    // bus. Only needed when the ack policy actually gates on quorum. The
    // `LiveCluster` guard is held in scope so the session outlives the loop.
    let _cluster_guard = if cfg.ack_policy.requires_quorum() {
        let mut live = file_cfg.cluster.to_live();
        if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
            live.egress_channel = format!("aeron:udp?endpoint={ep}");
        }
        // DEDICATED cluster runtime (own Aeron thread, same aeron dir) so the
        // cluster session never contends with the tx_data publish / receipts work.
        let cluster_rt = match args.aeron_dir.as_ref() {
            Some(dir) => {
                AeronRuntime::spawn_with_dir(dir).context("spawn cluster AeronRuntime with dir")?
            }
            None => AeronRuntime::spawn_default().context("spawn cluster AeronRuntime")?,
        };
        let (guard, mut observer) =
            cluster_watermark_observer(cluster_rt, live).context("connect cluster watermark")?;
        // Blocking egress poll on a dedicated thread → durable count → bus.
        let wm_tx = subscription.watermarks.clone();
        std::thread::Builder::new()
            .name("cluster-watermark".into())
            .spawn(move || {
                while let Some(position) = observer.next_position() {
                    let _ = wm_tx.send(QuorumWatermark { position });
                }
            })
            .context("spawn cluster watermark thread")?;
        tracing::info!("kardamom-ingress: on-quorum watermark via Aeron Cluster egress");
        Some(guard)
    } else {
        None
    };

    let proxy = IngressProxy::new(cfg, publication, subscription);
    let handle = proxy.start().await.context("IngressProxy::start")?;
    tracing::info!(jsonrpc_addr = %handle.jsonrpc_addr, "JSON-RPC listening");

    wait_for_shutdown().await;
    tracing::info!("kardamom-ingress: shutdown signal received");
    handle.jsonrpc_handle.stop().ok();
    handle.jsonrpc_handle.stopped().await;
    recorder_stop.signal();
    for h in recorder_handles {
        let _ = h.join();
    }
    drop(rt);
    Ok(())
}

/// Spawn one archive recorder thread per tx_data shard. Each connects its own
/// (thread-confined) archive session, starts recording its shard's tx_data
/// publication, reports its startup outcome on `ready`, and holds the
/// recording alive until `stop` is set. The recording itself runs in the
/// ArchivingMediaDriver; the thread only keeps the session connected and
/// re-adopts an existing recording on restart.
fn spawn_tx_data_recorders(
    aeron_dir: Option<PathBuf>,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    shards: u8,
    stop: Arc<kardamom_log::shutdown::Gate>,
    ready: std::sync::mpsc::Sender<(u8, Result<i64, String>)>,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..shards)
        .map(|sid| {
            let aeron_dir = aeron_dir.clone();
            let channels = channels.clone();
            let aeron_cfg = aeron_cfg.clone();
            let stop = stop.clone();
            let ready = ready.clone();
            std::thread::Builder::new()
                .name(format!("ingress-tx-data-recorder-{sid}"))
                .spawn(move || {
                    if let Err(e) = run_tx_data_recorder(
                        aeron_dir.as_deref(),
                        &channels,
                        &aeron_cfg,
                        sid,
                        stop,
                        ready,
                    ) {
                        tracing::error!(shard = sid, error = %e, "tx_data recorder exited with error");
                    }
                })
                .expect("spawn tx_data recorder thread")
        })
        .collect()
}

/// Block until every one of the `shards` recorder threads has reported on
/// `ready`, failing on the first reported error (or on timeout). This is the
/// F13.2 barrier: publish/RPC must not start before the recordings are active.
fn wait_for_recorders(
    ready: &std::sync::mpsc::Receiver<(u8, Result<i64, String>)>,
    shards: u8,
) -> Result<()> {
    // Generous per-recorder budget: the publications are already open, so the
    // recording normally materialises within one catalog-poll tick (~500ms);
    // the timeout only bounds a wedged/unreachable archive.
    const RECORDER_READY_TIMEOUT: Duration = Duration::from_secs(60);
    for _ in 0..shards {
        match ready.recv_timeout(RECORDER_READY_TIMEOUT) {
            Ok((sid, Ok(recording_id))) => {
                tracing::info!(
                    shard = sid,
                    recording_id,
                    "tx_data recording confirmed active"
                );
            }
            Ok((sid, Err(e))) => {
                anyhow::bail!("tx_data recorder for shard {sid} failed to start: {e}");
            }
            Err(e) => {
                anyhow::bail!(
                    "timed out ({RECORDER_READY_TIMEOUT:?}) waiting for a tx_data recording \
                     to become active: {e}"
                );
            }
        }
    }
    Ok(())
}

fn run_tx_data_recorder(
    aeron_dir: Option<&std::path::Path>,
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    sid: u8,
    stop: Arc<kardamom_log::shutdown::Gate>,
    ready: std::sync::mpsc::Sender<(u8, Result<i64, String>)>,
) -> Result<()> {
    let session = match connect_archive(aeron_dir, aeron_cfg) {
        Ok(s) => s,
        Err(e) => {
            let _ = ready.send((sid, Err(format!("connect archive: {e}"))));
            return Err(e).context("connect archive");
        }
    };
    let mut should_stop = || stop.is_set();
    let recorder = match Recorder::start_stream(
        session.archive,
        &channels.tx_data_channel(sid),
        channels.tx_data_stream_id(sid),
        RecorderKind::TxData { sequencer_id: sid },
        &mut should_stop,
    ) {
        Ok(Some(r)) => r,
        Ok(None) => {
            // Stopped before the recording materialised (shutdown during
            // startup); report it so a waiting barrier doesn't hang.
            let _ = ready.send((sid, Err("stopped before the recording materialised".into())));
            return Ok(());
        }
        Err(e) => {
            let _ = ready.send((sid, Err(format!("start tx_data recording: {e}"))));
            return Err(e).context("start tx_data recording");
        }
    };
    tracing::info!(
        shard = sid,
        recording_id = recorder.recording_id(),
        "ingress: recording tx_data shard"
    );
    let _ = ready.send((sid, Ok(recorder.recording_id())));
    // Hold the recording (and its archive session) alive until shutdown —
    // parked on the gate's Condvar, not sleep-polled (push-model-spec 1a).
    stop.wait();
    Ok(())
}

// ---------------------------------------------------------------------------
// IngressPublication adapter over M TxDataPublisherHandle.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LiveIngressPublication {
    tx_data: Vec<TxDataPublisherHandle>,
}

impl LiveIngressPublication {
    fn open(
        rt: &AeronRuntime,
        channels: &kardamom_log::config::ChannelsConfig,
        shards: u8,
    ) -> Result<Self, IngressError> {
        let mut tx_data = Vec::with_capacity(shards as usize);
        for sid in 0..shards {
            let h = TxDataPublisherHandle::open(rt, channels, sid)
                .map_err(|e| IngressError::Internal(format!("open tx_data[{sid}]: {e}")))?;
            tx_data.push(h);
        }
        Ok(Self { tx_data })
    }
}

#[async_trait]
impl IngressPublication for LiveIngressPublication {
    async fn publish_tx_data(
        &self,
        shard: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError> {
        let pub_handle = self
            .tx_data
            .get(shard)
            .ok_or_else(|| IngressError::Internal(format!("shard {shard} out of range")))?
            .clone();
        // The Aeron publish blocks via the runtime's command channel; do
        // it off the reactor thread so we don't stall the JSON-RPC server.
        tokio::task::spawn_blocking(move || pub_handle.publish(&envelope))
            .await
            .map_err(|e| IngressError::Internal(format!("publish_tx_data join: {e}")))?
            .map(|_| ())
            .map_err(|e| IngressError::Internal(format!("publish_tx_data: {e}")))
    }
}

// ---------------------------------------------------------------------------
// IngressSubscription adapter. Pumps each log handle's mpsc receiver into a
// tokio::sync::broadcast::Sender so the proxy's broadcast::Receiver-based
// trait surface can fan out to multiple watchers.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LiveIngressSubscription {
    receipts: broadcast::Sender<Receipt>,
    watermarks: broadcast::Sender<QuorumWatermark>,
    local_fsync: broadcast::Sender<FsyncWatermark>,
    block_boundaries: broadcast::Sender<BlockBoundary>,
    tx_errors: broadcast::Sender<TxError>,
}

impl LiveIngressSubscription {
    fn open(
        rt: &AeronRuntime,
        channels: &kardamom_log::config::ChannelsConfig,
        recorder_id: u8,
        executor_count: u32,
    ) -> Result<Self, IngressError> {
        let (receipts_tx, _) = broadcast::channel::<Receipt>(1024);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(1024);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(1024);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(1024);
        let (tx_errors_tx, _) = broadcast::channel::<TxError>(1024);

        let mds = channels.tx_receipts_mds_enabled();
        if mds {
            tracing::info!(
                executor_count,
                control_channel = %channels.tx_receipts_control_channel,
                "tx_receipts MDS fan-in: aggregating per-replica executor endpoints"
            );
        }

        // tx_receipts → Receipt fan-out.
        //
        // MDS (fan-in): open ONE control-mode=manual subscription on
        // `tx_receipts_control_channel` and attach each executor replica's
        // unicast endpoint (0..executor_count) as a destination. N executors
        // replay the SAME canonical order and emit IDENTICAL receipts, so the
        // proxy dedups by tx hash downstream (first-wins) — this layer just
        // aggregates the streams. Legacy IPC: a plain subscription on the
        // shared `tx_receipts_channel`.
        let receipts_sub = if mds {
            let sub = TxReceiptsSubscriberHandle::open_mds(rt, channels)
                .map_err(|e| IngressError::Internal(format!("open tx_receipts (MDS): {e}")))?;
            attach_executor_endpoints(channels, executor_count, false, |uri| {
                sub.add_destination(uri)
            })
            .map_err(|e| IngressError::Internal(format!("attach tx_receipts destination: {e}")))?;
            sub
        } else {
            TxReceiptsSubscriberHandle::open(rt, channels)
                .map_err(|e| IngressError::Internal(format!("open tx_receipts: {e}")))?
        };
        // `into_receiver()`: the handle's AeronRuntime clone must NOT travel
        // into the pump task — that ownership cycle keeps the runtime alive
        // forever (see `TxReceiptsSubscriberHandle::into_receiver`). Harmless
        // here today only because `main` returns without joining on the
        // streams; it made the validator unkillable by SIGTERM.
        let mut receipts_rx = receipts_sub.into_receiver();
        let tx = receipts_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, r)) = receipts_rx.recv().await {
                let _ = tx.send(r);
            }
        });

        // Quorum/durable watermark: in the cluster-only topology this bus is fed
        // by the Aeron Cluster egress observer spawned in `main` (cluster mode
        // replaced the standalone sealer that used to publish it on Aeron), not
        // by an Aeron `quorum_watermark` subscription here. The bus + its
        // `subscribe_watermark()` surface are unchanged; only the producer moved.

        // Per-recorder fsync watermark
        let mut fsync_sub = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::Internal(format!("open fsync watermark: {e}")))?;
        let tx = local_fsync_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, w)) = fsync_sub.recv().await {
                let _ = tx.send(w);
            }
        });

        // tx_receipts → BlockBoundary fan-out (the `tx_receipts_stream_id + 1`
        // side-stream). Same MDS vs IPC branch as the receipt stream above:
        // attach the same per-replica executor endpoints to the boundary MDS
        // subscription.
        let mut boundary_sub = if mds {
            let sub = TxReceiptsBoundarySubscriberHandle::open_mds(rt, channels).map_err(|e| {
                IngressError::Internal(format!("open tx_receipts boundaries (MDS): {e}"))
            })?;
            attach_executor_endpoints(channels, executor_count, true, |uri| {
                sub.add_destination(uri)
            })
            .map_err(|e| {
                IngressError::Internal(format!("attach tx_receipts boundary destination: {e}"))
            })?;
            sub
        } else {
            TxReceiptsBoundarySubscriberHandle::open(rt, channels)
                .map_err(|e| IngressError::Internal(format!("open tx_receipts boundaries: {e}")))?
        };
        let tx = block_boundaries_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, b)) = boundary_sub.recv().await {
                let _ = tx.send(b);
            }
        });

        // tx_errors → TxError fan-out
        let mut errors_sub = TxErrorsSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_errors: {e}")))?;
        let tx = tx_errors_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, e)) = errors_sub.recv().await {
                let _ = tx.send(e);
            }
        });

        Ok(Self {
            receipts: receipts_tx,
            watermarks: watermarks_tx,
            local_fsync: local_fsync_tx,
            block_boundaries: block_boundaries_tx,
            tx_errors: tx_errors_tx,
        })
    }
}

impl IngressSubscription for LiveIngressSubscription {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipts.subscribe()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermarks.subscribe()
    }
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark> {
        self.local_fsync.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundaries.subscribe()
    }
    fn subscribe_tx_errors(&self) -> broadcast::Receiver<TxError> {
        self.tx_errors.subscribe()
    }
}

/// Attach each executor replica's per-replica receipt endpoint
/// (`channels.tx_receipts_endpoint(0..executor_count)`) to one MDS
/// subscription via `attach` (a closure over the handle's `add_destination`).
///
/// STATIC MEMBERSHIP (Consul-watch fallback): this runs once at startup over
/// the fixed `0..executor_count` index space. The executor job is a count-based
/// Nomad job with `distinct_hosts`, so replica indices are stable and a
/// restarting replica keeps its index/endpoint — the static attach therefore
/// stays correct across restarts. The full design watches the
/// `executor-receipts` Consul service and add/removes destinations on
/// membership change; see TODO(consul-watch) on
/// `ChannelsConfig::tx_receipts_executor_count`.
fn attach_executor_endpoints<F>(
    channels: &kardamom_log::config::ChannelsConfig,
    executor_count: u32,
    boundary: bool,
    mut attach: F,
) -> Result<(), IngressError>
where
    F: FnMut(&str) -> Result<(), kardamom_log::error::LogError>,
{
    if executor_count == 0 {
        tracing::warn!(
            "tx_receipts MDS enabled but executor_count is 0 — ingress will receive no \
             receipts; set --executor-count / KARDAMOM_EXECUTOR_COUNT or \
             channels.tx_receipts_executor_count"
        );
    }
    // Receipts and boundaries are distinct streams on distinct endpoints (ports):
    // each manual subscription binds its destination socket, so they must not
    // share an endpoint. See ChannelsConfig::tx_receipts_endpoint.
    let kind = if boundary { "boundary" } else { "receipt" };
    for i in 0..executor_count {
        let endpoint = if boundary {
            channels.tx_receipts_boundary_endpoint(i)
        } else {
            channels.tx_receipts_endpoint(i)
        }
        .ok_or_else(|| {
            IngressError::Internal(format!(
                "tx_receipts {kind} endpoint({i}) is None (MDS misconfigured)"
            ))
        })?;
        attach(&endpoint)
            .map_err(|e| IngressError::Internal(format!("add_destination {endpoint}: {e}")))?;
        tracing::info!(replica = i, kind, %endpoint, "attached executor endpoint to MDS");
    }
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl-C received");
    }
}
