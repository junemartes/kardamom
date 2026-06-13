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

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use kardamom_ingress::channels::{InMemoryStateDb, IngressPublication, IngressSubscription};
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::proxy::IngressProxy;
use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, QuorumSubscriberHandle, TxDataPublisherHandle,
    TxErrorsSubscriberHandle, TxReceiptsBoundarySubscriberHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
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
    /// Number of tx_data shards (M). Defaults to 8.
    #[arg(long, default_value_t = 8)]
    shards: u32,
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
    // v0 config loading: validate the path exists; runtime tunables come
    // from defaults + CLI flags. A future revision will derive Deserialize
    // for IngressConfig (Duration via humantime_serde) so the TOML drives
    // every knob.
    let _raw =
        std::fs::read_to_string(&args.config).context("read ingress config (presence check)")?;

    let mut cfg = IngressConfig {
        jsonrpc_bind: args.jsonrpc_bind,
        partition_count_m: args.shards,
        ack_policy: args.ack_policy.into(),
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
        ack_policy = ?cfg.ack_policy,
        "kardamom-ingress starting"
    );

    let channels = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    let publication = LiveIngressPublication::open(&rt, &channels, args.shards as u8)
        .context("open IngressPublication")?;
    let subscription = LiveIngressSubscription::open(&rt, &channels, args.recorder_id)
        .context("open IngressSubscription")?;
    let state_db = Arc::new(InMemoryStateDb::new());

    let proxy = IngressProxy::new(cfg, publication, subscription, state_db);
    let handle = proxy.start().await.context("IngressProxy::start")?;
    tracing::info!(jsonrpc_addr = %handle.jsonrpc_addr, "JSON-RPC listening");

    wait_for_shutdown().await;
    tracing::info!("kardamom-ingress: shutdown signal received");
    handle.jsonrpc_handle.stop().ok();
    handle.jsonrpc_handle.stopped().await;
    drop(rt);
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
    ) -> Result<Self, IngressError> {
        let (receipts_tx, _) = broadcast::channel::<Receipt>(1024);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(1024);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(1024);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(1024);
        let (tx_errors_tx, _) = broadcast::channel::<TxError>(1024);

        // tx_receipts → Receipt fan-out
        let mut receipts_sub = TxReceiptsSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_receipts: {e}")))?;
        let tx = receipts_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, r)) = receipts_sub.recv().await {
                let _ = tx.send(r);
            }
        });

        // Quorum watermark
        let mut q_sub = QuorumSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open quorum watermark: {e}")))?;
        let tx = watermarks_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, w)) = q_sub.recv().await {
                let _ = tx.send(w);
            }
        });

        // Per-recorder fsync watermark
        let mut fsync_sub = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::Internal(format!("open fsync watermark: {e}")))?;
        let tx = local_fsync_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, w)) = fsync_sub.recv().await {
                let _ = tx.send(w);
            }
        });

        // tx_receipts → BlockBoundary fan-out
        let mut boundary_sub = TxReceiptsBoundarySubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_receipts boundaries: {e}")))?;
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
