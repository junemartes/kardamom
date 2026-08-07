//! `kardamom-ingress`: standalone proxy (ingress) service process.
//!
//! Opens M tx_data publishers + a receipt-cache publisher + subscribers for
//! receipts / quorum watermark / fsync watermark / receipt-cache /
//! block-boundary streams. Wires them into an [`IngressProxy`] and starts
//! its JSON-RPC server (plus optional TCP/UDS binary protocol listeners).
//! Idles on SIGTERM / Ctrl-C.
//!
//! The live Aeron adapters behind the proxy's channel traits live in
//! `kardamom_ingress::aeron_adapters`; the tx_data archive-recorder threads
//! (and their F13.2 ready barrier) in [`recorders`].

mod recorders;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_ingress::aeron_adapters::{LiveIngressPublication, LiveIngressSubscription};
use kardamom_ingress::cluster::cluster_watermark_observer;
use kardamom_ingress::config::{IngressConfig, IngressFileConfig};
use kardamom_ingress::proxy::IngressProxy;
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_types::QuorumWatermark;

use recorders::{spawn_tx_data_recorders, wait_for_recorders};

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
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("ingress", args.metrics_addr, &args.host_id)?;
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
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

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
        let cluster_rt =
            AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
        let (guard, mut observer) =
            cluster_watermark_observer(cluster_rt, live).context("connect cluster watermark")?;
        // Blocking egress poll on a dedicated thread → durable count → bus.
        let wm_tx = subscription.watermark_sender();
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
