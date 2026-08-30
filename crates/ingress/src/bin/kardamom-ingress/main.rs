//! `kardamom-ingress`: the standalone proxy (ingress) service process.
//!
//! This opens M tx_data publishers, a receipt-cache publisher, and
//! subscribers for the receipts, quorum-watermark, fsync-watermark,
//! receipt-cache, and block-boundary streams. It wires them into an
//! [`IngressProxy`] and starts its JSON-RPC server, plus optional TCP and
//! UDS binary protocol listeners. It idles on SIGTERM or Ctrl-C.
//!
//! The live Aeron adapters behind the proxy's channel traits live in
//! `kardamom_ingress::aeron_adapters`. The tx_data archive-recorder
//! threads, and their ready barrier, live in [`recorders`].

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
    /// Number of executor replicas to attach to the tx_receipts MDS
    /// (fan-in) subscription at startup. Used only when MDS is enabled,
    /// with `tx_receipts_control_channel` set. When unset on the CLI,
    /// this falls back to `channels.tx_receipts_executor_count` from the
    /// log config.
    ///
    /// Static-membership fallback: ingress attaches replicas `0..N` once
    /// at startup. The real design watches Consul for the
    /// `executor-receipts` service, and adds or removes destinations as
    /// membership changes. See the TODO(consul-watch) on
    /// `ChannelsConfig::tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<u32>,
    /// Number of tx_data shards (M). Defaults to 8.
    #[arg(long, default_value_t = 8)]
    shards: u32,
    /// Records each per-shard tx_data publication to the Aeron Archive, so
    /// the executor can replay full transaction envelopes on crash
    /// recovery, through `kardamom_log::replay`. Off by default, since
    /// single-host IPC has no archive. The cluster sets this on the node
    /// where the ArchivingMediaDriver runs.
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
    /// topology runs a `QuorumAggregator` yet. Nothing publishes the
    /// quorum watermark, so a quorum-gated policy would park every submit
    /// forever. Change the default back to `on-quorum`, the design's
    /// production default, once the aggregator is wired in.
    #[arg(long, default_value = "on-offer")]
    ack_policy: AckPolicyArg,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9006")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
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
    #[arg(long, env = "KARDAMOM_CHAIN_ID", default_value_t = 1)]
    chain_id: u64,
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("ingress", args.metrics_addr, &args.host_id)?;
    kardamom_ingress::metrics::describe();
    // v0 config loading: runtime tunables come from defaults and CLI
    // flags. The TOML file supplies the optional `[cluster]` section, the
    // Aeron Cluster client connection that the on-quorum watermark
    // observer below uses. A future revision will derive Deserialize for
    // the full IngressConfig.
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
    // Clear the binary-protocol binds for the v0 deployment. An operator
    // who wants them enabled can set them in a follow-up that drives the
    // config from TOML.
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

    // These are archive recorders for tx_data, one per shard, co-located
    // with the publishers here. They make the full transaction envelopes
    // durable, so the executor can replay them on crash recovery. Without
    // them, only the canonical order survives a restart, not the bytes
    // needed to re-execute.
    //
    // Each recorder reports its startup outcome on `recorder_ready_rx`.
    // `main` blocks on all of them, after the tx_data publications open
    // and before it serves RPC, so no transaction can be accepted before
    // its shard's recording is active. Recovery replays from record 0 and
    // needs every envelope, so a gap at the start of the stream would
    // permanently break executor crash recovery. A recorder startup
    // failure is fatal: the operator asked for --archive-durability, so
    // serving without it would be a silent lie.
    let recorder_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
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

    // tx_receipts MDS membership: prefer the CLI or env
    // `--executor-count`, and fall back to the log-config field. The
    // proxy reads this only when MDS is enabled.
    let executor_count = args
        .executor_count
        .unwrap_or(channels.tx_receipts_executor_count);

    let publication = LiveIngressPublication::open(&rt, &channels, args.shards as u8)
        .context("open IngressPublication")?;

    // This is the recorder barrier; see the recorder-spawn comment above.
    // With the tx_data publications now open, every shard's recording can
    // start. This waits for all of them to be confirmed active, or to
    // fail startup, before the JSON-RPC server accepts its first
    // transaction.
    if args.archive_durability {
        wait_for_recorders(&recorder_ready_rx, args.shards as u8)
            .context("archive durability requested but tx_data recorders failed to start")?;
    }
    let subscription =
        LiveIngressSubscription::open(&rt, &channels, args.recorder_id, executor_count)
            .context("open IngressSubscription")?;

    // This connects the cluster watermark to the on-quorum ack gate. In
    // the cluster-only topology, no standalone sealer publishes the
    // durable watermark. Instead, the ingress connects to the Aeron
    // Cluster (Raft) as a client, and folds its egress progress into an
    // increasing durable count. A record or boundary on egress is a
    // Raft-quorum-durability signal, and this feeds the proxy's
    // watermark bus. This step runs only when the ack policy gates on
    // quorum. The `LiveCluster` guard stays in scope, so the session
    // outlives the loop.
    let _cluster_guard = if cfg.ack_policy.requires_quorum() {
        let mut live = file_cfg.cluster.to_live();
        if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
            live.egress_channel = format!("aeron:udp?endpoint={ep}");
        }
        // This is a dedicated cluster runtime, with its own Aeron thread
        // and the same aeron dir, so the cluster session never contends
        // with the tx_data publish and receipts work.
        let cluster_rt =
            AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
        let (guard, mut observer) =
            cluster_watermark_observer(cluster_rt, live).context("connect cluster watermark")?;
        // A dedicated thread runs a blocking egress poll, which produces
        // a durable count, which this code sends to the bus.
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
    recorder_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    for h in recorder_handles {
        let _ = h.join();
    }
    drop(rt);
    Ok(())
}
