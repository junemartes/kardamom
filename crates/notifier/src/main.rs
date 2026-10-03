//! `kardamom-notifier`: the transaction status fan-out service. It taps
//! `tx_status`, `tx_receipts` and `tx_errors`, keeps a ring of recent
//! events, and serves WebSocket subscriptions and webhooks. See the
//! library docs for the module map.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_notifier::feed::Feed;
use kardamom_notifier::hub::{Hub, HubConfig};
use kardamom_notifier::metrics;
use kardamom_notifier::ring::RingConfig;
use kardamom_notifier::server::{Hooks, ListenConfig, serve};
use kardamom_notifier::shard::InstanceSet;
use kardamom_notifier::taps::Taps;
use kardamom_notifier::webhooks::{Webhooks, WebhooksConfig};
use kardamom_obs::bin::wait_for_shutdown;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-notifier",
    version,
    about = "kardamom notifier: transaction status events for clients"
)]
struct Args {
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Number of executor replicas whose `tx_receipts` endpoints to
    /// attach under MDS. Falls back to
    /// `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<u32>,
    /// The address of the WebSocket feed and the webhook endpoint.
    #[arg(long, env = "KARDAMOM_NOTIFIER_BIND", default_value = "127.0.0.1:8547")]
    bind: SocketAddr,
    /// The connection cap of the listener.
    #[arg(
        long,
        env = "KARDAMOM_NOTIFIER_MAX_CONNECTIONS",
        default_value_t = 10_000
    )]
    max_connections: u32,
    /// How many minutes of events the ring keeps.
    #[arg(long, env = "KARDAMOM_NOTIFIER_RING_MINUTES", default_value_t = 10)]
    ring_minutes: u64,
    /// The most events the ring keeps, whatever their age.
    #[arg(
        long,
        env = "KARDAMOM_NOTIFIER_RING_MAX_EVENTS",
        default_value = "1000000"
    )]
    ring_max_events: NonZeroUsize,
    /// The live feed buffer per subscriber. A subscriber further behind
    /// gets a lag marker.
    #[arg(long, env = "KARDAMOM_NOTIFIER_FEED_BUFFER", default_value = "65536")]
    feed_buffer: NonZeroUsize,
    /// Events per replay page.
    #[arg(long, env = "KARDAMOM_NOTIFIER_REPLAY_PAGE", default_value = "1000")]
    replay_page: NonZeroUsize,
    /// The directory of the webhook subscriptions, outboxes and cursors.
    #[arg(
        long,
        env = "KARDAMOM_NOTIFIER_DIR",
        default_value = "/opt/kardamom/notifier"
    )]
    dir: PathBuf,
    /// A fully delivered outbox longer than this many bytes is cut.
    #[arg(
        long,
        env = "KARDAMOM_NOTIFIER_OUTBOX_RETAIN_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    outbox_retain_bytes: u64,
    /// The webhook POST timeout, in milliseconds.
    #[arg(
        long,
        env = "KARDAMOM_NOTIFIER_WEBHOOK_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    webhook_timeout_ms: u64,
    /// This instance's index in the instance set.
    #[arg(long, env = "KARDAMOM_NOTIFIER_INSTANCE_INDEX", default_value_t = 0)]
    instance_index: u32,
    /// The instance count the subscriptions are sharded across.
    #[arg(long, env = "KARDAMOM_NOTIFIER_INSTANCE_COUNT", default_value = "1")]
    instance_count: NonZeroU32,
    /// The base URLs of the other instances, comma separated. A webhook
    /// registration is forwarded to each.
    #[arg(long, env = "KARDAMOM_NOTIFIER_PEERS", value_delimiter = ',')]
    peers: Vec<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9008")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("notifier", args.metrics_addr, args.host_id.as_str()).await?;
    metrics::describe();

    let instances = InstanceSet::new(args.instance_index, args.instance_count)?;
    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let mut plane = StreamPlane::from_config(&log_cfg, &format!("notifier-{}", instances.index()))
        .context("build the stream plane")?;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let executor_count = args
        .executor_count
        .and_then(NonZeroU32::new)
        .or(plane.channels().tx_receipts_executor_count);
    let taps = Taps::open(&rt, &mut plane, executor_count).context("open the taps")?;
    let shutdown = plane.cancellation().child_token();

    let (hub, handle) = Hub::new(
        HubConfig {
            ring: RingConfig {
                max_age: Duration::from_secs(args.ring_minutes.saturating_mul(60)),
                max_events: args.ring_max_events,
            },
            feed_buffer: args.feed_buffer,
            ingest_buffer: args.feed_buffer,
        },
        shutdown.clone(),
    );
    let (webhooks, registrar) = Webhooks::start(
        WebhooksConfig {
            dir: args.dir,
            instances,
            retain_bytes: args.outbox_retain_bytes,
            request_timeout: Duration::from_millis(args.webhook_timeout_ms),
            queue: args.feed_buffer,
        },
        &handle,
        shutdown.clone(),
    )
    .context("start the webhooks")?;
    let hub_task = tokio::spawn(hub.run());
    let webhooks_task = tokio::spawn(webhooks.run());
    let tap_tasks = taps.spawn(handle.ingest());
    let listener = serve(
        ListenConfig {
            bind: args.bind,
            max_connections: args.max_connections,
        },
        Feed::new(handle, args.replay_page),
        Hooks::new(registrar, args.peers).context("build the webhook forwarder")?,
        shutdown.clone(),
    )
    .await
    .context("bind the listener")?;
    tracing::info!(
        addr = %listener.addr,
        instance = instances.index(),
        instances = instances.count(),
        "kardamom-notifier starting"
    );

    wait_for_shutdown().await;
    tracing::info!("kardamom-notifier: shutdown signal received");
    shutdown.cancel();
    listener.stop().await;
    let _ = webhooks_task.await;
    let _ = hub_task.await;
    // The taps end when the runtime closes, after the plane.
    plane.shutdown().await;
    close_runtime(rt);
    for task in tap_tasks {
        let _ = task.await;
    }
    Ok(())
}

/// End the Aeron runtime. The subscriptions close with it, so the tap
/// tasks end afterwards.
fn close_runtime(_rt: AeronRuntime) {}
