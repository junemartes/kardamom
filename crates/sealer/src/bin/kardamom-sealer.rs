//! `kardamom-sealer`: block sealer process.
//!
//! Loads a TOML [`SealerConfig`], opens a tx_ordering publisher + a brief
//! tx_ordering subscriber (to bootstrap `block_number` from the tail), and
//! runs [`Sealer::run_forever`] until SIGTERM / Ctrl-C.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxOrderingPublisherHandle, TxOrderingSubscriberHandle,
};
use kardamom_log::config::{AeronConfig, ChannelsConfig, LogConfig};
use kardamom_log::publisher::QuorumPublisher;
use kardamom_log::recorder::{
    Recorder, connect_archive, connect_client, run_durable_watermark_loop,
};
use kardamom_sealer::clock::SystemClock;
use kardamom_sealer::emitter::{BoundaryPublisher, PublishError};
use kardamom_sealer::{Sealer, SealerConfig};
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sealer",
    version,
    about = "kardamom block sealer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SealerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults. The sealer's own
    /// `channel_b_uri` / `channel_b_boundary_stream_id` (from `--config`)
    /// always take precedence for the tx_ordering channel it publishes, so
    /// this only supplies the other channels' transport.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`). If unset, uses the
    /// default location embedded in the C client (typically
    /// `/dev/shm/aeron-<user>`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9003")]
    metrics_addr: SocketAddr,
    /// Host identifier; stamped as a global label on every metric.
    /// Defaults to the config file's `host_id`.
    #[arg(long, env = "KARDAMOM_HOST_ID")]
    host_id: Option<String>,
    /// Enable the **archive-at-the-sealer** durability sidecar (the locked
    /// durability decision, replacing the custom recorders + Q-of-N quorum
    /// aggregator). When set, the sealer connects its co-located Aeron
    /// Archive, records its own tx_ordering MDC publication, and publishes the
    /// recording's byte-durable position as the single durable watermark
    /// (`QuorumWatermark` on `quorum_watermark_channel`) that ingress's
    /// `--ack-policy on-quorum` gate consumes. Requires MDC to be enabled in
    /// the `--log-config` and `channel_b_mdc_control` to be set. Off by
    /// default so the single-host IPC path is unaffected.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Poll cadence (ms) for the durable-watermark sidecar.
    #[arg(long, default_value_t = 1)]
    durable_poll_interval_ms: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let cfg: SealerConfig = toml::from_str(&raw).context("parse config")?;
    cfg.validate().context("validate config")?;

    let host_id = args
        .host_id
        .clone()
        .unwrap_or_else(|| cfg.host_id.to_string());
    kardamom_obs::init(
        "sealer",
        args.metrics_addr,
        &host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    kardamom_sealer::metrics::describe();

    tracing::info!(
        host_id = cfg.host_id,
        tick_interval_ms = cfg.tick_interval_ms,
        "kardamom-sealer starting"
    );

    // Start from the resolved channels (UDP/MDC from --log-config, or IPC
    // defaults). The boundary stream id is always the sealer's
    // `channel_b_boundary_stream_id` (the single source of truth). For the
    // *channel*: in the legacy shared-channel path the sealer's own
    // `channel_b_uri` wins (it must be byte-identical to the subscribers'
    // tx_ordering_channel); in the MDC path the resolved channels already
    // carry the MDC publisher list, so we leave `tx_ordering_channel`
    // untouched and the sealer publishes via its own MDC control endpoint.
    let mut channels = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    channels.tx_ordering_stream_id = cfg.channel_b_boundary_stream_id;
    let mdc = channels.tx_ordering_mdc_enabled();
    if !mdc {
        channels.tx_ordering_channel = cfg.channel_b_uri.clone();
    }

    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Bootstrap: drain the tx_ordering tail and pick `max(block_number) + 1`.
    // (Under MDC this subscribes to every publisher's control endpoint.)
    let initial_block = bootstrap_block_number(&rt, &channels).await?;
    tracing::info!(
        initial_block,
        mdc,
        "tx_ordering bootstrap complete; this sealer resumes from block_number"
    );

    let publisher = if mdc {
        let ctl = cfg.channel_b_mdc_control.as_deref().context(
            "tx_ordering MDC is enabled in the log config but the sealer config did not set \
             channel_b_mdc_control (this sealer's ip:port control endpoint)",
        )?;
        TxOrderingPublisherHandle::open_mdc(&rt, &channels, ctl)
            .context("open TxOrderingPublisherHandle (MDC)")?
    } else {
        TxOrderingPublisherHandle::open(&rt, &channels)
            .context("open TxOrderingPublisherHandle")?
    };
    let adapter = TxOrderingBoundaryAdapter::new(publisher);

    // Archive-at-the-sealer durability sidecar (the locked durability
    // decision). Runs on its own OS thread because the AeronArchive +
    // QuorumPublisher are thread-confined (`!Send`). It records this sealer's
    // own tx_ordering MDC publication and publishes the recording's durable
    // position as the single durable watermark ingress gates on.
    let mut durability_handle: Option<std::thread::JoinHandle<()>> = None;
    let durability_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if args.archive_durability {
        anyhow::ensure!(
            mdc,
            "--archive-durability requires tx_ordering MDC (set tx_ordering_mdc_* in --log-config)"
        );
        let control_uri = channels.tx_ordering_mdc_control_for(
            cfg.channel_b_mdc_control
                .as_deref()
                .context("--archive-durability requires channel_b_mdc_control in the sealer config")?,
        )?;
        let resolved = LogConfig::resolve(args.log_config.as_deref())
            .context("resolve log config for durability")?;
        durability_handle = Some(spawn_durability_sidecar(
            args.aeron_dir.clone(),
            control_uri,
            channels.clone(),
            resolved.aeron,
            Duration::from_millis(args.durable_poll_interval_ms),
            durability_stop.clone(),
        ));
    }

    // Spawn a tail-tracker: subscribe to tx_ordering, forward every
    // observed fragment's position into the adapter's `last_pos`. The
    // executor checks `BoundaryStart.end_tx_idx == last_processed_pos`
    // for alignment, so the sealer must stamp the boundary with the
    // actual stream tail — not just positions it has itself published.
    let mut tail_sub = TxOrderingSubscriberHandle::open(&rt, &channels)
        .context("open TxOrderingSubscriberHandle for tail tracker")?;
    let last_pos = adapter.last_pos_handle();
    tokio::spawn(async move {
        while let Some((pos, _msg)) = tail_sub.recv().await {
            *last_pos.lock().unwrap() = pos;
        }
    });

    let sealer = Sealer::new(cfg.clone(), SystemClock, adapter, initial_block)
        .context("construct Sealer")?;

    // Race the sealer's run_forever against a shutdown signal.
    let run_handle = tokio::spawn(async move {
        if let Err(e) = sealer.run_forever().await {
            tracing::error!(error = %e, "sealer.run_forever exited with error");
        }
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-sealer: shutdown signal received; aborting run loop");
    run_handle.abort();
    let _ = run_handle.await;
    durability_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(h) = durability_handle {
        let _ = h.join();
    }
    drop(rt);
    Ok(())
}

/// Spawn the archive-at-the-sealer durability sidecar on a dedicated OS
/// thread. Connects the co-located Aeron Archive, records the sealer's own
/// tx_ordering MDC publication (`control_uri`), and publishes the recording's
/// byte-durable position as the single durable watermark on
/// `quorum_watermark_channel`. The thread runs `run_durable_watermark_loop`
/// until `stop` is set. Errors are logged (the sidecar is best-effort liveness
/// — if it dies, ingress's `on-quorum` acks stall but no committed data is at
/// risk, mirroring the old quorum aggregator's liveness-only contract).
fn spawn_durability_sidecar(
    aeron_dir: Option<PathBuf>,
    control_uri: String,
    channels: ChannelsConfig,
    aeron_cfg: AeronConfig,
    poll_interval: Duration,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("sealer-durability".into())
        .spawn(move || {
            if let Err(e) = run_durability_sidecar(
                aeron_dir.as_deref(),
                &control_uri,
                &channels,
                &aeron_cfg,
                poll_interval,
                stop,
            ) {
                tracing::error!(error = %e, "sealer durability sidecar exited with error");
            }
        })
        .expect("spawn sealer durability sidecar thread")
}

fn run_durability_sidecar(
    aeron_dir: Option<&std::path::Path>,
    control_uri: &str,
    channels: &ChannelsConfig,
    aeron_cfg: &AeronConfig,
    poll_interval: Duration,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    let session = connect_archive(aeron_dir, aeron_cfg).context("connect archive")?;
    let client = connect_client(aeron_dir).context("connect aeron client")?;
    // recorder_id 0: there is exactly one archive (single durable watermark).
    let recorder = Recorder::start_b_mdc(
        session.archive,
        control_uri,
        channels,
        0,
        aeron_cfg.archive_dir.clone(),
    )
    .context("start_b_mdc recording")?;
    let publisher = QuorumPublisher::open(&client, channels).context("open QuorumPublisher")?;
    tracing::info!(
        recording_id = recorder.recording_id(),
        "sealer durability sidecar: recording tx_ordering MDC; publishing durable watermark"
    );
    let should_stop = move || stop.load(std::sync::atomic::Ordering::SeqCst);
    run_durable_watermark_loop(&recorder, &publisher, poll_interval, should_stop)
        .context("durable watermark loop")?;
    Ok(())
}

/// Subscribe to tx_ordering, drain whatever is currently in the stream, and
/// return `max(BoundaryStart::block_number) + 1`. If the stream is empty
/// (genesis) returns 1.
async fn bootstrap_block_number(rt: &AeronRuntime, channels: &ChannelsConfig) -> Result<u64> {
    let mut sub = TxOrderingSubscriberHandle::open(rt, channels)
        .context("open TxOrderingSubscriberHandle for bootstrap")?;
    // Quiesce window: drain whatever is buffered. We can't tell apart "no
    // history" from "history hasn't arrived yet"; cap at 2 s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut max_seen: Option<u64> = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some((_pos, TxOrderingMessage::BoundaryStart(b)))) => {
                max_seen = Some(max_seen.map_or(b.block_number, |m| m.max(b.block_number)));
            }
            Ok(Some(_)) => {} // TxRef — ignore for bootstrap
            Ok(None) => break,
            Err(_) => {} // timeout — keep polling until deadline
        }
    }
    Ok(max_seen.map_or(1, |n| n + 1))
}

/// `BoundaryPublisher` impl over a `TxOrderingPublisherHandle`. Tracks the
/// last published position so `current_tx_tail` returns a sensible value —
/// in the MDS topology the same stream carries both `TxRef` (from racing
/// sequencers) and `BoundaryStart` (from this sealer), so the "tail" we
/// know about is the position we last published into. Acceptable proxy for
/// `end_tx_idx` since downstream consumers reconstruct block membership
/// from in-stream order anyway.
struct TxOrderingBoundaryAdapter {
    pub_handle: TxOrderingPublisherHandle,
    last_pos: Arc<Mutex<BPosition>>,
}

impl TxOrderingBoundaryAdapter {
    /// Hand out a clone of the shared `last_pos` cell so an external
    /// tail-tracker task can keep it up-to-date as TxRefs land on
    /// tx_ordering from the racing sequencers.
    fn last_pos_handle(&self) -> Arc<Mutex<BPosition>> {
        self.last_pos.clone()
    }

    fn new(pub_handle: TxOrderingPublisherHandle) -> Self {
        Self {
            pub_handle,
            last_pos: Arc::new(Mutex::new(BPosition::ZERO)),
        }
    }
}

impl BoundaryPublisher for TxOrderingBoundaryAdapter {
    fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError> {
        match self.pub_handle.publish_boundary(msg) {
            Ok(pos) => {
                *self.last_pos.lock().unwrap() = pos;
                Ok(pos)
            }
            Err(e) => Err(PublishError::Fatal(e.to_string())),
        }
    }

    fn current_tx_tail(&self) -> BPosition {
        *self.last_pos.lock().unwrap()
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
