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
        TxOrderingPublisherHandle::open(&rt, &channels).context("open TxOrderingPublisherHandle")?
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
        let control_uri =
            channels.tx_ordering_mdc_control_for(cfg.channel_b_mdc_control.as_deref().context(
                "--archive-durability requires channel_b_mdc_control in the sealer config",
            )?)?;
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

    // tx_ordering canonicalisation. The sealer is the SOLE publisher of the
    // canonical tx_ordering stream (the one executors read), so it must define
    // the single total order every downstream reader observes. The boundary
    // alignment key (`end_tx_idx`) is the cumulative COUNT of canonical
    // TxRef/DepositRef records — a logical, publisher- and position-independent
    // index (see BPosition::from_index) the executor matches against its own
    // processed-record count.
    if mdc {
        // CANONICAL REPUBLISH (cluster / MDC). Subscribe to the merged
        // sequencer INPUT publications, dedup the racing-replica TxRef/
        // DepositRef copies, and republish each survivor onto our own
        // canonical publication — incrementing `canonical_count` under
        // `publish_lock` so the boundary emitter stamps `end_tx_idx` exactly
        // between records (no record can be republished between the count read
        // and the boundary offer). Executors subscribe to our canonical
        // endpoint only (a single image → one total order), and count the same
        // deduped record set, so the counts match exactly.
        let mut input_sub = TxOrderingSubscriberHandle::open_input(&rt, &channels)
            .context("open TxOrderingSubscriberHandle (canonical republish input)")?;
        let republish_pub = adapter.pub_handle_clone();
        let count = adapter.count_handle();
        let publish_lock = adapter.publish_lock_handle();
        tokio::spawn(async move {
            // 8192 ids ≫ the publish spread of a tx's replica copies.
            let mut dedup = CanonicalDedup::new(8192);
            while let Some((_in_pos, msg)) = input_sub.recv().await {
                // Sequencers only emit TxRef/DepositRef; drop any stray
                // boundary, and collapse replica duplicates.
                let keep = match &msg {
                    TxOrderingMessage::TxRef(r) => dedup.first_seen(r.tx_hash.0),
                    TxOrderingMessage::DepositRef(d) => dedup.first_seen(d.source_hash.0),
                    TxOrderingMessage::BoundaryStart(_) => false,
                };
                if !keep {
                    continue;
                }
                // Hold publish_lock across the offer AND the count bump so the
                // boundary emitter can't read a count that doesn't match the
                // records on the wire. Count ONLY on a successful offer: a
                // dropped record never reaches the executor, so neither side
                // counts it — the alignment stays consistent.
                let _g = publish_lock.lock().unwrap();
                match republish_pub.publish(&msg) {
                    Ok(_pos) => *count.lock().unwrap() += 1,
                    Err(e) => tracing::warn!(
                        error = %e,
                        "canonical tx_ordering republish failed; dropping record"
                    ),
                }
            }
            tracing::info!("tx_ordering canonical republish loop exited (input closed)");
        });
    } else {
        // SINGLE-HOST / IPC fallback. Sequencers and the sealer share one
        // ordered tx_ordering channel; the executor reads that same stream. A
        // passive tail-tracker counts the canonical records (TxRef/DepositRef)
        // it observes so `end_tx_idx` carries the same cumulative count the
        // executor computes — without republishing (republishing onto the
        // shared channel we also read would loop).
        let mut tail_sub = TxOrderingSubscriberHandle::open(&rt, &channels)
            .context("open TxOrderingSubscriberHandle for tail tracker")?;
        let count = adapter.count_handle();
        tokio::spawn(async move {
            while let Some((_pos, msg)) = tail_sub.recv().await {
                if matches!(
                    msg,
                    TxOrderingMessage::TxRef(_) | TxOrderingMessage::DepositRef(_)
                ) {
                    *count.lock().unwrap() += 1;
                }
            }
        });
    }

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
    // Stop predicate shared by recording startup and the watermark loop. Borrowed
    // mutably for `start_b_mdc` (which may abort cleanly during startup), then
    // moved into the loop once that borrow ends.
    let mut should_stop = || stop.load(std::sync::atomic::Ordering::SeqCst);
    // recorder_id 0: there is exactly one archive (single durable watermark).
    let recorder = match Recorder::start_b_mdc(
        session.archive,
        control_uri,
        channels,
        0,
        aeron_cfg.archive_dir.clone(),
        &mut should_stop,
    )
    .context("start_b_mdc recording")?
    {
        Some(r) => r,
        None => {
            tracing::info!("sealer durability sidecar: stopped before recording started");
            return Ok(());
        }
    };
    let publisher = QuorumPublisher::open(&client, channels).context("open QuorumPublisher")?;
    tracing::info!(
        recording_id = recorder.recording_id(),
        "sealer durability sidecar: recording tx_ordering MDC; publishing durable watermark"
    );
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

/// `BoundaryPublisher` impl over the sealer's **canonical**
/// `TxOrderingPublisherHandle`. The sealer is the SOLE publisher of the
/// canonical tx_ordering stream: the republish loop (see `main`) republishes
/// every deduped `TxRef`/`DepositRef` here, and this adapter publishes the
/// block `BoundaryStart`s here. Both share `canonical_count` (the cumulative
/// count of canonical records published) and `publish_lock`.
///
/// `end_tx_idx` correctness: the executor compares its own cumulative
/// processed-record count against `BoundaryStart.end_tx_idx` (decoded via
/// `BPosition::as_index`). So the boundary MUST be stamped with the count of
/// records republished BEFORE it, with NO record interleaved between the count
/// read and the boundary offer. We guarantee this by (a) re-stamping
/// `end_tx_idx` from `canonical_count` here (ignoring whatever the emitter
/// computed) and (b) taking `publish_lock` across both the republish
/// offer+count bump and this read+boundary offer, so no record can be
/// published between the read and the boundary. The count (not an Aeron byte
/// position) sidesteps the per-publication term spaces of an MDC merge and the
/// offer-return vs frame-start ambiguity that broke the old position key.
struct TxOrderingBoundaryAdapter {
    pub_handle: TxOrderingPublisherHandle,
    canonical_count: Arc<Mutex<u64>>,
    publish_lock: Arc<Mutex<()>>,
}

impl TxOrderingBoundaryAdapter {
    /// Shared `canonical_count` cell — the republish loop (MDC) bumps it after
    /// each successful canonical offer; the IPC tail-tracker bumps it per
    /// observed TxRef/DepositRef. This adapter reads it to stamp `end_tx_idx`.
    fn count_handle(&self) -> Arc<Mutex<u64>> {
        self.canonical_count.clone()
    }

    /// Lock serialising every offer to the canonical publication (republish
    /// loop + boundary emitter), so `end_tx_idx` is stamped exactly between
    /// records.
    fn publish_lock_handle(&self) -> Arc<Mutex<()>> {
        self.publish_lock.clone()
    }

    /// Clone of the canonical publisher handle for the republish loop. The
    /// underlying Aeron publication is one multi-offer stream and the SDK
    /// serialises offers internally; `publish_lock` adds the `canonical_count`
    /// atomicity on top.
    fn pub_handle_clone(&self) -> TxOrderingPublisherHandle {
        self.pub_handle.clone()
    }

    fn new(pub_handle: TxOrderingPublisherHandle) -> Self {
        Self {
            pub_handle,
            canonical_count: Arc::new(Mutex::new(0)),
            publish_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl BoundaryPublisher for TxOrderingBoundaryAdapter {
    fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError> {
        // Serialise with the republish loop and stamp end_tx_idx (the canonical
        // record count, encoded as a BPosition) atomically.
        let _g = self.publish_lock.lock().unwrap();
        let mut stamped = msg.clone();
        stamped.end_tx_idx = BPosition::from_index(*self.canonical_count.lock().unwrap());
        match self.pub_handle.publish_boundary(&stamped) {
            Ok(pos) => Ok(pos),
            Err(e) => Err(PublishError::Fatal(e.to_string())),
        }
    }

    fn current_tx_tail(&self) -> BPosition {
        BPosition::from_index(*self.canonical_count.lock().unwrap())
    }
}

/// Bounded first-seen window for canonical-id dedup, FIFO-evicted. Mirrors the
/// executor's reader-side dedup: the P sequencer replicas per shard each
/// republish the same `(tx_hash)` / `(source_hash)` onto their input
/// publications, so the sealer must collapse them to one canonical record
/// before republishing (else `last_pos` would advance on a duplicate the
/// executor skips, breaking boundary alignment). Single sequencer per shard
/// today ⇒ no duplicates ⇒ this is a no-op, but it keeps the canonical stream
/// correct once replicas exist.
struct CanonicalDedup {
    seen: std::collections::HashSet<[u8; 32]>,
    fifo: std::collections::VecDeque<[u8; 32]>,
    capacity: usize,
}

impl CanonicalDedup {
    fn new(capacity: usize) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            fifo: std::collections::VecDeque::new(),
            capacity,
        }
    }

    /// Records `id`; returns `false` if it is already in the window.
    fn first_seen(&mut self, id: [u8; 32]) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.fifo.push_back(id);
        if self.fifo.len() > self.capacity
            && let Some(evicted) = self.fifo.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
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
