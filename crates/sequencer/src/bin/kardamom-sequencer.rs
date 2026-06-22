//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the canonical tx_ordering publisher + a tx_errors publisher for
//! rejection signals, and runs the sequencer main loop on a dedicated
//! blocking thread until SIGTERM / Ctrl-C.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxOrderingPublisherHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::deposit::{DepositSubscriber, process_deposit};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{
    BPosition, Deposit, DepositRef, Receipt, StateDatabase, StateError, TxEnvelope, TxError,
    TxOrderingMessage, TxRef,
};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sequencer",
    version,
    about = "kardamom sequencer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults (preserves local/e2e
    /// behaviour); multi-host deployments point this at the rendered UDP
    /// channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Override the partition index from the config.
    #[arg(long)]
    partition_index: Option<u32>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<u32>,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
    /// This sequencer's tx_ordering MDC control endpoint (`ip:port`). Required
    /// when the resolved `LogConfig` enables tx_ordering MDC
    /// (`tx_ordering_mdc_control_template` set); ignored otherwise. Must match
    /// one of the `tx_ordering_mdc_publishers` entries in the channels config.
    #[arg(long, env = "KARDAMOM_TX_ORDERING_MDC_CONTROL")]
    tx_ordering_mdc_control: Option<String>,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Overrides/sets
    /// the [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "sequencer",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;

    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = m;
    }
    if let Some(id) = args.sequencer_id {
        cfg.sequencer_id = id;
    } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
        cfg.sequencer_id = cfg.partition_index as u8;
    }
    if let Some(c) = args.core_id {
        cfg.core_id = Some(c);
    }
    // Per-node cluster egress endpoint: the cluster client's egress_channel is
    // this node's reachable address (the node IP differs per replica), so it's
    // injected by the Nomad job rather than baked into the static config
    // template. Plain config mutation — `egress_channel` is a non-feature field,
    // so no `cluster` feature gate is needed (it's a no-op when cluster is off).
    if cfg.cluster.enabled
        && let Some(ep) = args.cluster_egress_endpoint.as_deref()
    {
        cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    cfg.validate().context("validate config")?;

    tracing::info!(
        partition_index = cfg.partition_index,
        sequencer_id = cfg.sequencer_id,
        "kardamom-sequencer starting"
    );

    let channels: ChannelsConfig = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // Reject `[cluster].enabled = true` when the binary was built without the
    // `cluster` feature — fail fast (before opening any Aeron handles) rather
    // than silently taking the Aeron path.
    #[cfg(not(feature = "cluster"))]
    if cfg.cluster.enabled {
        anyhow::bail!("[cluster].enabled=true but binary built without the 'cluster' feature");
    }

    let shard_id = cfg.sequencer_id;
    let tx_data_sub = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
        .context("open TxDataSubscriberHandle")?;
    // Whether tx_ordering is carried by the Aeron Cluster instead of the
    // direct Aeron tx_ordering channel. Only true when built with the
    // `cluster` feature AND `[cluster].enabled`.
    let cluster_enabled = cfg_cluster_enabled(&cfg);
    // tx_ordering publisher: MDC (per-publisher control endpoint) in the
    // cluster, shared IPC channel single-host. When MDC is enabled the
    // control endpoint is mandatory. SKIPPED entirely in cluster mode (the
    // cluster ingress publisher carries tx_ordering instead).
    let tx_ordering_pub = if cluster_enabled {
        None
    } else if channels.tx_ordering_mdc_enabled() {
        let ctl = args.tx_ordering_mdc_control.as_deref().context(
            "tx_ordering MDC is enabled in the log config but --tx-ordering-mdc-control \
             (KARDAMOM_TX_ORDERING_MDC_CONTROL) was not supplied",
        )?;
        Some(
            TxOrderingPublisherHandle::open_mdc(&rt, &channels, ctl)
                .context("open TxOrderingPublisherHandle (MDC)")?,
        )
    } else {
        Some(
            TxOrderingPublisherHandle::open(&rt, &channels)
                .context("open TxOrderingPublisherHandle")?,
        )
    };
    let tx_deposits_sub = TxDepositsSubscriberHandle::open(&rt, &channels)
        .context("open TxDepositsSubscriberHandle")?;
    let tx_errors_pub =
        TxErrorsPublisherHandle::open(&rt, &channels).context("open TxErrorsPublisherHandle")?;

    let shutdown = Shutdown::new();
    let shutdown_for_main = shutdown.clone();
    let shutdown_for_deposits = shutdown.clone();

    let state_db = Arc::new(EmptyStateDatabase);
    let cfg_clone = cfg.clone();

    // The cluster-session guard (`LiveCluster`) and its dedicated Aeron runtime
    // must outlive both publish loops; bind them in the outer scope so they drop
    // only after the `join_*` awaits below. `None` on the Aeron path.
    #[cfg(feature = "cluster")]
    let cluster_guard;

    // Build the two `spawn_blocking` loops, branching on `cfg.cluster.enabled`
    // so each branch uses its concrete publisher type — both impl
    // `TxOrderingRefPublisher`, so the generic `spawn_publish_loops` helper
    // below works for either. (A `Box<dyn TxOrderingRefPublisher>` would NOT
    // auto-impl the trait, so we keep concrete types.)
    let (join_main, join_deposits) = {
        #[cfg(feature = "cluster")]
        if cfg.cluster.enabled {
            // DEDICATED cluster runtime (own Aeron thread, same aeron dir) so
            // the cluster session never contends with the tx_data subscription
            // on the main `rt`.
            let cluster_rt = match args.aeron_dir.as_ref() {
                Some(dir) => AeronRuntime::spawn_with_dir(dir)
                    .context("spawn cluster AeronRuntime with dir")?,
                None => AeronRuntime::spawn_default().context("spawn cluster AeronRuntime")?,
            };
            let (guard, cluster_pub) = kardamom_sequencer::outbound::cluster::cluster_ref_publisher(
                cluster_rt,
                cfg.cluster.to_live(),
            )
            .context("connect cluster ref publisher")?;
            cluster_guard = Some(guard);
            tracing::info!("kardamom-sequencer: tx_ordering via Aeron Cluster");
            // Clone shares the single session thread; offers serialise through it.
            spawn_publish_loops(
                cfg_clone,
                state_db,
                LiveTxDataSub::new(tx_data_sub),
                cluster_pub.clone(),
                cluster_pub,
                LiveTxErrorPub::new(tx_errors_pub),
                LiveDepositSub::new(tx_deposits_sub),
                shutdown_for_main,
                shutdown_for_deposits,
            )
        } else {
            cluster_guard = None;
            spawn_aeron_loops(
                cfg_clone,
                state_db,
                tx_data_sub,
                tx_ordering_pub.expect("tx_ordering_pub opened on the Aeron path"),
                tx_errors_pub,
                tx_deposits_sub,
                shutdown_for_main,
                shutdown_for_deposits,
            )
        }

        #[cfg(not(feature = "cluster"))]
        spawn_aeron_loops(
            cfg_clone,
            state_db,
            tx_data_sub,
            tx_ordering_pub.expect("tx_ordering_pub opened on the Aeron path"),
            tx_errors_pub,
            tx_deposits_sub,
            shutdown_for_main,
            shutdown_for_deposits,
        )
    };

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    shutdown.signal();
    match join_main.await {
        Ok(Ok(())) => tracing::info!("sequencer main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer task panicked"),
    }
    match join_deposits.await {
        Ok(Ok(())) => tracing::info!("sequencer deposit pump returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer deposit pump returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer deposit task panicked"),
    }
    // Drop the cluster session (if any) only after both loops have stopped.
    #[cfg(feature = "cluster")]
    drop(cluster_guard);
    drop(rt);
    Ok(())
}

/// Whether tx_ordering should be carried by the Aeron Cluster. True only when
/// built with the `cluster` feature AND `[cluster].enabled`. Without the
/// feature this is always `false` (and `enabled=true` is rejected earlier with
/// a clear error), so the Aeron path is taken unconditionally.
fn cfg_cluster_enabled(cfg: &SequencerConfig) -> bool {
    #[cfg(feature = "cluster")]
    {
        cfg.cluster.enabled
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = cfg;
        false
    }
}

type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Spawn the main sequencer loop + the deposit pump over a pair of
/// `TxOrderingRefPublisher`s (`main_pub` for the canonical `TxRef` loop,
/// `deposit_pub` for the `DepositRef` pump). Generic over the publisher type so
/// the Aeron and cluster branches share one implementation; both supply
/// concrete publishers that impl the trait.
#[allow(clippy::too_many_arguments)]
fn spawn_publish_loops<P>(
    cfg: SequencerConfig,
    state_db: Arc<EmptyStateDatabase>,
    mut tx_data: LiveTxDataSub,
    main_pub: P,
    deposit_pub: P,
    mut tx_errors: LiveTxErrorPub,
    mut deposit_sub: LiveDepositSub,
    shutdown_for_main: Shutdown,
    shutdown_for_deposits: Shutdown,
) -> (LoopHandle, LoopHandle)
where
    P: TxOrderingRefPublisher + Send + 'static,
{
    // The sequencer main loop is sync (std::thread + std::thread::sleep
    // backoff). Hand it to spawn_blocking so the async runtime stays
    // responsive for shutdown handling.
    let mut main_pub = main_pub;
    let join_main = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut sequencer = Sequencer::new(cfg, state_db);
        sequencer.run(
            &mut tx_data,
            &mut main_pub,
            &mut tx_errors,
            shutdown_for_main,
        )
    });

    // Independent pump for tx_deposits → DepositRef on tx_ordering. The
    // deposit path is not nonce-gated; it's a simple poll → publish loop
    // that runs alongside the canonical TxData → TxRef path.
    let mut deposit_pub = deposit_pub;
    let join_deposits = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut backoff_us = 1u64;
        loop {
            if shutdown_for_deposits.is_signaled() {
                return Ok(());
            }
            match process_deposit(&mut deposit_sub, &mut deposit_pub) {
                Ok(true) => backoff_us = 1,
                Ok(false) => {
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = backoff_us.saturating_mul(2).min(100);
                }
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    });

    (join_main, join_deposits)
}

/// Aeron-backed variant: the tx_ordering publisher carries both `TxRef` (main
/// loop) and `DepositRef` (deposit pump). Cloning the handle is sound — the
/// underlying Aeron publication is a single multi-publisher stream and the SDK
/// serialises offers internally — so both loops share one publication.
#[allow(clippy::too_many_arguments)]
fn spawn_aeron_loops(
    cfg: SequencerConfig,
    state_db: Arc<EmptyStateDatabase>,
    tx_data_sub: TxDataSubscriberHandle,
    tx_ordering_pub: TxOrderingPublisherHandle,
    tx_errors_pub: TxErrorsPublisherHandle,
    tx_deposits_sub: TxDepositsSubscriberHandle,
    shutdown_for_main: Shutdown,
    shutdown_for_deposits: Shutdown,
) -> (LoopHandle, LoopHandle) {
    let tx_ordering_pub_for_deposits = tx_ordering_pub.clone();
    spawn_publish_loops(
        cfg,
        state_db,
        LiveTxDataSub::new(tx_data_sub),
        LiveTxOrderingRefPub::new(tx_ordering_pub),
        LiveTxOrderingRefPub::new(tx_ordering_pub_for_deposits),
        LiveTxErrorPub::new(tx_errors_pub),
        LiveDepositSub::new(tx_deposits_sub),
        shutdown_for_main,
        shutdown_for_deposits,
    )
}

// ---------------------------------------------------------------------------
// Adapters: log::aeron_live handles → sequencer trait surface.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    handle: TxDataSubscriberHandle,
}

impl LiveTxDataSub {
    fn new(handle: TxDataSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl TxDataSubscriber for LiveTxDataSub {
    fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        Ok(self.handle.try_recv())
    }
}

struct LiveDepositSub {
    handle: TxDepositsSubscriberHandle,
}

impl LiveDepositSub {
    fn new(handle: TxDepositsSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl DepositSubscriber for LiveDepositSub {
    fn poll(&mut self) -> Result<Option<(BPosition, Deposit)>, SequencerError> {
        Ok(self.handle.try_recv())
    }
}

struct LiveTxOrderingRefPub {
    handle: TxOrderingPublisherHandle,
}

impl LiveTxOrderingRefPub {
    fn new(handle: TxOrderingPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxOrderingRefPublisher for LiveTxOrderingRefPub {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError> {
        publish_ordering(&self.handle, TxOrderingMessage::TxRef(*r))
    }

    fn try_publish_deposit_ref(&mut self, r: &DepositRef) -> Result<(), SequencerError> {
        publish_ordering(&self.handle, TxOrderingMessage::DepositRef(*r))
    }
}

fn publish_ordering(
    handle: &TxOrderingPublisherHandle,
    msg: TxOrderingMessage,
) -> Result<(), SequencerError> {
    match handle.publish(&msg) {
        Ok(_pos) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            // The AeronRuntime publish loop retries on back-pressure
            // internally (up to 1024 attempts); a returned error means
            // we hit the cap. Surface as Backpressure so the sequencer
            // state machine rewinds and retries on the next pass.
            if msg.contains("back-pressure") {
                Err(SequencerError::Backpressure)
            } else {
                tracing::error!(error = %msg, "tx_ordering publish failed");
                Err(SequencerError::Backpressure)
            }
        }
    }
}

/// Live `TxErrorPublisher` wrapping a `TxErrorsPublisherHandle`. Sequencer-
/// emitted rejections (duplicate / past-nonce today) are published on the
/// `tx_errors` Aeron channel; ingress consumes them to release parked
/// clients early. Publish failures are logged and dropped — the canonical
/// state has already advanced (or the tx was rejected), so there is nothing
/// to roll back.
struct LiveTxErrorPub {
    handle: TxErrorsPublisherHandle,
}

impl LiveTxErrorPub {
    fn new(handle: TxErrorsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxErrorPublisher for LiveTxErrorPub {
    fn publish_error(&mut self, e: TxError) {
        if let Err(err) = self.handle.publish(&e) {
            tracing::warn!(error = %err, "tx_errors publish failed (dropped)");
        }
    }
}

// ---------------------------------------------------------------------------
// Empty StateDatabase: cache-miss hydration always seeds at nonce 0. Sound for
// fresh chains; on warm restarts the operator should wire a real read-only
// libmdbx snapshot here. Lives in the bin because it's a deployment choice.
// ---------------------------------------------------------------------------

struct EmptyStateDatabase;

#[derive(Debug)]
struct EmptyStateError;

impl std::fmt::Display for EmptyStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyStateDatabase: no state available")
    }
}

impl std::error::Error for EmptyStateError {}
impl StateError for EmptyStateError {}

impl StateDatabase for EmptyStateDatabase {
    type Error = EmptyStateError;

    fn basic(&self, _: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Ok(None)
    }
    fn storage(&self, _: Address, _: B256) -> Result<U256, Self::Error> {
        Ok(U256::ZERO)
    }
    fn code_by_hash(&self, _: B256) -> Result<Bytes, Self::Error> {
        Ok(Bytes::new())
    }
    fn get_receipt(&self, _: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(None)
    }
    fn get_tx_position(&self, _: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(None)
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
