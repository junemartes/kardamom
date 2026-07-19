//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the Aeron Cluster (Raft) ref publisher (tx_ordering) + a tx_errors publisher
//! for rejection signals, and runs the sequencer main loop on a dedicated
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
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::deposit::{DepositSubscriber, process_deposit};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{
    BPosition, Deposit, Receipt, StateDatabase, StateError, TxDataLoc, TxEnvelope, TxError,
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
    /// Replica-group shard rotation: the effective partition becomes
    /// `(partition_index + partition_offset) % partition_count`, and
    /// `sequencer_id` follows it (unless explicitly overridden). Lets a
    /// second Nomad group of racing replicas reuse the same node-derived
    /// `--partition-index` while serving a rotated shard, so the two
    /// replicas of any shard land on different nodes deterministically.
    /// Incompatible with an explicit `--sequencer-id` (the tx_data
    /// subscription and `TxRef.shard_id` must both follow the rotated
    /// shard).
    /// Racing replicas are safe by construction: refs encode
    /// deterministically from the shared per-shard tx_data stream and the
    /// Aeron Cluster dedups records by canonical_id first-seen — the same
    /// mechanism that already absorbs the M duplicate DepositRefs.
    #[arg(long, default_value_t = 0)]
    partition_offset: u32,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
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
    if args.partition_offset != 0 {
        // An explicit --sequencer-id combined with rotation would subscribe
        // to tx_data stream `sequencer_id` while the wrong-shard guard
        // filters on the rotated `partition_index`: the replica would
        // silently drop every envelope (and its TxRefs would stamp a
        // shard_id its twin doesn't, breaking byte-identical dedup).
        anyhow::ensure!(
            args.sequencer_id.is_none(),
            "--sequencer-id cannot be combined with --partition-offset: \
             the sequencer id must follow the rotated shard \
             (sequencer_id == partition_index)"
        );
        let raw_index = cfg.partition_index;
        cfg.rotate_partition(args.partition_offset);
        tracing::info!(
            raw_index,
            offset = args.partition_offset,
            rotated = cfg.partition_index,
            "partition-offset: rotated shard assignment (racing replica group)"
        );
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
    // injected by the Nomad job rather than baked into the static config template.
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
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

    let shard_id = cfg.sequencer_id;
    let tx_data_sub = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
        .context("open TxDataSubscriberHandle")?;
    let tx_deposits_sub = TxDepositsSubscriberHandle::open(&rt, &channels)
        .context("open TxDepositsSubscriberHandle")?;
    let tx_errors_pub =
        TxErrorsPublisherHandle::open(&rt, &channels).context("open TxErrorsPublisherHandle")?;

    let shutdown = Shutdown::new();
    let shutdown_for_main = shutdown.clone();
    let shutdown_for_deposits = shutdown.clone();

    let state_db = Arc::new(EmptyStateDatabase);
    tracing::info!(
        "nonce floors: no committed-state reader wired (EmptyStateDatabase); \
         cold senders seed at 0. NOTE: a restarted replica does NOT regain \
         coverage of established senders (F02.1 re-opened — the floor \
         fast-forward was removed for publishing canonical nonce gaps)"
    );
    let cfg_clone = cfg.clone();

    // tx_ordering is ALWAYS published to the Aeron Cluster (Raft) ingress. The
    // cluster-session guard (`LiveCluster`) + its dedicated Aeron runtime must
    // outlive both publish loops, so bind the guard in the outer scope; it is
    // dropped only after the `join_*` awaits below.
    //
    // DEDICATED cluster runtime (own Aeron thread, same aeron dir) so the cluster
    // session never contends with the tx_data subscription on the main `rt`.
    let cluster_rt = match args.aeron_dir.as_ref() {
        Some(dir) => {
            AeronRuntime::spawn_with_dir(dir).context("spawn cluster AeronRuntime with dir")?
        }
        None => AeronRuntime::spawn_default().context("spawn cluster AeronRuntime")?,
    };
    let (cluster_guard, cluster_pub) =
        kardamom_sequencer::outbound::cluster::cluster_ref_publisher(
            cluster_rt,
            cfg.cluster.to_live(),
        )
        .context("connect cluster ref publisher")?;
    tracing::info!("kardamom-sequencer: tx_ordering via Aeron Cluster");
    // Clone shares the single session thread; offers serialise through it. Both
    // loops use `cluster_pub` (impl `TxOrderingRefPublisher`) — the canonical
    // `TxRef` loop and the `DepositRef` pump.
    let (join_main, join_deposits) = spawn_publish_loops(
        cfg_clone,
        state_db,
        LiveTxDataSub::new(tx_data_sub),
        cluster_pub.clone(),
        cluster_pub,
        LiveTxErrorPub::new(tx_errors_pub),
        LiveDepositSub::new(tx_deposits_sub),
        shutdown_for_main,
        shutdown_for_deposits,
    );

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
    // Drop the cluster session only after both loops have stopped.
    drop(cluster_guard);
    drop(rt);
    Ok(())
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
    fn poll(&mut self) -> Result<Option<(TxDataLoc, TxEnvelope)>, SequencerError> {
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
// Empty StateDatabase: cache-miss hydration always seeds at nonce 0, i.e. a
// floor that can only LAG the true next nonce (never lead it — so no valid tx
// is ever spuriously rejected). KNOWN LIMITATION (F02.1, re-opened): a
// restarted replica buffers established senders' traffic against the stale
// floor and does not regain coverage of them; the floor fast-forward that
// closed this was removed for adopting client-abandoned gaps into the
// canonical stream (fatal to executors). Wiring a real committed-state
// reader here is the sound fix. Lives in the bin because it's a deployment
// choice.
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
