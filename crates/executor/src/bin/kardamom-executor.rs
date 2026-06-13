//! `kardamom-executor`: standalone executor service process.
//!
//! Opens M tx_data subscribers + 1 tx_ordering subscriber + 1 tx_receipts
//! publisher via the log layer's Aeron runtime, wires them into the
//! executor's reader/exec/commit thread topology, and runs until SIGTERM /
//! Ctrl-C. The state backend in this v0 deployment is the in-memory
//! `MockStateDatabase`; operators wanting libmdbx persistence swap in a
//! `WriterApplyingQueue` over `state::StateWriter` in a follow-up.

use std::path::PathBuf;
use std::sync::mpsc as sync_mpsc;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_executor::{
    CMessage, DepositSubscription, Executor, ExecutorConfig, ExecutorError, MockStateDatabase,
    MutatingSnapshotSource, TxDataSubscription, TxOrderingSubscription, TxReceiptsPublication,
    WriterApplyingQueue,
};
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxOrderingSubscriberHandle,
    TxReceiptsPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_types::{BPosition, Deposit, TxEnvelope, TxOrderingMessage};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-executor",
    version,
    about = "kardamom executor process"
)]
struct Args {
    /// Path to the TOML config file (schema: `ExecutorConfig`).
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
    /// Number of tx_data shards to subscribe to (defaults to 8 — matches
    /// the default `partition_count` in the sequencer).
    #[arg(long, default_value_t = 8)]
    shards: u8,
    /// L2 chain id (used for revm).
    #[arg(long, default_value_t = 1)]
    chain_id: u64,
    /// Block number to seed the executor's monotone counter with on
    /// startup. Normally 1 for genesis; operators replay from a snapshot
    /// by bumping this.
    #[arg(long, default_value_t = 1)]
    initial_block: u64,
    /// Path to a genesis TOML (schema: `kardamom_types::Genesis`). The
    /// chain id is taken from this file (must match `--chain-id` if both
    /// are set), and every `[[alloc]]` entry seeds the in-memory state
    /// DB with the listed balance / nonce / code so revm has account
    /// state to debit on the first transaction from each sender.
    #[arg(long)]
    chain: Option<PathBuf>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9004")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "executor",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    kardamom_executor::metrics::describe();
    // Validate the config file exists so misconfigured deployments fail
    // fast. v0 doesn't yet drive ExecutorConfig from the TOML — operators
    // tune via the CLI flags above.
    let _raw =
        std::fs::read_to_string(&args.config).context("read executor config (presence check)")?;

    tracing::info!(
        shards = args.shards,
        chain_id = args.chain_id,
        initial_block = args.initial_block,
        "kardamom-executor starting"
    );

    let channels = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    // M tx_data subscriptions, one per shard. We bridge each handle's
    // async `recv()` to a synchronous `next()` (what the executor's reader
    // thread expects) through a `std::sync::mpsc::channel`, with a
    // dedicated tokio pump task per shard.
    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(args.shards as usize);
    for shard_id in 0..args.shards {
        let mut handle = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
            .with_context(|| format!("open TxDataSubscriberHandle shard={shard_id}"))?;
        let (tx, rx) = sync_mpsc::channel::<(BPosition, TxEnvelope)>();
        tokio::spawn(async move {
            while let Some(item) = handle.recv().await {
                if tx.send(item).is_err() {
                    break;
                }
            }
        });
        a_subs.push(Box::new(LiveTxDataSub {
            sequencer_id: shard_id,
            rx,
        }));
    }

    // 1 tx_ordering subscription, same async→sync bridge.
    let mut tx_ordering_handle = TxOrderingSubscriberHandle::open(&rt, &channels)
        .context("open TxOrderingSubscriberHandle")?;
    let (b_tx, b_rx) = sync_mpsc::channel::<(BPosition, TxOrderingMessage)>();
    tokio::spawn(async move {
        while let Some(item) = tx_ordering_handle.recv().await {
            if b_tx.send(item).is_err() {
                break;
            }
        }
    });
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(LiveTxOrderingSub { rx: b_rx });

    // 1 tx_deposits subscription. The executor's deposit reader thread
    // joins `(deposit_position, Deposit)` envelopes against the
    // `DepositRef`s observed on tx_ordering; `source_hash` is the dedup
    // key. Same async→sync bridge as tx_ordering above.
    let mut tx_deposits_handle = TxDepositsSubscriberHandle::open(&rt, &channels)
        .context("open TxDepositsSubscriberHandle")?;
    let (d_tx, d_rx) = sync_mpsc::channel::<(BPosition, Deposit)>();
    tokio::spawn(async move {
        while let Some(item) = tx_deposits_handle.recv().await {
            if d_tx.send(item).is_err() {
                break;
            }
        }
    });
    let dep_sub: Box<dyn DepositSubscription> = Box::new(LiveTxDepositsSub { rx: d_rx });

    // tx_receipts publication.
    let c_handle = TxReceiptsPublisherHandle::open(&rt, &channels)
        .context("open TxReceiptsPublisherHandle")?;
    let c_pub = LiveTxReceiptsPub { handle: c_handle };

    // Load genesis if provided; seed the in-memory state DB and adopt
    // its chain_id when present.
    let genesis = match args.chain.as_ref() {
        Some(path) => Some(load_genesis(path)?),
        None => None,
    };
    let chain_id = genesis
        .as_ref()
        .map(|g| g.chain_id)
        .unwrap_or(args.chain_id);
    if let Some(g) = &genesis
        && args.chain_id != 1
        && args.chain_id != g.chain_id
    {
        anyhow::bail!(
            "--chain-id {} conflicts with genesis chain_id {}",
            args.chain_id,
            g.chain_id
        );
    }

    // In-memory state backend (v0). Sharing one MockStateDatabase between
    // the snapshot source (read) and the state-writer queue (write) gives
    // the executor a self-consistent view; future revisions plug in
    // `state::StateWriter` here.
    let db = build_genesis_db(genesis.as_ref());
    let snapshots = MutatingSnapshotSource(db.clone());
    let sw_queue = WriterApplyingQueue::new(db);
    let sw_signal = ImmediateSignal;

    let cfg = ExecutorConfig {
        chain_id,
        ..ExecutorConfig::default()
    };
    let initial_block = args.initial_block;

    // The executor's main loop is sync (std::thread spawns underneath).
    // Run it inside spawn_blocking so the runtime stays responsive for
    // shutdown handling.
    let join = tokio::task::spawn_blocking(move || -> Result<(), ExecutorError> {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            Some(dep_sub),
            c_pub,
            snapshots,
            sw_signal,
            sw_queue,
            initial_block,
        )
    });

    wait_for_shutdown().await;
    tracing::info!("kardamom-executor: shutdown signal received; dropping runtime");
    // Dropping the AeronRuntime closes every subscription, which causes
    // the tokio pump tasks to return None, which closes the sync mpsc
    // channels, which surfaces TxDataClosed / TxOrderingClosed to the
    // executor — clean shutdown.
    drop(rt);
    match join.await {
        Ok(Ok(())) => tracing::info!("executor main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "executor main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "executor task panicked"),
    }
    Ok(())
}

/// Load a kardamom genesis TOML and run its semantic validation
/// (chain_id != 0, no duplicate alloc addresses).
fn load_genesis(path: &std::path::Path) -> Result<kardamom_types::Genesis> {
    let raw = std::fs::read_to_string(path).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    Ok(genesis)
}

/// Seed a fresh `MockStateDatabase` from a `Genesis` (or build an empty one
/// if none was supplied). Every `AllocEntry` becomes one account with the
/// declared balance, nonce, and code (if any). The code's keccak256 hash is
/// stored as the account's `code_hash` and the code bytes are made
/// retrievable via `code_by_hash`.
fn build_genesis_db(genesis: Option<&kardamom_types::Genesis>) -> MockStateDatabase {
    use alloy_primitives::{B256, keccak256};
    let mut builder = MockStateDatabase::builder();
    let Some(g) = genesis else {
        return builder.build();
    };
    for entry in &g.alloc {
        let nonce = entry.nonce.unwrap_or(0);
        let code_hash = entry
            .code
            .as_ref()
            .map(|c| keccak256(c.as_ref()))
            .unwrap_or(B256::ZERO);
        tracing::info!(
            address = ?entry.address,
            balance = %entry.balance,
            nonce,
            has_code = entry.code.is_some(),
            "seeding genesis account"
        );
        builder = builder.account(entry.address, entry.balance, nonce, code_hash);
        if let Some(code) = entry.code.as_ref() {
            // `Genesis::AllocEntry.code` is `alloy_primitives::Bytes`;
            // MockStateDatabase wants `bytes::Bytes`. They're separate
            // newtypes that share the same underlying buffer.
            builder = builder.code(code_hash, code.0.clone());
        }
    }
    builder.build()
}

// ---------------------------------------------------------------------------
// Adapters: async log handles → sync executor traits.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    sequencer_id: u8,
    rx: sync_mpsc::Receiver<(BPosition, TxEnvelope)>,
}

impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(BPosition, TxEnvelope), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

struct LiveTxOrderingSub {
    rx: sync_mpsc::Receiver<(BPosition, TxOrderingMessage)>,
}

impl TxOrderingSubscription for LiveTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}

struct LiveTxDepositsSub {
    rx: sync_mpsc::Receiver<(BPosition, Deposit)>,
}

impl DepositSubscription for LiveTxDepositsSub {
    fn next(&mut self) -> Result<(BPosition, Deposit), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::DepositsClosed)
    }
}

struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self
                .handle
                .publish_receipt(&r)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_receipt: {e}"))),
            CMessage::BlockBoundary(b) => self
                .handle
                .publish_boundary(&b)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_boundary: {e}"))),
        }
    }
}

struct ImmediateSignal;

impl kardamom_executor::StateWriterSignal for ImmediateSignal {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
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
