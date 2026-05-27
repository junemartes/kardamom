//! kardamom-da-watcher: L1 deposit monitor CLI.
//!
//! Parses an `--l1-rpc <URL>` + `--lockbox <ADDRESS>` (plus an optional
//! `--poll-interval <DURATION>`, default 12s), constructs an
//! [`da_watcher::RpcL1Source`] over an alloy HTTP provider, spawns the
//! watcher loop, and waits for ctrl-c. Deposits the watcher emits are
//! forwarded to a `DepositPublisher`.
//!
//! The Aeron-backed `DepositPublisher` adapter (wraps
//! `kardamom_log::TxDepositsPublisher`) lives behind a future production wiring
//! commit — `rusteron_client::Aeron` is `!Send + !Sync` so it needs a
//! dedicated thread + Send-able channel, the same pattern the sequencer
//! and executor binaries are following. Until that adapter lands, this
//! binary uses the [`TracingDepositPublisher`] below: every deposit gets
//! logged at `info` so smoke tests + dry runs work end-to-end.
//!
//! This mirrors the staging pattern in `kardamom-sequencer`: the CLI
//! scaffold + RPC reads are correct and exercisable today; the publish
//! sink lands in a follow-up alongside the per-binary Aeron client wiring.

use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use clap::Parser;
use tokio::signal;

use kardamom_da_watcher::{
    DaWatcherConfig, DepositPublisher, PublishError, RpcL1Source, spawn as spawn_watcher,
};
use kardamom_types::{BPosition, Deposit};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-da-watcher",
    version,
    about = "L1 deposit monitor — tails finalized L1 blocks and publishes Deposits onto tx_deposits"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoint (e.g. `http://127.0.0.1:8545`).
    #[arg(long)]
    l1_rpc: String,
    /// L1 address of the `ETHLockbox` proxy this L2 chain id maps to.
    #[arg(long)]
    lockbox: String,
    /// Polling cadence in seconds (default 12).
    #[arg(long, default_value_t = 12)]
    poll_interval_secs: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let lockbox = Address::from_str(&args.lockbox)
        .map_err(|e| anyhow::anyhow!("--lockbox is not a valid address: {e}"))?;
    let cfg = DaWatcherConfig {
        lockbox,
        poll_interval: Duration::from_secs(args.poll_interval_secs),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        let provider = ProviderBuilder::new()
            .connect(&args.l1_rpc)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to L1 RPC {}: {e}", args.l1_rpc))?;
        let source = RpcL1Source::new(provider);
        let publisher = TracingDepositPublisher;

        tracing::info!(
            l1_rpc = %args.l1_rpc,
            ?lockbox,
            poll_interval = ?cfg.poll_interval,
            "kardamom-da-watcher starting; the Aeron-backed publisher adapter \
             is staged in a follow-up — this binary currently logs each \
             observed deposit at info level"
        );

        let handle = spawn_watcher(publisher, source, cfg);
        // Wait for ctrl-c, then ask the watcher to exit at the next tick
        // boundary. Drop on the shutdown channel is also enough to signal,
        // but explicit send() gives a clearer log line.
        signal::ctrl_c()
            .await
            .map_err(|e| anyhow::anyhow!("ctrl-c handler failed: {e}"))?;
        let _ = handle.shutdown.send(());
        handle
            .task
            .await
            .map_err(|e| anyhow::anyhow!("watcher task panicked: {e}"))?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

/// Placeholder [`DepositPublisher`] that logs each deposit at `info`. The
/// production Aeron-backed adapter (wraps `kardamom_log::TxDepositsPublisher`)
/// replaces this in a follow-up; see the module doc.
struct TracingDepositPublisher;

impl DepositPublisher for TracingDepositPublisher {
    fn publish(&self, deposit: &Deposit) -> Result<BPosition, PublishError> {
        tracing::info!(
            target: "kardamom_da_watcher",
            source_hash = ?deposit.source_hash,
            from = ?deposit.from,
            to = ?deposit.to,
            mint = deposit.mint,
            gas_limit = deposit.gas_limit,
            "deposit observed (tracing publisher — not yet on tx_deposits)"
        );
        Ok(BPosition::default())
    }
}
