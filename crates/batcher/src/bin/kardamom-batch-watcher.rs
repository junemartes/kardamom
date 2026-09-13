//! `kardamom-batch-watcher`: the honest challenger. It compares pending
//! optimistic claims against the validator's prover spool. On divergence,
//! it submits `challengeBlock` at the first divergent offset, with the
//! single-block proof files the prover made (`zk-host --prove` on the
//! spooled frame).
//!
//! A thin driver over [`kardamom_batcher::BatchWatcher`]. This makes
//! the optimistic mode's liveness assumption concrete: at least one honest
//! watcher runs. Slashing pays that watcher, so the assumption has an
//! incentive behind it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_batcher::error::BatcherError;
use std::ops::ControlFlow;

use kardamom_batcher::live::poll::{PollLoop, Retry, parse_interval_secs};
use kardamom_batcher::{BatchWatcher, WatchOutcome};

#[derive(Debug, Parser)]
#[command(name = "kardamom-batch-watcher", version)]
struct Args {
    #[arg(long, env = "KARDAMOM_L1_RPC_URL")]
    l1_rpc_url: String,
    #[arg(long, env = "KARDAMOM_WATCHER_KEY")]
    private_key: String,
    #[arg(long, env = "KARDAMOM_PROOF_ORACLE")]
    oracle: Address,
    /// The validator's prover spool. The ground-truth roots come from here,
    /// and single-block proof files are read from here.
    #[arg(long, env = "KARDAMOM_SPOOL_DIR")]
    spool_dir: PathBuf,
    /// 0 means run once and stop.
    #[arg(long, default_value = "15", value_parser = parse_interval_secs)]
    interval_secs: Option<Duration>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let signer: PrivateKeySigner = args.private_key.parse().context("parse --private-key")?;
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(args.l1_rpc_url.parse().context("parse --l1-rpc-url")?);

    Watcher::new(args.oracle, &args.spool_dir, args.interval_secs)
        .run(&provider)
        .await;
    Ok(())
}

/// The `kardamom-batch-watcher` reactor: the challenge driver and the
/// poll gate. Each tick compares the pending claim against the prover
/// spool, challenges on divergence, then gates the next tick on the
/// outcome.
struct Watcher {
    watcher: BatchWatcher,
    gate: PollLoop,
}

impl Watcher {
    fn new(oracle: Address, spool_dir: &Path, interval: Option<Duration>) -> Self {
        Self {
            watcher: BatchWatcher::new(oracle, spool_dir),
            gate: PollLoop::new(interval),
        }
    }

    /// Tick until the gate stops the loop.
    async fn run(&self, provider: &(impl alloy_provider::Provider + Clone)) {
        while let ControlFlow::Continue(()) = self.tick(provider).await {}
    }

    async fn tick(&self, provider: &(impl alloy_provider::Provider + Clone)) -> ControlFlow<()> {
        let outcome = self.watcher.watch_and_challenge(provider.clone()).await;
        Self::report(outcome);
        self.gate.gate(Retry::AfterInterval).await
    }

    /// Log one watch attempt's outcome.
    fn report(outcome: Result<WatchOutcome, BatcherError>) {
        match outcome {
            Ok(WatchOutcome::Challenged {
                batch_index,
                block_offset,
            }) => {
                tracing::warn!(
                    batch_index,
                    block_offset,
                    "CHALLENGE submitted — divergence proven"
                );
            }
            Ok(WatchOutcome::ClaimHonest { batch_index }) => {
                tracing::debug!(batch_index, "pending claim matches the spool");
            }
            Ok(WatchOutcome::ProofNotReady {
                batch_index,
                divergent_block,
            }) => {
                tracing::warn!(
                    batch_index,
                    divergent_block,
                    "divergence detected — awaiting single-block proof (zk-host --prove)"
                );
            }
            Ok(WatchOutcome::NothingPending) => tracing::debug!("no pending claims"),
            Err(e) => tracing::error!(error = %e, "watch attempt failed"),
        }
    }
}
