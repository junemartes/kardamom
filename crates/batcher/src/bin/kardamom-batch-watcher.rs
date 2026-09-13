//! `kardamom-batch-watcher`: the honest challenger. It compares pending
//! optimistic claims against the validator's prover spool. On divergence,
//! it submits `challengeBlock` at the first divergent offset, with the
//! single-block proof files the prover made (`zk-host --prove` on the
//! spooled frame).
//!
//! A thin driver over [`kardamom_batcher::watch_and_challenge`]. This makes
//! the optimistic mode's liveness assumption concrete: at least one honest
//! watcher runs. Slashing pays that watcher, so the assumption has an
//! incentive behind it.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_batcher::error::BatcherError;
use std::ops::ControlFlow;

use kardamom_batcher::live::poll::{PollLoop, Retry, parse_interval_secs};
use kardamom_batcher::{WatchOutcome, watch_and_challenge};

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

    let watcher = Watcher {
        oracle: args.oracle,
        spool_dir: args.spool_dir,
        gate: PollLoop::new(args.interval_secs),
    };
    while let ControlFlow::Continue(()) = watcher.tick(&provider).await {}
    Ok(())
}

/// One `kardamom-batch-watcher` reactor tick: compare the pending claim
/// against the prover spool, challenge on divergence, then gate the next
/// tick on the outcome.
struct Watcher {
    oracle: Address,
    spool_dir: PathBuf,
    gate: PollLoop,
}

impl Watcher {
    async fn tick(&self, provider: &(impl alloy_provider::Provider + Clone)) -> ControlFlow<()> {
        let outcome = watch_and_challenge(provider.clone(), self.oracle, &self.spool_dir).await;
        report_watch_outcome(outcome);
        self.gate.gate(Retry::AfterInterval).await
    }
}

/// Log one [`watch_and_challenge`] attempt's outcome.
fn report_watch_outcome(outcome: Result<WatchOutcome, BatcherError>) {
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
