//! `kardamom-batch-claimer`: posts optimistic per-block batch claims to the
//! `KardamomProofOracle` from the validator's prover spool.
//!
//! A thin poster over [`kardamom_batcher::BatchClaimer`]. On each tick,
//! it claims the next posted batch the spool has covered, and bonds from
//! the oracle's `minBond`. The bond is the only permission check; the key
//! pays gas and the bond, which is refunded on honest finalization.

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
use kardamom_batcher::{BatchClaimer, ClaimOutcome};

#[derive(Debug, Parser)]
#[command(name = "kardamom-batch-claimer", version)]
struct Args {
    #[arg(long, env = "KARDAMOM_L1_RPC_URL")]
    l1_rpc_url: String,
    #[arg(long, env = "KARDAMOM_CLAIMER_KEY")]
    private_key: String,
    #[arg(long, env = "KARDAMOM_PROOF_ORACLE")]
    oracle: Address,
    /// The validator's prover spool (per-block expected-outputs).
    #[arg(long, env = "KARDAMOM_SPOOL_DIR")]
    spool_dir: PathBuf,
    /// 0 means run once and stop.
    #[arg(long, default_value = "30", value_parser = parse_interval_secs)]
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

    Claimer::new(args.oracle, &args.spool_dir, args.interval_secs)
        .run(&provider)
        .await;
    Ok(())
}

/// The `kardamom-batch-claimer` reactor: the claim poster and the poll
/// gate. Each tick claims the next posted batch the spool has covered,
/// then gates the next tick on the outcome.
struct Claimer {
    claimer: BatchClaimer,
    gate: PollLoop,
}

impl Claimer {
    fn new(oracle: Address, spool_dir: &Path, interval: Option<Duration>) -> Self {
        Self {
            claimer: BatchClaimer::new(oracle, spool_dir),
            gate: PollLoop::new(interval),
        }
    }

    /// Tick until the gate stops the loop.
    async fn run(&self, provider: &(impl alloy_provider::Provider + Clone)) {
        while let ControlFlow::Continue(()) = self.tick(provider).await {}
    }

    async fn tick(&self, provider: &(impl alloy_provider::Provider + Clone)) -> ControlFlow<()> {
        let outcome = self.claimer.claim_next(provider.clone()).await;
        self.gate.gate(Self::report(outcome)).await
    }

    /// Log one claim attempt's outcome. Retry immediately on a successful
    /// claim, so the claimer catches up without waiting out the poll
    /// interval.
    fn report(outcome: Result<ClaimOutcome, BatcherError>) -> Retry {
        match outcome {
            Ok(ClaimOutcome::Claimed { batch_index }) => {
                tracing::info!(batch_index, "batch claimed");
                return Retry::Now;
            }
            Ok(ClaimOutcome::NoBatchPosted { batch_index }) => {
                tracing::debug!(batch_index, "batch not posted yet");
            }
            Ok(ClaimOutcome::SpoolNotReady {
                batch_index,
                missing_block,
            }) => {
                tracing::debug!(batch_index, missing_block, "spool not caught up");
            }
            Err(e) => tracing::error!(error = %e, "claim attempt failed"),
        }
        Retry::AfterInterval
    }
}
