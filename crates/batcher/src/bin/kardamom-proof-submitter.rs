//! `kardamom-proof-submitter`: posts batch validity proofs to the
//! `KardamomProofOracle`, aligned with the settlement's L1-as-truth batch
//! cursor. See the no-std-exec-core spec.
//!
//! A thin poster over [`kardamom_batcher::submit_next_proof`]. On each
//! tick, it asks the oracle for the next unproven batch, looks for the
//! prover's output files (`--proofs-dir/batch-<first>-<last>/`, the
//! zk-host layout), and submits when both exist. Submission is
//! permissionless; the proof is the authorization, and the key only pays
//! gas.

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
use kardamom_batcher::{ProofSubmitter, SubmitOutcome};

#[derive(Debug, Parser)]
#[command(name = "kardamom-proof-submitter", version)]
struct Args {
    /// L1 JSON-RPC endpoint.
    #[arg(long, env = "KARDAMOM_L1_RPC_URL")]
    l1_rpc_url: String,
    /// Gas-paying key (hex). Submission itself is permissionless.
    #[arg(long, env = "KARDAMOM_SUBMITTER_KEY")]
    private_key: String,
    /// The deployed `KardamomProofOracle` proxy.
    #[arg(long, env = "KARDAMOM_PROOF_ORACLE")]
    oracle: Address,
    /// The prover's output directory (zk-host batch layout).
    #[arg(long, env = "KARDAMOM_PROOFS_DIR")]
    proofs_dir: PathBuf,
    /// Poll interval in seconds. 0 = submit once and exit.
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

    Submitter::new(args.oracle, &args.proofs_dir, args.interval_secs)
        .run(&provider)
        .await;
    Ok(())
}

/// The `kardamom-proof-submitter` reactor: the proof submitter and the
/// poll gate. Each tick submits the next batch's proof when it is ready,
/// then gates the next tick on the outcome.
struct Submitter {
    submitter: ProofSubmitter,
    gate: PollLoop,
}

impl Submitter {
    fn new(oracle: Address, proofs_dir: &Path, interval: Option<Duration>) -> Self {
        Self {
            submitter: ProofSubmitter::new(oracle, proofs_dir),
            gate: PollLoop::new(interval),
        }
    }

    /// Tick until the gate stops the loop.
    async fn run(&self, provider: &(impl alloy_provider::Provider + Clone)) {
        while let ControlFlow::Continue(()) = self.tick(provider).await {}
    }

    async fn tick(&self, provider: &(impl alloy_provider::Provider + Clone)) -> ControlFlow<()> {
        let outcome = self.submitter.submit_next(provider.clone()).await;
        self.gate.gate(Self::report(outcome)).await
    }

    /// Log one submit attempt's outcome. Retry immediately on a
    /// successful submit, to try the next batch without waiting out the
    /// poll interval.
    fn report(outcome: Result<SubmitOutcome, BatcherError>) -> Retry {
        match outcome {
            Ok(SubmitOutcome::Submitted { batch_index }) => {
                tracing::info!(batch_index, "proof submitted; root advanced");
                return Retry::Now;
            }
            Ok(SubmitOutcome::NoBatchPosted { batch_index }) => {
                tracing::debug!(batch_index, "batch not posted yet");
            }
            Ok(SubmitOutcome::ProofNotReady { batch_index }) => {
                tracing::debug!(batch_index, "proof files not ready yet");
            }
            Err(e) => tracing::error!(error = %e, "submission attempt failed"),
        }
        Retry::AfterInterval
    }
}
