//! `kardamom-proof-submitter`: posts batch validity proofs to the
//! `KardamomProofOracle`, aligned with the settlement's L1-as-truth batch
//! cursor (spec: no-std-exec-core, PR 4).
//!
//! Thin poster over [`kardamom_batcher::submit_next_proof`]: each tick it
//! asks the oracle for the next unproven batch, looks for the prover's
//! output files (`--proofs-dir/batch-<first>-<last>/`, the zk-host layout),
//! and submits when both exist. Submission is permissionless — the proof is
//! the authorization; the key only pays gas.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_batcher::{SubmitOutcome, submit_next_proof};

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
    #[arg(long, default_value_t = 30)]
    interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let signer: PrivateKeySigner = args.private_key.parse().context("parse --private-key")?;
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(args.l1_rpc_url.parse().context("parse --l1-rpc-url")?);

    loop {
        match submit_next_proof(provider.clone(), args.oracle, &args.proofs_dir).await {
            Ok(SubmitOutcome::Submitted { batch_index }) => {
                tracing::info!(batch_index, "proof submitted; root advanced");
                continue; // immediately try the next batch
            }
            Ok(SubmitOutcome::NoBatchPosted { batch_index }) => {
                tracing::debug!(batch_index, "batch not posted yet");
            }
            Ok(SubmitOutcome::ProofNotReady { batch_index }) => {
                tracing::debug!(batch_index, "proof files not ready yet");
            }
            Err(e) => tracing::error!(error = %e, "submission attempt failed"),
        }
        if args.interval_secs == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }
}
