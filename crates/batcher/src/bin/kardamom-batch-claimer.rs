//! `kardamom-batch-claimer`: posts optimistic per-block batch claims to the
//! `KardamomProofOracle` from the validator's prover spool (spec: PR 5).
//!
//! Thin poster over [`kardamom_batcher::claim_next_batch`]: each tick it
//! claims the next posted batch the spool has covered, bonding from the
//! oracle's `minBond`. Permissioned only by the bond; the key pays gas +
//! bond (refunded on honest finalization).

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_batcher::{ClaimOutcome, claim_next_batch};

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
        match claim_next_batch(provider.clone(), args.oracle, &args.spool_dir).await {
            Ok(ClaimOutcome::Claimed { batch_index }) => {
                tracing::info!(batch_index, "batch claimed");
                continue;
            }
            Ok(ClaimOutcome::NoBatchPosted { batch_index }) => {
                tracing::debug!(batch_index, "batch not posted yet")
            }
            Ok(ClaimOutcome::SpoolNotReady {
                batch_index,
                missing_block,
            }) => {
                tracing::debug!(batch_index, missing_block, "spool not caught up")
            }
            Err(e) => tracing::error!(error = %e, "claim attempt failed"),
        }
        if args.interval_secs == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }
}
