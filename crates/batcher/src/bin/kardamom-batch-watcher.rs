//! `kardamom-batch-watcher`: the honest challenger (spec: PR 5). Compares
//! pending optimistic claims against the validator's prover spool and, on
//! divergence, submits `challengeBlock` at the FIRST divergent offset with
//! the single-block proof files the prover produced (`zk-host --prove` on
//! the spooled frame).
//!
//! Thin driver over [`kardamom_batcher::watch_and_challenge`]. This is the
//! liveness assumption of the optimistic mode made concrete: at least one
//! honest watcher runs. Slashing pays it, so the assumption is incentivized.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use clap::Parser;
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
    /// The validator's prover spool: the ground truth roots are compared
    /// from here, and single-block proof files are read from here.
    #[arg(long, env = "KARDAMOM_SPOOL_DIR")]
    spool_dir: PathBuf,
    #[arg(long, default_value_t = 15)]
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
        match watch_and_challenge(provider.clone(), args.oracle, &args.spool_dir).await {
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
                tracing::debug!(batch_index, "pending claim matches the spool")
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
        if args.interval_secs == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }
}
