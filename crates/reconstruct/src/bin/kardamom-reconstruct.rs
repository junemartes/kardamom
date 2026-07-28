//! `kardamom-reconstruct` — rebuild L2 state from L1 data alone.
//!
//! The bottom-of-stack data-availability recovery tool. Given an L1 endpoint, a
//! `KardamomL2Settlement` address, and a DA blob store, it walks the
//! `BatchPosted` event log, fetches each batch's blobs by the versioned hashes
//! L1 committed to, decodes them back into ordered blocks, and re-executes them
//! through the shared engine into a fresh trie-aware state DB — producing the
//! reconstructed head + canonical state root.
//!
//! Scope: L2 transactions (deposits are re-derivable from L1 events, a
//! documented follow-up — see `kardamom_engine::replay`). With `--expect-root`
//! it exits non-zero on any mismatch, so it doubles as a chaos-suite assertion.

use std::path::PathBuf;

use alloy_primitives::{Address, B256};
use alloy_provider::ProviderBuilder;
use anyhow::{Context, bail};
use clap::Parser;
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::l1::{read_posted_batches, recover_blocks};
use kardamom_reconstruct::reconstruct_state;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "kardamom-reconstruct", version)]
struct Cli {
    /// L1 JSON-RPC endpoint (holds the `BatchPosted` commitments).
    #[arg(long, env = "KARDAMOM_L1_RPC")]
    l1_rpc: String,

    /// `KardamomL2Settlement` proxy address.
    #[arg(long)]
    settlement: Address,

    /// DA blob store directory (the *bytes* behind the on-chain commitments).
    #[arg(long)]
    da_store: PathBuf,

    /// Kardamom genesis TOML (schema: `kardamom_types::Genesis`). Supplies the
    /// chain id and the initial allocation the reconstruction must start from.
    #[arg(long)]
    chain: PathBuf,

    /// First L1 block to scan for `BatchPosted` events.
    #[arg(long, default_value_t = 0)]
    from_block: u64,

    /// Output directory for the reconstructed libMDBX state DB.
    #[arg(long)]
    state_dir: PathBuf,

    /// Optional expected state root; if set, the tool asserts the reconstructed
    /// root matches and exits non-zero otherwise (chaos-suite gate).
    #[arg(long)]
    expect_root: Option<B256>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let raw = std::fs::read_to_string(&cli.chain).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    let chain_id = genesis.chain_id;
    let (accounts, code) = genesis.to_alloc();

    let provider = ProviderBuilder::new()
        .connect(&cli.l1_rpc)
        .await
        .with_context(|| format!("connect L1 RPC {}", cli.l1_rpc))?;

    let descriptors = read_posted_batches(&provider, cli.settlement, cli.from_block)
        .await
        .context("read BatchPosted events")?;
    info!(
        batches = descriptors.len(),
        settlement = %cli.settlement,
        "read posted batches from L1"
    );
    if descriptors.is_empty() {
        bail!(
            "no BatchPosted events found at {} — nothing to reconstruct",
            cli.settlement
        );
    }

    let store = FsBlobStore::open(&cli.da_store).context("open DA blob store")?;
    let blocks = recover_blocks(&descriptors, &store).context("recover blocks from DA store")?;
    info!(
        blocks = blocks.len(),
        "recovered blocks from DA; re-executing"
    );

    let outcome = reconstruct_state(&cli.state_dir, chain_id, &accounts, &code, &blocks)
        .context("re-execute reconstructed blocks")?;

    info!(
        head_block = outcome.head_block,
        blocks_applied = outcome.blocks_applied,
        txs_applied = outcome.txs_applied,
        state_root = %outcome.state_root,
        "reconstruction complete"
    );
    // Machine-readable line for scripts/chaos assertions.
    println!(
        "reconstructed head={} blocks={} txs={} state_root={:#x}",
        outcome.head_block, outcome.blocks_applied, outcome.txs_applied, outcome.state_root
    );

    if let Some(expected) = cli.expect_root
        && expected != outcome.state_root
    {
        bail!(
            "state root mismatch: reconstructed {:#x} != expected {:#x}",
            outcome.state_root,
            expected
        );
    }
    Ok(())
}
