//! `kardamom-reconstruct` — rebuild L2 state from L1 data alone.
//!
//! The bottom-of-stack data-availability recovery tool. Given an L1
//! endpoint, a `KardamomL2Settlement` address, and a DA blob store, it
//! walks the `BatchPosted` event log, fetches each batch's blobs by the
//! versioned hashes L1 committed to, decodes them back into ordered
//! blocks, and re-executes them through the shared engine into a fresh
//! trie-aware state DB. This produces the reconstructed head and
//! canonical state root.
//!
//! Scope: L2 transactions. Deposits are re-derivable from L1 events, a
//! documented follow-up (see `kardamom_engine::replay`). With
//! `--expect-root` it exits non-zero on any mismatch, so it also works as
//! a chaos-suite assertion.

use std::path::PathBuf;

use alloy_primitives::{Address, B256};
use alloy_provider::ProviderBuilder;
use anyhow::{Context, bail};
use clap::Parser;
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::frame::BlockFrame;
use kardamom_batcher::l1::{read_posted_batches, recover_blocks};
use kardamom_reconstruct::Reconstruction;
use kardamom_state::Durability;
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

    /// DA blob store directory: the bytes behind the on-chain commitments.
    #[arg(long)]
    da_store: PathBuf,

    /// Kardamom genesis TOML (schema: `kardamom_types::Genesis`). Supplies
    /// the chain id and the initial allocation the reconstruction starts
    /// from.
    #[arg(long)]
    chain: PathBuf,

    /// First L1 block to scan for `BatchPosted` events.
    #[arg(long, default_value_t = 0)]
    from_block: u64,

    /// Output directory for the reconstructed libMDBX state DB.
    #[arg(long)]
    state_dir: PathBuf,

    /// Optional expected state root. If set, the tool checks that the
    /// reconstructed root matches, and exits non-zero otherwise
    /// (chaos-suite gate).
    #[arg(long)]
    expect_root: Option<B256>,

    /// Skip the fdatasync of each block commit. Only for a check that
    /// reads the result on the same host after this process exits and then
    /// discards it: one sync per block is most of the run time on a slow
    /// disk. Never for a state an operator keeps: the tool refuses it
    /// together with `--executor-image`, whose output outlives this
    /// process.
    #[arg(long, conflicts_with = "executor_image")]
    no_sync: bool,

    /// Write the image an executor resumes on: after the root check,
    /// remove the trie, the hashed mirror and the stored root, which an
    /// executor's trie-off writer would leave stale. Needs a payload that
    /// carries the canonical cursor through the last block.
    #[arg(long)]
    executor_image: bool,

    /// Optional last L2 block to re-execute. The posted batches must
    /// reach it; later blocks are left out, so the root compares with a
    /// state committed at that block.
    #[arg(long)]
    through_block: Option<u64>,
}

/// Keep the blocks through `through`, and refuse a batch set that ends
/// before it.
fn truncate(blocks: Vec<BlockFrame>, through: Option<u64>) -> anyhow::Result<Vec<BlockFrame>> {
    let Some(through) = through else {
        return Ok(blocks);
    };
    let end = blocks.iter().map(|b| b.block_number).max().unwrap_or(0);
    if end < through {
        bail!("posted batches end at block {end}, before block {through}");
    }
    Ok(blocks
        .into_iter()
        .filter(|b| b.block_number <= through)
        .collect())
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
    let blocks = truncate(blocks, cli.through_block)?;
    info!(
        blocks = blocks.len(),
        "recovered blocks from DA; re-executing"
    );

    let durability = if cli.no_sync {
        Durability::SafeNoSync
    } else {
        Durability::Durable
    };
    let outcome = Reconstruction {
        state_dir: &cli.state_dir,
        durability,
    }
    .run(chain_id, &accounts, &code, &blocks)
    .context("re-execute reconstructed blocks")?;

    info!(
        head_block = outcome.head_block,
        blocks_applied = outcome.blocks_applied,
        txs_applied = outcome.txs_applied,
        state_root = %outcome.state_root,
        "reconstruction complete"
    );
    // Machine-readable line for scripts and chaos assertions. The cursor
    // is the canonical end index a consumer resumes from; `none` means the
    // last block's payload predates the field, so the state is correct
    // and not resumable.
    println!(
        "reconstructed head={} blocks={} txs={} state_root={:#x} end_tx_idx={}",
        outcome.head_block,
        outcome.blocks_applied,
        outcome.txs_applied,
        outcome.state_root,
        outcome
            .head_end_tx_idx
            .map_or("none".to_string(), |end| end.to_string())
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
    if cli.executor_image {
        if outcome.head_end_tx_idx.is_none() {
            bail!(
                "block {} carries no canonical cursor (a version 2 payload): an executor cannot resume on this state",
                outcome.head_block
            );
        }
        kardamom_reconstruct::strip_to_executor_image(&cli.state_dir)
            .context("write the executor image")?;
        info!("executor image written: trie, hashed mirror and stored root removed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: u64) -> BlockFrame {
        BlockFrame {
            block_number: n,
            ..BlockFrame::default()
        }
    }

    #[test]
    fn truncate_keeps_the_blocks_through_the_target_and_refuses_a_short_set() {
        let blocks = || vec![block(1), block(2), block(3)];
        let kept = truncate(blocks(), Some(2)).unwrap();
        assert_eq!(
            kept.iter().map(|b| b.block_number).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(truncate(blocks(), None).unwrap().len(), 3);
        let err = truncate(blocks(), Some(4)).unwrap_err().to_string();
        assert!(err.contains("end at block 3, before block 4"), "{err}");
    }
}
