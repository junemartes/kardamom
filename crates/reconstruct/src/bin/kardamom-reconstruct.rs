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
//! Scope: L2 transactions, interop deliveries, and, with `--lockbox`, the
//! L1 deposits each block's L1 origin names, derived from the lockbox logs.
//! With `--expect-root` it exits non-zero on any mismatch, so it also works
//! as a chaos-suite assertion.

use std::path::{Path, PathBuf};

use alloy_primitives::Bytes;
use alloy_primitives::{Address, B256};
use alloy_provider::ProviderBuilder;
use anyhow::{Context, bail};
use clap::Parser;
use kardamom_batcher::da::{DaProxy, PayloadSource};
use kardamom_batcher::error::BatcherError;
use kardamom_batcher::frame::BlockFrame;
use kardamom_batcher::indexer::IndexerClient;
use kardamom_batcher::l1::{read_posted_batches, recover_blocks};
use kardamom_da_watcher::RpcL1Source;
use kardamom_engine::ReplayBlock;
use kardamom_reconstruct::{L1Epochs, Reconstruction, block_frame_to_replay};
use kardamom_state::Durability;
use tracing::{info, warn};

/// Where the payloads come from: the proxy, or the indexer's archive.
enum Source {
    Proxy(DaProxy),
    Indexer(IndexerClient),
}

impl PayloadSource for Source {
    fn fetch_payload(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError> {
        match self {
            Self::Proxy(p) => p.fetch_payload(da_cert),
            Self::Indexer(i) => i.fetch_payload(da_cert),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "kardamom-reconstruct", version)]
struct Cli {
    /// L1 JSON-RPC endpoint (holds the `BatchPosted` commitments).
    #[arg(long, env = "KARDAMOM_L1_RPC")]
    l1_rpc: String,

    /// `KardamomL2Settlement` proxy address.
    #[arg(long)]
    settlement: Address,

    /// The EigenDA proxy (`http://host:port`): the bytes behind the
    /// on-chain certificates, while EigenDA retains them.
    #[arg(
        long,
        env = "KARDAMOM_DA_PROXY",
        required_unless_present = "indexer_url"
    )]
    da_proxy: Option<String>,

    /// The inbox indexer's API (`http://host:port`): the same payloads,
    /// archived past EigenDA's retention.
    #[arg(long, env = "KARDAMOM_INDEXER_URL", conflicts_with = "da_proxy")]
    indexer_url: Option<String>,

    /// Kardamom genesis TOML (schema: `kardamom_types::Genesis`). Supplies
    /// the chain id and the initial allocation the reconstruction starts
    /// from.
    #[arg(long)]
    chain: PathBuf,

    /// `ETHLockbox` proxy address. The rebuild derives the deposits of
    /// each L1 epoch a block leads with from this contract's logs. Without
    /// it, the rebuild leaves deposits out, and a chain with deposits
    /// rebuilds to a wrong root.
    #[arg(long)]
    lockbox: Option<Address>,

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

    /// Write the seed a sealer cluster with no state starts from: the
    /// rebuilt head, its canonical end, and the next nonce of each
    /// sender. The sealer reads it at
    /// `-Dkardamom.cluster.seedSnapshot`. Needs a payload that carries
    /// the canonical cursor through the last block.
    #[arg(long)]
    sealer_seed: Option<PathBuf>,
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

impl Source {
    /// Refuse an indexer source that is not running. The proxy has no
    /// lifecycle record to read.
    async fn require_running(&self) -> anyhow::Result<()> {
        match self {
            Self::Proxy(_) => Ok(()),
            Self::Indexer(indexer) => indexer
                .require_running()
                .await
                .context("the indexer source is not running"),
        }
    }
}

impl Cli {
    /// The replay blocks of `frames`, each led by the L1 epochs its origin
    /// step names, read through `provider` from the `--lockbox` logs.
    /// Without `--lockbox`, the blocks lead with no epoch.
    async fn with_l1_epochs<P>(
        &self,
        provider: P,
        frames: &[BlockFrame],
    ) -> anyhow::Result<Vec<ReplayBlock>>
    where
        P: alloy_provider::Provider + Send + Sync + 'static,
    {
        let blocks = frames.iter().map(block_frame_to_replay).collect();
        let Some(lockbox) = self.lockbox else {
            warn!("no --lockbox: the rebuild leaves L1 deposits out");
            return Ok(blocks);
        };
        L1Epochs::new(RpcL1Source::new(provider), lockbox)
            .attach(blocks)
            .await
            .context("derive the L1 epochs")
    }

    /// The payload source the flags name. clap guarantees one of the two.
    fn payload_source(&self) -> anyhow::Result<Source> {
        match (&self.da_proxy, &self.indexer_url) {
            (Some(url), _) => Ok(Source::Proxy(DaProxy::new(url).context("DA proxy client")?)),
            (None, Some(url)) => Ok(Source::Indexer(IndexerClient::new(url))),
            (None, None) => bail!("--da-proxy or --indexer-url is required"),
        }
    }
}

/// The seed `--sealer-seed` asks for: where to write it, the rebuilt
/// state it reads, and the senders of the rebuilt blocks in the order they
/// last sent.
struct SeedRequest<'a> {
    path: &'a Path,
    state_dir: &'a Path,
    chain_id: u64,
    senders: kardamom_reconstruct::SenderOrder,
}

impl SeedRequest<'_> {
    /// Build the seed of the state that `outcome` rebuilt, and write it.
    fn write(&self, outcome: &kardamom_engine::ReplayOutcome) -> anyhow::Result<()> {
        let seed = kardamom_reconstruct::SeedInput {
            state_dir: self.state_dir,
            chain_id: self.chain_id,
            outcome,
            senders: &self.senders,
        }
        .seed()
        .context("build the sealer seed")?;
        seed.write(self.path)?;
        info!(
            path = %self.path.display(),
            block = seed.block,
            end_tx_idx = seed.end_tx_idx,
            senders = seed.senders.len(),
            "sealer seed written"
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let raw = std::fs::read_to_string(&cli.chain).context("read genesis TOML")?;
    let genesis: kardamom_types::Genesis = toml::from_str(&raw).context("parse genesis TOML")?;
    genesis.validate().context("validate genesis")?;
    let (accounts, code) = genesis.to_alloc();
    let replay_genesis = kardamom_engine::ReplayGenesis::of(&genesis, &accounts, &code);

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

    let source = cli.payload_source()?;
    source.require_running().await?;
    let blocks =
        recover_blocks(&descriptors, &source).context("recover blocks from the DA layer")?;
    let blocks = truncate(blocks, cli.through_block)?;
    let blocks = cli.with_l1_epochs(provider, &blocks).await?;
    info!(
        blocks = blocks.len(),
        epochs = blocks.iter().map(|b| b.l1_epochs.len()).sum::<usize>(),
        "recovered blocks from DA; re-executing"
    );

    // The seed needs the senders of the rebuilt blocks, in canonical
    // order. Read them before the replay.
    let seed = cli.sealer_seed.as_deref().map(|path| SeedRequest {
        path,
        state_dir: &cli.state_dir,
        chain_id: genesis.chain_id,
        senders: kardamom_reconstruct::SenderOrder::of(
            blocks.iter().flat_map(|b| b.txs.iter().map(|t| t.sender)),
        ),
    });

    let durability = if cli.no_sync {
        Durability::SafeNoSync
    } else {
        Durability::Durable
    };
    let outcome = Reconstruction {
        state_dir: &cli.state_dir,
        durability,
    }
    .run(&replay_genesis, blocks)
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
    if let Some(seed) = &seed {
        seed.write(&outcome)?;
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
