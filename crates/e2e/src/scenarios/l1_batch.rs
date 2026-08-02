//! l1-batch (#39): the live batcher's L2 → L1 round trip.
//!
//! Asserts, against the in-cluster anvil L1, that the batcher service is
//! actually settling the chain: `lastBatchIndex` advances while we watch
//! (live posting, not a stale backlog), the `BatchPosted` history is dense —
//! batch indices 1..N, block ranges contiguous from block 1 with no gap and
//! no overlap (the exact precondition `kardamom-reconstruct`'s concatenating
//! decoder relies on) — and L1 coverage tracks the executor's head. Root
//! parity of the reconstructed state is the chain-semantics-e2e drill's job
//! (`s8_da_parity_batcher_matches_validator`); this case pins the service
//! wiring in the cluster.

use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::ProviderBuilder;
use anyhow::{Context, Result, ensure};
use kardamom_batcher::l1::read_posted_batches;
use kardamom_batcher::settlement::IKardamomL2Settlement;

use super::Target;
use crate::harness::metrics::poll_until;

/// How far (in L2 blocks) L1 coverage may trail the executor's head. The
/// batcher groups 5 blocks per batch with a 3 s flush, so steady-state lag is
/// a handful of blocks; 60 tolerates a slow L1 round trip under CI load.
const COVERAGE_SLACK_BLOCKS: f64 = 60.0;

pub async fn l1_batch(t: &Target, l1_rpc: &str, settlement: Address) -> Result<()> {
    let provider = ProviderBuilder::new()
        .connect(l1_rpc)
        .await
        .with_context(|| format!("connect L1 RPC {l1_rpc}"))?;
    let contract = IKardamomL2Settlement::new(settlement, &provider);

    // 1. Live posting: the CAS counter must advance by ≥2 while we watch —
    //    one advance could be a lone startup post; two proves a loop.
    let start = contract
        .lastBatchIndex()
        .call()
        .await
        .context("read lastBatchIndex")?;
    let last = poll_until(
        &format!("lastBatchIndex advances past {start}+2"),
        Duration::from_secs(120),
        Duration::from_secs(1),
        || async {
            let v = contract
                .lastBatchIndex()
                .call()
                .await
                .context("poll lastBatchIndex")?;
            Ok((v >= start + 2).then_some(v))
        },
    )
    .await?;

    // 2. Dense history from genesis: indices 1..N, block ranges contiguous
    //    from block 1.
    let posted = read_posted_batches(&provider, settlement, 0)
        .await
        .context("read BatchPosted events")?;
    ensure!(
        posted.len() as u64 >= last,
        "lastBatchIndex={last} but only {} BatchPosted events",
        posted.len()
    );
    let mut expect_start: u64 = 1;
    for (i, d) in posted.iter().enumerate() {
        ensure!(
            d.index == i as u64 + 1,
            "batch indices not dense: position {i} holds index {}",
            d.index
        );
        ensure!(
            d.l2_block_start == expect_start,
            "batch {} covers blocks {}..={} but the previous batch ended at {} — gap or overlap",
            d.index,
            d.l2_block_start,
            d.l2_block_end,
            expect_start - 1
        );
        ensure!(
            d.l2_block_end >= d.l2_block_start,
            "batch {} has inverted block range {}..={}",
            d.index,
            d.l2_block_start,
            d.l2_block_end
        );
        ensure!(
            !d.versioned_hashes.is_empty(),
            "batch {} carries no blobs",
            d.index
        );
        expect_start = d.l2_block_end + 1;
    }
    let covered = posted.last().map(|d| d.l2_block_end).unwrap_or(0);

    // 3. Coverage tracks the executed head.
    let exec_block = t.executor_metric(super::EXEC_BLOCK_NUMBER).await?;
    ensure!(
        covered as f64 >= exec_block - COVERAGE_SLACK_BLOCKS,
        "L1 coverage lags: covered through block {covered} but the executor is at {exec_block}"
    );

    println!(
        "==> l1-batch: {} batches on L1, dense blocks 1..={covered}, executor at {exec_block}",
        posted.len()
    );
    Ok(())
}
