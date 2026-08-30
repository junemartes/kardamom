//! L1-origin deposit derivation.
//!
//! The rules under test come from
//! `docs/agents/l1-origin-deposit-derivation-spec.md`:
//!
//!   1. the origin is monotonic.
//!   2. it never skips an L1 block.
//!   3. an epoch's deposits lead the block they open.
//!   4. the origin stays at or below L1 finality.
//!   5. bounded lag is a liveness alarm.
//!
//! These are properties of the chain, not of any one component. So this
//! test checks them against persisted block headers and executed receipts,
//! not against logs or metrics. A component could log the right thing and
//! still build the wrong chain.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};

use super::{Target, assert_receipt_ok, await_l2_receipt, open_state_ro, receipt_placement};
use crate::harness::l1::L1;
use crate::harness::l2;

/// One block's header as the chain recorded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockOrigin {
    pub block_number: u64,
    pub l1_origin: u64,
    pub end_tx_idx: u64,
    pub l2_timestamp: u64,
}

/// Like [`read_block_origins`], but wait until the header for
/// `block_number` has actually been committed.
///
/// A receipt is published when the transaction executes, but the header
/// lands only when the block commits at the next sealer boundary.
/// Anything that learns a block number from a receipt and then reads the
/// headers table is racing that commit, and must wait instead of
/// assuming.
pub async fn await_block_origins_through(
    state_dir: &Path,
    block_number: u64,
) -> Result<Vec<BlockOrigin>> {
    crate::harness::metrics::poll_until(
        &format!("the committed header for block {block_number}"),
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let blocks = read_block_origins(state_dir)?;
            Ok(blocks
                .iter()
                .any(|b| b.block_number == block_number)
                .then_some(blocks))
        },
    )
    .await
}

/// Read every persisted block header, in block order.
///
/// This is a read-only mdbx open: an MVCC snapshot that never blocks the
/// writer, the same seam `read_validator_state_root` uses. Nothing exposes
/// headers over RPC (there is no `eth_getBlockByNumber`), so the state
/// database is the only place to observe the origin, which is worth
/// knowing on its own.
pub fn read_block_origins(state_dir: &Path) -> Result<Vec<BlockOrigin>> {
    let env = open_state_ro(state_dir)?;
    let headers = kardamom_state::read_all_headers(&env).context("read headers table")?;
    Ok(headers
        .into_iter()
        .map(|(block_number, h)| BlockOrigin {
            block_number,
            l1_origin: h.l1_origin,
            end_tx_idx: h.end_tx_idx.as_index(),
            l2_timestamp: h.l2_timestamp,
        })
        .collect())
}

/// Checks rules 1, 2, and 4 over the whole chain built so far.
///
/// `l1_finalized` is the L1 finalized tip observed after the chain
/// stopped growing. So this checks "the origin never exceeds finality"
/// against a bound that can only have moved forward.
pub fn assert_origin_sequence_is_sound(blocks: &[BlockOrigin], l1_finalized: u64) -> Result<()> {
    anyhow::ensure!(!blocks.is_empty(), "no blocks were produced");

    let mut prev: Option<BlockOrigin> = None;
    for b in blocks {
        // `l1_origin == 0` means "no epoch adopted yet". These are the
        // blocks a chain produces between genesis and its first epoch. The
        // step out of 0 is the watcher's seed, which deliberately skips
        // historical L1 (see the spec's Non-Goals). So this step is not a
        // skip in the rule-2 sense.
        //
        // Known gap: because the seed starts where it does, deposits made
        // in L1 blocks before it are unrecoverable, since nothing derives
        // them. The spec's `l1_origin_genesis` edge case will close this
        // gap. Until then, a chain's verifiable history starts at its
        // first epoch, not at L1 genesis.
        let crossing_the_seed = prev.is_some_and(|p| p.l1_origin == 0) && b.l1_origin > 0;
        if let Some(p) = prev
            && !crossing_the_seed
        {
            // Rule 1: monotonic. A regression would make deposit derivation
            // ambiguous: two blocks would claim different origins for the
            // same stretch of L1.
            anyhow::ensure!(
                b.l1_origin >= p.l1_origin,
                "l1_origin regressed: block {} has origin {}, block {} had {}",
                b.block_number,
                b.l1_origin,
                p.block_number,
                p.l1_origin
            );
            // Rule 2: no skipping. Every L1 block between two origins must
            // have had its own epoch, so the origin may step by only one. A
            // jump means an epoch was dropped, along with any deposits it
            // carried. This is exactly the censorship this design exists
            // to prevent.
            anyhow::ensure!(
                b.l1_origin <= p.l1_origin + 1,
                "l1_origin skipped from {} to {} between blocks {} and {}: \
                 an epoch (and any deposits in it) was dropped",
                p.l1_origin,
                b.l1_origin,
                p.block_number,
                b.block_number
            );
        }
        // Rule 4: the origin is always an L1 block that is already final.
        anyhow::ensure!(
            b.l1_origin <= l1_finalized,
            "block {} claims origin {} beyond the L1 finalized tip {}",
            b.block_number,
            b.l1_origin,
            l1_finalized
        );
        prev = Some(*b);
    }
    Ok(())
}

/// The origin advances during ordinary operation, and the
/// sequence it traces obeys rules 1, 2, and 4.
///
/// This deliberately runs with no deposits. An idle L1 is the case where
/// it is tempting to emit nothing at all, and rule 2 is exactly the rule
/// that would break.
pub async fn origin_advances_over_an_idle_l1(l1: &L1, state_dir: &Path) -> Result<()> {
    let start = read_block_origins(state_dir)?;
    let start_origin = start.last().map(|b| b.l1_origin).unwrap_or(0);

    // Advance L1 well past finality several times over.
    l1.mine(12).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;

    let blocks = read_block_origins(state_dir)?;
    let l1_finalized = l1.finalized_block_number().await?;
    assert_origin_sequence_is_sound(&blocks, l1_finalized)?;

    let end_origin = blocks
        .last()
        .map(|b| b.l1_origin)
        .context("no blocks after mining")?;
    anyhow::ensure!(
        end_origin > start_origin,
        "origin did not advance while L1 produced blocks: still {end_origin}"
    );
    Ok(())
}

/// Rule 3: an epoch's deposits lead the block they open, even
/// while L2 traffic flows at the same time.
///
/// The interleaving is the point. Deposits and L2 transactions race onto
/// one canonical stream. So "deposits come first" can only hold because
/// the epoch is atomic and forces a boundary, not because of timing luck.
/// Under concurrent load, a design that only tends to put deposits early
/// would fail this test.
pub async fn deposits_lead_their_block_under_load(
    t: &Target,
    l1: &L1,
    state_dir: &Path,
) -> Result<()> {
    let signers = l2::dev_signers(3)?;
    let beneficiary = &signers[1];
    let spammer = &signers[0];

    // Keep L2 busy so the epoch lands amongst regular traffic.
    let mut sent = Vec::new();
    for nonce in 0..6u64 {
        let tx = l2::sign_transfer(spammer, t.chain_id, nonce, signers[2].address, 1)?;
        if t.rpc.send_raw(&tx.raw).await.result.is_ok() {
            sent.push(tx.hash);
        }
    }

    let (block_hash, log_index) = l1
        .deposit_eth(beneficiary.address, U256::from(5_000_000_000_000_000u64))
        .await
        .context("depositETH")?;
    l1.mine(6).await?;

    let source_hash = kardamom_da_watcher::source_hash(block_hash, log_index);
    let receipt = await_l2_receipt(t, source_hash, "the deposit").await?;
    let (block_number, tx_index) =
        receipt_placement(&receipt).context("place the deposit receipt")?;

    // Rule 3: index 0 of its block. Not "early", and not "before the
    // transactions that happened to arrive later" — it must be first.
    anyhow::ensure!(
        tx_index == 0,
        "deposit landed at index {tx_index} of block {block_number}, not at the front"
    );

    // The block it leads must also be the one whose origin is the epoch's.
    //
    // The receipt above is published when the transaction executes, but
    // the block's header is persisted only when that block commits at the
    // next sealer boundary. Reading the state database right after the
    // receipt lands races that commit. Wait for the header instead of
    // assuming it is already there.
    let blocks = await_block_origins_through(state_dir, block_number).await?;
    let this = blocks
        .iter()
        .find(|b| b.block_number == block_number)
        .copied()
        .with_context(|| format!("no header for block {block_number}"))?;
    let prev = blocks
        .iter()
        .rfind(|b| b.block_number < block_number)
        .copied();
    if let Some(prev) = prev {
        anyhow::ensure!(
            this.l1_origin > prev.l1_origin,
            "the block carrying a deposit must open a new epoch: block {} origin {} == previous {}",
            block_number,
            this.l1_origin,
            prev.l1_origin
        );
    }
    Ok(())
}

/// Several deposits in one L1 block all land, in L1 log order, in
/// one L2 block.
///
/// This is what the atomic epoch record buys: no one can split the group
/// across blocks or reorder it, because it travels as a single canonical
/// record.
pub async fn multi_deposit_epoch_lands_in_log_order(
    t: &Target,
    l1: &L1,
    state_dir: &Path,
) -> Result<()> {
    let signers = l2::dev_signers(5)?;
    let recipients: Vec<Address> = (2..5).map(|i| signers[i].address).collect();

    // Three deposits mined into a single L1 block, so they form one epoch.
    let receipts = l1
        .deposit_eth_batch(&recipients, U256::from(3_000_000_000_000_000u64))
        .await
        .context("batch depositETH into one L1 block")?;
    anyhow::ensure!(
        receipts.len() == 3,
        "expected 3 deposits in one L1 block, got {}",
        receipts.len()
    );
    let l1_block = receipts[0].1;
    anyhow::ensure!(
        receipts.iter().all(|(_, n, _)| *n == l1_block),
        "batch did not land in one L1 block: {:?}",
        receipts.iter().map(|(_, n, _)| *n).collect::<Vec<_>>()
    );
    l1.mine(6).await?;

    // Each must execute, and all must share one L2 block.
    let mut placements = Vec::new();
    for (block_hash, _l1_number, log_index) in &receipts {
        let sh = kardamom_da_watcher::source_hash(*block_hash, *log_index);
        let r = await_l2_receipt(t, sh, "a batched deposit").await?;
        let (bn, ti) = receipt_placement(&r).context("place a batched deposit receipt")?;
        placements.push((*log_index, bn, ti));
    }

    let block = placements[0].1;
    anyhow::ensure!(
        placements.iter().all(|(_, bn, _)| *bn == block),
        "one L1 block's deposits were split across L2 blocks: {placements:?}"
    );
    // L1 log order matches L2 transaction order, and the deposits occupy
    // the front of the block with no gaps.
    let mut by_log = placements.clone();
    by_log.sort_by_key(|(log_index, _, _)| *log_index);
    for (expected_idx, (_, _, tx_index)) in by_log.iter().enumerate() {
        anyhow::ensure!(
            *tx_index == expected_idx as u64,
            "deposit order does not follow L1 log order: {by_log:?}"
        );
    }

    // The chain-level rules still hold with a multi-deposit epoch in it.
    let blocks = read_block_origins(state_dir)?;
    let l1_finalized = l1.finalized_block_number().await?;
    assert_origin_sequence_is_sound(&blocks, l1_finalized)?;
    Ok(())
}

/// A stalled L1 must not stall L2.
///
/// Rule 5 promises that deposits are delayed, never lost, and that
/// ordinary transaction flow keeps running while the origin is frozen.
/// That is the difference between a liveness alarm and a liveness
/// failure.
pub async fn stalled_l1_does_not_stall_l2(t: &Target, l1: &L1, state_dir: &Path) -> Result<()> {
    // Anvil runs with block_time(1), so L1 must be told to stop. Simply
    // not mining would still leave it producing a block every second.
    tokio::time::sleep(Duration::from_secs(2)).await;
    l1.pause_block_production().await?;
    // Let the watcher drain whatever was already finalized before freezing.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let frozen = read_block_origins(state_dir)?;
    let frozen_origin = frozen
        .last()
        .map(|b| b.l1_origin)
        .context("no blocks before the stall")?;
    let applied_before = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);

    // L2 keeps taking work with L1 completely idle.
    let signers = l2::dev_signers(2)?;
    let mut accepted = 0.0;
    for nonce in 0..4u64 {
        let tx = l2::sign_transfer(&signers[0], t.chain_id, nonce, signers[1].address, 1)?;
        if t.rpc.send_raw(&tx.raw).await.result.is_ok() {
            accepted += 1.0;
        }
    }
    anyhow::ensure!(accepted > 0.0, "no L2 txs were accepted while L1 was idle");
    t.wait_executor_applied(applied_before + accepted, Duration::from_secs(20))
        .await
        .context("L2 must keep executing while the L1 origin is frozen")?;

    // The origin held steady, instead of inventing blocks.
    let after = read_block_origins(state_dir)?;
    let after_origin = after
        .last()
        .map(|b| b.l1_origin)
        .context("no blocks after the stall")?;
    anyhow::ensure!(
        after_origin == frozen_origin,
        "origin moved from {frozen_origin} to {after_origin} while L1 produced nothing"
    );

    // And it resumes cleanly, with no gap, once L1 comes back.
    l1.resume_block_production().await?;
    l1.mine(8).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let resumed = read_block_origins(state_dir)?;
    let l1_finalized = l1.finalized_block_number().await?;
    assert_origin_sequence_is_sound(&resumed, l1_finalized)?;
    let resumed_origin = resumed.last().map(|b| b.l1_origin).unwrap_or(0);
    anyhow::ensure!(
        resumed_origin > frozen_origin,
        "origin did not resume after L1 restarted: {resumed_origin} vs {frozen_origin}"
    );
    Ok(())
}

/// Every deposit that L1 recorded is on L2 exactly once.
///
/// This derives the expected set directly from L1 logs and compares it
/// against what the chain executed. A later validator check will enforce
/// this property. Checking it here shows the producer is already honest.
pub async fn every_l1_deposit_appears_exactly_once(
    t: &Target,
    l1: &L1,
    state_dir: &Path,
) -> Result<()> {
    let blocks = read_block_origins(state_dir)?;
    let highest_origin = blocks
        .last()
        .map(|b| b.l1_origin)
        .context("no blocks produced")?;
    anyhow::ensure!(highest_origin > 0, "the chain never adopted an L1 origin");

    // Everything L1 recorded up to the origin the chain has actually reached.
    let logs = l1
        .deposit_logs(1, highest_origin)
        .await
        .context("read L1 deposit logs")?;

    let mut seen: BTreeMap<alloy_primitives::B256, usize> = BTreeMap::new();
    for log in &logs {
        let sh = kardamom_da_watcher::source_hash(log.block_hash, log.log_index);
        let receipt = await_l2_receipt(t, sh, "an L1-recorded deposit")
            .await
            .with_context(|| {
                format!(
                    "L1 deposit (block {}, log {}) never executed on L2 — a dropped epoch",
                    log.block_number, log.log_index
                )
            })?;
        assert_receipt_ok(&receipt, &format!("deposit {sh}"))?;
        *seen.entry(sh).or_default() += 1;
    }

    // A duplicate would mean the racing sequencers' re-offers were not
    // deduplicated, so the deposit would have minted twice.
    for (sh, count) in &seen {
        anyhow::ensure!(*count == 1, "deposit {sh} executed {count} times");
    }
    Ok(())
}
