//! S8 — `da_parity_batcher_matches_validator`.
//!
//! "The batcher's state matches the validator's", proven the only way that
//! statement is falsifiable: take what the live pipeline actually executed,
//! post it to L1 as **real EIP-4844 blobs**, throw the originals away, then
//! rebuild the chain from L1 data alone and require the resulting state root
//! to equal the root the validator independently computed and attests to.
//!
//! Where the canonical order comes from: the pipeline's own **receipts**.
//! Each carries `blockNumber` + `transactionIndex`, so the scenario recovers
//! the real block grouping and intra-block order of the transactions it
//! submitted — no re-implementation of the executor's reader, and nothing
//! invented. (`kardamom-reconstruct`'s own inputs are then exactly what a
//! recovery operator would have: L1 logs + blobs + genesis.)
//!
//! Two honest limits, both inherent to today's code rather than to the test:
//!
//! - **Deposit-free by construction.** Deposits are deliberately absent from
//!   the DA payload (the `kardamom-reconstruct` crate documents it), so a workload with
//!   deposits could never reconstruct — the minted ETH would simply be
//!   missing and every later balance would diverge. Lifting this needs a
//!   protocol change, not test plumbing: see
//!   `docs/agents/l1-origin-deposit-derivation-spec.md`, which derives
//!   deposits from an `l1_origin` carried per block. S8 extends to deposits
//!   for free once that lands.
//! - **`l2_timestamp` is synthesized.** The receipt does not expose the
//!   block's timestamp and no RPC serves it, so the rebuilt blocks carry
//!   monotonic placeholders. For the transfer-only workload here the
//!   timestamp cannot affect state (no `TIMESTAMP` opcode is executed), so
//!   root parity is unaffected — but a workload that reads the block
//!   timestamp would need a real source first.

use std::collections::BTreeMap;
use std::path::Path;

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da_store::FsBlobStore;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_types::{BPosition, TxEnvelope};

use super::{Target, assert_receipt_ok, await_l2_receipt, receipt_placement};
use crate::harness::l1::L1;
use crate::harness::l2::{self, SignedTransfer};

pub struct Params {
    pub senders: usize,
    pub txs_per_sender: usize,
    pub sender_base: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            senders: 3,
            txs_per_sender: 8,
            sender_base: 1,
        }
    }
}

/// One executed transaction, located in the canonical chain by its receipt.
struct Executed {
    tx: SignedTransfer,
    block_number: u64,
    transaction_index: u64,
}

/// Run the workload and recover, from the pipeline's own receipts, the
/// canonical blocks it produced.
pub async fn run_workload(t: &Target, p: &Params) -> Result<Vec<ClosedBlock>> {
    let signers = l2::dev_signers((p.sender_base + p.senders) as u32)?;
    let to = Address::from([0x88u8; 20]);

    let mut planned: Vec<SignedTransfer> = Vec::new();
    for signer in &signers[p.sender_base..] {
        for n in 0..p.txs_per_sender {
            planned.push(l2::sign_transfer(signer, t.chain_id, n as u64, to, 1)?);
        }
    }

    let mut set = tokio::task::JoinSet::new();
    for tx in planned.clone() {
        let rpc = t.rpc.clone();
        set.spawn(async move {
            let out = rpc.send_raw(&tx.raw).await;
            (tx, out)
        });
    }
    while let Some(j) = set.join_next().await {
        let (tx, out) = j.context("submit join")?;
        out.result
            .map_err(|e| anyhow::anyhow!("sender {} nonce {}: {e}", tx.sender, tx.nonce))?;
    }

    // Locate every transaction in the canonical chain. The receipt appears at
    // execute time, so this needs no drain — but the values are the
    // executor's own, not the test's guess.
    let mut executed = Vec::with_capacity(planned.len());
    for tx in planned {
        let receipt = await_l2_receipt(t, tx.hash, &format!("workload tx {}", tx.hash)).await?;
        let (block_number, transaction_index) = receipt_placement(&receipt)
            .with_context(|| format!("place receipt for {}", tx.hash))?;
        assert_receipt_ok(
            &receipt,
            &format!("tx {} (DA parity needs a clean workload)", tx.hash),
        )?;
        executed.push(Executed {
            tx,
            block_number,
            transaction_index,
        });
    }

    // Group into the canonical blocks the pipeline actually sealed.
    let mut by_block: BTreeMap<u64, Vec<Executed>> = BTreeMap::new();
    for e in executed {
        by_block.entry(e.block_number).or_default().push(e);
    }
    let mut blocks = Vec::with_capacity(by_block.len());
    for (block_number, mut txs) in by_block {
        txs.sort_by_key(|e| e.transaction_index);
        let recorded: Vec<RecordedTx> = txs
            .iter()
            .map(|e| RecordedTx {
                // Positions do not reach the blob payload (only
                // block/timestamp + per-tx bytes do), so the canonical index
                // is a faithful stand-in here.
                position: BPosition::from_index(e.transaction_index),
                envelope: TxEnvelope {
                    correlation_id: e.transaction_index,
                    raw_tx: bytes::Bytes::copy_from_slice(e.tx.raw.as_ref()),
                    sender: e.tx.sender,
                    tx_hash: e.tx.hash,
                },
            })
            .collect();
        let end = recorded.len() as u64;
        blocks.push(ClosedBlock {
            block_number,
            // Synthesized — see the module docs.
            l2_timestamp: 1_700_000_000 + block_number,
            end_tx_idx: BPosition::from_index(end),
            remote_epochs: vec![],
            txs: recorded,
        });
    }
    anyhow::ensure!(!blocks.is_empty(), "workload produced no blocks");
    Ok(blocks)
}

/// Post `blocks` to the settlement contract as real EIP-4844 blob
/// transactions, one batch per block, and assert L1's compare-and-set batch
/// indices advance densely.
pub async fn post_to_l1(
    l1: &L1,
    settlement: Address,
    blocks: &[ClosedBlock],
    da_store: &FsBlobStore,
) -> Result<()> {
    let provider = l1.wallet(crate::harness::l1::BATCHER_KEY)?;
    let cfg = BatcherConfig::default();
    let mut prev_index = 0u64;
    for block in blocks {
        let batch = pack_blocks(&cfg, std::slice::from_ref(block))
            .with_context(|| format!("pack block {}", block.block_number))?;
        let next = post_batch(&provider, settlement, prev_index, &batch, da_store)
            .await
            .with_context(|| format!("post block {} to L1", block.block_number))?;
        anyhow::ensure!(
            next == prev_index + 1,
            "batch index jumped {prev_index} -> {next}"
        );
        prev_index = next;
    }
    anyhow::ensure!(
        prev_index as usize == blocks.len(),
        "posted {prev_index} batches for {} blocks",
        blocks.len()
    );
    Ok(())
}

/// Rebuild the chain from L1 alone and require the root to equal
/// `expected_root` (the validator's live root).
///
/// Runs the real `kardamom-reconstruct` binary rather than the library, so
/// the operator-facing path — including its `--expect-root` gate, which had
/// no caller anywhere before this scenario — is what gets exercised.
pub fn reconstruct_and_compare(
    l1_rpc: &str,
    settlement: Address,
    da_dir: &Path,
    genesis: &Path,
    state_dir: &Path,
    expected_root: B256,
) -> Result<()> {
    let bin = crate::harness::services::bin("kardamom-reconstruct")?;
    let out = std::process::Command::new(bin)
        .args(["--l1-rpc", l1_rpc])
        .args(["--settlement", &settlement.to_string()])
        .arg("--da-store")
        .arg(da_dir)
        .arg("--chain")
        .arg(genesis)
        .arg("--state-dir")
        .arg(state_dir)
        .args(["--expect-root", &format!("{expected_root:#x}")])
        .output()
        .context("run kardamom-reconstruct")?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    anyhow::ensure!(
        out.status.success(),
        "kardamom-reconstruct --expect-root {expected_root:#x} FAILED — L1 data does not \
         rebuild the validator's state:\n{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    anyhow::ensure!(
        stdout.contains("reconstructed head="),
        "kardamom-reconstruct produced no result line:\n{stdout}"
    );

    // NON-VACUITY: a root comparison that passes proves nothing unless it can
    // fail. Re-run against a deliberately wrong root and require the gate to
    // reject it — otherwise a silently-disabled `--expect-root` (or a
    // reconstruct that quietly produced no blocks) would look like success.
    let mut wrong = expected_root.0;
    wrong[0] ^= 0xFF;
    let control_dir = state_dir.with_extension("control");
    let bin = crate::harness::services::bin("kardamom-reconstruct")?;
    let control = std::process::Command::new(bin)
        .args(["--l1-rpc", l1_rpc])
        .args(["--settlement", &settlement.to_string()])
        .arg("--da-store")
        .arg(da_dir)
        .arg("--chain")
        .arg(genesis)
        .arg("--state-dir")
        .arg(&control_dir)
        .args(["--expect-root", &format!("{:#x}", B256::from(wrong))])
        .output()
        .context("run kardamom-reconstruct (non-vacuity control)")?;
    anyhow::ensure!(
        !control.status.success(),
        "kardamom-reconstruct ACCEPTED a wrong expected root — the parity gate is vacuous"
    );
    Ok(())
}

/// Verify that the L1 log alone yields the batches just posted (what a
/// recovery operator starts from).
pub async fn assert_batches_on_l1(
    l1: &L1,
    settlement: Address,
    expected: usize,
    da_store: &FsBlobStore,
) -> Result<()> {
    let provider = l1.provider();
    let descriptors = read_posted_batches(&provider, settlement, 0)
        .await
        .context("read BatchPosted logs")?;
    anyhow::ensure!(
        descriptors.len() == expected,
        "L1 shows {} batches, expected {expected}",
        descriptors.len()
    );
    for (i, d) in descriptors.iter().enumerate() {
        anyhow::ensure!(
            d.index == i as u64 + 1,
            "batch index {} out of order at position {i}",
            d.index
        );
        anyhow::ensure!(
            !d.versioned_hashes.is_empty(),
            "batch {} has no blobs",
            d.index
        );
    }
    // And the blobs L1 committed to are fetchable + decodable.
    let frames = recover_blocks(&descriptors, da_store).context("recover blocks from blobs")?;
    anyhow::ensure!(
        frames.len() == expected,
        "recovered {} block frames from {expected} batches",
        frames.len()
    );
    Ok(())
}
