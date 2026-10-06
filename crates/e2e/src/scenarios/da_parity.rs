//! `da_parity_batcher_matches_validator`.
//!
//! This test proves "the batcher's state matches the validator's state" the
//! only way that claim is falsifiable. It takes what the live pipeline
//! actually executed, disperses it through the EigenDA proxy, posts the
//! certificates to L1, and throws away the originals. Then it rebuilds the
//! chain from L1 data alone. The rebuilt state root must equal the root the
//! validator computed on its own and attests to.
//!
//! The workload holds an L1 deposit and transfers, one of which spends the
//! deposit. The canonical order comes from two sources, and the scenario
//! invents no data:
//!
//! - The pipeline's own receipts. Each receipt carries `blockNumber` and
//!   `transactionIndex`, so the scenario recovers the block and the
//!   in-block order of each transaction it submitted.
//! - The validator's block headers. Each one carries the block's
//!   timestamp, its canonical end index and its L1 origin: the fields the
//!   batcher posts for each block.
//!
//! The payload holds no deposit. `kardamom-reconstruct --lockbox` derives
//! the deposits from the L1 origins and the lockbox logs, so its inputs are
//! exactly what a recovery operator has: L1, the DA proxy, and genesis.

use std::collections::BTreeMap;
use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_types::{BPosition, TxEnvelope};

use super::derivation::{BlockOrigin, await_block_origins_through};
use super::{Target, assert_receipt_ok, await_l2_receipt, receipt_placement};
use crate::harness::l1::L1;
use crate::harness::l2::{self, SignedTransfer};

/// The wei the workload's deposit mints on L2.
const DEPOSIT_WEI: u64 = 1_000_000_000_000_000_000;
/// The blocks one posted batch holds. A batch per block would cost one
/// L1 block per L2 block, and the scenario posts every committed block.
const BLOCKS_PER_BATCH: usize = 64;

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

/// What the workload left on the chain: each transaction it placed, and
/// the newest block that holds one of them or the deposit.
pub struct Workload {
    executed: Vec<Executed>,
    head: u64,
}

/// One sender's dense transfer run: `txs_per_sender` transfers, nonces
/// `0..txs_per_sender`.
///
/// # Errors
/// Returns an error when signing a transfer fails.
fn sign_sender_run(
    signer: &l2::DerivedSigner,
    chain_id: u64,
    txs_per_sender: usize,
    to: Address,
) -> Result<Vec<SignedTransfer>> {
    (0..txs_per_sender)
        .map(|n| l2::sign_transfer(signer, chain_id, n as u64, to, 1))
        .collect()
}

/// Deposit on L1 for `to` through the lockbox, and wait until the deposit
/// executes on L2. Returns the L2 block that holds it.
///
/// # Errors
/// Returns an error when the deposit fails on L1, or when its L2 receipt
/// does not appear or failed.
async fn deposit_and_await(t: &Target, l1: &L1, to: Address) -> Result<u64> {
    let (block_hash, log_index) = l1
        .deposit_eth(to, U256::from(DEPOSIT_WEI))
        .await
        .context("depositETH")?;
    // The da-watcher reads only finalized logs. With `--slots-in-an-epoch 1`,
    // a few blocks move the finalized block past the deposit.
    l1.mine(6).await?;
    let source_hash = kardamom_types::epoch::source_hash(block_hash, log_index);
    let receipt = await_l2_receipt(t, source_hash, "the deposit").await?;
    assert_receipt_ok(&receipt, "the deposit")?;
    Ok(receipt_placement(&receipt)?.block)
}

/// Locate `tx` in the canonical chain by its receipt. The receipt appears
/// when the transaction executes, so this needs no drain. The values come
/// from the executor, not from a guess by the test.
///
/// # Errors
/// Returns an error when the receipt is missing, unplaceable, or failed.
async fn place(t: &Target, tx: SignedTransfer) -> Result<Executed> {
    let receipt = await_l2_receipt(t, tx.hash, &format!("workload tx {}", tx.hash)).await?;
    let placement =
        receipt_placement(&receipt).with_context(|| format!("place receipt for {}", tx.hash))?;
    assert_receipt_ok(
        &receipt,
        &format!("tx {} (DA parity needs a clean workload)", tx.hash),
    )?;
    Ok(Executed {
        tx,
        block_number: placement.block,
        transaction_index: placement.index,
    })
}

/// Run the workload: one L1 deposit, the transfer runs, and one transfer
/// that spends the deposit. Then locate each transfer by its receipt.
///
/// # Errors
/// Returns an error when the deposit or a transfer fails, or when a
/// receipt is missing or unplaceable.
pub async fn run_workload(t: &Target, l1: &L1, p: &Params) -> Result<Workload> {
    // The senders, then one more signer: the deposit's beneficiary.
    let beneficiary_index = p
        .sender_base
        .checked_add(p.senders)
        .context("sender_base + senders overflows")?;
    let signers = l2::dev_signers_total(
        beneficiary_index
            .checked_add(1)
            .context("signer count overflows")?,
    )?;
    let to = Address::from([0x88u8; 20]);

    let beneficiary = &signers[beneficiary_index];
    let deposit_block = deposit_and_await(t, l1, beneficiary.address).await?;

    let runs = signers[p.sender_base..beneficiary_index]
        .iter()
        .map(|signer| sign_sender_run(signer, t.chain_id, p.txs_per_sender, to))
        .collect::<Result<Vec<Vec<SignedTransfer>>>>()?;
    let spend = l2::sign_transfer(beneficiary, t.chain_id, 0, to, 1)?;
    let planned: Vec<SignedTransfer> = runs.into_iter().flatten().chain([spend]).collect();

    super::submit_all(t, planned.clone()).await?;

    let mut executed = Vec::with_capacity(planned.len());
    for tx in planned {
        executed.push(place(t, tx).await?);
    }
    let head = executed
        .iter()
        .map(|e| e.block_number)
        .chain([deposit_block])
        .max()
        .unwrap_or(deposit_block);
    Ok(Workload { executed, head })
}

impl Workload {
    /// The newest block that holds the deposit or a workload transaction.
    #[must_use]
    pub fn head(&self) -> u64 {
        self.head
    }

    /// The canonical blocks, from block 1 through the newest header in
    /// `state_dir` (at least through [`Self::head`]). Each block carries its
    /// real timestamp, end index and L1 origin, and the workload's
    /// transactions in their receipt order. A block that holds none of them
    /// is empty in the payload: its epochs come back from L1 at rebuild
    /// time.
    ///
    /// # Errors
    /// Returns an error when the headers do not reach the head in time, or
    /// when a transaction's block has no header.
    pub async fn canonical_blocks(self, state_dir: &Path) -> Result<Vec<ClosedBlock>> {
        let headers = await_block_origins_through(state_dir, self.head).await?;
        let mut by_block = self.executed.into_iter().fold(
            BTreeMap::<u64, Vec<Executed>>::new(),
            |mut by_block, e| {
                by_block.entry(e.block_number).or_default().push(e);
                by_block
            },
        );
        let blocks: Vec<ClosedBlock> = headers
            .into_iter()
            .map(|header| Self::closed_block(header, by_block.remove(&header.block_number)))
            .collect();
        anyhow::ensure!(
            by_block.is_empty(),
            "workload blocks {:?} have no header",
            by_block.keys().collect::<Vec<_>>()
        );
        Ok(blocks)
    }

    /// The block `header` describes, holding `txs` in receipt order.
    fn closed_block(header: BlockOrigin, txs: Option<Vec<Executed>>) -> ClosedBlock {
        let mut txs = txs.unwrap_or_default();
        txs.sort_by_key(|e| e.transaction_index);
        let recorded = txs
            .iter()
            .map(|e| RecordedTx {
                // The DA payload does not carry positions, so the in-block
                // index is a faithful stand-in here.
                position: BPosition::from_index(e.transaction_index),
                envelope: TxEnvelope {
                    correlation_id: e.transaction_index,
                    raw_tx: bytes::Bytes::copy_from_slice(e.tx.raw.as_ref()),
                    sender: e.tx.sender,
                    tx_hash: e.tx.hash,
                    max_inclusion_block: u64::MAX,
                },
            })
            .collect();
        ClosedBlock {
            block_number: header.block_number,
            l2_timestamp: header.l2_timestamp,
            end_tx_idx: BPosition::from_index(header.end_tx_idx),
            l1_origin: header.l1_origin,
            remote_epochs: vec![],
            txs: recorded,
        }
    }
}

/// Post `blocks` to the settlement contract, [`BLOCKS_PER_BATCH`] blocks
/// to a batch: each payload goes through the DA proxy `da`, and its
/// certificate goes on L1. Check that L1's compare-and-set batch indices
/// advance with no gaps. Returns the number of batches posted.
///
/// # Errors
/// Returns an error when a batch fails to pack or post, or when the
/// posted batch index skips or repeats.
pub async fn post_to_l1(
    l1: &L1,
    settlement: Address,
    blocks: &[ClosedBlock],
    da: &DaProxy,
) -> Result<u64> {
    let provider = l1.wallet(crate::harness::l1::BATCHER_KEY)?;
    let mut prev_index = 0u64;
    for chunk in blocks.chunks(BLOCKS_PER_BATCH) {
        prev_index = post_chunk(&provider, settlement, prev_index, chunk, da).await?;
    }
    Ok(prev_index)
}

/// Post one batch of `chunk` after batch `prev_index`, and require L1 to
/// give it the next index.
///
/// # Errors
/// Returns an error when the batch fails to pack or post, or when the
/// index L1 gives it is not `prev_index + 1`.
async fn post_chunk(
    provider: &impl alloy_provider::Provider,
    settlement: Address,
    prev_index: u64,
    chunk: &[ClosedBlock],
    da: &DaProxy,
) -> Result<u64> {
    let first = chunk.first().map_or(0, |b| b.block_number);
    let batch = pack_blocks(&BatcherConfig::default(), chunk)
        .with_context(|| format!("pack the blocks from {first}"))?;
    let next = post_batch(provider, settlement, prev_index, &batch, da)
        .await
        .with_context(|| format!("post the blocks from {first} to L1"))?;
    anyhow::ensure!(
        Some(next) == prev_index.checked_add(1),
        "batch index jumped {prev_index} -> {next}"
    );
    Ok(next)
}

/// A `kardamom-reconstruct` run against one chain: the L1 endpoint, the
/// settlement contract, the lockbox, and the DA proxy and genesis to
/// rebuild from. The main run and its non-vacuity control share it.
pub struct Reconstruct<'a> {
    pub l1_rpc: &'a str,
    pub settlement: Address,
    pub lockbox: Address,
    pub da_proxy: &'a str,
    pub genesis: &'a Path,
}

impl Reconstruct<'_> {
    /// Run `kardamom-reconstruct` against `state_dir`, requiring the
    /// rebuilt state root to equal `expect_root`.
    ///
    /// # Errors
    /// Returns an error when the binary is not built or fails to run.
    fn run(&self, state_dir: &Path, expect_root: B256) -> Result<std::process::Output> {
        let bin = crate::harness::services::bin("kardamom-reconstruct")?;
        std::process::Command::new(bin)
            .args(["--l1-rpc", self.l1_rpc])
            .args(["--settlement", &self.settlement.to_string()])
            .args(["--lockbox", &self.lockbox.to_string()])
            .args(["--da-proxy", self.da_proxy])
            .arg("--chain")
            .arg(self.genesis)
            .arg("--state-dir")
            .arg(state_dir)
            .args(["--expect-root", &format!("{expect_root:#x}")])
            .output()
            .context("run kardamom-reconstruct")
    }

    /// Rebuild the chain from L1 and the DA proxy alone into `state_dir`.
    /// Require the root to equal `expected_root`, the live root.
    ///
    /// This runs the real `kardamom-reconstruct` binary, not the library.
    /// This exercises the operator-facing path, including its
    /// `--expect-root` gate.
    ///
    /// # Errors
    /// Returns an error when the binary is not built, when it fails to run,
    /// when it rejects the correct root, or when the non-vacuity control
    /// (a deliberately wrong root) is accepted.
    pub fn compare(&self, state_dir: &Path, expected_root: B256) -> Result<()> {
        let out = self.run(state_dir, expected_root)?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        anyhow::ensure!(
            out.status.success(),
            "kardamom-reconstruct --expect-root {expected_root:#x} FAILED — L1 data does not \
             rebuild the live state:\n{stdout}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        anyhow::ensure!(
            stdout.contains("reconstructed head="),
            "kardamom-reconstruct produced no result line:\n{stdout}"
        );

        // Non-vacuity check: a passing root comparison proves nothing
        // unless it can also fail. Run the check again against a wrong
        // root, and require the gate to reject it. Otherwise, a disabled
        // `--expect-root` gate, or a reconstruct that silently produced no
        // blocks, would look like a pass.
        let mut wrong = expected_root.0;
        wrong[0] ^= 0xFF;
        let control_dir = state_dir.with_extension("control");
        let control = self.run(&control_dir, B256::from(wrong))?;
        anyhow::ensure!(
            !control.status.success(),
            "kardamom-reconstruct ACCEPTED a wrong expected root — the parity gate is vacuous"
        );
        Ok(())
    }
}

/// What L1 must show after [`post_to_l1`]: the batches it posted, and the
/// blocks they hold.
pub struct Posted {
    pub batches: u64,
    pub blocks: usize,
}

/// Verify that the L1 log alone yields the batches just posted, and that
/// the DA proxy `da` serves every certificate in it. This is what a
/// recovery operator starts from.
///
/// # Errors
/// Returns an error when the batch count, ordering, or certificate
/// presence does not match `expected`, or when recovering blocks from the
/// DA proxy fails.
pub async fn assert_batches_on_l1(
    l1: &L1,
    settlement: Address,
    expected: &Posted,
    da: &DaProxy,
) -> Result<()> {
    let provider = l1.provider()?;
    let descriptors = read_posted_batches(&provider, settlement, 0)
        .await
        .context("read BatchPosted logs")?;
    anyhow::ensure!(
        u64::try_from(descriptors.len()) == Ok(expected.batches),
        "L1 shows {} batches, expected {}",
        descriptors.len(),
        expected.batches
    );
    for (i, d) in descriptors.iter().enumerate() {
        anyhow::ensure!(
            d.index == i as u64 + 1,
            "batch index {} out of order at position {i}",
            d.index
        );
        anyhow::ensure!(
            !d.da_cert.is_empty(),
            "batch {} has no DA certificate",
            d.index
        );
    }
    // Check that the payloads L1 committed to can be fetched and decoded.
    let frames = recover_blocks(&descriptors, da).context("recover blocks from the DA proxy")?;
    anyhow::ensure!(
        frames.len() == expected.blocks,
        "recovered {} block frames from {} batches, expected {}",
        frames.len(),
        expected.batches,
        expected.blocks
    );
    Ok(())
}
