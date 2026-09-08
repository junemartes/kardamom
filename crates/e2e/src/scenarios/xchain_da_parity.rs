//! S13 — `xchain_da_parity`: the interop chain rebuilt from its OWN DA.
//!
//! The S8 guarantee, extended to cross-chain traffic per the recorded
//! decision (spec §16 Q8): `RemoteEpoch` records are posted into the
//! DESTINATION's own DA batches, so a chain that delivered peer messages is
//! self-reconstructible with no dependency on the peer being alive. Proven
//! the S8 way: run the real S12 delivery flow (mock feed → watcher binary →
//! sequencer relay → sealer → 0x7D execution), recover the canonical blocks
//! from the pipeline's own receipts — remote-epoch records attached to the
//! blocks they LED, re-derived through the SAME shared rule
//! ([`derive_remote_epoch`]) the watcher used — post them to anvil as real
//! EIP-4844 blobs, throw the originals away, and rebuild from L1 data alone.
//!
//! Parity target: S8 compared against the validator's root, but the
//! validator's whole-block strategy does not execute 0x7D yet (it fail-stops
//! by design), so this scenario roots the EXECUTOR's final state instead —
//! after a graceful shutdown froze it, [`kardamom_state::bootstrap_trie_from_state`]
//! builds the canonical MPT root offline from the flat tables (the same
//! adoption path a validator uses on an executor checkpoint). The
//! `kardamom-reconstruct --expect-root` gate (with S8's non-vacuity control)
//! then does the comparison, and the rebuilt DB is additionally required to
//! reproduce the interop substance: `Inbox.delivered`/`nextSeq`, the
//! receiver's stored calldata, and the 0x7D receipts keyed by
//! `remote_source_hash`.
//!
//! S8's honest limits carry over: deposit-free by construction (no
//! `depositETH` runs here) and synthesized `l2_timestamp`s (no workload
//! contract reads TIMESTAMP — the Outbox/Inbox predeploys read only
//! `block.chainid`).

use std::collections::BTreeMap;
use std::path::Path;

use alloy_primitives::{B256, U256};
use anyhow::{Context, Result};
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_types::xchain::{
    INBOX, Inbox, RemoteEpochRecord, derive_remote_epoch, remote_source_hash, xchain_tx_sender,
};
use kardamom_types::{BPosition, StateDatabase, TX_TYPE_XCHAIN, TxEnvelope};

use super::xchain::{self, DeliveryOutcome};
use super::{
    Target, assert_receipt_ok, await_l2_receipt, open_state_ro, receipt_field, receipt_placement,
};
use crate::harness::l2;

/// What [`collect_canonical_blocks`] recovered from the live pipeline.
pub struct CanonicalBlocks {
    /// The canonical blocks, remote-epoch records attached to the block each
    /// one led — the batcher input whose DA round-trip must reproduce the
    /// executor's state.
    pub blocks: Vec<ClosedBlock>,
    /// The three delivered 0x7D receipts (seq order), as the live RPC served
    /// them — the oracle the reconstructed DB's receipts are compared to.
    pub xchain_receipts: Vec<serde_json::Value>,
}

/// One user tx, located in the canonical chain by receipt placement.
struct PlacedTx {
    at: super::Placement,
    tx: l2::SignedTransfer,
}

/// The delivered 0x7D receipts (seq order), and where each one landed.
struct XChainReceipts {
    receipts: Vec<serde_json::Value>,
    placed: Vec<super::Placement>,
}

/// Accumulates one canonical block's remote-epoch records and user txs,
/// keyed by block number in [`DerivedRecords::group_into_blocks`].
#[derive(Default)]
struct Acc {
    remote_epochs: Vec<RemoteEpochRecord>,
    max_xchain_index: Option<u64>,
    txs: Vec<(u64, TxEnvelope)>,
}

impl Acc {
    /// Record one remote-epoch record and the xchain placements it
    /// covers, tracking the highest xchain index seen so far.
    fn push_record(&mut self, record: RemoteEpochRecord, placements: &[super::Placement]) {
        self.remote_epochs.push(record);
        self.max_xchain_index = placements
            .iter()
            .map(|p| p.index)
            .chain(self.max_xchain_index)
            .max();
    }

    /// Record one user tx at its placed index.
    fn push_tx(&mut self, index: u64, tx: &l2::SignedTransfer) {
        self.txs.push((
            index,
            TxEnvelope {
                correlation_id: index,
                raw_tx: bytes::Bytes::copy_from_slice(tx.raw.as_ref()),
                sender: tx.sender,
                tx_hash: tx.hash,
            },
        ));
    }

    /// Build the closed block for `block_number`. Replay executes a
    /// block's records first, then its txs — which is only faithful if
    /// that is how the live chain ordered them, so this checks it.
    fn into_closed_block(mut self, block_number: u64) -> Result<ClosedBlock> {
        self.txs.sort_by_key(|(i, _)| *i);
        if let (Some(max_x), Some((min_user, _))) = (self.max_xchain_index, self.txs.first()) {
            anyhow::ensure!(
                *min_user > max_x,
                "block {block_number}: user tx at index {min_user} precedes a 0x7D at {max_x}"
            );
        }
        let recorded: Vec<RecordedTx> = self
            .txs
            .into_iter()
            .map(|(index, envelope)| RecordedTx {
                // Positions do not reach the blob payload — the canonical
                // index is a faithful stand-in (the S8 idiom).
                position: BPosition::from_index(index),
                envelope,
            })
            .collect();
        let end = recorded.len() as u64;
        Ok(ClosedBlock {
            block_number,
            // Synthesized — see the module docs.
            // `block_number` is a wire/L2 block value; saturating keeps a
            // synthesized timestamp finite instead of wrapping.
            l2_timestamp: 1_700_000_000u64.saturating_add(block_number),
            end_tx_idx: BPosition::from_index(end),
            remote_epochs: self.remote_epochs,
            txs: recorded,
        })
    }
}

/// The two remote-epoch records [`CanonicalCollect::derive_records`]
/// re-derives: record A opens the block the first two messages share,
/// record B opens the later block the third message opens on its own.
struct DerivedRecords {
    a: RemoteEpochRecord,
    b: RemoteEpochRecord,
}

impl DerivedRecords {
    /// Group the derived records and the placed user txs into canonical
    /// blocks, in the order the live chain executed them.
    fn group_into_blocks(
        self,
        xchain_placed: &[super::Placement],
        placed: Vec<PlacedTx>,
    ) -> Result<Vec<ClosedBlock>> {
        let by_block = [
            (self.a, &xchain_placed[0..2]),
            (self.b, &xchain_placed[2..3]),
        ]
        .into_iter()
        .fold(
            BTreeMap::<u64, Acc>::new(),
            |mut by_block, (record, placements)| {
                by_block
                    .entry(placements[0].block)
                    .or_default()
                    .push_record(record, placements);
                by_block
            },
        );
        let by_block = placed.into_iter().fold(by_block, |mut by_block, p| {
            by_block
                .entry(p.at.block)
                .or_default()
                .push_tx(p.at.index, &p.tx);
            by_block
        });

        let blocks = by_block
            .into_iter()
            .map(|(block_number, acc)| acc.into_closed_block(block_number))
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!blocks.is_empty(), "workload produced no blocks");
        Ok(blocks)
    }
}

/// State for recovering the canonical blocks one `delivery` run produced:
/// the target and the outcome it returned. The steps below read this as
/// state instead of taking it as a loose parameter at every step.
struct CanonicalCollect<'a> {
    t: &'a Target,
    outcome: &'a DeliveryOutcome,
}

impl CanonicalCollect<'_> {
    /// Completeness is fenced, not assumed: one final transfer at the
    /// sender's next nonce is submitted and awaited. Per-sender nonce
    /// ordering means its receipt proves every earlier user tx
    /// (including any nudge whose submit timed out but executed late) is
    /// in the canonical chain — and, being deterministic bytes, a
    /// timed-out in-flight duplicate at the same nonce IS the fence tx,
    /// so nothing can land after collection.
    ///
    /// Submit the completeness fence at the sender's next nonce, and
    /// return the full user-tx list (the outcome's, plus the fence
    /// unless it is already present).
    async fn submit_completeness_fence(
        &self,
        sender: &l2::DerivedSigner,
        payee: alloy_primitives::Address,
    ) -> Result<Vec<l2::SignedTransfer>> {
        let fence = l2::sign_transfer(sender, self.t.chain_id, self.outcome.next_nonce, payee, 1)?;
        // A duplicate submit of an already-executed tx errors — that's fine,
        // the receipt await below is the authority.
        let _ = self.t.rpc.send_raw(&fence.raw).await;
        let mut user_txs: Vec<l2::SignedTransfer> = self.outcome.user_txs.clone();
        if !user_txs.iter().any(|tx| tx.hash == fence.hash) {
            user_txs.push(fence);
        }
        Ok(user_txs)
    }

    /// Locate every user tx in the canonical chain, by receipt placement.
    async fn locate_user_txs(&self, user_txs: Vec<l2::SignedTransfer>) -> Result<Vec<PlacedTx>> {
        let mut placed = Vec::with_capacity(user_txs.len());
        for tx in user_txs {
            let receipt =
                await_l2_receipt(self.t, tx.hash, &format!("user tx nonce {}", tx.nonce)).await?;
            assert_receipt_ok(&receipt, &format!("user tx nonce {}", tx.nonce))?;
            let at = receipt_placement(&receipt)?;
            placed.push(PlacedTx { at, tx });
        }
        Ok(placed)
    }

    /// The delivered 0x7D receipts, and where each one's messages opened
    /// a block. Also checks that seq 3 is still pending, since the DA
    /// set built from these two records would be falsified by a third
    /// appearing now.
    async fn await_xchain_receipts(&self) -> Result<XChainReceipts> {
        let mut receipts = Vec::with_capacity(3);
        let mut placed: Vec<super::Placement> = Vec::with_capacity(3);
        for seq in 0..3u64 {
            let source_hash = remote_source_hash(xchain::ORIGIN_CHAIN_ID, seq);
            let r = await_l2_receipt(self.t, source_hash, &format!("xchain seq {seq}")).await?;
            assert_receipt_ok(&r, &format!("xchain seq {seq}"))?;
            placed.push(receipt_placement(&r)?);
            receipts.push(r);
        }
        let pending = self
            .t
            .rpc
            .receipt(remote_source_hash(xchain::ORIGIN_CHAIN_ID, 3))
            .await
            .result
            .map_err(|e| anyhow::anyhow!("receipt probe for pending seq 3: {e}"))?;
        anyhow::ensure!(
            pending.is_none(),
            "seq 3 must still be pending when the DA set is collected: {pending:?}"
        );
        Ok(XChainReceipts { receipts, placed })
    }

    /// Re-derive the two remote-epoch records through the SHARED rule —
    /// byte-identical to what the watcher published and the sealer
    /// ordered: one copy of the derivation, or the parity proves nothing
    /// (the `derive_remote_epoch` contract) — and check their placement.
    fn derive_records(&self, xchain_placed: &[super::Placement]) -> Result<DerivedRecords> {
        let a = derive_remote_epoch(
            self.t.chain_id,
            xchain::ORIGIN_CHAIN_ID,
            0,
            &self.outcome.messages[0..2],
        )
        .context("derive origin-block-100 record")?;
        let b = derive_remote_epoch(
            self.t.chain_id,
            xchain::ORIGIN_CHAIN_ID,
            2,
            &self.outcome.messages[2..3],
        )
        .context("derive origin-block-101 record")?;
        anyhow::ensure!(
            xchain_placed[0].block == xchain_placed[1].block
                && xchain_placed[1].index.checked_sub(xchain_placed[0].index) == Some(1),
            "record A's two messages must open one block contiguously (got {:?} / {:?})",
            (xchain_placed[0].block, xchain_placed[0].index),
            (xchain_placed[1].block, xchain_placed[1].index)
        );
        anyhow::ensure!(
            xchain_placed[2].block > xchain_placed[0].block,
            "record B must open a later block (got {} after {})",
            xchain_placed[2].block,
            xchain_placed[0].block
        );
        Ok(DerivedRecords { a, b })
    }
}

/// # Errors
/// Returns an error when the completeness fence or a user tx's receipt
/// fails, when seq 3 is not still pending, when the derived records do
/// not match the live placement, or when a block orders a user tx before
/// a 0x7D record that must have preceded it.
pub async fn collect_canonical_blocks(
    t: &Target,
    outcome: &DeliveryOutcome,
) -> Result<CanonicalBlocks> {
    let signers = l2::dev_signers_total(2)?;
    let sender = &signers[0];
    let payee = signers[1].address;

    let collect = CanonicalCollect { t, outcome };
    let user_txs = collect.submit_completeness_fence(sender, payee).await?;
    let placed = collect.locate_user_txs(user_txs).await?;
    let xchain = collect.await_xchain_receipts().await?;
    let records = collect.derive_records(&xchain.placed)?;
    let blocks = records.group_into_blocks(&xchain.placed, placed)?;
    Ok(CanonicalBlocks {
        blocks,
        xchain_receipts: xchain.receipts,
    })
}

/// The canonical MPT root of a STOPPED trie-off executor DB, built offline
/// from the flat state tables — the validator's checkpoint-adoption path
/// ([`kardamom_state::bootstrap_trie_from_state`]), reused as the parity
/// target because the executor persists no root of its own.
///
/// # Errors
/// Returns an error when the state dir cannot be opened, or when the
/// flat state fails to root.
pub fn executor_state_root(state_dir: &Path) -> Result<B256> {
    let env = kardamom_state::StateEnvBuilder::new(state_dir)
        .open()
        .context("open stopped executor state dir")?;
    kardamom_state::bootstrap_trie_from_state(&env).context("root the executor's flat state")
}

/// Table-by-table first-diffs between two stopped state DBs
/// ([`kardamom_state::deep_compare`]) — the forensic dump the root-parity
/// failure path prints, so a mismatch in CI names rows instead of two
/// opaque hashes.
///
/// # Errors
/// Returns an error when either state dir cannot be opened.
pub fn state_diff(a_dir: &Path, b_dir: &Path) -> Result<Vec<String>> {
    let a = kardamom_state::StateEnvBuilder::new(a_dir)
        .open()
        .context("open state dir a")?;
    let b = kardamom_state::StateEnvBuilder::new(b_dir)
        .open()
        .context("open state dir b")?;
    kardamom_state::deep_compare(&a, &b).context("deep_compare")
}

/// Beyond the root gate: the rebuilt DB must reproduce the interop SUBSTANCE
/// byte-for-byte against the live executor's DB — Inbox lane state, the
/// receiver's stored calldata word, and the 0x7D receipts (keyed by
/// `remote_source_hash`) matching the receipts the live RPC served.
///
/// # Errors
/// Returns an error when a storage slot read fails, when the lane state,
/// receiver storage, or a 0x7D receipt does not match between the two
/// databases (or does not match the expected value), or when a receipt
/// the rebuilt DB should hold is missing.
pub fn assert_reconstructed_interop_state(
    recon_dir: &Path,
    executor_dir: &Path,
    outcome: &DeliveryOutcome,
    canonical: &CanonicalBlocks,
) -> Result<()> {
    let origin = xchain::ORIGIN_CHAIN_ID;

    // Lane state: equal across the two DBs AND equal to the expected values
    // (equality alone could be vacuously satisfied by two empty DBs).
    let next_seq_slot = Inbox::next_seq_slot(origin);
    let live_next = xchain::read_slot(executor_dir, INBOX, next_seq_slot)?;
    let recon_next = xchain::read_slot(recon_dir, INBOX, next_seq_slot)?;
    anyhow::ensure!(
        live_next == U256::from(3) && recon_next == live_next,
        "Inbox.nextSeq[{origin}] must be 3 on both sides (live {live_next}, rebuilt {recon_next})"
    );
    (0..3u64).try_for_each(|seq| {
        let slot = Inbox::delivered_slot(origin, seq);
        let live = xchain::read_slot(executor_dir, INBOX, slot)?;
        let recon = xchain::read_slot(recon_dir, INBOX, slot)?;
        anyhow::ensure!(
            live == U256::from(1) && recon == live,
            "Inbox.delivered[{origin}][{seq}] must be success on both sides \
             (live {live}, rebuilt {recon})"
        );
        Ok(())
    })?;
    let stored = xchain::read_slot(recon_dir, outcome.receiver, B256::ZERO)?;
    anyhow::ensure!(
        B256::from(stored.to_be_bytes::<32>()) == outcome.payload_word,
        "the rebuilt receiver contract must hold the delivered calldata word: got {stored:#x}"
    );

    // The 0x7D receipts, reproduced. The rebuilt DB indexes them by the same
    // canonical id the live chain used (remote_source_hash), and every
    // RPC-visible field the live receipt carried must match.
    let env = open_state_ro(recon_dir)?;
    let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot rebuilt state")?;
    for (seq, live) in canonical.xchain_receipts.iter().enumerate() {
        let source_hash = remote_source_hash(origin, seq as u64);
        let pos = snap
            .get_tx_position(source_hash)
            .context("tx-hash index lookup")?
            .with_context(|| format!("rebuilt DB must index the seq-{seq} 0x7D receipt"))?;
        let receipt = snap
            .get_receipt(pos)
            .context("receipt lookup")?
            .with_context(|| format!("rebuilt DB must hold the seq-{seq} 0x7D receipt"))?;
        anyhow::ensure!(receipt.tx_type == TX_TYPE_XCHAIN, "receipt must be 0x7D");
        anyhow::ensure!(receipt.status, "rebuilt delivery seq {seq} must succeed");
        anyhow::ensure!(
            receipt.tx_hash == source_hash && receipt.effective_gas_price == 0,
            "rebuilt delivery must keep its canonical id and stay fee-free"
        );
        anyhow::ensure!(
            receipt.from == xchain_tx_sender(origin) && receipt.to == Some(INBOX),
            "rebuilt delivery must be aliased-Outbox → Inbox"
        );
        let live_gas = receipt_field(live, "gasUsed")
            .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .with_context(|| format!("live receipt for seq {seq} has no gasUsed"))?;
        anyhow::ensure!(
            receipt.gas_used == live_gas,
            "seq {seq}: rebuilt gas_used {} != live {live_gas}",
            receipt.gas_used
        );
        let live_logs = live
            .get("logs")
            .and_then(|l| l.as_array())
            .map_or(0, Vec::len);
        anyhow::ensure!(
            receipt.logs.len() == live_logs,
            "seq {seq}: rebuilt {} log(s) != live {live_logs}",
            receipt.logs.len()
        );
    }
    Ok(())
}
