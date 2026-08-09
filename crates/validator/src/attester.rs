//! Withdrawal attester: the validator's L1-facing seam.
//!
//! After the validator commits a block range and advances its MPT state root,
//! the attester (1) collects the withdrawals initiated in that range from the
//! re-executed block receipts, (2) builds the output root
//! `keccak(VERSION ++ stateRoot ++ withdrawalsRoot)`, and (3) posts it to the L1
//! `WithdrawalOutputOracle`. It is off the hot path and posts with its own L1
//! key — a dishonest attester is caught by the (milestone-1 permissioned,
//! later ZK) challenge.
//!
//! This module is split so the pure parts ([`collect_withdrawal_leaves`],
//! [`build_output`], [`AttestState`]) are unit-tested in isolation and the
//! [`OutputPoster`] is exercised against anvil by the integration test.
//!
//! The production driver is [`spawn_attester`]: a background task fed by the
//! binary with per-block withdrawal leaves
//! ([`AttesterHandle::submit_leaves`] — see [`AttestingWriterQueue`], a
//! drop-in [`StateWriterQueue`] wrapper that does it automatically) and
//! per-block committed state roots ([`AttesterHandle::submit_root`], from the
//! existing snapshot poller). Every `post_interval_blocks` it builds and posts
//! one output covering all pending leaves. Leaves are only discarded after a
//! successful post, so a failed proposal (L1 hiccup, or re-proposing a range
//! whose output was deleted by a challenge) is retried with the accumulated
//! set instead of being dropped.
//!
//! Challenge recovery: on startup the driver resumes below the latest
//! **non-deleted** on-chain output, so a challenged-and-deleted output is
//! re-attested from the leaves the validator re-collects on replay. Follow-up
//! (needs an `OutputDeleted` watcher + historical leaf storage): reacting to a
//! deletion of an *older* range mid-run without a restart.

use std::collections::BTreeMap;
use std::str::FromStr;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, TxHash, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::sol;
use kardamom_engine::{CMessage, ExecutorError, StateWriterQueue, TxReceiptsPublication};
use kardamom_types::withdrawals;
use kardamom_types::{BlockBoundary, BlockDelta, Receipt};

sol! {
    #[sol(rpc)]
    contract IWithdrawalOutputOracle {
        struct Output {
            bytes32 outputRoot;
            uint64 l2BlockNumber;
            uint64 timestamp;
            bool deleted;
        }
        function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber) external returns (uint256);
        function outputCount() external view returns (uint256);
        function getOutput(uint256 index) external view returns (Output memory);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AttesterError {
    #[error("L1 provider error: {0}")]
    Provider(String),
    #[error("attester config error: {0}")]
    Config(String),
}

impl From<alloy_contract::Error> for AttesterError {
    fn from(e: alloy_contract::Error) -> Self {
        AttesterError::Provider(e.to_string())
    }
}

impl From<alloy_provider::PendingTransactionError> for AttesterError {
    fn from(e: alloy_provider::PendingTransactionError) -> Self {
        AttesterError::Provider(e.to_string())
    }
}

/// Collect the withdrawal leaves initiated in `delta`'s block range, ordered by
/// withdrawal nonce. Scans every receipt's logs for `MessagePassed` events from
/// the `L2ToL1MessagePasser` predeploy (the per-receipt scan is
/// [`receipt_withdrawal_leaves`]). Returns the leaves in the canonical order
/// the on-chain Merkle proof indexes into.
pub fn collect_withdrawal_leaves(delta: &BlockDelta) -> Vec<B256> {
    let mut found: Vec<(U256, B256)> = delta
        .receipts
        .iter()
        .flat_map(receipt_withdrawal_leaves)
        .collect();
    found.sort_by_key(|(nonce, _)| *nonce);
    found.into_iter().map(|(_, leaf)| leaf).collect()
}

/// The committed output for a block range: its withdrawals root and the output
/// root posted to L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Output {
    pub state_root: B256,
    pub withdrawals_root: B256,
    pub output_root: B256,
}

/// Build the output root from the committed `state_root` and the range's
/// withdrawal `leaves`.
pub fn build_output(state_root: B256, leaves: &[B256]) -> Output {
    let withdrawals_root = withdrawals::withdrawals_root(leaves);
    let output_root = withdrawals::output_root(state_root, withdrawals_root);
    Output {
        state_root,
        withdrawals_root,
        output_root,
    }
}

/// Posts output roots to the L1 [`WithdrawalOutputOracle`]. Generic over the
/// alloy provider so it works with a wallet-backed HTTP provider in production
/// and an anvil-backed provider in tests.
pub struct OutputPoster<P> {
    provider: P,
    oracle: alloy_primitives::Address,
}

impl<P: Provider<Ethereum> + Clone> OutputPoster<P> {
    pub fn new(provider: P, oracle: alloy_primitives::Address) -> Self {
        Self { provider, oracle }
    }

    /// Propose `output_root` covering up to L2 block `l2_block_number`. Sends the
    /// tx, waits for inclusion, returns its hash.
    pub async fn propose_output(
        &self,
        output_root: B256,
        l2_block_number: u64,
    ) -> Result<TxHash, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let receipt = oracle
            .proposeOutput(output_root, l2_block_number)
            .send()
            .await?
            .get_receipt()
            .await?;
        Ok(receipt.transaction_hash)
    }

    /// Number of outputs already proposed (used to resume / skip on restart).
    pub async fn output_count(&self) -> Result<u64, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let n = oracle.outputCount().call().await?;
        Ok(n.try_into().unwrap_or(u64::MAX))
    }

    /// Highest L2 block covered by a **non-deleted** on-chain output, or `None`
    /// if none exists. This is the resume floor: deleted (challenged) outputs
    /// are skipped so a restarted attester re-attests the challenged range
    /// (its leaves are re-collected from the validator's replay) instead of
    /// leaving it stranded.
    pub async fn latest_attested_block(&self) -> Result<Option<u64>, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let mut i = self.output_count().await?;
        while i > 0 {
            i -= 1;
            let o = oracle.getOutput(U256::from(i)).call().await?;
            if !o.deleted {
                return Ok(Some(o.l2BlockNumber));
            }
            tracing::warn!(
                output_index = i,
                l2_block = o.l2BlockNumber,
                "on-chain output was deleted by a challenge; resuming attestation below it"
            );
        }
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Attestation driver: cadence + leaf accumulation state, and the spawn helper
// the validator binary wires in.
// ---------------------------------------------------------------------------

/// Configuration for the background attestation task ([`spawn_attester`]).
#[derive(Debug, Clone)]
pub struct AttesterConfig {
    /// L1 JSON-RPC endpoint the outputs are posted to.
    pub l1_rpc_url: String,
    /// Address of the deployed `WithdrawalOutputOracle` proxy.
    pub oracle: Address,
    /// Hex private key of the attester (the oracle's permissioned proposer).
    pub private_key: String,
    /// Post one output per this many L2 blocks (>= 1).
    pub post_interval_blocks: u64,
}

/// Pure attestation state: pending per-block withdrawal leaves and the posting
/// cadence. Driven by the [`spawn_attester`] loop; unit-tested in isolation.
///
/// Leaves survive failed posts (they are only cleared by
/// [`mark_attested`](Self::mark_attested)), which is what carries a challenged
/// / failed range's withdrawals forward into the next successful output.
#[derive(Debug, Default)]
pub struct AttestState {
    /// block number -> that block's withdrawal leaves (nonce-ordered).
    pending: BTreeMap<u64, Vec<B256>>,
    /// Highest L2 block covered by a successfully posted (non-deleted) output.
    last_attested: u64,
    /// Post cadence in blocks.
    interval: u64,
}

impl AttestState {
    pub fn new(last_attested: u64, interval: u64) -> Self {
        Self {
            pending: BTreeMap::new(),
            last_attested,
            interval: interval.max(1),
        }
    }

    /// Record a committed block's withdrawal leaves. Blocks at/below the
    /// attested floor are already covered by an on-chain output and dropped.
    pub fn on_leaves(&mut self, block: u64, leaves: Vec<B256>) {
        if block > self.last_attested && !leaves.is_empty() {
            self.pending.insert(block, leaves);
        }
    }

    /// True once `block`'s state root warrants a new output.
    pub fn due(&self, block: u64) -> bool {
        block >= self.last_attested + self.interval
    }

    /// All pending leaves for blocks `<= block`, concatenated in block order
    /// (== global withdrawal-nonce order, the tree's canonical leaf order).
    /// Non-destructive: cleared only by [`mark_attested`] after a successful
    /// post, so a failed post retries with the same accumulated set.
    pub fn leaves_through(&self, block: u64) -> Vec<B256> {
        self.pending
            .range(..=block)
            .flat_map(|(_, l)| l.iter().copied())
            .collect()
    }

    /// A successful post covered everything up to `block`.
    pub fn mark_attested(&mut self, block: u64) {
        self.last_attested = block;
        self.pending = self.pending.split_off(&(block + 1));
    }

    pub fn last_attested(&self) -> u64 {
        self.last_attested
    }
}

enum AttesterMsg {
    Leaves { block: u64, leaves: Vec<B256> },
    Root { block: u64, state_root: B256 },
}

/// Feed side of the attestation task. Cheap to clone; both methods are
/// non-blocking and safe to call from sync (engine) threads.
#[derive(Clone)]
pub struct AttesterHandle {
    tx: tokio::sync::mpsc::UnboundedSender<AttesterMsg>,
}

impl AttesterHandle {
    /// Submit a committed block's withdrawal leaves
    /// (`collect_withdrawal_leaves` output). Call once per block, in order.
    pub fn submit_leaves(&self, block: u64, leaves: Vec<B256>) {
        let _ = self.tx.send(AttesterMsg::Leaves { block, leaves });
    }

    /// Submit a block's committed MPT state root (from the trie-aware writer's
    /// snapshot). The attester posts an output when the cadence is due.
    pub fn submit_root(&self, block: u64, state_root: B256) {
        let _ = self.tx.send(AttesterMsg::Root { block, state_root });
    }
}

/// Drop-in [`StateWriterQueue`] wrapper that extracts each submitted block's
/// withdrawal leaves and feeds them to the attester before forwarding the
/// delta to the inner (validator) writer queue.
///
/// ⚠ **Only useful for deltas that actually carry receipts.** The live engine
/// finalizes every `BlockDelta` with an EMPTY receipts vec (receipts travel
/// on tx_receipts instead — see `kardamom_engine::delta::PendingDelta::
/// finalize`), so wiring the attester here collected nothing and every posted
/// output carried `leaves=0` — i.e. no withdrawal could ever be attested, and
/// therefore none could ever be finalized on L1. The binary now feeds leaves
/// from the receipt stream via [`AttestingReceiptSink`]; this wrapper is kept
/// for delta sources that do populate receipts (offline replay) and for its
/// unit tests.
pub struct AttestingWriterQueue<Q: StateWriterQueue> {
    inner: Q,
    handle: AttesterHandle,
}

impl<Q: StateWriterQueue> AttestingWriterQueue<Q> {
    pub fn new(inner: Q, handle: AttesterHandle) -> Self {
        Self { inner, handle }
    }
}

impl<Q: StateWriterQueue> StateWriterQueue for AttestingWriterQueue<Q> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        self.handle
            .submit_leaves(block.block_number, collect_withdrawal_leaves(&delta));
        self.inner.submit(block, delta)
    }
}

/// Collect the withdrawal leaves carried by ONE receipt's logs, in nonce
/// order. The receipt-stream analogue of [`collect_withdrawal_leaves`].
pub fn receipt_withdrawal_leaves(receipt: &Receipt) -> Vec<(U256, B256)> {
    let mut found = Vec::new();
    for log in &receipt.logs {
        if log.address != withdrawals::MESSAGE_PASSER {
            continue;
        }
        if let Some((nonce, leaf)) = withdrawals::decode_message_passed(&log.topics, &log.data) {
            found.push((nonce, leaf));
        }
    }
    found
}

/// Drop-in [`TxReceiptsPublication`] wrapper that feeds the attester each
/// block's withdrawal leaves, taken from the RECEIPT STREAM.
///
/// This is the seam that actually works in the live pipeline: the engine
/// hands the state writer a `BlockDelta` whose `receipts` vec is always empty
/// (receipts travel on tx_receipts), so collecting leaves from the delta —
/// which is what the binary used to do via [`AttestingWriterQueue`] — always
/// found zero and left every attested output with `leaves=0`. No withdrawal
/// could be proven on L1 as a result.
///
/// Leaves are buffered per block and flushed on the block boundary, so the
/// attester still receives them once per block, in nonce order, and always
/// BEFORE that block's state root (receipts stream at execute time, the root
/// only after commit).
pub struct AttestingReceiptSink<P: TxReceiptsPublication> {
    inner: P,
    handle: AttesterHandle,
    /// (block, leaves) accumulated since the last boundary.
    pending: BTreeMap<u64, Vec<(U256, B256)>>,
}

impl<P: TxReceiptsPublication> AttestingReceiptSink<P> {
    pub fn new(inner: P, handle: AttesterHandle) -> Self {
        Self {
            inner,
            handle,
            pending: BTreeMap::new(),
        }
    }

    /// Flush every block up to and including `block` to the attester.
    fn flush_through(&mut self, block: u64) {
        let tail = self.pending.split_off(&(block + 1));
        let flushed = std::mem::replace(&mut self.pending, tail);
        for (b, mut leaves) in flushed {
            leaves.sort_by_key(|(nonce, _)| *nonce);
            self.handle
                .submit_leaves(b, leaves.into_iter().map(|(_, leaf)| leaf).collect());
        }
    }
}

impl<P: TxReceiptsPublication> TxReceiptsPublication for AttestingReceiptSink<P> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match &msg {
            CMessage::Receipt(r) => {
                let leaves = receipt_withdrawal_leaves(r);
                if !leaves.is_empty() {
                    self.pending
                        .entry(r.block_number)
                        .or_default()
                        .extend(leaves);
                }
            }
            CMessage::BlockBoundary(b) => self.flush_through(b.block_number),
        }
        self.inner.publish(msg)
    }
}

/// Spawn the background attestation task. Must be called from within a tokio
/// runtime. Returns the feed handle and the task's `JoinHandle`; the task runs
/// until every `AttesterHandle` clone is dropped.
///
/// On startup it resumes from the latest **non-deleted** on-chain output (see
/// [`OutputPoster::latest_attested_block`]); a deleted (challenged) latest
/// output is therefore re-attested from the leaves the validator re-collects
/// on replay. A failed `proposeOutput` keeps its leaves pending and retries at
/// the next cadence point.
pub fn spawn_attester(
    cfg: AttesterConfig,
) -> Result<(AttesterHandle, tokio::task::JoinHandle<()>), AttesterError> {
    let key = cfg.private_key.trim().trim_start_matches("0x");
    let signer = PrivateKeySigner::from_str(key)
        .map_err(|e| AttesterError::Config(format!("invalid attester private key: {e}")))?;
    let url = cfg
        .l1_rpc_url
        .parse()
        .map_err(|e| AttesterError::Config(format!("invalid --l1-rpc-url: {e}")))?;
    let provider = ProviderBuilder::new().wallet(signer).connect_http(url);
    let poster = OutputPoster::new(provider, cfg.oracle);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let interval = cfg.post_interval_blocks;
    let task = tokio::spawn(async move {
        let resume = match poster.latest_attested_block().await {
            Ok(b) => b,
            Err(e) => {
                // Not fatal: start from genesis; the oracle's monotonicity
                // check rejects any overlap with already-attested ranges.
                tracing::warn!(error = %e, "attester: could not read resume point; starting at 0");
                None
            }
        };
        let mut state = AttestState::new(resume.unwrap_or(0), interval);
        tracing::info!(
            last_attested = state.last_attested(),
            post_interval_blocks = interval,
            "attester task started"
        );
        while let Some(msg) = rx.recv().await {
            match msg {
                AttesterMsg::Leaves { block, leaves } => state.on_leaves(block, leaves),
                AttesterMsg::Root { block, state_root } => {
                    if !state.due(block) {
                        continue;
                    }
                    let leaves = state.leaves_through(block);
                    let output = build_output(state_root, &leaves);
                    match poster.propose_output(output.output_root, block).await {
                        Ok(tx_hash) => {
                            tracing::info!(
                                l2_block = block,
                                leaves = leaves.len(),
                                output_root = %output.output_root,
                                l1_tx = %tx_hash,
                                "attester posted output"
                            );
                            state.mark_attested(block);
                        }
                        Err(e) => {
                            // Keep the leaves pending (carry them forward) and
                            // retry at the next cadence point.
                            tracing::warn!(
                                l2_block = block,
                                error = %e,
                                "attester failed to post output; will retry with accumulated leaves"
                            );
                        }
                    }
                }
            }
        }
        tracing::info!("attester task stopping (all handles dropped)");
    });
    Ok((AttesterHandle { tx }, task))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use alloy_sol_types::SolValue;
    use kardamom_types::{Receipt, WireLog};

    /// Build a MessagePassed log the way the predeploy emits it.
    fn message_passed_log(nonce: u64, sender: Address, target: Address, value: u64) -> WireLog {
        let leaf =
            withdrawals::withdrawal_leaf(U256::from(nonce), sender, target, U256::from(value));
        let mut data = Vec::new();
        data.extend_from_slice(&U256::from(value).to_be_bytes::<32>());
        data.extend_from_slice(leaf.as_slice());
        WireLog {
            address: withdrawals::MESSAGE_PASSER,
            topics: vec![
                withdrawals::message_passed_topic0(),
                B256::from(U256::from(nonce)),
                B256::from_slice(&(sender,).abi_encode()),
                B256::from_slice(&(target,).abi_encode()),
            ],
            data: data.into(),
        }
    }

    fn delta_with_logs(logs: Vec<WireLog>) -> BlockDelta {
        BlockDelta {
            block_number: 1,
            accounts: vec![],
            storage: vec![],
            code: vec![],
            receipts: vec![Receipt {
                logs,
                ..Default::default()
            }],
        }
    }

    #[test]
    fn collects_and_orders_leaves() {
        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        // Emit out of order; expect nonce-sorted leaves.
        let delta = delta_with_logs(vec![
            message_passed_log(1, s, t, 200),
            message_passed_log(0, s, t, 100),
        ]);
        let leaves = collect_withdrawal_leaves(&delta);
        assert_eq!(leaves.len(), 2);
        assert_eq!(
            leaves[0],
            withdrawals::withdrawal_leaf(U256::ZERO, s, t, U256::from(100u64))
        );
        assert_eq!(
            leaves[1],
            withdrawals::withdrawal_leaf(U256::from(1u64), s, t, U256::from(200u64))
        );
    }

    #[test]
    fn ignores_non_message_passed_logs() {
        let mut foreign = message_passed_log(0, Address::ZERO, Address::ZERO, 1);
        foreign.address = Address::from([0xff; 20]); // not the predeploy
        let delta = delta_with_logs(vec![foreign]);
        assert!(collect_withdrawal_leaves(&delta).is_empty());
    }

    #[test]
    fn attest_state_accumulates_and_clears_only_on_success() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, 4);
        st.on_leaves(1, vec![l(1)]);
        st.on_leaves(2, vec![]); // empty blocks are not stored
        st.on_leaves(3, vec![l(2), l(3)]);
        assert!(!st.due(3));
        assert!(st.due(4));

        // Post attempt at block 4: concatenated in block (== nonce) order.
        assert_eq!(st.leaves_through(4), vec![l(1), l(2), l(3)]);
        // Simulated post FAILURE: nothing cleared — the same set is retried
        // (the F06.1 carry-forward: a failed/challenged range's withdrawals
        // ride into the next attempt instead of being dropped).
        assert_eq!(st.leaves_through(4), vec![l(1), l(2), l(3)]);

        st.on_leaves(5, vec![l(4)]);
        // Success at block 8 covers everything through 8.
        assert_eq!(st.leaves_through(8), vec![l(1), l(2), l(3), l(4)]);
        st.mark_attested(8);
        assert_eq!(st.last_attested(), 8);
        assert!(st.leaves_through(8).is_empty());
        assert!(!st.due(11));
        assert!(st.due(12));

        // Stale leaves (block already attested) are ignored.
        st.on_leaves(7, vec![l(9)]);
        assert!(st.leaves_through(12).is_empty());
    }

    #[test]
    fn leaves_through_excludes_blocks_past_the_output_boundary() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, 1);
        st.on_leaves(1, vec![l(1)]);
        st.on_leaves(2, vec![l(2)]);
        // Output for block 1 must not swallow block 2's leaves.
        assert_eq!(st.leaves_through(1), vec![l(1)]);
        st.mark_attested(1);
        assert_eq!(st.leaves_through(2), vec![l(2)]);
    }

    #[test]
    fn attesting_writer_queue_feeds_leaves_and_forwards() {
        struct Recorder(Vec<u64>);
        impl StateWriterQueue for Recorder {
            fn submit(&mut self, b: BlockBoundary, _d: BlockDelta) -> Result<(), ExecutorError> {
                self.0.push(b.block_number);
                Ok(())
            }
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = AttesterHandle { tx };
        let mut q = AttestingWriterQueue::new(Recorder(Vec::new()), handle);

        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        let mut delta = delta_with_logs(vec![message_passed_log(0, s, t, 100)]);
        delta.block_number = 9;
        let boundary = BlockBoundary {
            block_number: 9,
            end_tx_idx: kardamom_types::BPosition::from_index(0),
            l2_timestamp: 0,
            l1_origin: 0,
        };
        q.submit(boundary, delta).unwrap();

        assert_eq!(q.inner.0, vec![9], "delta must be forwarded");
        match rx.try_recv().expect("leaves message sent") {
            AttesterMsg::Leaves { block, leaves } => {
                assert_eq!(block, 9);
                assert_eq!(
                    leaves,
                    vec![withdrawals::withdrawal_leaf(
                        U256::ZERO,
                        s,
                        t,
                        U256::from(100u64)
                    )]
                );
            }
            _ => panic!("expected Leaves message"),
        }
    }

    #[test]
    fn build_output_matches_types_helpers() {
        let sr = B256::from([0xab; 32]);
        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        let leaves = vec![withdrawals::withdrawal_leaf(
            U256::ZERO,
            s,
            t,
            U256::from(5u64),
        )];
        let out = build_output(sr, &leaves);
        assert_eq!(out.withdrawals_root, withdrawals::withdrawals_root(&leaves));
        assert_eq!(
            out.output_root,
            withdrawals::output_root(sr, out.withdrawals_root)
        );
    }
}
