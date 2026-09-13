//! The feed side of the attestation driver: the leaf collectors, and the
//! [`TxReceiptsPublication`] wrapper that feeds the attester each block's
//! withdrawal leaves from the receipt stream.

use alloy_primitives::{B256, U256};
use kardamom_engine::{CMessage, ExecutorError, TxReceiptsPublication};
use kardamom_types::Receipt;
use kardamom_types::withdrawals;

use crate::block_accum::BlockAccumulator;

/// Collect the withdrawal leaves carried by one receipt's logs. Scans the
/// receipt's logs for `MessagePassed` events from the `L2ToL1MessagePasser`
/// predeploy. The caller sorts by nonce to get the canonical order the
/// on-chain Merkle proof indexes into (see [`AttestingReceiptSink::flush_through`]).
fn receipt_withdrawal_leaves(receipt: &Receipt) -> Vec<(U256, B256)> {
    receipt
        .logs
        .iter()
        .filter(|log| log.address == withdrawals::MESSAGE_PASSER)
        .filter_map(|log| withdrawals::decode_message_passed(&log.topics, &log.data))
        .collect()
}

/// A message on the attester's feed channel. `pub(super)` so
/// [`super::spawn_attester`] can build the channel and match on it; the
/// binary and other crates only ever see [`AttesterHandle`].
pub(super) enum AttesterMsg {
    Leaves { block: u64, leaves: Vec<B256> },
    Root { block: u64, state_root: B256 },
}

/// Feed side of the attestation task. Cheap to clone. Both methods do not
/// block, and are safe to call from sync (engine) threads.
#[derive(Clone)]
pub struct AttesterHandle {
    pub(super) tx: tokio::sync::mpsc::UnboundedSender<AttesterMsg>,
}

impl AttesterHandle {
    /// Build a handle paired with the receiver [`super::spawn_attester`]'s
    /// task drives.
    pub(super) fn channel() -> (Self, tokio::sync::mpsc::UnboundedReceiver<AttesterMsg>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Submit a committed block's withdrawal leaves, in nonce order. Call
    /// once per block, in order.
    pub fn submit_leaves(&self, block: u64, leaves: Vec<B256>) {
        let _ = self.tx.send(AttesterMsg::Leaves { block, leaves });
    }

    /// Submit a block's committed MPT state root, from the trie-aware
    /// writer's snapshot. The attester posts an output when the cadence is due.
    pub fn submit_root(&self, block: u64, state_root: B256) {
        let _ = self.tx.send(AttesterMsg::Root { block, state_root });
    }
}

/// Drop-in [`TxReceiptsPublication`] wrapper that feeds the attester each
/// block's withdrawal leaves, taken from the receipt stream.
///
/// This is the seam that actually works in the live pipeline: the engine
/// hands the state writer a `BlockDelta` whose `receipts` vector is always
/// empty (receipts travel on `tx_receipts` instead), so collecting leaves
/// from the delta finds zero. This sink collects them from the receipt
/// stream instead.
///
/// Leaves are buffered per block and flushed on the block boundary, so the
/// attester still receives them once per block, in nonce order, and
/// always before that block's state root. Receipts stream at execute
/// time; the root arrives only after commit.
pub struct AttestingReceiptSink<P: TxReceiptsPublication> {
    inner: P,
    handle: AttesterHandle,
    /// Leaves accumulated per block since the last boundary.
    pending: BlockAccumulator<(U256, B256)>,
}

impl<P: TxReceiptsPublication> AttestingReceiptSink<P> {
    pub fn new(inner: P, handle: AttesterHandle) -> Self {
        Self {
            inner,
            handle,
            pending: BlockAccumulator::new(),
        }
    }

    /// Flush every block up to and including `block` to the attester.
    ///
    /// The boundary block is always submitted, even with no leaves. On the
    /// `tx_receipts` stream, every receipt for a block precedes that
    /// block's `BlockBoundary`. So this submission is the attester's proof
    /// that receipts through `block` are complete, which is what lets it
    /// hold a state root back until it can pair with the full leaf set
    /// for its block (see [`AttestState::on_root`](super::state::AttestState::on_root)).
    /// An empty submission is not a no-op: "this block had no withdrawals"
    /// is exactly the fact the attester needs to attest it.
    fn flush_through(&mut self, block: u64) {
        let mut flushed = self.pending.drain_through(block);
        let own = flushed.remove(&block).unwrap_or_default();
        // Ascending order, with the boundary block last, so the
        // completeness marker never overtakes leaves for the blocks it
        // covers.
        for (b, leaves) in flushed.into_iter().chain(std::iter::once((block, own))) {
            self.submit_sorted(b, leaves);
        }
    }

    /// Sort `leaves` by withdrawal nonce (their canonical tree order) and
    /// submit them for block `b`.
    fn submit_sorted(&self, b: u64, mut leaves: Vec<(U256, B256)>) {
        leaves.sort_by_key(|(nonce, _)| *nonce);
        self.handle
            .submit_leaves(b, leaves.into_iter().map(|(_, leaf)| leaf).collect());
    }

    /// Buffers `r`'s withdrawal leaves for a later flush. Does nothing if
    /// the receipt has no withdrawal leaves.
    fn buffer_withdrawals(&mut self, r: &Receipt) {
        let leaves = receipt_withdrawal_leaves(r);
        if leaves.is_empty() {
            return;
        }
        self.pending.push_all(r.block_number, leaves);
    }
}

impl<P: TxReceiptsPublication> TxReceiptsPublication for AttestingReceiptSink<P> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match &msg {
            CMessage::Receipt(r) => self.buffer_withdrawals(r),
            CMessage::BlockBoundary(b) => self.flush_through(b.block_number),
        }
        self.inner.publish(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use alloy_sol_types::SolValue;
    use kardamom_types::{Receipt, WireLog};

    /// Build a `MessagePassed` log the way the predeploy emits it.
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

    fn receipt_with_logs(logs: Vec<WireLog>) -> Receipt {
        Receipt {
            logs,
            ..Default::default()
        }
    }

    #[test]
    fn collects_and_orders_leaves() {
        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        // Emit out of order; the caller sorts by nonce.
        let receipt = receipt_with_logs(vec![
            message_passed_log(1, s, t, 200),
            message_passed_log(0, s, t, 100),
        ]);
        let mut found = receipt_withdrawal_leaves(&receipt);
        found.sort_by_key(|(nonce, _)| *nonce);
        let leaves: Vec<B256> = found.into_iter().map(|(_, leaf)| leaf).collect();
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
        foreign.address = Address::from([0xff; 20]); // This is not the predeploy.
        let receipt = receipt_with_logs(vec![foreign]);
        assert!(receipt_withdrawal_leaves(&receipt).is_empty());
    }
}
