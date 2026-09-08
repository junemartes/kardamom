//! The marker arms: `on_epoch` and `on_remote_epoch`. Both apply no
//! transaction; they check the marker, then advance the origin cursor and
//! the record's alignment count.

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord, SnapshotSource};

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::EpochObserver;

use super::exec_thread::{ExecState, Flow};
use super::ports::{StateWriterQueue, StateWriterSignal};

impl<S, Q, P, E> ExecState<S, Q, P, E>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    E: EpochObserver + 'static,
{
    pub(super) fn on_epoch(
        &mut self,
        tx_idx: TxIndex,
        epoch: &EpochRecord,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        // The epoch marker consumes one slot and applies no tx. It exists so
        // the L1 origin advances at a point every replica agrees on, and so
        // the deposits that follow start at a slot the sealer also reserved.
        self.check_in_order("Epoch", tx_idx, position)?;
        // Check this before the epoch's deposits apply, so a rejected epoch
        // fail-stops instead of committing.
        if let Some(obs) = self.epoch_observer.as_mut() {
            obs.observe(epoch)?;
        }
        tracing::debug!(
            target: "kardamom_executor::exec",
            block = self.current_block,
            l1_number = epoch.l1_number,
            deposits = epoch.deposits.len(),
            "epoch marker: L1 origin advances"
        );
        Ok(Flow::Continue)
    }

    pub(super) fn on_remote_epoch(
        &mut self,
        tx_idx: TxIndex,
        record: &RemoteEpochRecord,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        // Like the L1 epoch marker: consumes one slot, applies no tx — the
        // pair's origin advances at a point every replica agrees on, and the
        // messages that follow start at a slot the sealer also reserved.
        self.check_in_order("RemoteEpoch", tx_idx, position)?;
        // Checked BEFORE the record's messages execute, so a rejected
        // record fail-stops instead of committing. The observer reads the
        // parent state through the same lookup order as
        // `apply_block_close_actions`: the live delta first, then the
        // unsettled parent blocks, then the committed snapshot. A read
        // that skipped the pipelined parents would seed the lane cursor
        // one block low and halt on the next honest record.
        let Self {
            delta,
            parent,
            snapshot,
            remote_epoch_observer,
            ..
        } = self;
        if let Some(obs) = remote_epoch_observer.as_mut() {
            let parent_storage = |addr: alloy_primitives::Address, slot: alloy_primitives::B256| {
                if let Some(v) = delta.storage.get(&(addr, slot)) {
                    return Ok(*v);
                }
                if let Some(v) = parent.as_ref().and_then(|p| p.storage.get(&(addr, slot))) {
                    return Ok(*v);
                }
                snapshot
                    .storage(addr, slot)
                    .map_err(|e| format!("parent read {addr}/{slot}: {e:?}"))
            };
            obs.observe(record, &parent_storage)?;
        }
        tracing::debug!(
            target: "kardamom_executor::exec",
            block = self.current_block,
            origin_chain_id = record.origin_chain_id,
            anchor_number = record.anchor_number,
            messages = record.messages.len(),
            "remote epoch marker: pair origin advances"
        );
        Ok(Flow::Continue)
    }
}
