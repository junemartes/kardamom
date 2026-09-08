//! The marker arms: `on_epoch` and `on_remote_epoch`. Both apply no
//! transaction; they check the marker, then advance the origin cursor and
//! the record's alignment count.

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord};

use crate::delta::ParentState;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{EpochObserver, RemoteEpochObserver};

use super::exec_thread::{ExecState, Flow};
use super::wiring::ExecPorts;

impl<W: ExecPorts> ExecState<W> {
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
        // record fail-stops instead of committing. `ParentState` reads the
        // live delta first, then the unsettled parent blocks, then the
        // committed snapshot. A read that skipped the pipelined parents
        // would seed the lane cursor one block low and halt on the next
        // honest record.
        if let Some(obs) = self.remote_epoch_observer.as_mut() {
            let parent_state = ParentState::new(&self.delta, self.parent.as_ref(), &self.snapshot);
            obs.observe(record, &parent_state)?;
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
