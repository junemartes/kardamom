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
        // record fail-stops instead of committing.
        if let Some(obs) = self.remote_epoch_observer.as_mut() {
            obs.observe(record)?;
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
