//! Live [`EpochPublisher`]/[`RemoteEpochPublisher`] adapters over the Aeron
//! `tx_deposits` and `tx_remote_epochs` publications. Split out of the main
//! binary file to keep it under the file line bound (`docs/STYLE.md` R3).

use kardamom_da_watcher::interop::RemoteEpochPublisher;
use kardamom_da_watcher::{EpochPublisher, PublishError};
use kardamom_log::aeron_live::{TxDepositsPublisherHandle, TxRemoteEpochsPublisherHandle};
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord};

/// Live [`EpochPublisher`] backed by an Aeron `tx_deposits` publication.
/// It publishes one epoch per finalized L1 block on `tx_deposits`. The
/// downstream sequencer forwards each one, unchanged, onto `tx_ordering` as
/// an origin-advancing record.
pub(crate) struct LiveTxDepositsPublisher {
    handle: TxDepositsPublisherHandle,
}

impl LiveTxDepositsPublisher {
    pub(crate) fn new(handle: TxDepositsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl EpochPublisher for LiveTxDepositsPublisher {
    fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError> {
        match self.handle.publish(epoch) {
            Ok(pos) => Ok(pos),
            Err(e) => {
                let msg = e.to_string();
                // Match Aeron's own `BACK_PRESSURED` token from
                // `offer_code_str`, not the prose "back-pressure".
                if msg.contains("BACK_PRESSURED") {
                    Err(PublishError::Backpressure)
                } else {
                    Err(PublishError::Transport(msg))
                }
            }
        }
    }
}

/// Live [`RemoteEpochPublisher`] backed by an Aeron `tx_remote_epochs`
/// publication — [`LiveTxDepositsPublisher`] for the interop path. One record
/// per peer-origin block that carried messages; the sequencer relays each
/// verbatim onto `tx_ordering` as a remote-origin-advancing record.
pub(crate) struct LiveRemoteEpochsPublisher {
    handle: TxRemoteEpochsPublisherHandle,
}

impl LiveRemoteEpochsPublisher {
    pub(crate) fn new(handle: TxRemoteEpochsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl RemoteEpochPublisher for LiveRemoteEpochsPublisher {
    /// A failed offer MUST be reported as failed. Both non-`Closed` variants
    /// are non-fatal — the watcher holds its cursor and re-derives the same
    /// batch, which is safe only because re-derivation is byte-identical, so
    /// cluster dedup on `canonical_id` absorbs a record that actually landed.
    /// The reverse error is unrecoverable: a publisher that reported a failed
    /// publish as complete would advance the cursor past a record that never
    /// existed, leaving a permanent hole in the pair's dense seq that the
    /// destination halts on and no retry can fill.
    fn publish(&self, record: &RemoteEpochRecord) -> Result<BPosition, PublishError> {
        match self.handle.publish(record) {
            Ok(pos) => Ok(pos),
            Err(e) => {
                let msg = e.to_string();
                // Match Aeron's own `BACK_PRESSURED` token from
                // `offer_code_str`, not the prose "back-pressure".
                if msg.contains("BACK_PRESSURED") {
                    Err(PublishError::Backpressure)
                } else {
                    Err(PublishError::Transport(msg))
                }
            }
        }
    }
}
