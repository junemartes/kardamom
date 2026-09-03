//! Adapters that convert log::aeron_live handles to the sequencer trait surface.

use kardamom_log::aeron_live::{
    TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxRemoteEpochsSubscriberHandle,
};
use kardamom_sequencer::epoch::EpochSubscriber;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::TxErrorPublisher;
use kardamom_sequencer::remote_epoch::RemoteEpochSubscriber;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord, TxDataLoc, TxEnvelope, TxError};

pub struct LiveTxDataSub {
    handle: TxDataSubscriberHandle,
    /// The lane the handle was opened on.
    lane: u8,
}

impl LiveTxDataSub {
    pub fn new(handle: TxDataSubscriberHandle, lane: u8) -> Self {
        Self { handle, lane }
    }
}

impl TxDataSubscriber for LiveTxDataSub {
    fn poll(&mut self) -> Result<Option<(TxDataLoc, TxEnvelope)>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        Ok(self.handle.try_recv())
    }

    fn lane(&self) -> u8 {
        self.lane
    }
}

pub struct LiveEpochSub {
    handle: TxDepositsSubscriberHandle,
}

impl LiveEpochSub {
    pub fn new(handle: TxDepositsSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl EpochSubscriber for LiveEpochSub {
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
        Ok(self.handle.try_recv())
    }
}

pub struct LiveRemoteEpochSub {
    handle: TxRemoteEpochsSubscriberHandle,
}

impl LiveRemoteEpochSub {
    pub fn new(handle: TxRemoteEpochsSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl RemoteEpochSubscriber for LiveRemoteEpochSub {
    fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError> {
        Ok(self.handle.try_recv())
    }
}

/// Live `TxErrorPublisher` that wraps a `TxErrorsPublisherHandle`.
/// The sequencer publishes rejections (today: duplicate or past-nonce) on
/// the `tx_errors` Aeron channel. Ingress reads them to release parked
/// clients early. A publish failure is logged and dropped: the canonical
/// state has already advanced, or the transaction was rejected, so there
/// is nothing to roll back.
pub struct LiveTxErrorPub {
    handle: TxErrorsPublisherHandle,
}

impl LiveTxErrorPub {
    pub fn new(handle: TxErrorsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxErrorPublisher for LiveTxErrorPub {
    fn publish_error(&mut self, e: TxError) {
        if let Err(err) = self.handle.publish(&e) {
            tracing::warn!(error = %err, "tx_errors publish failed (dropped)");
        }
    }
}
