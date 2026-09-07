//! Adapters that convert log::aeron_live handles to the sequencer trait surface.

use kardamom_log::aeron_live::{
    TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxRemoteEpochsSubscriberHandle,
};
use kardamom_sequencer::epoch::EpochSubscriber;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::outbound::TxErrorPublisher;
use kardamom_sequencer::remote_epoch::RemoteEpochSubscriber;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord, TxError};

/// The live tx_data subscription set: one handle per lane the sequencer
/// reads. The own lane comes first. A resize adds the old lanes. The poll
/// walks the lanes in rotation, so a busy old lane cannot starve the own
/// lane.
pub struct LiveTxDataSub {
    lanes: Vec<(u8, TxDataSubscriberHandle)>,
    next: usize,
}

impl LiveTxDataSub {
    pub fn new(lanes: Vec<(u8, TxDataSubscriberHandle)>) -> Self {
        assert!(!lanes.is_empty(), "a sequencer reads at least one lane");
        Self { lanes, next: 0 }
    }
}

impl TxDataSubscriber for LiveTxDataSub {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        let n = self.lanes.len();
        for i in 0..n {
            let idx = (self.next + i) % n;
            let (lane, handle) = &mut self.lanes[idx];
            if let Some((loc, envelope)) = handle.try_recv() {
                self.next = (idx + 1) % n;
                return Ok(Some(Inbound {
                    lane: *lane,
                    loc,
                    envelope,
                }));
            }
        }
        Ok(None)
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
