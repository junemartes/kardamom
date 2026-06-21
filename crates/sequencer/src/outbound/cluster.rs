//! `ClusterRefPublisher` — the sequencer's `TxOrderingRefPublisher` backed by
//! cluster ingress.
//!
//! Plugged into the sequencer in place of the Aeron `kardamom_log` publisher in
//! cluster mode. The sequencer actor is unchanged: it still calls
//! `try_publish_ref` / `try_publish_deposit_ref`, and a back-pressured (or
//! not-yet-connected) cluster offer surfaces as `SequencerError::Backpressure`
//! so the existing rewind/retry path applies.

use kardamom_types::{DepositRef, TxRef};

use crate::SequencerError;
use crate::outbound::TxOrderingRefPublisher;

use kardamom_cluster_adapter::gateway::{ClusterIngress, OfferOutcome};
use kardamom_cluster_adapter::wire;

use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveError, LiveIngress, live};

/// Cloning shares the single underlying cluster session: each clone holds a
/// clone of the ingress (a `Sender<OfferReq>`), so offers from every clone
/// serialise through the one session thread — correct single-writer behaviour
/// for the sequencer's main loop + deposit pump publishing concurrently.
#[derive(Clone)]
pub struct ClusterRefPublisher<I: ClusterIngress + Clone> {
    ingress: I,
}

impl<I: ClusterIngress + Clone> ClusterRefPublisher<I> {
    pub fn new(ingress: I) -> Self {
        Self { ingress }
    }

    fn offer(&mut self, bytes: &[u8]) -> Result<(), SequencerError> {
        match self.ingress.offer(bytes) {
            OfferOutcome::Accepted => Ok(()),
            // Both back-pressure and a transient disconnect map to the
            // sequencer's recoverable back-pressure: rewind and retry. Failover
            // / reconnect is handled inside the cluster client.
            OfferOutcome::BackPressured | OfferOutcome::NotConnected => {
                Err(SequencerError::Backpressure)
            }
        }
    }
}

impl<I: ClusterIngress + Clone> TxOrderingRefPublisher for ClusterRefPublisher<I> {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError> {
        let bytes = wire::encode_ingress_txref(r);
        self.offer(&bytes)
    }

    fn try_publish_deposit_ref(&mut self, r: &DepositRef) -> Result<(), SequencerError> {
        let bytes = wire::encode_ingress_depositref(r);
        self.offer(&bytes)
    }
}

/// Connect to the cluster and wrap ingress as a `TxOrderingRefPublisher`.
/// The returned `LiveCluster` guard MUST be kept alive while the publisher is used.
pub fn cluster_ref_publisher(
    rt: kardamom_log::aeron_live::AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterRefPublisher<LiveIngress>), LiveError> {
    let (cluster, ingress, _egress) = live::connect(rt, cfg)?;
    Ok((cluster, ClusterRefPublisher::new(ingress)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_cluster_adapter::gateway::fakes::FakeIngress;
    use kardamom_cluster_adapter::wire::{EgressItem, decode_egress, encode_egress_record, split_ingress};
    use kardamom_types::{BPosition, TxOrderingMessage};

    fn txref() -> TxRef {
        TxRef::new(B256::repeat_byte(0x11), 2, BPosition { term_id: 0, term_offset: 100 }, 0)
    }

    #[test]
    fn publishes_txref_as_ingress_envelope() {
        let ingress = FakeIngress::new();
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        let r = txref();
        pubr.try_publish_ref(&r).unwrap();
        let sent = ingress.accepted();
        assert_eq!(sent.len(), 1);
        // The Java service would relay payload from offset 1; that round-trips
        // back to the same TxRef.
        let (_cid, relayed) = split_ingress(&sent[0]).unwrap();
        match decode_egress(&encode_egress_record(0, relayed)).unwrap() {
            EgressItem::Record { msg, .. } => assert_eq!(msg, TxOrderingMessage::TxRef(r)),
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn publishes_depositref_as_ingress_envelope() {
        let ingress = FakeIngress::new();
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        let r = DepositRef::new(B256::repeat_byte(0x22), BPosition { term_id: 1, term_offset: 7 });
        pubr.try_publish_deposit_ref(&r).unwrap();
        assert_eq!(ingress.accepted().len(), 1);
    }

    #[test]
    fn backpressure_maps_to_sequencer_error() {
        let ingress = FakeIngress::new();
        ingress.set_outcome(OfferOutcome::BackPressured);
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        assert!(matches!(pubr.try_publish_ref(&txref()), Err(SequencerError::Backpressure)));
        // Nothing was accepted by the gateway.
        assert!(ingress.accepted().is_empty());
    }

    #[test]
    fn not_connected_maps_to_backpressure() {
        let ingress = FakeIngress::new();
        ingress.set_outcome(OfferOutcome::NotConnected);
        let mut pubr = ClusterRefPublisher::new(ingress);
        assert!(matches!(pubr.try_publish_ref(&txref()), Err(SequencerError::Backpressure)));
    }
}
