//! `ClusterRefPublisher` — the sequencer's `TxOrderingRefPublisher` backed by
//! cluster ingress.
//!
//! Plugged into the sequencer in place of the Aeron `kardamom_log` publisher in
//! cluster mode. The sequencer actor is unchanged: it still calls
//! `try_publish_ref` / `try_publish_epoch`, and a back-pressured (or
//! not-yet-connected) cluster offer surfaces as `SequencerError::Backpressure`
//! so the existing rewind/retry path applies.

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{EpochRecord, TxRef};

use crate::SequencerError;
use crate::outbound::TxOrderingRefPublisher;

use kardamom_cluster_adapter::gateway::{ClusterIngress, OfferOutcome};
use kardamom_cluster_adapter::wire;

use kardamom_cluster_adapter::{
    LiveCluster, LiveClusterConfig, LiveEgress, LiveError, LiveIngress, live,
};

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
    fn try_publish_ref(
        &mut self,
        r: &TxRef,
        sender: alloy_primitives::Address,
        nonce: u64,
    ) -> Result<(), SequencerError> {
        let bytes = wire::encode_ingress_txref(r, sender, nonce);
        self.offer(&bytes)
    }

    fn try_publish_ref_batch(
        &mut self,
        refs: &[(TxRef, alloy_primitives::Address, u64)],
    ) -> (usize, Option<SequencerError>) {
        match refs {
            [] => (0, None),
            [(one, sender, nonce)] => match self.try_publish_ref(one, *sender, *nonce) {
                Ok(()) => (1, None),
                Err(e) => (0, Some(e)),
            },
            many => {
                let entries: Vec<Vec<u8>> = many
                    .iter()
                    .map(|(r, sender, nonce)| wire::encode_ingress_txref(r, *sender, *nonce))
                    .collect();
                let frame = wire::encode_ingress_batch(&entries);
                match self.offer(&frame) {
                    Ok(()) => (many.len(), None),
                    Err(e) => (0, Some(e)),
                }
            }
        }
    }

    fn try_publish_epoch(&mut self, e: &EpochRecord) -> Result<(), SequencerError> {
        // KIND_ORIGIN_RECORD, not a plain record: the sealer must close the
        // open block and adopt this epoch's L1 number before relaying it.
        // Epochs are never batched — one per L1 block, and each one forces a
        // boundary, so there is nothing to amortize.
        let bytes = wire::encode_ingress_epoch(e)
            .map_err(|err| SequencerError::EncodeFailed(format!("epoch: {err}")))?;
        self.offer(&bytes)
    }

    fn try_publish_remote_epoch(&mut self, r: &RemoteEpochRecord) -> Result<(), SequencerError> {
        // KIND_REMOTE_ORIGIN_RECORD, not KIND_ORIGIN_RECORD: the sealer must
        // advance the marker for THIS origin chain and must not stamp a peer
        // chain's anchor into an L2 block boundary. Never batched — one per
        // origin block that carried messages, each forcing a boundary.
        let bytes = wire::encode_ingress_remote_epoch(r)
            .map_err(|err| SequencerError::EncodeFailed(format!("remote epoch: {err}")))?;
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

/// [`cluster_ref_publisher`], but KEEPING the egress receiver. The cluster
/// broadcasts every relayed record and boundary to publisher sessions too
/// (load-bearing for the executors' broadcast; previously discarded here) —
/// the boundary frames carry `end_tx_idx`, the global canonical count the
/// lag-resync watermark trigger runs on
/// (docs/agents/sequencer-lag-resync-spec.md). Dropping the returned
/// `LiveEgress` restores the old discard behaviour.
pub fn cluster_ref_publisher_with_egress(
    rt: kardamom_log::aeron_live::AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterRefPublisher<LiveIngress>, LiveEgress), LiveError> {
    // Boundaries + contiguity rejects ONLY: line-rate record frames are
    // dropped at the session thread instead of being allocated + channelled
    // to a receiver that discards them (the session thread also services the
    // publish offers). The reject frames (#85 fix B) are the sealer telling
    // THIS publisher a known sender's ref would seal a nonce gap — the
    // watermark thread forwards them into the rewind path.
    let (cluster, ingress, egress) = live::connect_with_egress_kind_filter(
        rt,
        cfg,
        &[
            wire::EGRESS_KIND_BOUNDARY,
            wire::EGRESS_KIND_CONTIGUITY_REJECT,
        ],
    )?;
    Ok((cluster, ClusterRefPublisher::new(ingress), egress))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_cluster_adapter::gateway::fakes::FakeIngress;
    use kardamom_cluster_adapter::wire::{
        EgressItem, decode_egress, encode_egress_record, split_ingress,
    };
    use kardamom_types::{BPosition, TxOrderingMessage};

    fn txref() -> TxRef {
        TxRef::new(
            B256::repeat_byte(0x11),
            2,
            BPosition {
                term_id: 0,
                term_offset: 100,
            },
            0,
        )
    }

    #[test]
    fn publishes_txref_as_ingress_envelope() {
        let ingress = FakeIngress::new();
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        let r = txref();
        let sender = alloy_primitives::Address::repeat_byte(0x55);
        pubr.try_publish_ref(&r, sender, 9).unwrap();
        let sent = ingress.accepted();
        assert_eq!(sent.len(), 1);
        // The guard header carries the sender + nonce for the sealer.
        assert_eq!(
            kardamom_cluster_adapter::wire::ingress_sender_nonce(&sent[0]).unwrap(),
            (sender, 9)
        );
        // The Java service relays from the canonical id; that round-trips
        // back to the same TxRef.
        let (_cid, relayed) = split_ingress(&sent[0]).unwrap();
        match decode_egress(&encode_egress_record(0, relayed)).unwrap() {
            EgressItem::Record { msg, .. } => assert_eq!(msg, TxOrderingMessage::TxRef(r)),
            other => panic!("expected Record, got {other:?}"),
        }
    }

    /// An epoch must go out as KIND_ORIGIN_RECORD, not a plain record: the
    /// kind byte is what makes the sealer close the block and adopt the
    /// origin, so getting it wrong would silently strand deposits mid-block.
    #[test]
    fn publishes_epoch_as_an_origin_record_envelope() {
        let ingress = FakeIngress::new();
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        let e = EpochRecord {
            l1_number: 4_242,
            l1_hash: B256::repeat_byte(0x22),
            deposits: vec![kardamom_types::Deposit {
                source_hash: B256::repeat_byte(0xD1),
                mint: 7,
                ..Default::default()
            }],
        };
        pubr.try_publish_epoch(&e).unwrap();

        let sent = ingress.accepted();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][0], wire::KIND_ORIGIN_RECORD);
        assert_eq!(
            u64::from_le_bytes(sent[0][33..41].try_into().unwrap()),
            4_242,
            "the sealer reads the origin from this fixed offset"
        );
        // Marker + 1 deposit.
        assert_eq!(u32::from_le_bytes(sent[0][41..45].try_into().unwrap()), 2);
    }

    /// A remote epoch must go out as KIND_REMOTE_ORIGIN_RECORD, carrying the
    /// origin chain id alongside the anchor: the sealer keys its marker on the
    /// pair, so relaying this as a plain KIND_ORIGIN_RECORD would merge every
    /// peer's progress into the L1 origin and stamp a peer's anchor into an L2
    /// block boundary.
    #[test]
    fn publishes_remote_epoch_as_a_remote_origin_record_envelope() {
        let ingress = FakeIngress::new();
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        let r = RemoteEpochRecord {
            origin_chain_id: 412_346,
            anchor_number: 8_888,
            anchor_hash: B256::repeat_byte(0x33),
            first_seq: 4,
            messages: vec![kardamom_types::xchain::XChainMessage {
                source_hash: B256::repeat_byte(0xE7),
                seq: 4,
                gas_limit: 100_000,
                ..Default::default()
            }],
        };
        pubr.try_publish_remote_epoch(&r).unwrap();

        let sent = ingress.accepted();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][0], wire::KIND_REMOTE_ORIGIN_RECORD);
        assert_eq!(&sent[0][1..33], r.canonical_id().as_slice());
        assert_eq!(
            u64::from_le_bytes(sent[0][33..41].try_into().unwrap()),
            412_346,
            "the sealer reads the origin chain id from this fixed offset"
        );
        assert_eq!(
            u64::from_le_bytes(sent[0][41..49].try_into().unwrap()),
            8_888,
            "…and the anchor position from this one"
        );
        // Marker + 1 message.
        assert_eq!(u32::from_le_bytes(sent[0][49..53].try_into().unwrap()), 2);
    }

    #[test]
    fn backpressure_maps_to_sequencer_error() {
        let ingress = FakeIngress::new();
        ingress.set_outcome(OfferOutcome::BackPressured);
        let mut pubr = ClusterRefPublisher::new(ingress.clone());
        assert!(matches!(
            pubr.try_publish_ref(&txref(), alloy_primitives::Address::ZERO, 0),
            Err(SequencerError::Backpressure)
        ));
        // Nothing was accepted by the gateway.
        assert!(ingress.accepted().is_empty());
    }

    #[test]
    fn not_connected_maps_to_backpressure() {
        let ingress = FakeIngress::new();
        ingress.set_outcome(OfferOutcome::NotConnected);
        let mut pubr = ClusterRefPublisher::new(ingress);
        assert!(matches!(
            pubr.try_publish_ref(&txref(), alloy_primitives::Address::ZERO, 0),
            Err(SequencerError::Backpressure)
        ));
    }
}
