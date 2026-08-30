//! `ClusterWatermarkObserver` computes the ingress on-quorum ack gate's
//! durable watermark, from Aeron Cluster (Raft) egress progress.
//!
//! In the cluster-only topology, no standalone sealer publishes a
//! quorum or durable watermark on Aeron. Instead, the ingress connects to
//! the cluster as a client and folds egress progress into a
//! [`ClusterWatermark`]. A record or boundary reaches egress only after
//! the leader's replicated state machine processes it, and that happens
//! only once a Raft quorum commits it. So the highest egress index or
//! boundary observed is the durable canonical count that the `on-quorum`
//! gate releases parked submits against.
//!
//! The bin runs [`ClusterWatermarkObserver::next_position`] on a dedicated
//! OS thread, because egress `recv()` blocks. It forwards each returned
//! count into the proxy's watermark broadcast bus as a `QuorumWatermark`.

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::watermark::ClusterWatermark;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, live};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_types::BPosition;

/// Folds cluster egress progress into a monotonic durable count.
pub struct ClusterWatermarkObserver<E: ClusterEgress> {
    egress: E,
    watermark: ClusterWatermark,
}

impl<E: ClusterEgress> ClusterWatermarkObserver<E> {
    pub fn new(egress: E) -> Self {
        Self {
            egress,
            watermark: ClusterWatermark::new(),
        }
    }

    /// Blocks for the next egress item, folds it into the watermark, and
    /// returns the highest durable canonical position:
    /// `from_index(count - 1)`, where `count` is the increasing durable
    /// record count. This value compares directly to a receipt's `tx_idx`
    /// in the proxy's on-quorum gate (`watermark >= receipt_position`).
    /// Returns `None` on a clean egress EOF. An item that does not move
    /// the count past 0, such as an empty-block boundary before any
    /// record, has no durable position yet, so this method keeps polling.
    /// It skips malformed frames and logs them.
    pub fn next_position(&mut self) -> Option<BPosition> {
        loop {
            let bytes = self.egress.recv()?;
            let count = match wire::decode_egress(&bytes) {
                Ok(EgressItem::Record { index, .. }) => self.watermark.observe_record(index),
                Ok(EgressItem::Boundary(b)) => {
                    self.watermark.observe_boundary(b.end_tx_idx.as_index())
                }
                // Replay control frames are per-session responses to a
                // REPLAY_FROM request. The ingress never sends one; it
                // derives a watermark only from live progress. Contiguity
                // rejects go only to the offering sequencer session.
                // Neither can arrive here, so this arm ignores them as a
                // safeguard.
                Ok(
                    EgressItem::ReplayDone { .. }
                    | EgressItem::ReplayUnavailable { .. }
                    | EgressItem::ContiguityReject { .. },
                ) => {
                    continue;
                }
                Err(e) => {
                    // The cluster stream is authoritative, so this should
                    // not happen in practice. This code drops the frame
                    // and keeps observing, but meters the drop. This way,
                    // a framing mismatch between the hand-kept Java and
                    // Rust envelopes shows as a counter, not just log
                    // volume at warn level.
                    metrics::counter!(crate::metrics::CLUSTER_FRAME_DROPPED_TOTAL).increment(1);
                    tracing::warn!(error = %e, "ingress watermark: dropping malformed cluster egress frame");
                    continue;
                }
            };
            if count == 0 {
                continue;
            }
            return Some(BPosition::from_index(count - 1));
        }
    }
}

/// Connects to the cluster and wraps its egress as a
/// [`ClusterWatermarkObserver`]. Keep the returned [`LiveCluster`] guard
/// alive for as long as the observer is polled.
pub fn cluster_watermark_observer(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterWatermarkObserver<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect_subscribed(rt, cfg)?;
    Ok((cluster, ClusterWatermarkObserver::new(egress)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
    use kardamom_cluster_adapter::wire::{
        encode_egress_boundary, encode_egress_record, encode_ingress_txref, split_ingress,
    };
    use kardamom_types::{BPosition, TxRef};

    /// A valid relayed-record egress frame at canonical `index`. The
    /// payload is a real `TxRef`, so `decode_egress` can parse it.
    fn record(index: u64, off: i32) -> Vec<u8> {
        let r = TxRef::new(
            B256::repeat_byte(off as u8),
            0,
            BPosition {
                term_id: 0,
                term_offset: off,
            },
            0,
        );
        let ingress = encode_ingress_txref(&r, alloy_primitives::Address::ZERO, 0);
        let (_cid, relayed) = split_ingress(&ingress).unwrap();
        encode_egress_record(index, relayed)
    }

    #[test]
    fn records_advance_the_durable_position() {
        let egress = FakeEgress::new();
        // Records at canonical index 0 and 1 give a durable count of 1 and
        // 2, so the highest durable position is from_index(0) and
        // from_index(1). These compare directly to receipts.
        egress.push(record(0, 10));
        egress.push(record(1, 20));
        egress.close();
        let mut obs = ClusterWatermarkObserver::new(egress);
        assert_eq!(obs.next_position(), Some(BPosition::from_index(0)));
        assert_eq!(obs.next_position(), Some(BPosition::from_index(1)));
        assert_eq!(obs.next_position(), None); // Clean EOF.
    }

    #[test]
    fn boundary_advances_to_end_tx_idx_and_never_regresses() {
        let egress = FakeEgress::new();
        // A boundary with end_tx_idx=42 gives count 42, so the durable
        // position is from_index(41).
        egress.push(encode_egress_boundary(7, 42, 1_700_000_000_250, 0));
        // A stale record (index 5, count 6) must not move the position
        // below from_index(41).
        egress.push(record(5, 99));
        egress.close();
        let mut obs = ClusterWatermarkObserver::new(egress);
        assert_eq!(obs.next_position(), Some(BPosition::from_index(41)));
        assert_eq!(obs.next_position(), Some(BPosition::from_index(41)));
    }
}
