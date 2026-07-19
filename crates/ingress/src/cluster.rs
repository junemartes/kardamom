//! `ClusterWatermarkObserver` — the ingress on-quorum ack gate's durable
//! watermark, derived from Aeron Cluster (Raft) egress progress.
//!
//! In the cluster-only topology there is no standalone sealer publishing a
//! quorum/durable watermark on Aeron; instead the ingress connects to the
//! cluster as a client and folds egress progress into a [`ClusterWatermark`].
//! A record (or boundary) only reaches egress after the leader's replicated
//! state machine processed it, which only happens once it is **committed by a
//! Raft quorum** — so the highest egress index/boundary observed IS the durable
//! canonical count the `on-quorum` gate releases parked submits against.
//!
//! The bin runs [`ClusterWatermarkObserver::next_position`] on a dedicated OS
//! thread (egress `recv()` blocks) and forwards each returned count into the
//! proxy's watermark broadcast bus as a `QuorumWatermark`.

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

    /// Block for the next egress item, fold it into the watermark, and return
    /// the highest **durable canonical position** — `from_index(count - 1)`,
    /// where `count` is the monotonic durable record count. This is directly
    /// comparable to a receipt's `tx_idx` in the proxy's on-quorum gate
    /// (`watermark >= receipt_position`). Returns `None` on clean egress EOF.
    /// An item that does not advance the count past 0 (e.g. an empty-block
    /// boundary before any record) carries no durable position yet — keep
    /// polling. Malformed frames are skipped (logged).
    pub fn next_position(&mut self) -> Option<BPosition> {
        loop {
            let bytes = self.egress.recv()?;
            let count = match wire::decode_egress(&bytes) {
                Ok(EgressItem::Record { index, .. }) => self.watermark.observe_record(index),
                Ok(EgressItem::Boundary(b)) => {
                    self.watermark.observe_boundary(b.end_tx_idx.as_index())
                }
                // Replay control frames are per-session responses to a
                // REPLAY_FROM request; the ingress never sends one (it derives
                // a watermark from live progress only), so these cannot arrive
                // on its session — ignore defensively.
                Ok(EgressItem::ReplayDone { .. } | EgressItem::ReplayUnavailable { .. }) => {
                    continue;
                }
                Err(e) => {
                    // The cluster stream is authoritative, so this should never
                    // happen in practice; drop the frame and keep observing —
                    // but meter the drop so a framing mismatch (the hand-kept
                    // Java/Rust envelope drifting) surfaces as a counter, not
                    // just warn-level log volume.
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

/// Connect to the cluster and wrap its egress as a [`ClusterWatermarkObserver`].
/// The returned [`LiveCluster`] guard MUST be kept alive for as long as the
/// observer is polled.
pub fn cluster_watermark_observer(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterWatermarkObserver<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect(rt, cfg)?;
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

    /// A valid relayed-record egress frame at canonical `index` (the payload is
    /// a real `TxRef` so `decode_egress` parses it).
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
        let ingress = encode_ingress_txref(&r);
        let (_cid, relayed) = split_ingress(&ingress).unwrap();
        encode_egress_record(index, relayed)
    }

    #[test]
    fn records_advance_the_durable_position() {
        let egress = FakeEgress::new();
        // Records at canonical index 0, 1 ⇒ durable count 1, 2 ⇒ highest durable
        // position from_index(0), from_index(1) — directly comparable to receipts.
        egress.push(record(0, 10));
        egress.push(record(1, 20));
        egress.close();
        let mut obs = ClusterWatermarkObserver::new(egress);
        assert_eq!(obs.next_position(), Some(BPosition::from_index(0)));
        assert_eq!(obs.next_position(), Some(BPosition::from_index(1)));
        assert_eq!(obs.next_position(), None); // clean EOF
    }

    #[test]
    fn boundary_advances_to_end_tx_idx_and_never_regresses() {
        let egress = FakeEgress::new();
        // Boundary end_tx_idx=42 ⇒ count 42 ⇒ durable position from_index(41).
        egress.push(encode_egress_boundary(7, 42, 1_700_000_000_250));
        // A stale record (index 5 ⇒ count 6) must NOT regress below from_index(41).
        egress.push(record(5, 99));
        egress.close();
        let mut obs = ClusterWatermarkObserver::new(egress);
        assert_eq!(obs.next_position(), Some(BPosition::from_index(41)));
        assert_eq!(obs.next_position(), Some(BPosition::from_index(41)));
    }
}
