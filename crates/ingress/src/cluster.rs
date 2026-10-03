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
//! The bin runs [`ClusterWatermarkObserver::next_event`] on a dedicated
//! OS thread, because egress `recv()` blocks. It forwards each returned
//! count into the proxy's watermark broadcast bus as a `QuorumWatermark`,
//! and each cluster status (the posted head, the DA-lag flag, the
//! retention floors) into the proxy's status channel.

use std::ops::ControlFlow;

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::watermark::ClusterWatermark;
use kardamom_cluster_adapter::wire::EgressItem;
use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, live};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_types::{BPosition, ClusterStatus};

/// What one egress poll observed: durable progress, or the chain's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// The highest durable canonical position.
    Durable(BPosition),
    /// The sealer's data-availability status.
    Status(ClusterStatus),
}

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

    /// Blocks for the next egress item and folds it into the watermark.
    /// Returns the highest durable canonical position,
    /// `from_index(count - 1)`, where `count` is the increasing durable
    /// record count, or the cluster status a status frame carries. The
    /// position compares directly to a receipt's `tx_idx` in the proxy's
    /// on-quorum gate (`watermark >= receipt_position`). Returns `None`
    /// on a clean egress EOF. An item that does not move the count past
    /// 0, such as an empty-block boundary before any record, has no
    /// durable position yet, so this method keeps polling. It skips
    /// malformed frames and logs them.
    pub fn next_event(&mut self) -> Option<Observed> {
        loop {
            if let ControlFlow::Break(result) = self.poll_event() {
                return result;
            }
        }
    }

    /// One [`Self::next_event`] poll. `Break(None)` means the egress
    /// ended. `Break(Some(observed))` carries the next durable position
    /// or the status. `Continue` means the frame moved nothing durable
    /// yet, or was skipped, so the caller polls again.
    fn poll_event(&mut self) -> ControlFlow<Option<Observed>> {
        let Some(bytes) = self.egress.recv() else {
            return ControlFlow::Break(None);
        };
        let count = match EgressItem::decode(&bytes) {
            Ok(EgressItem::Record { index, .. }) => self.watermark.observe_record(index),
            Ok(EgressItem::Boundary(b)) => self.watermark.observe_boundary(b.end_tx_idx.as_index()),
            Ok(EgressItem::Status(status)) => {
                return ControlFlow::Break(Some(Observed::Status(status)));
            }
            // Replay control frames are per-session responses to a
            // REPLAY_FROM request. The ingress never sends one; it
            // derives a watermark only from live progress. Every reject
            // goes only to the offering sequencer session. None can
            // arrive here, so this arm ignores them as a safeguard.
            Ok(
                EgressItem::ReplayDone { .. }
                | EgressItem::ReplayUnavailable { .. }
                | EgressItem::ContiguityReject { .. }
                | EgressItem::RemoteOriginReject { .. }
                | EgressItem::PastDeadline { .. }
                | EgressItem::WindowFull { .. }
                | EgressItem::DaLagReject { .. },
            ) => return ControlFlow::Continue(()),
            Err(e) => {
                // The cluster stream is authoritative, so this should
                // not happen in practice. This code drops the frame
                // and keeps observing, but meters the drop. This way,
                // a framing mismatch between the hand-kept Java and
                // Rust envelopes shows as a counter, not just log
                // volume at warn level.
                metrics::counter!(crate::metrics::CLUSTER_FRAME_DROPPED_TOTAL).increment(1);
                tracing::warn!(error = %e, "ingress watermark: dropping malformed cluster egress frame");
                return ControlFlow::Continue(());
            }
        };
        let Some(last) = count.checked_sub(1) else {
            return ControlFlow::Continue(());
        };
        ControlFlow::Break(Some(Observed::Durable(BPosition::from_index(last))))
    }
}

/// Connects to the cluster and wraps its egress as a
/// [`ClusterWatermarkObserver`]. Keep the returned [`LiveCluster`] guard
/// alive for as long as the observer is polled.
///
/// # Errors
///
/// Returns `LiveError` if the cluster connection fails.
pub fn cluster_watermark_observer(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterWatermarkObserver<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect_with(
        rt,
        cfg,
        live::ConnectOptions {
            subscribe: true,
            ..Default::default()
        },
    )?;
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
    /// payload is a real `TxRef`, so `EgressItem::decode` can parse it.
    fn record(index: u64, off: i32) -> Vec<u8> {
        let byte = u8::try_from(off).expect("test offsets fit in a u8");
        let r = TxRef::new(
            B256::repeat_byte(byte),
            0,
            BPosition {
                term_id: 0,
                term_offset: off,
            },
            0,
        );
        let ingress = encode_ingress_txref(&r, alloy_primitives::Address::ZERO, 0, u64::MAX);
        let (_cid, relayed) = split_ingress(&ingress).unwrap();
        encode_egress_record(index, relayed).unwrap()
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
        assert_eq!(
            obs.next_event(),
            Some(Observed::Durable(BPosition::from_index(0)))
        );
        assert_eq!(
            obs.next_event(),
            Some(Observed::Durable(BPosition::from_index(1)))
        );
        assert_eq!(obs.next_event(), None); // Clean EOF.
    }

    /// A status frame is its own event: it moves no durable position, and
    /// a reject to another session is skipped.
    #[test]
    fn a_status_frame_is_observed_and_a_reject_is_skipped() {
        let egress = FakeEgress::new();
        let status = ClusterStatus {
            posted_head: 5,
            sealed_head: 9,
            budget_blocks: 10,
            halted: false,
            retained_frames: 40,
            floor_index: 3,
            floor_block: 2,
        };
        egress.push(kardamom_cluster_adapter::wire::encode_status(&status));
        egress.push(kardamom_cluster_adapter::wire::encode_da_lag_reject(
            alloy_primitives::Address::ZERO,
            0,
            &status,
        ));
        egress.push(record(0, 10));
        egress.close();
        let mut obs = ClusterWatermarkObserver::new(egress);
        assert_eq!(obs.next_event(), Some(Observed::Status(status)));
        assert_eq!(
            obs.next_event(),
            Some(Observed::Durable(BPosition::from_index(0)))
        );
        assert_eq!(obs.next_event(), None);
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
        assert_eq!(
            obs.next_event(),
            Some(Observed::Durable(BPosition::from_index(41)))
        );
        assert_eq!(
            obs.next_event(),
            Some(Observed::Durable(BPosition::from_index(41)))
        );
    }
}
