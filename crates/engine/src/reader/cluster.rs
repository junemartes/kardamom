//! `ClusterTxOrderingSubscription` — the executor's `TxOrderingSubscription`
//! backed by cluster egress.
//!
//! Plugged into the executor's tx_ordering reader thread in cluster mode. The
//! reader is unchanged: it calls `next()` and gets canonical-ordered
//! `(BPosition, TxOrderingMessage)` records. Leader failover / reconnect is
//! handled inside the cluster client, so the reader never sees an image
//! rotation. The cluster has already deduped and totally ordered the stream;
//! the executor's own `DedupWindow` still provides idempotency across any
//! reconnect overlap.

use kardamom_types::{BPosition, TxOrderingMessage};

use crate::ExecutorError;
use crate::reader::TxOrderingSubscription;

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::wire::{self, EgressItem};

use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, live};

pub struct ClusterTxOrderingSubscription<E: ClusterEgress> {
    egress: E,
}

impl<E: ClusterEgress> ClusterTxOrderingSubscription<E> {
    pub fn new(egress: E) -> Self {
        Self { egress }
    }
}

impl<E: ClusterEgress> TxOrderingSubscription for ClusterTxOrderingSubscription<E> {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        loop {
            let Some(bytes) = self.egress.recv() else {
                return Err(ExecutorError::TxOrderingClosed);
            };
            match wire::decode_egress(&bytes) {
                Ok(EgressItem::Record { index, msg }) => {
                    return Ok((BPosition::from_index(index), msg));
                }
                Ok(EgressItem::Boundary(b)) => {
                    let pos = b.end_tx_idx;
                    return Ok((pos, TxOrderingMessage::BoundaryStart(b)));
                }
                Err(e) => {
                    // A malformed frame is dropped (logged); the cluster stream
                    // is authoritative, so this should never happen in practice.
                    tracing::warn!(error = %e, "dropping malformed cluster egress frame");
                    continue;
                }
            }
        }
    }
}

/// Aeron Cluster (Raft) sealer client config — the `[cluster]` TOML section
/// shared by every role that reads the canonical stream (executor, validator).
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct ClusterConfig {
    /// Retained for config-file backward compatibility; IGNORED — cluster mode
    /// is the only mode, so clients always connect to the cluster.
    pub enabled: bool,
    /// "memberId=host:port,…" cluster ingress endpoints.
    pub ingress_endpoints: String,
    pub initial_leader_member_id: i32,
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI, e.g. "aeron:udp?endpoint=<ip>:<port>".
    pub egress_channel: String,
    pub egress_stream_id: i32,
    pub keep_alive_interval_ms: u64,
}

impl ClusterConfig {
    /// Sane stream-id / keepalive defaults when the TOML omits them.
    pub fn defaults_applied(mut self) -> Self {
        if self.ingress_stream_id == 0 {
            self.ingress_stream_id = 101;
        }
        if self.egress_stream_id == 0 {
            self.egress_stream_id = 102;
        }
        if self.keep_alive_interval_ms == 0 {
            self.keep_alive_interval_ms = 1000;
        }
        self
    }

    pub fn to_live(&self) -> LiveClusterConfig {
        let c = self.clone().defaults_applied();
        LiveClusterConfig {
            ingress_endpoints: c.ingress_endpoints,
            initial_leader_member_id: c.initial_leader_member_id,
            ingress_stream_id: c.ingress_stream_id,
            egress_channel: c.egress_channel,
            egress_stream_id: c.egress_stream_id,
            keep_alive_interval_ms: c.keep_alive_interval_ms,
        }
    }
}

/// Connect to the cluster and wrap egress as a `TxOrderingSubscription`.
/// The returned `LiveCluster` guard MUST be kept alive while the subscription is used.
pub fn cluster_tx_ordering_subscription(
    rt: kardamom_log::aeron_live::AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterTxOrderingSubscription<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect(rt, cfg)?;
    Ok((cluster, ClusterTxOrderingSubscription::new(egress)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
    use kardamom_cluster_adapter::wire::{
        encode_egress_boundary, encode_egress_record, encode_ingress_txref, split_ingress,
    };
    use kardamom_types::TxRef;

    fn relayed_txref(shard: u8, off: i32) -> Vec<u8> {
        let r = TxRef::new(
            B256::repeat_byte(off as u8),
            shard,
            BPosition {
                term_id: 0,
                term_offset: off,
            },
            0,
        );
        let ingress = encode_ingress_txref(&r);
        let (_cid, relayed) = split_ingress(&ingress).unwrap();
        relayed.to_vec()
    }

    #[test]
    fn yields_records_with_monotonic_bposition() {
        let egress = FakeEgress::new();
        egress.push(encode_egress_record(0, &relayed_txref(1, 10)));
        egress.push(encode_egress_record(1, &relayed_txref(2, 20)));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);

        let (p0, m0) = sub.next().unwrap();
        assert_eq!(p0, BPosition::from_index(0));
        assert!(matches!(m0, TxOrderingMessage::TxRef(_)));
        let (p1, _m1) = sub.next().unwrap();
        assert_eq!(p1, BPosition::from_index(1));
        // Stream closed ⇒ TxOrderingClosed (the reader treats this as clean EOF).
        assert!(matches!(sub.next(), Err(ExecutorError::TxOrderingClosed)));
    }

    #[test]
    fn yields_boundary_with_fields_intact() {
        let egress = FakeEgress::new();
        egress.push(encode_egress_boundary(7, 42, 1_700_000_000_250));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);
        let (pos, msg) = sub.next().unwrap();
        match msg {
            TxOrderingMessage::BoundaryStart(b) => {
                assert_eq!(b.block_number, 7);
                assert_eq!(b.end_tx_idx.as_index(), 42);
                assert_eq!(b.l2_timestamp, 1_700_000_000_250);
                assert_eq!(pos, b.end_tx_idx);
            }
            other => panic!("expected boundary, got {other:?}"),
        }
    }

    #[test]
    fn malformed_frame_is_skipped_not_fatal() {
        let egress = FakeEgress::new();
        egress.push(vec![0xFF, 0x00]); // bad egress kind
        egress.push(encode_egress_boundary(1, 0, 0));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);
        // The malformed frame is skipped; the next good frame is returned.
        let (_pos, msg) = sub.next().unwrap();
        assert!(matches!(msg, TxOrderingMessage::BoundaryStart(_)));
    }
}
