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

use kardamom_executor::ExecutorError;
use kardamom_executor::reader::TxOrderingSubscription;
use kardamom_types::{BPosition, TxOrderingMessage};

use crate::gateway::ClusterEgress;
use crate::wire::{self, EgressItem};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::fakes::FakeEgress;
    use crate::wire::{encode_egress_boundary, encode_egress_record, encode_ingress_txref, split_ingress};
    use alloy_primitives::B256;
    use kardamom_types::TxRef;

    fn relayed_txref(shard: u8, off: i32) -> Vec<u8> {
        let r = TxRef::new(B256::repeat_byte(off as u8), shard, BPosition { term_id: 0, term_offset: off });
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
