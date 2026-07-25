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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

use crate::ExecutorError;
use crate::reader::TxOrderingSubscription;

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::wire::{self, EgressItem};

use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, live};

/// Shared delivery cursor: the next canonical record index + boundary block
/// number this consumer expects. Written by the subscription on every
/// delivery; read by the live session thread to compose the `REPLAY_FROM`
/// request it sends on every (re)connect.
#[derive(Clone)]
pub struct ReplayCursor {
    pub next_index: Arc<AtomicU64>,
    pub next_block: Arc<AtomicU64>,
}

impl ReplayCursor {
    pub fn new(next_index: u64, next_block: u64) -> Self {
        Self {
            next_index: Arc::new(AtomicU64::new(next_index)),
            next_block: Arc::new(AtomicU64::new(next_block)),
        }
    }

    /// Fresh consumer at genesis: no records seen, first boundary is block 1
    /// (`CanonicalSealerState.GENESIS_BLOCK_NUMBER`).
    pub fn genesis() -> Self {
        Self::new(0, 1)
    }
}

/// Bound on the out-of-order catch-up buffers — a replay bigger than this
/// cannot be reordered in memory and fails loudly instead of ballooning.
const MAX_PENDING: usize = 1 << 20;

pub struct ClusterTxOrderingSubscription<E: ClusterEgress> {
    egress: E,
    cursor: ReplayCursor,
    /// Out-of-order buffers, keyed by canonical index / block number. Frames
    /// arrive out of order only around a session re-establishment, when the
    /// service's replay interleaves with live broadcasts.
    pending_records: BTreeMap<u64, TxOrderingMessage>,
    pending_boundaries: BTreeMap<u64, BlockBoundaryStart>,
    /// True while a replay is outstanding (a key gap was observed). In this
    /// mode a record may only be delivered once the boundary at `next_block`
    /// proves it precedes it (`end_tx_idx > index`); in live mode frames
    /// arrive in emission order, so an absent boundary is a FUTURE boundary.
    catching_up: bool,
    /// Whether deliveries re-export the sealer's boundary stream as
    /// `kardamom_sealer_*` metrics. Only ONE role per host should emit them
    /// (the executor — the blessed observation point the probes/dashboards
    /// scrape); the validator shares this subscription but suppresses the
    /// emission so its exporter doesn't publish a second, lagging copy of the
    /// series (see `crate::metrics`).
    emit_sealer_metrics: bool,
}

impl<E: ClusterEgress> ClusterTxOrderingSubscription<E> {
    pub fn new(egress: E) -> Self {
        Self::with_cursor(egress, ReplayCursor::genesis())
    }

    pub fn with_cursor(egress: E, cursor: ReplayCursor) -> Self {
        Self {
            egress,
            cursor,
            pending_records: BTreeMap::new(),
            pending_boundaries: BTreeMap::new(),
            catching_up: false,
            emit_sealer_metrics: true,
        }
    }

    /// Disable the `kardamom_sealer_*` re-export (validator role).
    pub fn suppress_sealer_metrics(mut self) -> Self {
        self.emit_sealer_metrics = false;
        self
    }

    /// Deliver the next in-order item from the buffers, if provably next.
    fn try_deliver(&mut self) -> Option<(BPosition, TxOrderingMessage)> {
        let ni = self.cursor.next_index.load(Ordering::Relaxed);
        let nb = self.cursor.next_block.load(Ordering::Relaxed);
        if let Some(b) = self.pending_boundaries.get(&nb)
            && b.end_tx_idx.as_index() <= ni
        {
            let b = self.pending_boundaries.remove(&nb).unwrap();
            self.cursor.next_block.store(nb + 1, Ordering::Relaxed);
            // The clustered sealer has no Prometheus endpoint; the EXECUTOR
            // re-exports its boundary stream here (see `crate::metrics`). The
            // validator constructs this subscription with the emission
            // suppressed.
            if self.emit_sealer_metrics {
                metrics::counter!(crate::metrics::SEALER_BOUNDARIES_TOTAL).increment(1);
                metrics::gauge!(crate::metrics::SEALER_BLOCK_NUMBER).set(b.block_number as f64);
            }
            return Some((b.end_tx_idx, TxOrderingMessage::BoundaryStart(b)));
        }
        // A record is deliverable when nothing proves an earlier boundary is
        // missing: either the boundary at `next_block` is buffered and closes
        // AFTER this record, or we are in live mode (in-order arrival).
        let record_is_next = match self.pending_boundaries.get(&nb) {
            Some(b) => b.end_tx_idx.as_index() > ni,
            None => !self.catching_up,
        };
        if record_is_next && let Some(msg) = self.pending_records.remove(&ni) {
            self.cursor.next_index.store(ni + 1, Ordering::Relaxed);
            return Some((BPosition::from_index(ni), msg));
        }
        None
    }

    /// Classify one egress item into the buffers (dedup + gap detection).
    fn ingest(&mut self, item: EgressItem) -> Result<(), ExecutorError> {
        let ni = self.cursor.next_index.load(Ordering::Relaxed);
        let nb = self.cursor.next_block.load(Ordering::Relaxed);
        match item {
            EgressItem::Record { index, msg } => {
                if index < ni {
                    return Ok(()); // replay/live overlap duplicate
                }
                if index > ni && !self.catching_up {
                    // A key gap can only follow a session re-establishment;
                    // the session thread has already sent REPLAY_FROM(cursor).
                    tracing::info!(
                        expected = ni,
                        got = index,
                        "cluster egress gap — entering replay catch-up"
                    );
                    self.catching_up = true;
                }
                self.pending_records.insert(index, msg);
            }
            EgressItem::Boundary(b) => {
                if b.block_number < nb {
                    return Ok(());
                }
                // Canonical-order guard (boundary-only gap across a session
                // reconnect): a boundary we still owe downstream
                // (block_number >= next_block) that seals at a record count
                // BELOW the delivery cursor proves we already delivered
                // records that canonically FOLLOW it — the missed boundary
                // was emitted during a session outage, the reconnect's first
                // live frame was exactly the next-index record (no key gap ⇒
                // no catch-up), and the replayed boundary arrived too late.
                // Delivering it now would seal its block with a later
                // block's records inside: a silent canonical-order
                // divergence between replicas. Fail-stop instead — a restart
                // resumes from the persisted cursor and the REPLAY_FROM on
                // connect re-delivers the whole window in order. (Entering
                // catch-up on every session re-establishment would PREVENT
                // the inversion, but the session thread lives in
                // kardamom-cluster-adapter and exposes no reconnect signal;
                // within one Aeron session frames are ordered, so this
                // condition has no false positives.)
                if b.end_tx_idx.as_index() < ni {
                    tracing::error!(
                        block = b.block_number,
                        boundary_end = b.end_tx_idx.as_index(),
                        delivered = ni,
                        "boundary sealing below the delivery cursor — boundary-only gap across a reconnect"
                    );
                    return Err(ExecutorError::BoundaryMisaligned {
                        end: b.end_tx_idx,
                        last_seen: BPosition::from_index(ni),
                    });
                }
                if b.block_number > nb && !self.catching_up {
                    tracing::info!(
                        expected = nb,
                        got = b.block_number,
                        "cluster boundary gap — entering replay catch-up"
                    );
                    self.catching_up = true;
                }
                self.pending_boundaries.insert(b.block_number, b);
            }
            EgressItem::ReplayDone { .. } => {
                self.catching_up = false;
            }
            EgressItem::ReplayUnavailable {
                oldest_index,
                oldest_block,
            } => {
                return Err(ExecutorError::ClusterReplayUnavailable {
                    from_index: ni,
                    oldest_index,
                    oldest_block,
                });
            }
        }
        if self.pending_records.len() > MAX_PENDING || self.pending_boundaries.len() > MAX_PENDING {
            return Err(ExecutorError::State(
                "cluster catch-up buffer overflow — replay window too large to reorder".into(),
            ));
        }
        Ok(())
    }
}

impl<E: ClusterEgress> TxOrderingSubscription for ClusterTxOrderingSubscription<E> {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        loop {
            if let Some(out) = self.try_deliver() {
                return Ok(out);
            }
            let Some(bytes) = self.egress.recv() else {
                return Err(ExecutorError::TxOrderingClosed);
            };
            match wire::decode_egress(&bytes) {
                Ok(item) => self.ingest(item)?,
                Err(e) => {
                    // A malformed frame is dropped (logged); the cluster stream
                    // is authoritative, so this should never happen in practice.
                    tracing::warn!(error = %e, "dropping malformed cluster egress frame");
                }
            }
        }
    }
}

// The `[cluster]` TOML section (shared by every role that connects to the
// cluster) has ONE definition, in `kardamom-cluster-adapter` next to the
// `LiveClusterConfig` it maps onto; re-exported here so the existing
// `kardamom_engine::reader::cluster::ClusterConfig` paths (executor,
// validator) keep resolving.
pub use kardamom_cluster_adapter::ClusterConfig;

/// Connect to the cluster and wrap egress as a `TxOrderingSubscription`.
/// The returned `LiveCluster` guard MUST be kept alive while the subscription is used.
///
/// `cursor` is the consumer's resume point — `(records applied, next block)`
/// from the persistent state on crash recovery, [`ReplayCursor::genesis`] on a
/// fresh start. The session thread sends `REPLAY_FROM(cursor)` on EVERY
/// session establishment (first connect and every re-connect), so the
/// canonical stream is gapless across session loss: the service re-offers the
/// retained frames the consumer missed, and the subscription's catch-up
/// ordering merges them with live broadcasts.
pub fn cluster_tx_ordering_subscription(
    rt: kardamom_log::aeron_live::AeronRuntime,
    cfg: LiveClusterConfig,
    cursor: ReplayCursor,
) -> Result<(LiveCluster, ClusterTxOrderingSubscription<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect_with_replay(
        rt,
        cfg,
        live::ReplayOnConnect {
            next_index: cursor.next_index.clone(),
            next_block: cursor.next_block.clone(),
        },
    )?;
    Ok((
        cluster,
        ClusterTxOrderingSubscription::with_cursor(egress, cursor),
    ))
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
    fn replay_gap_is_reordered_and_deduped() {
        // Live-ahead frames arrive FIRST (the session reconnected mid-stream);
        // the replayed range then fills the gap. Canonical order: r0 r1 b1(end2)
        // r2 r3 b2(end4) r4 r5.
        let egress = FakeEgress::new();
        // Live-ahead: record 5 and boundary 2 arrive before the replay.
        egress.push(encode_egress_record(5, &relayed_txref(1, 5)));
        egress.push(encode_egress_boundary(2, 4, 2_000));
        // Replayed frames (emission order), incl. a duplicate of record 5's
        // predecessor range and both boundaries.
        for i in 0..5 {
            egress.push(encode_egress_record(i, &relayed_txref(1, i as i32)));
        }
        egress.push(encode_egress_boundary(1, 2, 1_000));
        egress.push(encode_egress_boundary(2, 4, 2_000)); // duplicate boundary
        egress.push(wire::encode_replay_done(6, 3));
        egress.close();

        let mut sub = ClusterTxOrderingSubscription::new(egress);
        let mut got = Vec::new();
        while let Ok((pos, msg)) = sub.next() {
            let tag = match msg {
                TxOrderingMessage::TxRef(_) => format!("r{}", pos.as_index()),
                TxOrderingMessage::BoundaryStart(b) => format!("b{}", b.block_number),
                other => format!("?{other:?}"),
            };
            got.push(tag);
        }
        assert_eq!(got, vec!["r0", "r1", "b1", "r2", "r3", "b2", "r4", "r5"]);
    }

    #[test]
    fn boundary_first_stream_delivers_in_emission_order() {
        // An idle chain emits boundaries with no records: b1(end0) b2(end0).
        let egress = FakeEgress::new();
        egress.push(encode_egress_boundary(1, 0, 1_000));
        egress.push(encode_egress_boundary(2, 0, 2_000));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);
        let (p1, m1) = sub.next().unwrap();
        assert!(matches!(m1, TxOrderingMessage::BoundaryStart(b) if b.block_number == 1));
        assert_eq!(p1.as_index(), 0);
        let (_p2, m2) = sub.next().unwrap();
        assert!(matches!(m2, TxOrderingMessage::BoundaryStart(b) if b.block_number == 2));
    }

    #[test]
    fn duplicates_below_cursor_are_skipped() {
        let egress = FakeEgress::new();
        egress.push(encode_egress_record(0, &relayed_txref(1, 10)));
        egress.push(encode_egress_record(0, &relayed_txref(1, 10))); // dup
        egress.push(encode_egress_record(1, &relayed_txref(1, 11)));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);
        assert_eq!(sub.next().unwrap().0, BPosition::from_index(0));
        assert_eq!(sub.next().unwrap().0, BPosition::from_index(1));
        assert!(matches!(sub.next(), Err(ExecutorError::TxOrderingClosed)));
    }

    #[test]
    fn replay_unavailable_is_fatal() {
        let egress = FakeEgress::new();
        egress.push(wire::encode_replay_unavailable(100, 7));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::new(egress);
        assert!(matches!(
            sub.next(),
            Err(ExecutorError::ClusterReplayUnavailable {
                oldest_index: 100,
                oldest_block: 7,
                ..
            })
        ));
    }

    #[test]
    fn resume_cursor_skips_already_applied_range() {
        // Consumer resumes at (records=3, next block=2): replayed frames below
        // the cursor are dropped, delivery starts exactly at the cursor.
        let egress = FakeEgress::new();
        for i in 0..5 {
            egress.push(encode_egress_record(i, &relayed_txref(1, i as i32)));
        }
        egress.push(encode_egress_boundary(1, 2, 1_000)); // below cursor: dup
        egress.push(encode_egress_boundary(2, 5, 2_000));
        egress.push(wire::encode_replay_done(5, 3));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(3, 2));
        let mut got = Vec::new();
        while let Ok((pos, msg)) = sub.next() {
            got.push(match msg {
                TxOrderingMessage::TxRef(_) => format!("r{}", pos.as_index()),
                TxOrderingMessage::BoundaryStart(b) => format!("b{}", b.block_number),
                other => format!("?{other:?}"),
            });
        }
        assert_eq!(got, vec!["r3", "r4", "b2"]);
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
        // Consumer resumed at (42 records applied, next block 7) — the
        // boundary is the next in-order item and must decode field-intact.
        let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(42, 7));
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

    // F07.2 regression: a boundary-only gap across a session reconnect.
    // Cursor at (records=2, next block=1): records 0..1 delivered, block 1 not
    // yet sealed. Boundary b1 was emitted during a brief session outage; the
    // reconnect's first live frame is record 2 — exactly the next index, so no
    // key gap is observed and live mode delivers it. The replayed boundary
    // b1(end=2) then arrives; it canonically precedes record 2, so delivering
    // it now would seal block 1 with block 2's first record inside. That
    // inversion must FAIL-STOP (restart + cursor replay recovers gaplessly),
    // never deliver.
    #[test]
    fn late_boundary_sealing_below_cursor_is_fatal() {
        let egress = FakeEgress::new();
        egress.push(encode_egress_record(2, &relayed_txref(1, 2)));
        egress.push(encode_egress_boundary(1, 2, 1_000));
        egress.close();
        let mut sub = ClusterTxOrderingSubscription::with_cursor(egress, ReplayCursor::new(2, 1));
        // Live mode: record 2 is next-index and delivers immediately.
        let (p, m) = sub.next().unwrap();
        assert_eq!(p.as_index(), 2);
        assert!(matches!(m, TxOrderingMessage::TxRef(_)));
        // The late replayed boundary proves the inversion → fatal.
        assert!(matches!(
            sub.next(),
            Err(ExecutorError::BoundaryMisaligned { .. })
        ));
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
