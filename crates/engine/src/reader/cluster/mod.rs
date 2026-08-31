//! `ClusterTxOrderingSubscription`: the executor's `TxOrderingSubscription`,
//! backed by cluster egress.
//!
//! This plugs into the executor's tx_ordering reader thread in cluster mode.
//! The reader is unchanged: it calls `next()` and gets canonical-ordered
//! `(BPosition, TxOrderingMessage)` records. The cluster client handles
//! leader failover and reconnect, so the reader never sees an image
//! rotation. The cluster has already deduped and totally ordered the
//! stream. The executor's own `DedupWindow` still gives idempotency across
//! any reconnect overlap.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

use crate::ExecutorError;
use crate::reader::TxOrderingSubscription;

use kardamom_cluster_adapter::gateway::ClusterEgress;
use kardamom_cluster_adapter::wire::{self, EgressItem};

use kardamom_cluster_adapter::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, live};

/// Shared delivery cursor: the next canonical record index and boundary
/// block number this consumer expects. The subscription writes it on every
/// delivery. The live session thread reads it to build the `REPLAY_FROM`
/// request it sends on every connect or reconnect.
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

/// Bound on the out-of-order catch-up buffers. A replay bigger than this
/// cannot be reordered in memory, and fails loudly instead of growing
/// without limit.
const MAX_PENDING: usize = 1 << 20;

pub struct ClusterTxOrderingSubscription<E: ClusterEgress> {
    egress: E,
    cursor: ReplayCursor,
    /// Out-of-order buffers, keyed by canonical index or block number. Frames
    /// arrive out of order only around a session re-establishment, when the
    /// service's replay interleaves with live broadcasts.
    pending_records: BTreeMap<u64, TxOrderingMessage>,
    pending_boundaries: BTreeMap<u64, BlockBoundaryStart>,
    /// True while a replay is outstanding, after a key gap was seen. In this
    /// mode, a record may be delivered only once the boundary at `next_block`
    /// proves it comes before it (`end_tx_idx > index`). In live mode, frames
    /// arrive in emission order, so an absent boundary is a future boundary.
    catching_up: bool,
    /// Whether deliveries re-export the sealer's boundary stream as
    /// `kardamom_sealer_*` metrics. Only one role per host should emit them:
    /// the executor, the chosen point probes and dashboards scrape. The
    /// validator shares this subscription, but suppresses the emission. This
    /// stops its exporter from publishing a second, lagging copy of the
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

    /// Deliver the next in-order item from the buffers, if it is provably next.
    fn try_deliver(&mut self) -> Option<(BPosition, TxOrderingMessage)> {
        let ni = self.cursor.next_index.load(Ordering::Relaxed);
        let nb = self.cursor.next_block.load(Ordering::Relaxed);
        if let Some(b) = self.pending_boundaries.get(&nb)
            && b.end_tx_idx.as_index() <= ni
        {
            let b = self.pending_boundaries.remove(&nb).unwrap();
            self.cursor.next_block.store(nb + 1, Ordering::Relaxed);
            // The clustered sealer has no Prometheus endpoint. The executor
            // re-exports its boundary stream here (see `crate::metrics`). The
            // validator builds this subscription with the emission
            // suppressed.
            if self.emit_sealer_metrics {
                metrics::counter!(crate::metrics::SEALER_BOUNDARIES_TOTAL).increment(1);
                metrics::gauge!(crate::metrics::SEALER_BLOCK_NUMBER).set(b.block_number as f64);
            }
            return Some((b.end_tx_idx, TxOrderingMessage::BoundaryStart(b)));
        }
        // A record can be delivered when nothing proves an earlier boundary
        // is missing: either the boundary at `next_block` is buffered and
        // closes after this record, or we are in live mode, with in-order
        // arrival.
        let record_is_next = match self.pending_boundaries.get(&nb) {
            Some(b) => b.end_tx_idx.as_index() > ni,
            None => !self.catching_up,
        };
        if record_is_next && let Some(msg) = self.pending_records.remove(&ni) {
            // Almost every record is one slot wide. An epoch claims the marker
            // plus one slot per deposit. So the cursor must skip the whole
            // range, or the next record reads as a gap.
            self.cursor
                .next_index
                .store(ni + slot_width(&msg), Ordering::Relaxed);
            return Some((BPosition::from_index(ni), msg));
        }
        None
    }

    /// Classify one egress item into the buffers: dedup and gap detection.
    fn ingest(&mut self, item: EgressItem) -> Result<(), ExecutorError> {
        let ni = self.cursor.next_index.load(Ordering::Relaxed);
        let nb = self.cursor.next_block.load(Ordering::Relaxed);
        match item {
            EgressItem::Record { index, msg } => {
                if index < ni {
                    return Ok(()); // replay/live overlap duplicate
                }
                if index > ni && !self.catching_up {
                    // A key gap can only follow a session re-establishment.
                    // The session thread has already sent REPLAY_FROM(cursor).
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
                // Canonical-order guard, for a boundary-only gap across a
                // session reconnect. A boundary we still owe downstream
                // (block_number >= next_block) that seals at a record count
                // below the delivery cursor proves we already delivered
                // records that canonically follow it. The missed boundary
                // was emitted during a session outage. The reconnect's
                // first live frame was exactly the next-index record (no
                // key gap, so no catch-up), and the replayed boundary
                // arrived too late. Delivering it now would seal its block
                // with a later block's records inside: a silent
                // canonical-order divergence between replicas. Fail-stop
                // instead. A restart resumes from the persisted cursor, and
                // the REPLAY_FROM on connect re-delivers the whole window
                // in order.
                //
                // Entering catch-up on every session re-establishment would
                // prevent the inversion. But the session thread lives in
                // kardamom-cluster-adapter and exposes no reconnect signal.
                // Frames within one Aeron session are ordered, so this
                // condition has no false positives.
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
            // Contiguity rejects are offered only to the offering sequencer
            // session. An executor session cannot receive one. Ignore it
            // defensively.
            EgressItem::ContiguityReject { .. } => {}
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
                    // A malformed frame is dropped, and logged. The cluster
                    // stream is authoritative, so this should never happen in
                    // practice.
                    tracing::warn!(error = %e, "dropping malformed cluster egress frame");
                }
            }
        }
    }
}

// The `[cluster]` TOML section is shared by every role that connects to
// the cluster. It has one definition, in `kardamom-cluster-adapter`, next
// to the `LiveClusterConfig` it maps onto. This re-export keeps the old
// `kardamom_engine::reader::cluster::ClusterConfig` paths (executor and
// validator) working.
pub use kardamom_cluster_adapter::ClusterConfig;

/// Connect to the cluster and wrap egress as a `TxOrderingSubscription`.
/// Keep the returned `LiveCluster` guard alive while the subscription is
/// used.
///
/// `cursor` is the consumer's resume point: `(records applied, next block)`
/// from the persistent state on crash recovery, or [`ReplayCursor::genesis`]
/// on a fresh start. The session thread sends `REPLAY_FROM(cursor)` on
/// every session establishment, both first connect and every reconnect. So
/// the canonical stream has no gaps across session loss: the service
/// re-offers the retained frames the consumer missed, and the
/// subscription's catch-up ordering merges them with live broadcasts.
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

/// Canonical slots one record occupies. See [`wire::epoch_slots`] for why an
/// epoch is the only record wider than a single slot.
fn slot_width(msg: &TxOrderingMessage) -> u64 {
    match msg {
        TxOrderingMessage::Epoch(e) => wire::epoch_slots(e),
        TxOrderingMessage::RemoteEpoch(r) => wire::remote_epoch_slots(r),
        TxOrderingMessage::TxRef(_)
        | TxOrderingMessage::DepositRef(_)
        | TxOrderingMessage::BoundaryStart(_) => 1,
    }
}

#[cfg(test)]
mod tests;
