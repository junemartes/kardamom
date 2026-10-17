//! `ClusterTxOrderingSubscription`: the executor's `TxOrderingSubscription`,
//! backed by cluster egress.
//!
//! This plugs into the executor's `tx_ordering` reader thread in cluster mode.
//! The reader is unchanged: it calls `next()` and gets canonical-ordered
//! `(BPosition, TxOrderingMessage)` records. The cluster client handles
//! leader failover and reconnect, so the reader never sees an image
//! rotation. The cluster has already deduped and totally ordered the
//! stream. The delivery cursor drops any record below the next expected
//! canonical index, so a replay/live overlap on reconnect is idempotent.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use kardamom_types::{BPosition, VoidRecord};

use crate::ExecutorError;
use crate::reader::{Ordered, TxOrderingSubscription};

use kardamom_cluster_adapter::gateway::{ClusterEgress, ClusterIngress};
use kardamom_cluster_adapter::wire::{self, EgressItem};
// The outcome of an ingress offer, public for a binary that publishes the
// posted cursor without a direct `kardamom-cluster-adapter` dependency.
pub use kardamom_cluster_adapter::gateway::OfferOutcome;

use kardamom_cluster_adapter::{
    LiveCluster, LiveClusterConfig, LiveEgress, LiveError, LiveIngress, live,
};

mod reorder;

pub(crate) use reorder::{BoundaryMetrics, Reorder};

/// Shared delivery cursor: the next canonical record index and boundary
/// block number this consumer expects. The subscription writes it on every
/// delivery. The live session thread reads it to build the `REPLAY_FROM`
/// request it sends on every connect or reconnect.
#[derive(Clone)]
pub struct ReplayCursor {
    pub(crate) next_index: Arc<AtomicU64>,
    pub(crate) next_block: Arc<AtomicU64>,
}

impl ReplayCursor {
    #[must_use]
    pub fn new(next_index: u64, next_block: u64) -> Self {
        Self {
            next_index: Arc::new(AtomicU64::new(next_index)),
            next_block: Arc::new(AtomicU64::new(next_block)),
        }
    }

    /// The next canonical index and the next block number.
    #[must_use]
    pub fn position(&self) -> (u64, u64) {
        (
            self.next_index.load(std::sync::atomic::Ordering::Relaxed),
            self.next_block.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Fresh consumer at genesis: no records seen, first boundary is block 1
    /// (`CanonicalSealerState.GENESIS_BLOCK_NUMBER`).
    #[must_use]
    pub fn genesis() -> Self {
        Self::new(0, 1)
    }
}

/// The ingress of a subscription that sends nothing to the sealer. It
/// reports no connection, so a vote through it has no effect.
pub struct NoIngress;

impl ClusterIngress for NoIngress {
    fn offer(&mut self, _payload: &[u8]) -> OfferOutcome {
        OfferOutcome::NotConnected
    }
}

pub struct ClusterTxOrderingSubscription<E: ClusterEgress, I: ClusterIngress = NoIngress> {
    egress: E,
    /// The session's ingress side. It carries only void votes; the canonical
    /// records come from the sequencers' own sessions.
    ingress: I,
    /// The delivery cursor and the pending set. The reader enters
    /// catch-up at each session-start marker, and after a key gap.
    reorder: Reorder,
}

impl<E: ClusterEgress> ClusterTxOrderingSubscription<E> {
    pub fn new(egress: E) -> Self {
        Self::with_cursor(egress, ReplayCursor::genesis())
    }

    pub fn with_cursor(egress: E, cursor: ReplayCursor) -> Self {
        Self {
            egress,
            ingress: NoIngress,
            reorder: Reorder::new(cursor),
        }
    }

    /// Give the subscription the session's ingress side, so the reader's
    /// void votes reach the sealer.
    pub fn with_ingress<I: ClusterIngress>(
        self,
        ingress: I,
    ) -> ClusterTxOrderingSubscription<E, I> {
        ClusterTxOrderingSubscription {
            egress: self.egress,
            ingress,
            reorder: self.reorder,
        }
    }
}

impl<E: ClusterEgress, I: ClusterIngress + Clone> ClusterTxOrderingSubscription<E, I> {
    /// A publisher of the batcher's posted cursor over this subscription's
    /// session. The clone shares the session thread, so the batcher's
    /// feed task publishes while the reader thread polls.
    #[must_use]
    pub fn posted_cursor_publisher(&self) -> PostedCursorPublisher<I> {
        PostedCursorPublisher {
            ingress: self.ingress.clone(),
        }
    }
}

impl<E: ClusterEgress, I: ClusterIngress + Clone> ClusterTxOrderingSubscription<E, I> {
    /// A publisher of this executor's recorded cursor over this
    /// subscription's session. `executor_id` is the executor's void voter
    /// id. The clone shares the session thread, so the executor's stream
    /// task publishes while the reader thread polls.
    #[must_use]
    pub fn recorded_cursor_publisher(&self, executor_id: u8) -> RecordedCursorPublisher<I> {
        RecordedCursorPublisher::new(self.ingress.clone(), executor_id)
    }
}

/// An executor's recorded cursor on the cluster ingress, as a system
/// record: every canonical index at or below the cursor is joined and
/// recorded by this executor, or voided. The sealer keeps the best cursor
/// over the executors as the floor of its record-lag guard.
#[derive(Clone)]
pub struct RecordedCursorPublisher<I: ClusterIngress> {
    ingress: I,
    executor_id: u8,
}

impl<I: ClusterIngress> RecordedCursorPublisher<I> {
    /// A publisher of the recorded cursor of executor `executor_id` over
    /// the session of `ingress`.
    pub fn new(ingress: I, executor_id: u8) -> Self {
        Self {
            ingress,
            executor_id,
        }
    }

    /// Offer `recorded_through` to the sealer. A refused offer is the
    /// caller's to retry: the next cursor publishes again, and the sealer
    /// keeps the highest cursor it saw.
    pub fn publish(&mut self, recorded_through: u64) -> OfferOutcome {
        self.ingress.offer(&wire::encode_ingress_recorded_cursor(
            self.executor_id,
            recorded_through,
        ))
    }
}

/// The batcher's confirmed cursor on the cluster ingress: the last L2
/// block posted to L1, as a system record. The sealer adopts it as the
/// floor of its DA-lag guard and of its egress retention, and the ingress
/// serves `safe` from it.
#[derive(Clone)]
pub struct PostedCursorPublisher<I: ClusterIngress> {
    ingress: I,
}

impl<I: ClusterIngress> PostedCursorPublisher<I> {
    /// Offer `posted_head` to the sealer. A refused offer is the
    /// caller's to retry: the next confirmed post publishes again, and
    /// the sealer sends its status to a session that announces itself.
    pub fn publish(&mut self, posted_head: u64) -> OfferOutcome {
        self.ingress
            .offer(&wire::encode_ingress_posted_cursor(posted_head))
    }
}

impl<E: ClusterEgress, I: ClusterIngress> ClusterTxOrderingSubscription<E, I> {
    /// Disable the `kardamom_sealer_*` re-export (validator role).
    #[must_use]
    pub fn suppress_sealer_metrics(mut self) -> Self {
        self.reorder.metrics = reorder::BoundaryMetrics::Validator;
        self
    }

    /// Classify one egress item into the buffers: dedup and gap detection.
    fn ingest(&mut self, item: EgressItem) -> Result<(), ExecutorError> {
        match item {
            EgressItem::Record { index, msg } => self.reorder.ingest_record(index, msg.into()),
            EgressItem::Boundary(b) => self.reorder.ingest_boundary(b),
            EgressItem::ReplayDone {
                up_to_index,
                up_to_block,
                ..
            } => self.reorder.end_replay(up_to_index, up_to_block),
            // A new session starts. Every later frame belongs to it, and
            // its replay is outstanding. A `REPLAY_DONE` of an earlier
            // session comes before this marker, so it cannot end this
            // catch-up.
            EgressItem::SessionStart { .. } => {
                self.reorder.catch_up();
                Ok(())
            }
            EgressItem::ReplayAhead {
                head_index,
                head_block,
                ..
            } => Err(self.reorder.behind_cursor(head_index, head_block)),
            // Every reject is offered only to the offering sequencer
            // session. An executor session cannot receive one. Ignore them
            // defensively. The status is broadcast to every session; the
            // ingress reads it, a consumer of the ordering does not. A
            // relayed reference repeats an entry without its bytes, and a
            // feed gap comes only on the member feed: both carry nothing
            // for this reader. The sealer's rules matter to a sequencer
            // only.
            EgressItem::ContiguityReject { .. }
            | EgressItem::SealerInfo { .. }
            | EgressItem::RemoteOriginReject { .. }
            | EgressItem::PastDeadline { .. }
            | EgressItem::WindowFull { .. }
            | EgressItem::DaLagReject { .. }
            | EgressItem::RecordLagReject { .. }
            | EgressItem::OriginGap { .. }
            | EgressItem::Status(_)
            | EgressItem::RelayedRef(_)
            | EgressItem::FeedGap { .. } => Ok(()),
            // This reader orders references. It drops a canonical entry
            // and logs it, as it drops a frame of an unknown record type.
            EgressItem::Tx { index, .. } => {
                tracing::warn!(index, "dropping a canonical entry on the reference path");
                Ok(())
            }
            EgressItem::ReplayUnavailable {
                oldest_index,
                oldest_block,
                ..
            } => Err(self.reorder.unavailable(oldest_index, oldest_block)),
        }
    }
}

impl<E: ClusterEgress, I: ClusterIngress> ClusterTxOrderingSubscription<E, I> {
    /// Receive and decode one egress frame, ingesting it into the
    /// reorder buffer. A malformed frame is dropped, and logged, not
    /// fatal: the cluster stream is authoritative, so this should never
    /// happen in practice. The loop in [`TxOrderingSubscription::next`]
    /// stays free of a match.
    fn ingest_one_frame(&mut self) -> Result<(), ExecutorError> {
        let Some(bytes) = self.egress.recv() else {
            return Err(ExecutorError::TxOrderingClosed);
        };
        match EgressItem::decode(&bytes) {
            Ok(item) => self.ingest(item),
            Err(e) => {
                tracing::warn!(error = %e, "dropping malformed cluster egress frame");
                Ok(())
            }
        }
    }
}

impl<E: ClusterEgress, I: ClusterIngress> TxOrderingSubscription
    for ClusterTxOrderingSubscription<E, I>
{
    fn next(&mut self) -> Result<(BPosition, Ordered), ExecutorError> {
        loop {
            let Some(out) = self.reorder.try_deliver()? else {
                self.ingest_one_frame()?;
                continue;
            };
            return Ok(out);
        }
    }

    fn vote(&mut self, voter_id: u8, void: &VoidRecord) -> OfferOutcome {
        self.ingress
            .offer(&wire::encode_void_request(voter_id, void))
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
///
/// # Errors
///
/// Returns `Err` when the cluster session fails to connect.
pub fn cluster_tx_ordering_subscription(
    rt: kardamom_log::aeron_live::AeronRuntime,
    cfg: LiveClusterConfig,
    cursor: ReplayCursor,
) -> Result<
    (
        LiveCluster,
        ClusterTxOrderingSubscription<LiveEgress, LiveIngress>,
    ),
    LiveError,
> {
    let (cluster, ingress, egress) = live::connect_with(
        rt,
        cfg,
        live::ConnectOptions {
            replay: Some(live::ReplayOnConnect {
                next_index: cursor.next_index.clone(),
                next_block: cursor.next_block.clone(),
            }),
            subscribe: true,
            ..Default::default()
        },
    )?;
    Ok((
        cluster,
        ClusterTxOrderingSubscription::with_cursor(egress, cursor).with_ingress(ingress),
    ))
}

#[cfg(test)]
mod tests;
