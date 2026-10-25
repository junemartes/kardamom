//! [`Reorder`]: the delivery cursor and the bounded pending set of the
//! canonical stream. Every transport of the canonical stream (the cluster
//! egress, the member feed, the serving service) ingests through it, so one
//! set of order rules holds for all of them.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage};

use super::ReplayCursor;
use crate::ExecutorError;
use crate::reader::Ordered;

/// Bound on the out-of-order catch-up buffers. A replay bigger than this
/// cannot be reordered in memory, and fails loudly instead of growing
/// without limit.
const MAX_PENDING: usize = 1 << 20;

/// Which gauge a delivered boundary sets. Only one role per host emits
/// the `kardamom_sealer_*` series: the executor, the chosen point that
/// probes and dashboards scrape. The validator exports the head under its
/// own name, so its exporter publishes no second, lagging copy of the
/// series (see `crate::metrics`). A reorder that only checks frames, and
/// delivers nothing downstream, sets no gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundaryMetrics {
    Sealer,
    Validator,
    Off,
}

/// The delivery cursor and the pending set of one canonical stream.
pub(crate) struct Reorder {
    pub(crate) cursor: ReplayCursor,
    /// Out-of-order buffers, keyed by canonical index or block number.
    /// Frames arrive out of order only around a session re-establishment,
    /// when the replay interleaves with live frames.
    pending_records: BTreeMap<u64, Ordered>,
    pending_boundaries: BTreeMap<u64, BlockBoundaryStart>,
    /// True while a replay is outstanding: after a session-start marker, a
    /// new request, or a key gap. In this mode, a record may be delivered
    /// only once the boundary at `next_block` proves it comes before it
    /// (`end_tx_idx > index`). Only `REPLAY_DONE` ends the mode. In live
    /// mode, frames arrive in emission order, so an absent boundary is a
    /// future boundary. A reader of a lossy source stays in this mode
    /// after `REPLAY_DONE` too: such a source can lose a boundary.
    ///
    /// A new session must enter this mode even with no key gap. The session
    /// can receive live frames before its replay, and the first live record
    /// can have exactly the next index. A boundary that precedes that record
    /// then arrives only in the replay. Live delivery would put the record
    /// in the open block, and the executor would execute it, and stream its
    /// receipt, with the wrong block number.
    catching_up: bool,
    /// The gauge that each delivered boundary and each replay end set.
    pub(crate) metrics: BoundaryMetrics,
    /// The block that the head gauge shows. The gauge does not move down:
    /// a `REPLAY_DONE` can show a head above a boundary that the reorder
    /// delivers after it, and a lower head would hide that lag.
    shown_head: Option<u64>,
}

impl Reorder {
    pub(crate) fn new(cursor: ReplayCursor) -> Self {
        Self {
            cursor,
            pending_records: BTreeMap::new(),
            pending_boundaries: BTreeMap::new(),
            catching_up: false,
            metrics: BoundaryMetrics::Sealer,
            shown_head: None,
        }
    }

    /// The next canonical index and the next block number to deliver.
    pub(crate) fn next(&self) -> (u64, u64) {
        self.cursor.position()
    }

    /// Hold the records until the replay of a new session is done. The
    /// pending set stays: its frames come from the same canonical stream.
    pub(crate) fn catch_up(&mut self) {
        self.catching_up = true;
    }

    /// Start a new replay from the delivery cursor. The replay carries
    /// every frame above the cursor again, so the pending set empties, and
    /// a record waits for the boundary of its block until the replay ends.
    pub(crate) fn restart(&mut self) {
        self.pending_records.clear();
        self.pending_boundaries.clear();
        self.catching_up = true;
    }

    /// Export one delivered boundary. The clustered sealer has no
    /// Prometheus endpoint. The executor re-exports its boundary stream
    /// as the `kardamom_sealer_*` series (see `crate::metrics`). The
    /// validator exports the head under its own name, so its readiness
    /// rule can compare the head with its committed block.
    fn record_boundary(&mut self, block_number: u64) {
        if self.metrics == BoundaryMetrics::Sealer {
            metrics::counter!(crate::metrics::SEALER_BOUNDARIES_TOTAL).increment(1);
        }
        self.show_head(block_number);
    }

    /// Set the head gauge to `block`, the newest sealed block that this
    /// reader knows of, unless the gauge shows that block or a higher one.
    fn show_head(&mut self, block: u64) {
        if self.shown_head.is_some_and(|shown| shown >= block) {
            return;
        }
        self.shown_head = Some(block);
        #[allow(
            clippy::cast_precision_loss,
            reason = "block numbers stay far below 2^52"
        )]
        let head = block as f64;
        match self.metrics {
            BoundaryMetrics::Sealer => {
                metrics::gauge!(crate::metrics::SEALER_BLOCK_NUMBER).set(head);
            }
            BoundaryMetrics::Validator => {
                metrics::gauge!(crate::metrics::VALIDATOR_SEALER_BLOCK_NUMBER).set(head);
            }
            BoundaryMetrics::Off => {}
        }
    }

    /// Deliver the next in-order item from the buffers, if it is provably
    /// next.
    ///
    /// # Errors
    ///
    /// Returns `Err` if advancing the cursor past a delivered record would
    /// overflow `next_index`. A wrapped cursor would rewind and re-deliver
    /// already-applied records, so this is fail-stop rather than silently
    /// wrong.
    pub(crate) fn try_deliver(&mut self) -> Result<Option<(BPosition, Ordered)>, ExecutorError> {
        let (ni, nb) = self.next();
        if let std::collections::btree_map::Entry::Occupied(entry) =
            self.pending_boundaries.entry(nb)
            && entry.get().end_tx_idx.as_index() <= ni
        {
            let b = entry.remove();
            // `next_block` only ever advances by one sealed block at a
            // time, so a real overflow here needs 2^64 boundaries
            // delivered; saturate rather than wrap and re-deliver block 0.
            self.cursor
                .next_block
                .store(nb.saturating_add(1), Ordering::Relaxed);
            self.record_boundary(b.block_number);
            return Ok(Some((
                b.end_tx_idx,
                TxOrderingMessage::BoundaryStart(b).into(),
            )));
        }
        // A record can be delivered when nothing proves an earlier boundary
        // is missing: either the boundary at `next_block` is buffered and
        // closes after this record, or we are in live mode, with in-order
        // arrival.
        let record_is_next = match self.pending_boundaries.get(&nb) {
            Some(b) => b.end_tx_idx.as_index() > ni,
            None => !self.catching_up,
        };
        if record_is_next && let Some(item) = self.pending_records.remove(&ni) {
            let next = ni.checked_add(item.slots()).ok_or_else(|| {
                ExecutorError::State(format!(
                    "cluster tx_ordering cursor overflow: {ni} + slot_width would exceed u64"
                ))
            })?;
            self.cursor.next_index.store(next, Ordering::Relaxed);
            return Ok(Some((BPosition::from_index(ni), item)));
        }
        Ok(None)
    }

    /// End the catch-up at a `REPLAY_DONE` marker. The marker carries the
    /// sealer's head: the next canonical index and the next block number.
    /// The frames before the marker come from the head or below, so a head
    /// below the delivery cursor means the sealer lost records that this
    /// consumer applied. Each new record then falls below the cursor and
    /// drops as a duplicate, so this fails instead. The sealer answers such
    /// a cursor with `REPLAY_AHEAD`; this check covers a sealer that
    /// answers `REPLAY_DONE` instead.
    ///
    /// The head gauge then shows the block before the next block of the
    /// marker. So a reader that is at the head, and has no boundary to
    /// deliver, still exports the head, and a reader below the head shows
    /// its lag before the boundaries come.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::ClusterBehindCursor`] when the head lies
    /// below the delivery cursor.
    pub(crate) fn end_replay(
        &mut self,
        up_to_index: u64,
        up_to_block: u64,
    ) -> Result<(), ExecutorError> {
        let (ni, nb) = self.next();
        if up_to_index < ni || up_to_block < nb {
            return Err(self.behind_cursor(up_to_index, up_to_block));
        }
        self.catching_up = false;
        if let Some(head) = up_to_block.checked_sub(1) {
            self.show_head(head);
        }
        Ok(())
    }

    /// The error for a sealer head below the delivery cursor.
    pub(crate) fn behind_cursor(&self, head_index: u64, head_block: u64) -> ExecutorError {
        let (next_index, next_block) = self.next();
        ExecutorError::ClusterBehindCursor {
            next_index,
            next_block,
            head_index,
            head_block,
        }
    }

    /// The error for a replay that starts below the retained floor.
    pub(crate) fn unavailable(&self, oldest_index: u64, oldest_block: u64) -> ExecutorError {
        ExecutorError::ClusterReplayUnavailable {
            from_index: self.next().0,
            oldest_index,
            oldest_block,
        }
    }

    /// Buffer one canonical record. Drops a duplicate below the delivery
    /// cursor silently: it is a replay/live overlap, not an error. Enters
    /// catch-up mode on a key gap, which can only follow a session
    /// re-establishment (the session thread has already sent
    /// `REPLAY_FROM(cursor)`).
    ///
    /// # Errors
    ///
    /// Returns `Err` when the pending set grows past its bound.
    pub(crate) fn ingest_record(&mut self, index: u64, item: Ordered) -> Result<(), ExecutorError> {
        let ni = self.next().0;
        if index < ni {
            return Ok(()); // replay/live overlap duplicate
        }
        if index > ni && !self.catching_up {
            tracing::info!(
                expected = ni,
                got = index,
                "cluster egress gap — entering replay catch-up"
            );
            self.catching_up = true;
        }
        self.pending_records.insert(index, item);
        self.check_pending_overflow()
    }

    /// Buffer one boundary, after the canonical-order guard and gap
    /// detection below. Drops a duplicate below the delivery cursor
    /// silently.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::BoundaryMisaligned`] for a boundary that
    /// seals below the delivery cursor, and `Err` when the pending set
    /// grows past its bound.
    pub(crate) fn ingest_boundary(&mut self, b: BlockBoundaryStart) -> Result<(), ExecutorError> {
        let (ni, nb) = self.next();
        if b.block_number < nb {
            return Ok(());
        }
        // Canonical-order guard. A boundary we still owe downstream
        // (block_number >= next_block) that seals at a record count below
        // the delivery cursor proves we already delivered records that
        // canonically follow it. Delivering it now would seal its block
        // with a later block's records inside: a silent canonical-order
        // divergence between replicas. Fail-stop instead. A restart
        // resumes from the persisted cursor, and the REPLAY_FROM on
        // connect re-delivers the whole window in order.
        //
        // The catch-up at each session-start marker, and at each new
        // request of the member feed, prevents this order on a reconnect.
        // A stream with no session-start marker, or a sealer fault, can
        // still give it. Frames within one Aeron session are ordered, so
        // this condition has no false positives.
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
        self.check_pending_overflow()
    }

    /// Fail loudly if either catch-up buffer has grown past `MAX_PENDING`,
    /// instead of growing without limit.
    fn check_pending_overflow(&self) -> Result<(), ExecutorError> {
        if self.pending_records.len() > MAX_PENDING || self.pending_boundaries.len() > MAX_PENDING {
            return Err(ExecutorError::State(
                "cluster catch-up buffer overflow — replay window too large to reorder".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "reorder_tests.rs"]
mod tests;
