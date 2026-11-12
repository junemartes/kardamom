//! Per-sender queues with bounded refill for case-owned transfer traffic.

use crate::load::plan::PlannedTx;
use std::collections::VecDeque;

/// Per-sender FIFO queues, consumed in rotation. This keeps each
/// sender's nonce order: a sender's nonce k is popped before k+1.
pub(crate) struct Queues {
    per_sender: Vec<VecDeque<PlannedTx>>,
    rr: usize,
    refill: Option<crate::load::stream::SignedBatches>,
}

impl Queues {
    /// Build from a set of per-sender pre-generated queues.
    #[must_use]
    pub(crate) fn new(queues: Vec<Vec<PlannedTx>>) -> Self {
        Self {
            per_sender: queues.into_iter().map(VecDeque::from).collect(),
            rr: 0,
            refill: None,
        }
    }

    pub(in crate::load) fn with_refill(
        mut self,
        refill: crate::load::stream::SignedBatches,
    ) -> Self {
        self.refill = Some(refill);
        self
    }

    pub(in crate::load) fn stop_refill(&mut self) {
        self.refill = None;
    }

    pub(in crate::load) async fn next(
        &mut self,
        tracker: &super::Tracker,
    ) -> anyhow::Result<Option<PlannedTx>> {
        if let Some(tx) = self.pop_next() {
            return Ok(Some(tx));
        }
        let Some(refill) = &mut self.refill else {
            return Ok(None);
        };
        let waiting = std::time::Instant::now();
        let batch = refill.receive().await?;
        tracing::info!(
            wait_us = waiting.elapsed().as_micros(),
            transactions = batch.planned().count(),
            "chaos load signed batch ready"
        );
        tracker.expect_streamed(refill.signers(), batch.planned());
        self.per_sender = batch.per_sender;
        Ok(self.pop_next())
    }

    /// Pop the next transaction in rotation across senders. Returns
    /// `None` when all queues are drained.
    pub(crate) fn pop_next(&mut self) -> Option<PlannedTx> {
        let n = self.per_sender.len();
        (0..n).find_map(|_| {
            let i = self.rr % n;
            // `rr` is a rotating counter, immediately taken mod `n`: a
            // wrap back to 0 is the intended behavior, not an edge case,
            // so wrapping (not a panic on overflow, unreachable in
            // practice, but not free to rule out on a hot path) is the
            // correct arithmetic here.
            self.rr = self.rr.wrapping_add(1) % n;
            self.per_sender[i].pop_front()
        })
    }

    /// Every queued transaction, for the tracker's expectations.
    pub(crate) fn planned(&self) -> impl Iterator<Item = &PlannedTx> {
        self.per_sender.iter().flatten()
    }

    /// The total transactions still queued. Test-only: production code
    /// drains with `pop_next` until it returns `None`, and never needs
    /// the count.
    #[cfg(test)]
    pub(super) fn remaining(&self) -> usize {
        self.per_sender.iter().map(VecDeque::len).sum()
    }
}
