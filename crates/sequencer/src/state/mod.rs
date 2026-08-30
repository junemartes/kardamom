//! Per-partition nonce-check state machine.
//!
//! This struct has a single owner: exactly one OS thread (the sequencer
//! event loop) holds it. There are no locks and no atomics. The
//! pure-functional design isolates the algorithm from the Aeron I/O
//! surface. Every nontrivial test in this crate exercises it directly.

use std::collections::HashMap;

use alloy_primitives::Address;

use crate::pending::{InsertOutcome, PendingBuffer};

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessAction<T> {
    Publish { nonce: u64, payload: T },
    ReportDuplicate { nonce: u64, expected_nonce: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum NonceOutcome {
    Matched,
    Buffered,
    /// This nonce was buffered. A further-future nonce (`evicted_nonce`)
    /// was dropped to make room, to keep the drainable low run. The
    /// dropped transaction is far in the future, and the client resubmits
    /// it before it is needed.
    BufferedEvicting {
        evicted_nonce: u64,
    },
    /// This nonce was itself the furthest in the future, and the buffer
    /// was full. So it was rejected, not buffered, to protect the
    /// drainable low run.
    RejectedTooFar {
        nonce: u64,
    },
    BufferedReplaced,
    BufferedDisabled,
    Past,
}

#[derive(Debug)]
pub struct ProcessResult<T> {
    pub actions: Vec<ProcessAction<T>>,
    pub outcome: NonceOutcome,
}

#[derive(Debug)]
pub struct PartitionState<T> {
    max_pending_per_sender: usize,
    next: HashMap<Address, u64>,
    pending: HashMap<Address, PendingBuffer<T>>,
}

impl<T> PartitionState<T> {
    pub fn new(max_pending_per_sender: usize) -> Self {
        Self {
            max_pending_per_sender,
            next: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    pub fn next_nonce(&self, sender: Address) -> u64 {
        self.next.get(&sender).copied().unwrap_or(0)
    }

    /// Returns the cached next nonce for `sender`, or `None` if this
    /// partition has never seen the sender. The cache-miss hydration path
    /// uses this: a `None` triggers a one-time canonical lookup against
    /// the state DB before it falls through to [`Self::process`].
    pub fn next_nonce_known(&self, sender: Address) -> Option<u64> {
        self.next.get(&sender).copied()
    }

    pub fn seed_next_nonce(&mut self, sender: Address, n: u64) {
        self.next.insert(sender, n);
    }

    /// Primary-side: handle an incoming transaction. Returns publish
    /// actions in canonical order. The caller drives the outbound
    /// publishers.
    pub fn process(&mut self, sender: Address, nonce: u64, payload: T) -> ProcessResult<T> {
        let expected = self.next_nonce(sender);
        if nonce < expected {
            return ProcessResult {
                actions: vec![ProcessAction::ReportDuplicate {
                    nonce,
                    expected_nonce: expected,
                }],
                outcome: NonceOutcome::Past,
            };
        }
        if nonce > expected {
            let buf = self
                .pending
                .entry(sender)
                .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
            let outcome = match buf.insert(nonce, payload) {
                InsertOutcome::Inserted => NonceOutcome::Buffered,
                InsertOutcome::Replaced => NonceOutcome::BufferedReplaced,
                InsertOutcome::EvictedFuture { evicted_nonce } => {
                    NonceOutcome::BufferedEvicting { evicted_nonce }
                }
                InsertOutcome::RejectedTooFar { nonce } => NonceOutcome::RejectedTooFar { nonce },
                InsertOutcome::DroppedBufferDisabled => NonceOutcome::BufferedDisabled,
            };
            return ProcessResult {
                actions: vec![],
                outcome,
            };
        }

        // nonce == expected: prefer the buffered entry at this nonce, if
        // one exists (for example, the backpressure-retry path). Then drain
        // the contiguous run.
        let first_payload = self
            .pending
            .get_mut(&sender)
            .and_then(|b| b.remove(nonce))
            .unwrap_or(payload);
        let mut actions = vec![ProcessAction::Publish {
            nonce,
            payload: first_payload,
        }];
        let mut advanced = nonce.saturating_add(1);
        if let Some(buf) = self.pending.get_mut(&sender) {
            for (n, p) in buf.drain_consecutive_from(advanced) {
                actions.push(ProcessAction::Publish {
                    nonce: n,
                    payload: p,
                });
                advanced = n.saturating_add(1);
            }
        }
        self.next.insert(sender, advanced);
        ProcessResult {
            actions,
            outcome: NonceOutcome::Matched,
        }
    }

    /// Push a payload back into the pending buffer, so the next call to
    /// `process(sender, nonce, _)` picks it up and publishes it. This also
    /// rewinds `next_nonce`, so the retry sees `nonce == expected`.
    ///
    /// [`crate::sequencer::Sequencer::run_once`] calls this when the
    /// canonical `TxRef` publish (`TxOrderingRefPublisher::try_publish_ref`)
    /// returns `Backpressure`. The state must not advance for a message
    /// whose ref did not actually land on B. This also marks the sender as
    /// "drain-pending", so a later call to [`Self::drain_pending`] can
    /// resume the publish without fresh ingress.
    pub fn reinsert_for_retry(&mut self, sender: Address, nonce: u64, payload: T) {
        // Rewind expected nonce so the retry treats it as a Match.
        self.next.insert(sender, nonce);
        let buf = self
            .pending
            .entry(sender)
            .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
        // This insert is unbounded. A capacity-enforcing insert here could
        // evict the lowest rebuffered nonce when the buffer is still full.
        // For example: a full future run, drained by `process`, plus the
        // in-flight ingress item, rebuffered after backpressure, overshoots
        // capacity by one. Evicting here would silently lose a ref that the
        // floor already rewound below it: a permanent per-sender gap. This
        // buffer accounted for the rebuffered items moments ago. Capacity
        // applies only to fresh ingress.
        buf.reinsert(nonce, payload);
    }

    /// Walk every sender whose pending buffer has an entry at its expected
    /// next nonce, and emit `Publish` actions for the contiguous run. The
    /// primary loop uses this to flush backpressured-then-rebuffered
    /// payloads without needing fresh ingress messages.
    ///
    /// Returns the publish actions in canonical order (per sender,
    /// ascending nonce). Senders are visited in arbitrary order. Within a
    /// sender, the nonces are strictly ascending and dense, which is the
    /// only order the canonical log cares about.
    pub fn drain_pending(&mut self) -> Vec<(Address, u64, T)> {
        let mut out = Vec::new();
        // Borrow `pending` and `next` as separate fields. This avoids
        // snapshotting the sender list into a `Vec` first.
        for (&sender, buf) in self.pending.iter_mut() {
            let expected = self.next.get(&sender).copied().unwrap_or(0);
            let mut advanced = expected;
            for (n, p) in buf.drain_consecutive_from(expected) {
                out.push((sender, n, p));
                advanced = n.saturating_add(1);
            }
            if advanced > expected {
                self.next.insert(sender, advanced);
            }
        }
        out
    }

    /// Advance `sender`'s expected nonce to an executed-truth floor. A
    /// receipt proves that every nonce below `floor` already executed on
    /// the canonical chain. Entries buffered below the floor are dropped:
    /// they are proven duplicates of executed transactions, so this can
    /// never create a canonical gap. Buffered entries at or after the
    /// floor become drainable by [`Self::drain_pending`] on the next loop
    /// iteration.
    ///
    /// Returns `Some((previous_next_nonce, dropped_count))` when the floor
    /// advanced, or `None` when it was already at or behind `next`.
    ///
    /// This is the sound replacement for the removed stream-adaptive
    /// fast-forward (see the note below). It advances only on execution
    /// evidence from the receipts stream, never on locally inferred stream
    /// gaps. A client-abandoned nonce hole produces no receipt, so it never
    /// advances the floor. See docs/agents/sequencer-lag-resync-spec.md.
    pub fn advance_floor(&mut self, sender: Address, floor: u64) -> Option<(u64, usize)> {
        let cur = self.next_nonce(sender);
        if floor <= cur {
            return None;
        }
        let dropped = self
            .pending
            .get_mut(&sender)
            .map(|b| b.drop_below(floor))
            .unwrap_or(0);
        self.next.insert(sender, floor);
        Some((cur, dropped))
    }

    // NOTE: `fast_forward_stalled` (the stream-adaptive nonce-floor
    // fast-forward) was removed. A sequencer cannot locally tell "the twin
    // already ordered the gap" (the rejoin case it was built for) apart
    // from "nobody ordered the gap" (a client-abandoned nonce hole: a
    // transaction dropped at ingress under overload, or during a chaos
    // outage, so it never reached tx_data at all).
    //
    // In the second case, both replicas adopt the same hole and publish a
    // canonical stream with a nonce gap. Every executor fail-stops on that
    // (revm's NonceTooHigh is fatal). A stalled sender must stall here,
    // where it is recoverable, and never poison the canonical stream.
    //
    // A sound fix needs a global signal, such as hydrating floors from a
    // canonical or receipt stream, not a local timeout. See
    // docs/reviews/2026-07-17-30-commit-review/fixes-CI-replay-loop.md
    // (round 4).
}

#[cfg(test)]
mod tests;
