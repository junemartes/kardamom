//! Per-partition nonce-check state machine.
//!
//! Single-owner: this struct is held by exactly one OS thread (the sequencer
//! event loop). No locks, no atomics. The pure-functional design isolates the
//! algorithm from the Aeron I/O surface; every nontrivial test in this crate
//! exercises it directly.

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
    /// Buffered this nonce; a further-future nonce (`evicted_nonce`) was dropped
    /// to make room, preserving the drainable low run. The dropped tx is
    /// far-future and re-submitted by the client before it is needed.
    BufferedEvicting {
        evicted_nonce: u64,
    },
    /// This nonce was itself the furthest-future and the buffer was full, so it
    /// was rejected (not buffered) to protect the drainable low run.
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

    /// Returns the cached next-nonce for `sender`, or `None` if the sender
    /// has never been seen by this partition. Used by the cache-miss
    /// hydration path: a `None` triggers a one-time canonical lookup
    /// against the state DB before falling through to [`Self::process`].
    pub fn next_nonce_known(&self, sender: Address) -> Option<u64> {
        self.next.get(&sender).copied()
    }

    pub fn seed_next_nonce(&mut self, sender: Address, n: u64) {
        self.next.insert(sender, n);
    }

    /// Primary-side: handle an incoming tx. Returns publish actions in
    /// canonical order; the caller drives the outbound publishers.
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

        // nonce == expected: prefer the buffered entry at this nonce if one
        // exists (e.g. backpressure-retry path), then drain the contiguous
        // run.
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

    /// Push a payload back into the pending buffer so the next call to
    /// `process(sender, nonce, _)` will pick it up and publish it. Also
    /// rewinds `next_nonce` so the retry sees `nonce == expected`.
    ///
    /// Used by [`crate::sequencer::Sequencer::run_once`] when the canonical
    /// `TxRef` publish (`TxOrderingRefPublisher::try_publish_ref`) returns
    /// `Backpressure` — we must NOT advance state for a message whose ref did
    /// not actually land on B. Also marks the sender as "drain-pending" so a
    /// subsequent call to [`Self::drain_pending`] can resume the publish
    /// without needing fresh ingress.
    pub fn reinsert_for_retry(&mut self, sender: Address, nonce: u64, payload: T) {
        // Rewind expected nonce so the retry treats it as a Match.
        self.next.insert(sender, nonce);
        let buf = self
            .pending
            .entry(sender)
            .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
        // UNBOUNDED insert: a capacity-enforcing insert here can EVICT the
        // lowest rebuffered nonce when the buffer is (still) full — e.g. a
        // full future-run drained by `process` plus the in-flight ingress item
        // rebuffered after backpressure overshoots capacity by one, and the
        // eviction would silently lose a ref the floor already rewound below
        // (a permanent per-sender gap). The rebuffered items were accounted
        // for by this buffer moments ago; capacity re-applies to fresh
        // ingress only.
        buf.reinsert(nonce, payload);
    }

    /// Walk every sender whose pending buffer has an entry at its expected
    /// next nonce, and emit `Publish` actions for the contiguous run. Used by
    /// the primary loop to flush backpressured-then-rebuffered payloads
    /// without needing fresh ingress messages.
    ///
    /// Returns the publish actions in canonical order (per sender, ascending
    /// nonce). Senders are visited in arbitrary order — but within a sender
    /// the nonces are strictly ascending and dense, which is the only
    /// ordering the canonical log cares about.
    pub fn drain_pending(&mut self) -> Vec<(Address, u64, T)> {
        let mut out = Vec::new();
        // Borrow `pending` and `next` as disjoint fields so we don't need to
        // snapshot the sender list into a `Vec` first.
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

    // NOTE — `fast_forward_stalled` (the F02.1/F02.2 "stream-adaptive
    // nonce-floor fast-forward") was REMOVED after CI run 29687514869: a
    // sequencer cannot locally distinguish "the twin already ordered the gap"
    // (the rejoin case it was built for) from "NOBODY ordered the gap" (a
    // client-abandoned nonce hole — txs dropped at ingress under overload or
    // during a chaos outage, so they never reached tx_data at all). In the
    // second case BOTH replicas adopt the same hole and publish a canonical
    // stream with a nonce gap, which every executor fail-stops on
    // (revm NonceTooHigh is fatal) — observed as all three executors
    // crash-looping in all five cluster-e2e shards (load: tx 3836 vs state
    // 3833 at the 600tps overload step; chaos: tx 4098 vs state 1818 after a
    // leader-kill window). A stalled sender must stall HERE, where it is
    // recoverable, never poison the canonical stream. This re-opens F02.1's
    // rejoined-replica-coverage finding; a sound fix needs a global signal
    // (e.g. hydrating floors from a canonical/receipt stream), not a local
    // timeout. See docs/reviews/2026-07-17-30-commit-review/
    // fixes-CI-replay-loop.md (round 4).
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    #[test]
    fn match_publishes_and_advances() {
        let mut st: PartitionState<u32> = PartitionState::new(4);
        let out = st.process(s(1), 0, 100);
        assert_eq!(
            out.actions,
            vec![ProcessAction::Publish {
                nonce: 0,
                payload: 100
            }]
        );
        assert_eq!(out.outcome, NonceOutcome::Matched);
        assert_eq!(st.next_nonce(s(1)), 1);
    }

    #[test]
    fn match_drains_subsequent_buffered() {
        let mut st: PartitionState<u32> = PartitionState::new(8);
        assert!(matches!(
            st.process(s(1), 1, 11).outcome,
            NonceOutcome::Buffered
        ));
        assert!(matches!(
            st.process(s(1), 2, 22).outcome,
            NonceOutcome::Buffered
        ));
        let out = st.process(s(1), 0, 0);
        assert_eq!(
            out.actions,
            vec![
                ProcessAction::Publish {
                    nonce: 0,
                    payload: 0
                },
                ProcessAction::Publish {
                    nonce: 1,
                    payload: 11
                },
                ProcessAction::Publish {
                    nonce: 2,
                    payload: 22
                },
            ]
        );
        assert_eq!(st.next_nonce(s(1)), 3);
    }

    #[test]
    fn past_reports_duplicate() {
        let mut st: PartitionState<u32> = PartitionState::new(4);
        st.process(s(1), 0, 0);
        st.process(s(1), 1, 1);
        let out = st.process(s(1), 0, 999);
        assert_eq!(
            out.actions,
            vec![ProcessAction::ReportDuplicate {
                nonce: 0,
                expected_nonce: 2
            }]
        );
        assert_eq!(out.outcome, NonceOutcome::Past);
        assert_eq!(st.next_nonce(s(1)), 2);
    }

    #[test]
    fn future_is_buffered() {
        let mut st: PartitionState<u32> = PartitionState::new(4);
        let out = st.process(s(1), 5, 55);
        assert_eq!(out.actions, vec![]);
        assert_eq!(out.outcome, NonceOutcome::Buffered);
        assert_eq!(st.next_nonce(s(1)), 0);
    }

    #[test]
    fn buffer_full_rejects_furthest_future() {
        // Capacity 2, buffer {5,6}; incoming 7 is the furthest-future → rejected,
        // NOT evicting the low run. (Old behaviour evicted 5 and wedged.)
        let mut st: PartitionState<u32> = PartitionState::new(2);
        st.process(s(1), 5, 5);
        st.process(s(1), 6, 6);
        let out = st.process(s(1), 7, 7);
        assert_eq!(out.outcome, NonceOutcome::RejectedTooFar { nonce: 7 });
    }

    #[test]
    fn overflow_then_expected_arrives_drains_full_run_no_wedge() {
        // The end-to-end regression guard: a sender floods far-future nonces past
        // capacity, then its expected nonce (0) finally arrives. With lowest-wins
        // the contiguous run 0..=3 survives and publishes in order — the sender
        // is never permanently wedged. (Old evict-oldest would have dropped 0's
        // successors and stalled the sender forever.)
        let mut st: PartitionState<u32> = PartitionState::new(4);
        // expected is 0; buffer the near run 1..=4 (fills capacity 4).
        for n in 1..=4u64 {
            st.process(s(1), n, n as u32);
        }
        // Flood far-future nonces — all rejected, near run untouched.
        for n in 50..70u64 {
            assert_eq!(
                st.process(s(1), n, n as u32).outcome,
                NonceOutcome::RejectedTooFar { nonce: n }
            );
        }
        // Expected 0 arrives → publishes 0,1,2,3,4 in order (the retained run).
        let out = st.process(s(1), 0, 0);
        let published: Vec<u64> = out
            .actions
            .iter()
            .filter_map(|a| match a {
                ProcessAction::Publish { nonce, .. } => Some(*nonce),
                _ => None,
            })
            .collect();
        assert_eq!(published, vec![0, 1, 2, 3, 4]);
        assert_eq!(st.next_nonce(s(1)), 5);
    }

    // CI first-record audit: rebuffering a backpressured batch must NEVER
    // lose a ref. A FULL future-run (capacity items) drained by `process`
    // plus the in-flight ingress item is capacity+1 items; re-inserting them
    // through the capacity-enforcing `insert` evicted the lowest nonce — a
    // silent, permanent per-sender gap. The reinsert path is now unbounded.
    #[test]
    fn full_buffer_backpressure_rebuffer_loses_nothing() {
        let cap = 4;
        let mut st: PartitionState<u32> = PartitionState::new(cap);
        // Fill the buffer to capacity with the future run 1..=cap.
        for n in 1..=cap as u64 {
            assert!(matches!(
                st.process(s(1), n, 100 + n as u32).outcome,
                NonceOutcome::Buffered
            ));
        }
        // Nonce 0 arrives: the full run drains for publishing (cap+1 items).
        let out = st.process(s(1), 0, 100);
        assert_eq!(out.actions.len(), cap + 1);
        // Backpressure on the FIRST publish: rebuffer the whole batch in
        // reverse, exactly as `flush_drained` does.
        let mut batch: Vec<(u64, u32)> = (0..=cap as u64).map(|n| (n, 100 + n as u32)).collect();
        while let Some((n, p)) = batch.pop() {
            st.reinsert_for_retry(s(1), n, p);
        }
        assert_eq!(st.next_nonce(s(1)), 0, "floor rewound to lowest");
        // The retry drain must return ALL cap+1 refs — nothing evicted.
        let drained = st.drain_pending();
        let nonces: Vec<u64> = drained.iter().map(|(_, n, _)| *n).collect();
        assert_eq!(nonces, (0..=cap as u64).collect::<Vec<_>>());
        assert_eq!(st.next_nonce(s(1)), cap as u64 + 1);
    }

    // CI first-record audit: a capacity-0 (buffering disabled) config must
    // still not lose a MATCHED ref that hit backpressure — the rebuffer path
    // bypasses the disabled-buffer drop too.
    #[test]
    fn disabled_buffer_still_rebuffers_backpressured_match() {
        let mut st: PartitionState<u32> = PartitionState::new(0);
        let out = st.process(s(1), 0, 100);
        assert_eq!(out.actions.len(), 1);
        st.reinsert_for_retry(s(1), 0, 100);
        let drained = st.drain_pending();
        assert_eq!(drained, vec![(s(1), 0, 100)]);
    }

    #[test]
    fn reinsert_for_retry_rewinds_next_nonce() {
        let mut st: PartitionState<u32> = PartitionState::new(4);
        st.process(s(1), 0, 100);
        assert_eq!(st.next_nonce(s(1)), 1);
        // Simulate backpressure: roll back, putting payload 100 back in the buffer.
        st.reinsert_for_retry(s(1), 0, 100);
        assert_eq!(st.next_nonce(s(1)), 0);
        // Retry: state machine re-publishes the buffered payload (100), not the
        // payload arg (999) — exactly-once at the canonical layer.
        let out = st.process(s(1), 0, 999);
        assert_eq!(
            out.actions,
            vec![ProcessAction::Publish {
                nonce: 0,
                payload: 100
            }]
        );
        assert_eq!(st.next_nonce(s(1)), 1);
    }
}
