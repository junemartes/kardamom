//! Per-partition nonce-check state machine.
//!
//! Single-owner: this struct is held by exactly one OS thread (the sequencer
//! event loop). No locks, no atomics. The pure-functional design isolates the
//! algorithm from the Aeron I/O surface; every nontrivial test in this crate
//! exercises it directly.

use std::collections::HashMap;
use std::time::{Duration, Instant};

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
    BufferedEvicting { evicted_nonce: u64 },
    BufferedReplaced,
    BufferedDisabled,
    Past,
}

#[derive(Debug)]
pub struct ProcessResult<T> {
    pub actions: Vec<ProcessAction<T>>,
    pub outcome: NonceOutcome,
}

/// A sender whose pending buffer holds only nonces strictly above its floor:
/// the gap `expected..lowest` is not in flight locally. Recorded the first
/// time [`PartitionState::fast_forward_stalled`] observes the configuration;
/// if the same `(expected, lowest)` pair is still stalled after the lag
/// bound, the floor fast-forwards to `lowest`. Any progress (a match
/// advancing `expected`, or a lower nonce arriving and shrinking the gap)
/// invalidates the mark and restarts the clock.
#[derive(Debug)]
struct StallMark {
    expected: u64,
    lowest: u64,
    since: Instant,
}

#[derive(Debug)]
pub struct PartitionState<T> {
    max_pending_per_sender: usize,
    next: HashMap<Address, u64>,
    pending: HashMap<Address, PendingBuffer<T>>,
    stalls: HashMap<Address, StallMark>,
}

impl<T> PartitionState<T> {
    pub fn new(max_pending_per_sender: usize) -> Self {
        Self {
            max_pending_per_sender,
            next: HashMap::new(),
            pending: HashMap::new(),
            stalls: HashMap::new(),
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
                InsertOutcome::EvictedOldest { evicted_nonce } => {
                    NonceOutcome::BufferedEvicting { evicted_nonce }
                }
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

    /// Stream-adaptive nonce-floor fast-forward.
    ///
    /// A replica that live-joins its shard's tx_data mid-stream (restart —
    /// there is no archive replay) hydrates established senders at a floor
    /// that lags the live stream: the state DB is empty or the committed
    /// nonce trails what the twin already ordered. Every subsequent tx from
    /// such a sender buffers as "future" against a gap that can never fill,
    /// so the replica silently stops covering the sender.
    ///
    /// This method detects that condition: a sender whose pending buffer
    /// holds nonces strictly above the floor, unchanged (same `expected`,
    /// same lowest buffered nonce) for at least `max_lag`, has its floor
    /// fast-forwarded to the lowest buffered nonce; the now-contiguous run is
    /// drained and returned for publishing, in the same shape as
    /// [`Self::drain_pending`]. A `max_lag` of zero fires immediately.
    ///
    /// Safety: the floor only ever skips forward, so per-publisher nonce
    /// order is preserved, and any ref the twin already offered is absorbed
    /// by the cluster's first-seen dedup. If the missing nonces were never
    /// ordered by anyone (both replicas of the shard down simultaneously —
    /// outside the P=2 design's failure envelope), the skipped prefix is
    /// lost either way; fast-forwarding surfaces the gap at the executor
    /// instead of freezing the sender here forever.
    pub fn fast_forward_stalled(
        &mut self,
        now: Instant,
        max_lag: Duration,
    ) -> Vec<(Address, u64, T)> {
        let mut out = Vec::new();
        // `pending` / `next` / `stalls` are borrowed as disjoint fields (same
        // pattern as `drain_pending`).
        for (&sender, buf) in self.pending.iter_mut() {
            let Some(lowest) = buf.lowest_nonce() else {
                self.stalls.remove(&sender);
                continue;
            };
            let expected = self.next.get(&sender).copied().unwrap_or(0);
            if lowest <= expected {
                // Drainable by the normal paths — not a stall.
                self.stalls.remove(&sender);
                continue;
            }
            let due = max_lag.is_zero()
                || match self.stalls.get(&sender) {
                    Some(m) if m.expected == expected && m.lowest == lowest => {
                        now.duration_since(m.since) >= max_lag
                    }
                    _ => {
                        // New stall configuration (or the gap moved): (re)arm.
                        self.stalls.insert(
                            sender,
                            StallMark {
                                expected,
                                lowest,
                                since: now,
                            },
                        );
                        false
                    }
                };
            if !due {
                continue;
            }
            let mut advanced = lowest;
            for (n, p) in buf.drain_consecutive_from(lowest) {
                out.push((sender, n, p));
                advanced = n.saturating_add(1);
            }
            self.next.insert(sender, advanced);
            self.stalls.remove(&sender);
        }
        out
    }
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
    fn buffer_full_evicts_oldest() {
        let mut st: PartitionState<u32> = PartitionState::new(2);
        st.process(s(1), 5, 5);
        st.process(s(1), 6, 6);
        let out = st.process(s(1), 7, 7);
        assert_eq!(
            out.outcome,
            NonceOutcome::BufferedEvicting { evicted_nonce: 5 }
        );
    }

    #[test]
    fn fast_forward_waits_for_the_lag_bound_then_adopts_the_gap() {
        let mut st: PartitionState<u32> = PartitionState::new(8);
        let lag = Duration::from_millis(50);
        let t0 = Instant::now();
        // Rejoin scenario: floor 0, live stream starts at nonce 10.
        st.process(s(1), 10, 110);
        st.process(s(1), 11, 111);
        // First observation arms the stall mark — nothing published yet.
        assert!(st.fast_forward_stalled(t0, lag).is_empty());
        // Still inside the lag bound.
        assert!(
            st.fast_forward_stalled(t0 + Duration::from_millis(10), lag)
                .is_empty()
        );
        // Past the bound: floor jumps to 10, contiguous run drains.
        let out = st.fast_forward_stalled(t0 + lag, lag);
        assert_eq!(out, vec![(s(1), 10, 110), (s(1), 11, 111)]);
        assert_eq!(st.next_nonce(s(1)), 12);
        // The live stream continues seamlessly from the adopted floor.
        let next = st.process(s(1), 12, 112);
        assert_eq!(next.outcome, NonceOutcome::Matched);
    }

    #[test]
    fn fast_forward_rearms_when_the_gap_shrinks() {
        let mut st: PartitionState<u32> = PartitionState::new(8);
        let lag = Duration::from_millis(50);
        let t0 = Instant::now();
        st.process(s(1), 10, 110);
        assert!(st.fast_forward_stalled(t0, lag).is_empty());
        // A lower (but still future) nonce arrives: the gap is filling in —
        // the mark must re-arm on the new configuration, not fire early.
        st.process(s(1), 8, 108);
        assert!(st.fast_forward_stalled(t0 + lag, lag).is_empty());
        // Only a full lag after the new configuration does it adopt nonce 8
        // (and stop at the 9→10 gap... which no longer exists: 8 then 10).
        let out = st.fast_forward_stalled(t0 + lag + lag, lag);
        assert_eq!(out, vec![(s(1), 8, 108)]);
        assert_eq!(st.next_nonce(s(1)), 9);
    }

    #[test]
    fn fast_forward_ignores_senders_without_a_gap() {
        let mut st: PartitionState<u32> = PartitionState::new(8);
        let t0 = Instant::now();
        st.process(s(1), 0, 100); // matched; no pending
        assert!(
            st.fast_forward_stalled(t0, Duration::ZERO).is_empty(),
            "matched sender must not be touched"
        );
        assert_eq!(st.next_nonce(s(1)), 1);
    }

    #[test]
    fn fast_forward_zero_lag_fires_immediately() {
        let mut st: PartitionState<u32> = PartitionState::new(8);
        st.process(s(1), 5, 55);
        let out = st.fast_forward_stalled(Instant::now(), Duration::ZERO);
        assert_eq!(out, vec![(s(1), 5, 55)]);
        assert_eq!(st.next_nonce(s(1)), 6);
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
