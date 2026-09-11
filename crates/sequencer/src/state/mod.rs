//! Per-partition nonce-check state machine.
//!
//! This struct has a single owner: exactly one OS thread (the sequencer
//! event loop) holds it. There are no locks and no atomics. The
//! pure-functional design isolates the algorithm from the Aeron I/O
//! surface. Every nontrivial test in this crate exercises it directly.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use alloy_primitives::Address;

use crate::pending::{InsertOutcome, PendingBuffer};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProcessAction<T> {
    Publish { nonce: u64, payload: T },
    ReportDuplicate { nonce: u64, expected_nonce: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NonceOutcome {
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
pub(crate) struct ProcessResult<T> {
    pub actions: Vec<ProcessAction<T>>,
    pub outcome: NonceOutcome,
}

/// One parked entry's expiry, as the deadline heap orders it: earliest
/// `at` first. The heap entry can be stale; [`PendingBuffer::expire`]
/// checks the deadline against the slot before anything expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ParkedDeadline {
    at: Instant,
    sender: Address,
    nonce: u64,
}

#[derive(Debug)]
pub(crate) struct PartitionState<T> {
    max_pending_per_sender: usize,
    /// The lifetime of an entry that waits on a nonce gap.
    tx_ttl: Duration,
    next: HashMap<Address, u64>,
    pending: HashMap<Address, PendingBuffer<T>>,
    /// The expiry heap, earliest deadline first. An entry here can be
    /// stale. The sweep checks each popped deadline against the buffer
    /// slot before it expires anything. So a replace, a rebuffer, a
    /// drain, or a floor drop needs no heap surgery.
    deadlines: BinaryHeap<Reverse<ParkedDeadline>>,
}

impl<T> PartitionState<T> {
    #[must_use]
    pub(crate) fn new(max_pending_per_sender: usize, tx_ttl: Duration) -> Self {
        Self {
            max_pending_per_sender,
            tx_ttl,
            next: HashMap::new(),
            pending: HashMap::new(),
            deadlines: BinaryHeap::new(),
        }
    }

    #[must_use]
    pub(crate) fn next_nonce(&self, sender: Address) -> u64 {
        self.next.get(&sender).copied().unwrap_or(0)
    }

    /// Returns the cached next nonce for `sender`, or `None` if this
    /// partition has never seen the sender. The sequencer holds no
    /// state-DB reader: a `None` means the caller must seed a floor
    /// (`Self::seed_next_nonce`, at 0 for a cold sender) before it falls
    /// through to [`Self::process`]. Committed truth arrives later through
    /// [`Self::advance_floor`]: from a receipt, or from the executor nonce
    /// lookup that a park triggers (`crate::lookup`).
    #[must_use]
    pub(crate) fn next_nonce_known(&self, sender: Address) -> Option<u64> {
        self.next.get(&sender).copied()
    }

    pub(crate) fn seed_next_nonce(&mut self, sender: Address, n: u64) {
        self.next.insert(sender, n);
    }

    /// Primary-side: handle an incoming transaction. Returns publish
    /// actions in canonical order. The caller drives the outbound
    /// publishers. A future nonce parks with a deadline of now plus
    /// `tx_ttl`.
    pub(crate) fn process(&mut self, sender: Address, nonce: u64, payload: T) -> ProcessResult<T> {
        self.process_at(Instant::now(), sender, nonce, payload)
    }

    /// [`Self::process`] with an explicit clock. Tests drive the expiry
    /// through this.
    pub(crate) fn process_at(
        &mut self,
        now: Instant,
        sender: Address,
        nonce: u64,
        payload: T,
    ) -> ProcessResult<T> {
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
            return ProcessResult {
                actions: vec![],
                outcome: self.park(now, sender, nonce, payload),
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
        let advanced = self.drain_consecutive_into(sender, nonce.saturating_add(1), &mut actions);
        self.next.insert(sender, advanced);
        ProcessResult {
            actions,
            outcome: NonceOutcome::Matched,
        }
    }

    /// Park a future nonce with a deadline of `now + tx_ttl`. A parked
    /// entry (inserted, replaced, or inserted with an eviction) also goes
    /// on the deadline heap; a rejected one does not.
    fn park(&mut self, now: Instant, sender: Address, nonce: u64, payload: T) -> NonceOutcome {
        let deadline = now + self.tx_ttl;
        let buf = self
            .pending
            .entry(sender)
            .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
        let (outcome, parked) = match buf.insert(nonce, payload, deadline) {
            InsertOutcome::Inserted => (NonceOutcome::Buffered, true),
            InsertOutcome::Replaced => (NonceOutcome::BufferedReplaced, true),
            InsertOutcome::EvictedFuture { evicted_nonce } => {
                (NonceOutcome::BufferedEvicting { evicted_nonce }, true)
            }
            InsertOutcome::RejectedTooFar { nonce } => {
                (NonceOutcome::RejectedTooFar { nonce }, false)
            }
            InsertOutcome::DroppedBufferDisabled => (NonceOutcome::BufferedDisabled, false),
        };
        if parked {
            self.deadlines.push(Reverse(ParkedDeadline {
                at: deadline,
                sender,
                nonce,
            }));
        }
        outcome
    }

    /// Drain `sender`'s pending buffer of the run starting at `from`,
    /// publishing each into `actions`, and return the nonce past the last
    /// one drained (`from` itself, if the sender has no buffer or nothing
    /// contiguous). [`Self::process`]'s tail after publishing the matched
    /// nonce.
    fn drain_consecutive_into(
        &mut self,
        sender: Address,
        from: u64,
        actions: &mut Vec<ProcessAction<T>>,
    ) -> u64 {
        let mut advanced = from;
        let Some(buf) = self.pending.get_mut(&sender) else {
            return advanced;
        };
        for (n, p) in buf.drain_consecutive_from(advanced) {
            actions.push(ProcessAction::Publish {
                nonce: n,
                payload: p,
            });
            advanced = n.saturating_add(1);
        }
        advanced
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
    pub(crate) fn reinsert_for_retry(&mut self, sender: Address, nonce: u64, payload: T) {
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

    /// Expire the parked entries whose deadline is at or before `now`.
    /// Returns `(sender, nonce)` for each expired entry, at most `max` per
    /// call. The cost is proportional to the popped heap entries, not to
    /// the senders.
    ///
    /// Only an entry above the sender's expected nonce can expire. Such an
    /// entry waits on a nonce gap. An entry at or below the expected nonce
    /// is drainable, or was rewound for a retry, and the state machine
    /// owns its fate. A popped deadline that no longer matches the slot is
    /// stale (see [`PendingBuffer::expire`]), and it expires nothing.
    pub(crate) fn sweep_expired(&mut self, now: Instant, max: usize) -> Vec<(Address, u64)> {
        std::iter::from_fn(|| self.pop_due(now).map(|d| self.expire_parked(d)))
            .flatten()
            .take(max)
            .collect()
    }

    /// Pop the earliest heap entry if its deadline is at or before `now`.
    fn pop_due(&mut self, now: Instant) -> Option<ParkedDeadline> {
        let Reverse(head) = self.deadlines.peek().copied()?;
        (head.at <= now).then(|| {
            self.deadlines.pop();
            head
        })
    }

    /// Expire one popped heap entry, if it still names a live parked
    /// entry above the sender's expected nonce.
    fn expire_parked(&mut self, d: ParkedDeadline) -> Option<(Address, u64)> {
        if d.nonce <= self.next_nonce(d.sender) {
            return None;
        }
        self.pending
            .get_mut(&d.sender)?
            .expire(d.nonce, d.at)
            .map(|_| (d.sender, d.nonce))
    }

    /// The number of parked entries in the pending buffers of every
    /// sender. Test-only: the production code reads the per-vslot depths.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.values().map(PendingBuffer::len).sum()
    }

    /// The parked entries per bucket, where `bucket` maps a sender to its
    /// bucket (the vslot). The cost is one call per sender with a buffer.
    /// A depth past `u32::MAX` saturates.
    pub(crate) fn pending_depth_by<F: Fn(Address) -> u8>(&self, bucket: F) -> [u32; 256] {
        self.pending
            .iter()
            .fold([0u32; 256], |mut depth, (sender, buf)| {
                let slot = &mut depth[usize::from(bucket(*sender))];
                *slot = slot.saturating_add(u32::try_from(buf.len()).unwrap_or(u32::MAX));
                depth
            })
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
    pub(crate) fn drain_pending(&mut self) -> Vec<(Address, u64, T)> {
        let mut out = Vec::new();
        // Borrow `pending` and `next` as separate fields. This avoids
        // snapshotting the sender list into a `Vec` first.
        for (&sender, buf) in &mut self.pending {
            Self::drain_one_sender(&mut self.next, buf, sender, &mut out);
        }
        out
    }

    /// One sender's drain, for [`Self::drain_pending`]'s loop: an
    /// associated function over the disjoint `next`/`pending` fields, so
    /// the loop's own `&mut self.pending` borrow and this call's
    /// `&mut self.next` borrow coexist.
    fn drain_one_sender(
        next: &mut HashMap<Address, u64>,
        buf: &mut PendingBuffer<T>,
        sender: Address,
        out: &mut Vec<(Address, u64, T)>,
    ) {
        let expected = next.get(&sender).copied().unwrap_or(0);
        if let Some(advanced) = Self::drain_sender_run(buf, sender, expected, out) {
            next.insert(sender, advanced);
        }
    }

    /// Drain `buf`'s contiguous run starting at `expected`, tagged with
    /// `sender`, into `out`. Returns the sender's new next-nonce if
    /// anything drained. The helper method for the inner loop of
    /// [`Self::drain_pending`].
    fn drain_sender_run(
        buf: &mut PendingBuffer<T>,
        sender: Address,
        expected: u64,
        out: &mut Vec<(Address, u64, T)>,
    ) -> Option<u64> {
        let mut advanced = None;
        for (n, p) in buf.drain_consecutive_from(expected) {
            out.push((sender, n, p));
            advanced = Some(n.saturating_add(1));
        }
        advanced
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
    /// This advances only on execution evidence from the receipts stream,
    /// never on locally inferred stream gaps. A client-abandoned nonce
    /// hole produces no receipt, so it never advances the floor.
    pub(crate) fn advance_floor(&mut self, sender: Address, floor: u64) -> Option<(u64, usize)> {
        let cur = self.next_nonce(sender);
        if floor <= cur {
            return None;
        }
        let dropped = self
            .pending
            .get_mut(&sender)
            .map_or(0, |b| b.drop_below(floor));
        self.next.insert(sender, floor);
        Some((cur, dropped))
    }

    // A sequencer cannot locally tell "the twin already ordered the gap"
    // apart from "nobody ordered the gap" (a client-abandoned nonce
    // hole). So a stalled sender must stall here, where it is
    // recoverable, and never poison the canonical stream with a locally
    // inferred gap.
}

#[cfg(test)]
mod proptest_tests;
#[cfg(test)]
mod tests;
