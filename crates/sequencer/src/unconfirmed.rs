//! #85 publish-confirmation ledger.
//!
//! An `Accepted` offer only proves the bytes landed on the Aeron publication
//! buffer, NOT that the Raft cluster committed them — a leader kill voids
//! the dead-leader window and the uncommitted tail, and continuing from the
//! optimistically advanced nonce state seals a canonical GAP. Every
//! published ref is retained here until a receipt for its sender at/above
//! its nonce proves canonical commitment (skip receipts count — ordering is
//! the claim); entries older than `resync.confirm_timeout_ms` are rewound
//! via `reinsert_for_retry` and re-published — the cluster dedup absorbs
//! copies that DID commit, and voided ones get ordered.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use alloy_primitives::Address;

/// Key into the #85 publish-confirmation ledger.
pub(crate) type UnconfirmedKey = (Address, u64);

/// Published-but-UNCONFIRMED refs (#85): see the module docs for why an
/// accepted offer is not a commitment. Owned by the sequencer loop; all
/// methods are single-threaded map/queue bookkeeping.
pub(crate) struct UnconfirmedLedger<T> {
    /// (sender, nonce) → (ref metadata, published-at). BTreeMap so
    /// per-sender ranges trim cheaply on confirmation and rewinds see
    /// ascending nonce order.
    entries: BTreeMap<UnconfirmedKey, (T, Instant)>,
    /// Publish-order expiry queue over `entries`, with LAZY deletion:
    /// confirmations remove from the map only; a popped queue entry counts
    /// as stale unless the map still holds the key WITH THE SAME
    /// published-at instant (a key can be re-queued by a reject-path rewind
    /// while its old queue entry is still buffered — timestamp equality
    /// disambiguates). Front-peek makes the confirm-timeout sweep O(1) when
    /// nothing has expired — the steady state — and amortized O(1) per
    /// entry overall; the previous full-map scan was O(rate ×
    /// receipt-latency) per iteration on the publish hot path, and worst
    /// exactly when the system was already in failover recovery.
    expiry: VecDeque<(Instant, UnconfirmedKey)>,
}

impl<T> UnconfirmedLedger<T> {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            expiry: VecDeque::new(),
        }
    }

    /// Number of published-but-unconfirmed refs currently retained.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Retain a just-published ref until a receipt proves canonical
    /// commitment, and queue it for the confirm-timeout sweep.
    pub(crate) fn record_published(&mut self, sender: Address, nonce: u64, meta: T) {
        let at = Instant::now();
        self.entries.insert((sender, nonce), (meta, at));
        self.expiry.push_back((at, (sender, nonce)));
    }

    /// A receipt at `confirmed` proves every one of OUR published refs for
    /// that sender at nonce <= `confirmed` survived into the committed
    /// canonical stream (per-sender order is preserved end to end) — drop
    /// them from the ledger. Their expiry-queue slots are deleted lazily by
    /// `sweep_expired`.
    pub(crate) fn confirm_through(&mut self, sender: Address, confirmed: u64) {
        let keys: Vec<_> = self
            .entries
            .range((sender, 0)..=(sender, confirmed))
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            self.entries.remove(&k);
        }
    }

    /// #85 fix B, committed-proof case — a sealer contiguity reject with
    /// nonce < expected: the ref sealed long ago (the guard's expected
    /// advanced past it) and its dedup entry aged out of the window. Drop
    /// the ledger entry exactly like a receipt confirmation. Without this,
    /// an entry with no confirming receipt (a sender whose ONLY tx is
    /// nonce 0: nonce-0 receipts are deposit-indistinguishable and never
    /// confirm) republishes every confirm-timeout FOREVER once the dedup
    /// horizon rolls past it (observed live: the smoke-gate accounts,
    /// 128+ rejects/run). Returns whether the entry was present.
    pub(crate) fn drop_committed(&mut self, sender: Address, nonce: u64) -> bool {
        self.entries.remove(&(sender, nonce)).is_some()
    }

    /// Sealer contiguity gap: refs for `sender` at `expected..nonce-1`
    /// vanished (voided offers). They are all in the ledger; take every
    /// retained ref at nonce >= `expected` for immediate republish instead
    /// of waiting out the confirm timeout. Returned in rewind-safe order
    /// (see `take_descending`).
    pub(crate) fn take_gap_rewinds(
        &mut self,
        sender: Address,
        expected: u64,
    ) -> Vec<(UnconfirmedKey, T)> {
        let keys: Vec<_> = self
            .entries
            .range((sender, expected)..=(sender, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        self.take_descending(keys)
    }

    /// Take refs whose confirmation has not arrived within `timeout` — the
    /// offer may have landed in a dead leader's void. The caller rewinds
    /// them via `reinsert_for_retry` so the next `drain_pending`
    /// re-publishes them; the cluster's first-seen dedup absorbs every copy
    /// that DID commit, and voided ones get ordered — no gap, no loss (this
    /// also un-wedges a sender whose refs vanished entirely: the ledger
    /// keeps re-offering until a receipt confirms). At most `max` per call,
    /// bounding the sweep per loop iteration.
    ///
    /// Expiry-queue mechanics: O(1) front-peek when nothing has expired
    /// (the steady state), amortized O(1) per entry overall. Entries whose
    /// map slot was confirmed away — or re-queued by a reject-path rewind
    /// with a NEWER published-at — are stale and skipped (lazy deletion).
    /// Returned in rewind-safe order (see `take_descending`).
    pub(crate) fn sweep_expired(
        &mut self,
        timeout: Duration,
        now: Instant,
        max: usize,
    ) -> Vec<(UnconfirmedKey, T)> {
        let mut stale: Vec<UnconfirmedKey> = Vec::new();
        while stale.len() < max {
            let Some((queued_at, key)) = self.expiry.front().copied() else {
                break;
            };
            if now.duration_since(queued_at) < timeout {
                break;
            }
            self.expiry.pop_front();
            if self
                .entries
                .get(&key)
                .is_some_and(|(_, at)| *at == queued_at)
            {
                stale.push(key);
            }
        }
        self.take_descending(stale)
    }

    /// Remove `keys` (ascending nonce per sender) from the map, returning
    /// `(key, meta)` in DESCENDING nonce order: `reinsert_for_retry` sets
    /// the sender's rewind floor on every call, so the LAST call per sender
    /// must carry the LOWEST nonce (same discipline as `flush_drained`'s
    /// backpressure rebuffer) — ascending order would strand the lower
    /// nonces beneath the floor forever.
    fn take_descending(&mut self, keys: Vec<UnconfirmedKey>) -> Vec<(UnconfirmedKey, T)> {
        keys.into_iter()
            .rev()
            .filter_map(|k| self.entries.remove(&k).map(|(meta, _)| (k, meta)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> Address {
        Address::repeat_byte(b)
    }

    #[test]
    fn record_confirm_then_sweep_rewinds_only_unconfirmed() {
        let mut l = UnconfirmedLedger::new();
        let a = addr(1);
        for n in 0..4u64 {
            l.record_published(a, n, n * 10);
        }
        assert_eq!(l.len(), 4);
        // A receipt at nonce 2 confirms nonces 0..=2 (skip receipts count —
        // ordering is the claim); nonce 3 stays unconfirmed.
        l.confirm_through(a, 2);
        assert_eq!(l.len(), 1);
        // Everything past the timeout: only the unconfirmed entry rewinds.
        let expired = l.sweep_expired(Duration::ZERO, Instant::now(), 256);
        assert_eq!(expired, vec![((a, 3), 30)]);
        assert_eq!(l.len(), 0);
        // Confirmed entries' queue slots were lazily deleted on the way — a
        // second sweep finds nothing.
        assert!(
            l.sweep_expired(Duration::ZERO, Instant::now(), 256)
                .is_empty()
        );
    }

    #[test]
    fn confirm_and_drop_are_per_sender_and_per_key() {
        let mut l = UnconfirmedLedger::new();
        let (a, b) = (addr(1), addr(2));
        l.record_published(a, 0, 1);
        l.record_published(b, 0, 2);
        // Confirming sender `a` must not touch sender `b`.
        l.confirm_through(a, 5);
        assert_eq!(l.len(), 1);
        // drop_committed reports presence exactly once.
        assert!(l.drop_committed(b, 0));
        assert!(!l.drop_committed(b, 0));
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn gap_rewind_takes_descending_nonces_from_expected() {
        let mut l = UnconfirmedLedger::new();
        let a = addr(1);
        let other = addr(2);
        for n in 5..9u64 {
            l.record_published(a, n, n);
        }
        l.record_published(other, 7, 99);
        // Sealer expected nonce 6: everything at/above the gap start is
        // taken; nonce 5 (below `expected`) and other senders stay.
        let taken = l.take_gap_rewinds(a, 6);
        // DESCENDING nonce order — the LAST entry carries the LOWEST nonce
        // so the caller's reinsert loop leaves the rewind floor there.
        assert_eq!(taken, vec![((a, 8), 8), ((a, 7), 7), ((a, 6), 6)]);
        assert_eq!(l.len(), 2);
        assert!(l.take_gap_rewinds(a, 6).is_empty());
    }

    #[test]
    fn sweep_respects_timeout_and_per_call_bound() {
        let mut l = UnconfirmedLedger::new();
        let a = addr(1);
        let t0 = Instant::now();
        for n in 0..5u64 {
            l.record_published(a, n, n);
        }
        // Nothing is stale under a large timeout.
        assert!(
            l.sweep_expired(Duration::from_secs(3600), t0, 256)
                .is_empty()
        );
        assert_eq!(l.len(), 5);
        // All entries past the timeout, but the per-call bound caps the
        // sweep at `max` (oldest first off the queue); the remainder waits
        // for the next iteration.
        let first = l.sweep_expired(Duration::ZERO, Instant::now(), 2);
        assert_eq!(first, vec![((a, 1), 1), ((a, 0), 0)]);
        let rest = l.sweep_expired(Duration::ZERO, Instant::now(), 256);
        assert_eq!(rest, vec![((a, 4), 4), ((a, 3), 3), ((a, 2), 2)]);
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn requeued_key_yields_a_single_rewind() {
        let mut l = UnconfirmedLedger::new();
        let a = addr(1);
        // A key can sit in the expiry queue twice (rewind + republish
        // re-records it); lazy deletion must yield the live entry exactly
        // once — the queue slot whose timestamp mismatches the map is stale.
        l.record_published(a, 0, 1);
        l.record_published(a, 0, 2);
        assert_eq!(l.len(), 1);
        let swept = l.sweep_expired(Duration::ZERO, Instant::now(), 256);
        assert_eq!(swept, vec![((a, 0), 2)]);
        assert_eq!(l.len(), 0);
    }
}
