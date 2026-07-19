//! Consumer-side dedup for the `tx_errors` stream under racing sequencer
//! replicas (F02.6).
//!
//! With P=2 replicas racing per shard, BOTH replicas nonce-order the same
//! tx_data stream, so a per-tx rejection (`DuplicatedTx` today) is emitted by
//! both and arrives up to P times at ingress. Worse, a rejection from one
//! replica can race a *success* from its twin: a replica whose nonce floor is
//! momentarily stale (e.g. a rejoiner fast-forwarding) may reject a tx its
//! twin accepted and ordered — the client must get the receipt, not the
//! losing replica's error.
//!
//! [`TxErrorDedup`] therefore keeps a short sliding window of terminal
//! observations per `(sender, nonce)`:
//! - [`record_success`](TxErrorDedup::record_success) — called by the receipt
//!   watcher for every (first-copy) receipt. A success observed within the
//!   window suppresses any rejection for the same key (success overrides
//!   rejection).
//! - [`observe_error`](TxErrorDedup::observe_error) — called by the tx_errors
//!   watcher. The FIRST error of a given reason class for a key within the
//!   window is processed; later copies (the twin's duplicate emission) are
//!   dropped.
//!
//! The window is a TTL, not a first-wins-forever set, because `(sender,
//! nonce)` keys legitimately recur: a client may resubmit the same duplicate
//! tx minutes later and must get a prompt rejection again, not a suppressed
//! one. Replica copies of one emission arrive within milliseconds of each
//! other (both replicas consume the same stream at roughly the same rate), and
//! a rejection-vs-success race resolves within the ordering→execution→receipt
//! latency, so a TTL of a few seconds comfortably covers both while keeping
//! recurrences independent. The map is additionally capacity-bounded (oldest
//! evicted) so it cannot grow without limit; an undersized capacity only means
//! a very late duplicate is reprocessed, which is harmless — the pending-map
//! release it drives is idempotent (the responder fires at most once).
//!
//! Dedup is keyed on `{sender, nonce, reason CLASS}` (the enum discriminant,
//! not the full value): racing replicas can disagree on payload details like
//! `DuplicatedTx::expected_nonce` while reporting the same rejection.

use std::collections::{HashMap, VecDeque};
use std::mem::Discriminant;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use kardamom_types::TxErrorReason;

/// Default sliding window. Must exceed the replica emission skew (ms) and the
/// rejection-vs-success race window (ordering → receipt latency, well under a
/// second), while staying far below a plausible client retry of the same
/// nonce.
pub const DEFAULT_WINDOW: Duration = Duration::from_secs(10);

/// Default entry capacity. Terminal marks are only needed for the in-flight
/// window; tens of thousands is orders of magnitude more than a few seconds
/// of traffic needs.
pub const DEFAULT_CAPACITY: usize = 1 << 16;

/// Last terminal observation for a `(sender, nonce)` key.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mark {
    /// A receipt was observed — the tx landed; rejections for this key within
    /// the window are the losing twin's and must be suppressed.
    Succeeded,
    /// An error of this reason class was already processed within the window.
    Rejected(Discriminant<TxErrorReason>),
}

struct Inner {
    map: HashMap<(Address, u64), (Mark, Instant)>,
    /// Insertion-time order for TTL purge + capacity eviction. Refreshed keys
    /// get a new entry; stale queue entries are skipped at purge time by
    /// comparing the stored timestamp.
    order: VecDeque<((Address, u64), Instant)>,
}

/// Thread-safe, bounded, TTL-windowed terminal-outcome tracker for tx_errors.
pub struct TxErrorDedup {
    inner: Mutex<Inner>,
    window: Duration,
    capacity: usize,
}

impl Default for TxErrorDedup {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW, DEFAULT_CAPACITY)
    }
}

impl TxErrorDedup {
    pub fn new(window: Duration, capacity: usize) -> Self {
        assert!(capacity > 0, "TxErrorDedup capacity must be > 0");
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            window,
            capacity,
        }
    }

    /// Record that a receipt for `(sender, nonce)` was observed. Any rejection
    /// for the same key arriving within the window is suppressed (success
    /// overrides rejection).
    pub fn record_success(&self, sender: Address, nonce: u64) {
        self.record_success_at(sender, nonce, Instant::now());
    }

    /// Whether an inbound error should be processed (`true`) or dropped as a
    /// replica duplicate / overridden-by-success (`false`). Processing an
    /// error records it, so the twin's copy of the same emission returns
    /// `false`.
    pub fn observe_error(&self, sender: Address, nonce: u64, reason: &TxErrorReason) -> bool {
        self.observe_error_at(sender, nonce, reason, Instant::now())
    }

    fn record_success_at(&self, sender: Address, nonce: u64, now: Instant) {
        let mut g = self.inner.lock().expect("TxErrorDedup poisoned");
        g.purge(now, self.window);
        g.insert((sender, nonce), Mark::Succeeded, now, self.capacity);
    }

    fn observe_error_at(
        &self,
        sender: Address,
        nonce: u64,
        reason: &TxErrorReason,
        now: Instant,
    ) -> bool {
        let key = (sender, nonce);
        let class = std::mem::discriminant(reason);
        let mut g = self.inner.lock().expect("TxErrorDedup poisoned");
        g.purge(now, self.window);
        match g.map.get(&key) {
            // The tx landed — this rejection lost the race to the twin's
            // success. Keep the success mark in place.
            Some((Mark::Succeeded, _)) => false,
            // Same rejection class already processed within the window — the
            // twin's duplicate copy.
            Some((Mark::Rejected(c), _)) if *c == class => false,
            // First sight (or a different rejection class): process it.
            _ => {
                g.insert(key, Mark::Rejected(class), now, self.capacity);
                true
            }
        }
    }
}

impl Inner {
    /// Drop entries older than `window`, skipping order-queue entries that
    /// were refreshed since they were enqueued.
    fn purge(&mut self, now: Instant, window: Duration) {
        while let Some(&(key, at)) = self.order.front() {
            if now.duration_since(at) < window {
                break;
            }
            self.order.pop_front();
            if let Some(&(_, cur_at)) = self.map.get(&key)
                && cur_at == at
            {
                self.map.remove(&key);
            }
        }
    }

    /// Insert/refresh `key`, evicting the oldest live entries past `capacity`.
    fn insert(&mut self, key: (Address, u64), mark: Mark, now: Instant, capacity: usize) {
        self.map.insert(key, (mark, now));
        self.order.push_back((key, now));
        while self.map.len() > capacity {
            let Some((old_key, old_at)) = self.order.pop_front() else {
                break; // unreachable: map.len() > 0 implies order entries exist
            };
            if let Some(&(_, cur_at)) = self.map.get(&old_key)
                && cur_at == old_at
            {
                self.map.remove(&old_key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(5);

    fn dedup() -> TxErrorDedup {
        TxErrorDedup::new(WINDOW, 16)
    }

    fn dup_reason(expected_nonce: u64) -> TxErrorReason {
        TxErrorReason::DuplicatedTx { expected_nonce }
    }

    #[test]
    fn twin_copy_of_the_same_rejection_is_dropped() {
        let d = dedup();
        let s = Address::repeat_byte(0x11);
        let now = Instant::now();
        assert!(
            d.observe_error_at(s, 7, &dup_reason(9), now),
            "first copy processed"
        );
        // The twin's copy — same key + class, possibly a different payload
        // (replicas can disagree on expected_nonce) — is dropped.
        assert!(!d.observe_error_at(s, 7, &dup_reason(10), now + Duration::from_millis(3)));
    }

    #[test]
    fn distinct_keys_are_independent() {
        let d = dedup();
        let now = Instant::now();
        assert!(d.observe_error_at(Address::repeat_byte(1), 0, &dup_reason(1), now));
        assert!(d.observe_error_at(Address::repeat_byte(1), 1, &dup_reason(2), now));
        assert!(d.observe_error_at(Address::repeat_byte(2), 0, &dup_reason(1), now));
    }

    #[test]
    fn same_rejection_after_the_window_is_processed_again() {
        // A client resubmitting the same duplicate tx later must get a prompt
        // rejection again — the window dedups replica copies, not recurrences.
        let d = dedup();
        let s = Address::repeat_byte(0x22);
        let now = Instant::now();
        assert!(d.observe_error_at(s, 3, &dup_reason(5), now));
        assert!(!d.observe_error_at(s, 3, &dup_reason(5), now + Duration::from_millis(10)));
        assert!(d.observe_error_at(
            s,
            3,
            &dup_reason(5),
            now + WINDOW + Duration::from_millis(1)
        ));
    }

    #[test]
    fn success_overrides_a_later_rejection() {
        // Twin A's receipt landed; twin B's rejection for the same tx must be
        // suppressed (the tx succeeded).
        let d = dedup();
        let s = Address::repeat_byte(0x33);
        let now = Instant::now();
        d.record_success_at(s, 4, now);
        assert!(!d.observe_error_at(s, 4, &dup_reason(4), now + Duration::from_millis(50)));
        // ...and the success mark is NOT displaced by the suppressed error.
        assert!(!d.observe_error_at(s, 4, &dup_reason(4), now + Duration::from_millis(80)));
    }

    #[test]
    fn success_mark_expires_with_the_window() {
        let d = dedup();
        let s = Address::repeat_byte(0x44);
        let now = Instant::now();
        d.record_success_at(s, 4, now);
        // A genuinely new rejection for the same key after the window (e.g. a
        // late resubmission of the landed tx) is processed normally.
        assert!(d.observe_error_at(
            s,
            4,
            &dup_reason(5),
            now + WINDOW + Duration::from_millis(1)
        ));
    }

    #[test]
    fn capacity_bound_evicts_oldest() {
        let d = TxErrorDedup::new(WINDOW, 4);
        let now = Instant::now();
        for i in 0..6u8 {
            assert!(d.observe_error_at(Address::repeat_byte(i), 0, &dup_reason(0), now));
        }
        // Oldest (0, 1) evicted → read as new again; newest still dedup.
        assert!(!d.observe_error_at(
            Address::repeat_byte(5),
            0,
            &dup_reason(0),
            now + Duration::from_millis(1)
        ));
        assert!(d.observe_error_at(
            Address::repeat_byte(0),
            0,
            &dup_reason(0),
            now + Duration::from_millis(1)
        ));
    }
}
