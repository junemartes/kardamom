//! Deterministic leader election for the block sealer.
//!
//! The byte-level lowest-recorder-id-among-caught-up-recorders rule lives in
//! [`kardamom_leases::Lease`] and is shared with the S2 sequencer / future S7
//! batcher. This module adds the per-recorder **freshness window** the sealer
//! cares about (a lease holder whose watermark has gone silent for >
//! `caught_up_stale_ms` is no longer eligible) and exposes a pure
//! [`elect`] helper used in tests + by [`crate::sealer::Sealer`].
//!
//! ## Why a separate module
//!
//! `kardamom_leases::Lease` keys eligibility entirely on byte lag: a recorder
//! whose watermark stops advancing but is "near the tail" still appears to
//! hold the lease forever. The sealer additionally treats a recorder whose
//! watermark hasn't been refreshed in `caught_up_stale_ms` ms as
//! *unhealthy* — necessary because the sealer ticks every 250 ms and would
//! otherwise wedge if the elected leader's host paused.
//!
//! The rule:
//! > Leader = `min(recorder_id)` among recorders whose
//! >   (a) most recent `FsyncWatermark.position` is within
//! >       `caught_up_lag_bytes` of the current B publication position,
//! >   AND (b) most recent watermark was observed within
//! >       `caught_up_stale_ms` ms.

use std::collections::BTreeMap;

use kardamom_types::BPosition;

/// Per-recorder state observed by the local sealer process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecorderState {
    pub recorder_id: u8,
    pub fsynced: BPosition,
    /// Wall-clock unix-ms of when the most recent watermark for this recorder
    /// was observed locally.
    pub last_seen_ms: u64,
}

/// Set of recorder states keyed by id. BTreeMap so iteration is id-ordered.
#[derive(Debug, Default, Clone)]
pub struct CaughtUpSet {
    by_id: BTreeMap<u8, RecorderState>,
}

impl CaughtUpSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, s: RecorderState) {
        self.by_id.insert(s.recorder_id, s);
    }

    /// Build a set from an iterator of states. Named `from_states` rather
    /// than `from_iter` so we don't shadow the std `FromIterator` trait method
    /// (clippy::should_implement_trait); implementing `FromIterator` itself
    /// would force every callsite to add a `.collect::<CaughtUpSet>()` turbofish
    /// which reads worse than the explicit constructor.
    pub fn from_states<I: IntoIterator<Item = RecorderState>>(iter: I) -> Self {
        let mut s = Self::new();
        for r in iter {
            s.insert(r);
        }
        s
    }

    pub fn states(&self) -> impl Iterator<Item = &RecorderState> {
        self.by_id.values()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// `BPosition` → absolute byte offset. Mirrors the same constant used in
/// `kardamom_leases::lease::within_window`: term length is **16 MiB**, which
/// must equal Aeron's `aeron.term.buffer.length` in production deployments.
pub const TERM_LEN_BYTES: i64 = 16 * 1024 * 1024;

#[inline]
pub fn bpos_to_abs(p: BPosition) -> i64 {
    (p.term_id as i64) * TERM_LEN_BYTES + p.term_offset as i64
}

/// Decide which sealer should emit. Returns `None` if no recorder is caught up
/// (no boundary is emitted; the chain pauses until quorum recovers).
///
/// Pure function over `(set, current_position, now_ms, caught_up_lag_bytes,
/// caught_up_stale_ms)` — every sealer that observes the same inputs computes
/// the same winner.
pub fn elect(
    set: &CaughtUpSet,
    current_position: BPosition,
    now_ms: u64,
    caught_up_lag_bytes: u64,
    caught_up_stale_ms: u64,
) -> Option<u8> {
    let cur_abs = bpos_to_abs(current_position);
    let lag_threshold = caught_up_lag_bytes as i64;
    set.states()
        .filter(|r| {
            let rec_abs = bpos_to_abs(r.fsynced);
            // Negative lag = recorder is past our last-known tail (a watermark
            // arrived between our snapshot reads); treat as caught up.
            let lag = cur_abs - rec_abs;
            let caught_up = lag <= lag_threshold;
            let fresh = now_ms.saturating_sub(r.last_seen_ms) <= caught_up_stale_ms;
            caught_up && fresh
        })
        .min_by_key(|r| r.recorder_id)
        .map(|r| r.recorder_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(term: i32, off: i32) -> BPosition {
        BPosition {
            term_id: term,
            term_offset: off,
        }
    }

    #[test]
    fn picks_lowest_caught_up_id() {
        let set = CaughtUpSet::from_states([
            RecorderState {
                recorder_id: 5,
                fsynced: pos(0, 1_000),
                last_seen_ms: 1_000,
            },
            RecorderState {
                recorder_id: 2,
                fsynced: pos(0, 1_000),
                last_seen_ms: 1_000,
            },
            RecorderState {
                recorder_id: 7,
                fsynced: pos(0, 1_000),
                last_seen_ms: 1_000,
            },
        ]);
        assert_eq!(elect(&set, pos(0, 1_000), 1_100, 0, 500), Some(2));
    }

    #[test]
    fn skips_lagging_recorder() {
        // Recorder 2 is 1 MB behind the tail; threshold is 64 KB; skip.
        let set = CaughtUpSet::from_states([
            RecorderState {
                recorder_id: 2,
                fsynced: pos(0, 0),
                last_seen_ms: 1_100,
            },
            RecorderState {
                recorder_id: 5,
                fsynced: pos(0, 1_000_000),
                last_seen_ms: 1_100,
            },
        ]);
        assert_eq!(
            elect(&set, pos(0, 1_000_000), 1_100, 64 * 1024, 500),
            Some(5)
        );
    }

    #[test]
    fn skips_stale_recorder() {
        let set = CaughtUpSet::from_states([
            RecorderState {
                recorder_id: 2,
                fsynced: pos(0, 1_000),
                last_seen_ms: 100, // 1 s ago, staleness threshold is 500 ms
            },
            RecorderState {
                recorder_id: 5,
                fsynced: pos(0, 1_000),
                last_seen_ms: 1_100,
            },
        ]);
        assert_eq!(elect(&set, pos(0, 1_000), 1_100, 64 * 1024, 500), Some(5));
    }

    #[test]
    fn returns_none_when_no_one_caught_up() {
        let set = CaughtUpSet::from_states([RecorderState {
            recorder_id: 2,
            fsynced: pos(0, 0),
            last_seen_ms: 100,
        }]);
        assert_eq!(elect(&set, pos(0, 1_000_000), 1_100, 64 * 1024, 500), None);
    }

    #[test]
    fn handles_term_rollover() {
        // current = (1, 100) → absolute = 16 MiB + 100. recorder 2 at
        // (0, 1_000_000) absolute = 1_000_000. Lag = 16 MiB - 1_000_000 + 100,
        // way above 64 KiB.
        let set = CaughtUpSet::from_states([
            RecorderState {
                recorder_id: 2,
                fsynced: pos(0, 1_000_000),
                last_seen_ms: 1_100,
            },
            RecorderState {
                recorder_id: 5,
                fsynced: pos(1, 100),
                last_seen_ms: 1_100,
            },
        ]);
        assert_eq!(elect(&set, pos(1, 100), 1_100, 64 * 1024, 500), Some(5));
    }

    #[test]
    fn empty_set_returns_none() {
        let set = CaughtUpSet::new();
        assert_eq!(elect(&set, pos(0, 0), 0, 0, 0), None);
    }

    #[test]
    fn term_len_constant_matches_lease_crate() {
        // The lease crate hard-codes TERM_LEN = 16 MiB. If that constant
        // changes there we must mirror it here, otherwise eligibility
        // decisions diverge between the sealer and the underlying lease.
        // The test is a compile-and-run reminder.
        assert_eq!(TERM_LEN_BYTES, 16 * 1024 * 1024);
    }
}
