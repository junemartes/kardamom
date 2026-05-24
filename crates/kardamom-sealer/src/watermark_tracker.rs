//! Lock-protected snapshot of every recorder's latest watermark.
//!
//! One writer task per watermark subscription updates the map; the tick loop
//! calls [`WatermarkTracker::snapshot`] once per tick to feed
//! [`crate::election::elect`].
//!
//! Contention is low (one write per recorder per ms; one read per 250 ms)
//! so a plain `std::sync::Mutex` is more than enough.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::election::{CaughtUpSet, RecorderState};

pub struct WatermarkTracker {
    expected: Vec<u8>,
    inner: Mutex<BTreeMap<u8, RecorderState>>,
}

impl WatermarkTracker {
    pub fn new(expected_recorder_ids: Vec<u8>) -> Self {
        Self {
            expected: expected_recorder_ids,
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    /// Apply a watermark observation.
    ///
    /// Updates from unknown recorder ids (i.e. not in the configured set) are
    /// silently dropped after a warn log. Observations older than the current
    /// stored `last_seen_ms` for the same recorder are ignored — both
    /// `last_seen_ms` and `fsynced` are monotonic and we never roll back.
    pub fn update(&self, state: RecorderState) {
        if !self.expected.contains(&state.recorder_id) {
            tracing::warn!(
                recorder_id = state.recorder_id,
                "watermark from unknown recorder id; dropping"
            );
            return;
        }
        let mut guard = self.inner.lock().expect("watermark mutex poisoned");
        if let Some(prev) = guard.get(&state.recorder_id)
            && prev.last_seen_ms > state.last_seen_ms
        {
            return;
        }
        guard.insert(state.recorder_id, state);
    }

    /// Snapshot the current set. Cheap (BTreeMap clone bounded by recorder count).
    pub fn snapshot(&self) -> CaughtUpSet {
        let guard = self.inner.lock().expect("watermark mutex poisoned");
        CaughtUpSet::from_iter(guard.values().copied())
    }

    /// Total observations currently held — primarily a test/observability hook.
    pub fn len(&self) -> usize {
        let guard = self.inner.lock().expect("watermark mutex poisoned");
        guard.len()
    }

    /// Convenience accessor that returns the most recent state for a given
    /// recorder, if any.
    pub fn get(&self, recorder_id: u8) -> Option<RecorderState> {
        let guard = self.inner.lock().expect("watermark mutex poisoned");
        guard.get(&recorder_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    fn rs(id: u8, off: i32, ts: u64) -> RecorderState {
        RecorderState {
            recorder_id: id,
            fsynced: BPosition {
                term_id: 0,
                term_offset: off,
            },
            last_seen_ms: ts,
        }
    }

    #[test]
    fn updates_in_place() {
        let tracker = WatermarkTracker::new(vec![1, 2, 3]);
        tracker.update(rs(2, 100, 1_000));
        let r2 = tracker.get(2).unwrap();
        assert_eq!(r2.fsynced.term_offset, 100);
        tracker.update(rs(2, 200, 1_100));
        let r2 = tracker.get(2).unwrap();
        assert_eq!(r2.fsynced.term_offset, 200);
    }

    #[test]
    fn ignores_unknown_recorder_id() {
        let tracker = WatermarkTracker::new(vec![1, 2]);
        tracker.update(rs(99, 100, 1_000));
        assert!(tracker.get(99).is_none());
    }

    #[test]
    fn older_observation_is_dropped() {
        let tracker = WatermarkTracker::new(vec![1]);
        tracker.update(rs(1, 100, 2_000));
        // Stale observation (older timestamp) is rejected.
        tracker.update(rs(1, 50, 1_000));
        let r1 = tracker.get(1).unwrap();
        assert_eq!(r1.fsynced.term_offset, 100);
        assert_eq!(r1.last_seen_ms, 2_000);
    }

    #[test]
    fn snapshot_contains_all_known_recorders() {
        let tracker = WatermarkTracker::new(vec![1, 2, 3]);
        tracker.update(rs(1, 10, 1_000));
        tracker.update(rs(2, 20, 1_000));
        let snap = tracker.snapshot();
        assert_eq!(snap.len(), 2);
        let ids: Vec<u8> = snap.states().map(|s| s.recorder_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
