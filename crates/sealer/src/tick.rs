//! Wall-clock tick alignment for boundary emission.
//!
//! `l2_timestamp` for a tick is always `floor(now / interval) * interval`.
//! Two sealers that disagree by < `interval` ms still produce the same
//! timestamp for the same tick — preserving determinism across leader change.
//! See spec §I3 / plan §tick-scheduling.

/// Round `now_ms` down to the nearest multiple of `interval_ms`.
///
/// # Panics
/// Panics if `interval_ms == 0`.
pub fn floor_to_tick(now_ms: u64, interval_ms: u64) -> u64 {
    assert!(interval_ms > 0, "tick interval must be > 0");
    (now_ms / interval_ms) * interval_ms
}

/// Compute the next tick boundary strictly greater than `now_ms`.
///
/// # Panics
/// Panics if `interval_ms == 0`.
pub fn next_tick(now_ms: u64, interval_ms: u64) -> u64 {
    assert!(interval_ms > 0, "tick interval must be > 0");
    floor_to_tick(now_ms, interval_ms) + interval_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_at_boundary_returns_self() {
        assert_eq!(floor_to_tick(1_000, 250), 1_000);
    }

    #[test]
    fn floor_mid_window_returns_window_start() {
        assert_eq!(floor_to_tick(1_123, 250), 1_000);
        assert_eq!(floor_to_tick(1_249, 250), 1_000);
    }

    #[test]
    fn next_tick_at_boundary_returns_following_boundary() {
        assert_eq!(next_tick(1_000, 250), 1_250);
    }

    #[test]
    fn next_tick_mid_window_returns_window_end() {
        assert_eq!(next_tick(1_123, 250), 1_250);
    }

    #[test]
    fn determinism_across_window() {
        // Every wall-clock-ms in [1_000, 1_249] floors to the same tick.
        // This is the property that lets the standby pick up the leader's
        // l2_timestamp deterministically during a takeover.
        for t in 1_000..1_250 {
            assert_eq!(floor_to_tick(t, 250), 1_000);
        }
        for t in 1_250..1_500 {
            assert_eq!(floor_to_tick(t, 250), 1_250);
        }
    }

    #[test]
    #[should_panic(expected = "tick interval must be > 0")]
    fn floor_panics_on_zero_interval() {
        let _ = floor_to_tick(1_000, 0);
    }

    #[test]
    #[should_panic(expected = "tick interval must be > 0")]
    fn next_panics_on_zero_interval() {
        let _ = next_tick(1_000, 0);
    }
}
