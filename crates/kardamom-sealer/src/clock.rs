//! Wall-clock abstraction.
//!
//! Distinct from tokio's monotonic clock (which is used for tick scheduling
//! and is mockable via `tokio::time::pause`). This trait only covers the
//! `l2_timestamp` derivation. The real implementation reads
//! `SystemTime::now()`; the mock implementation lets tests advance time
//! deterministically.
//!
//! Wall-clock source policy (S0-3): v0 uses the host's
//! `SystemTime`. Operators are responsible for chrony/ntpd. PTP is a
//! follow-up if cross-host skew becomes the dominant non-determinism
//! source for `l2_timestamp`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock Unix-epoch milliseconds.
pub trait WallClock: Send + Sync + 'static {
    fn unix_ms(&self) -> u64;
}

/// Production clock. Reads `SystemTime::now()`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl WallClock for SystemClock {
    fn unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before 1970")
            .as_millis() as u64
    }
}

/// Test-only clock. Time only moves when [`Self::set`] or [`Self::advance`]
/// is called.
#[derive(Clone, Debug)]
pub struct MockClock(Arc<AtomicU64>);

impl MockClock {
    pub fn new(start_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start_ms)))
    }
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
    pub fn advance(&self, delta_ms: u64) {
        self.0.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl WallClock for MockClock {
    fn unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_recent_unix_ms() {
        let clock = SystemClock;
        let now = clock.unix_ms();
        // Sanity bracket: between 2025-01-01 and 2030-01-01.
        assert!(now > 1_735_689_600_000);
        assert!(now < 1_893_456_000_000);
    }

    #[test]
    fn mock_clock_is_settable() {
        let clock = MockClock::new(1_000);
        assert_eq!(clock.unix_ms(), 1_000);
        clock.set(2_500);
        assert_eq!(clock.unix_ms(), 2_500);
        clock.advance(125);
        assert_eq!(clock.unix_ms(), 2_625);
    }

    #[test]
    fn mock_clock_clones_share_state() {
        // The supervisor and emitter both hold a `Clone` of the same clock;
        // advancing through one handle must be visible through the other.
        let a = MockClock::new(0);
        let b = a.clone();
        a.advance(7);
        assert_eq!(b.unix_ms(), 7);
        b.set(42);
        assert_eq!(a.unix_ms(), 42);
    }
}
