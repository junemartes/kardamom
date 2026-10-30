//! The health of the shared Redis connection: which failure replaces
//! it, and how soon a failed replacement tries again.
//!
//! A command that times out does not prove the connection dead. A
//! primary that stalls answers late on the same connection, and a fresh
//! connection reaches the same stalled primary. So a timeout replaces
//! the connection only after a stall with no reply at all, and reads
//! continue on it meanwhile. A connection error proves the connection
//! dead, and replaces it at once.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redis::{ErrorKind, RedisError, ServerErrorKind};

use crate::error::CacheError;

/// A stall replaces the connection after this many command timeouts
/// with no reply. A frozen primary stalls past it, so the client still
/// follows a Sentinel failover. A primary that pauses for less, for
/// example in a fork for a snapshot, keeps its clients.
const STALL_TIMEOUTS: u32 = 3;

/// The pause after the first failed replacement. Each further failure
/// doubles it, up to [`BACKOFF_LAST`].
const BACKOFF_FIRST: Duration = Duration::from_millis(100);
const BACKOFF_LAST: Duration = Duration::from_secs(2);

pub(crate) struct Link {
    epoch: Instant,
    /// Milliseconds from `epoch` to the last reply on the connection.
    replied: AtomicU64,
    /// The connection failed with a connection error, and no new one
    /// replaces it yet.
    lost: AtomicBool,
    /// A replacement is in flight. One at a time.
    replacing: AtomicBool,
    /// The pause after the next failed replacement, in milliseconds.
    backoff: AtomicU64,
    stall: Duration,
}

impl Link {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            epoch: Instant::now(),
            replied: AtomicU64::new(0),
            lost: AtomicBool::new(false),
            replacing: AtomicBool::new(false),
            backoff: AtomicU64::new(Self::millis(BACKOFF_FIRST)),
            stall: timeout.saturating_mul(STALL_TIMEOUTS),
        }
    }

    /// The server replied on the connection.
    pub(crate) fn replied(&self) {
        self.replied.store(self.now(), Ordering::Release);
    }

    /// Whether `error` must replace the connection. A connection error
    /// marks the connection lost. A primary that turned replica refuses
    /// writes on a live connection, so that connection goes too.
    pub(crate) fn must_replace(&self, error: &CacheError) -> bool {
        match error {
            CacheError::Timeout => self.stalled(),
            CacheError::Redis(e) if Self::connection_failed(e) => {
                self.lost.store(true, Ordering::Release);
                true
            }
            CacheError::Redis(e) => {
                self.replied();
                Self::not_primary(e)
            }
            _ => false,
        }
    }

    /// Whether the connection failed with a connection error and no new
    /// one replaces it yet. A command on it fails at once.
    pub(crate) fn lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    /// Claim the one replacement. False when another one is in flight.
    pub(crate) fn claim(&self) -> bool {
        self.replacing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// A new connection replaces the old one.
    pub(crate) fn replaced(&self) {
        self.replied();
        self.backoff
            .store(Self::millis(BACKOFF_FIRST), Ordering::Release);
        self.lost.store(false, Ordering::Release);
        self.replacing.store(false, Ordering::Release);
    }

    /// A replacement failed. Wait out the backoff, then let the next
    /// failure try again.
    pub(crate) async fn failed(&self) {
        tokio::time::sleep(self.next_pause()).await;
        self.replacing.store(false, Ordering::Release);
    }

    /// The pause after this failed replacement. The next one doubles
    /// it, up to [`BACKOFF_LAST`].
    fn next_pause(&self) -> Duration {
        let pause = self.backoff.load(Ordering::Acquire);
        let next = pause.saturating_mul(2).min(Self::millis(BACKOFF_LAST));
        self.backoff.store(next, Ordering::Release);
        Duration::from_millis(pause)
    }

    fn stalled(&self) -> bool {
        let quiet = self
            .now()
            .saturating_sub(self.replied.load(Ordering::Acquire));
        quiet >= Self::millis(self.stall)
    }

    fn connection_failed(e: &RedisError) -> bool {
        e.is_io_error() || e.is_connection_dropped() || e.is_connection_refusal()
    }

    fn not_primary(e: &RedisError) -> bool {
        matches!(
            e.kind(),
            ErrorKind::Server(ServerErrorKind::ReadOnly | ServerErrorKind::MasterDown)
        )
    }

    fn now(&self) -> u64 {
        Self::millis(self.epoch.elapsed())
    }

    /// Whole milliseconds. A process does not run for 2^64 ms, so the
    /// conversion saturates only in theory.
    fn millis(span: Duration) -> u64 {
        u64::try_from(span.as_millis()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
#[path = "link_tests.rs"]
mod tests;
