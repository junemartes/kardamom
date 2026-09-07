//! Per-IP token-bucket rate limit. It runs before any costly work, so
//! abusive clients get rejected at near-zero CPU cost.

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

use governor::clock::DefaultClock;
use governor::state::keyed::DashMapStateStore;
use governor::{Quota, RateLimiter};

/// A keyed limiter: one `governor` bucket per client IP, backed by a
/// `DashMap`.
type IpLimiter = RateLimiter<IpAddr, DashMapStateStore<IpAddr>, DefaultClock>;

/// Sweeps idle buckets every this many `check` calls. This amortizes the
/// sweep's cost over many calls, instead of paying it on every one.
const SWEEP_EVERY: u32 = 4096;

/// Marker error returned when an IP's bucket has no tokens left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RateLimited;

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("rate limit exceeded")
    }
}

impl std::error::Error for RateLimited {}

/// Per-IP `governor` token bucket. A new IP gets a fresh bucket on its
/// first call. A bucket that stays idle long enough to fully refill is
/// indistinguishable from a fresh one, so [`Self::check`] sweeps those out
/// on a schedule. Without this, one entry per distinct client IP would
/// grow the map without limit.
pub(crate) struct PerIpLimiter {
    limiter: IpLimiter,
    calls_since_sweep: AtomicU32,
}

impl PerIpLimiter {
    #[must_use]
    pub(crate) fn new(per_sec: NonZeroU32, burst: NonZeroU32) -> Self {
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        Self {
            limiter: RateLimiter::dashmap(quota),
            calls_since_sweep: AtomicU32::new(0),
        }
    }

    /// Checks and consumes one token for `ip`'s bucket.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimited`] when the bucket has no tokens left.
    pub(crate) fn check(&self, ip: IpAddr) -> Result<(), RateLimited> {
        let result = self.limiter.check_key(&ip).map_err(|_| RateLimited);
        self.maybe_sweep();
        result
    }

    /// Runs the amortized sweep once every [`SWEEP_EVERY`] calls.
    fn maybe_sweep(&self) {
        if self.calls_since_sweep.fetch_add(1, Ordering::Relaxed) < SWEEP_EVERY {
            return;
        }
        self.calls_since_sweep.store(0, Ordering::Relaxed);
        self.sweep();
    }

    /// Drops every bucket whose state is indistinguishable from fresh.
    /// Split out from [`Self::maybe_sweep`] so a test can call it
    /// directly, without driving thousands of `check` calls.
    fn sweep(&self) {
        self.limiter.retain_recent();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.limiter.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;
    use std::time::Duration;

    #[test]
    fn allows_within_burst_and_denies_overflow() {
        let lim = PerIpLimiter::new(nonzero!(1u32), nonzero!(3u32));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        // The fourth call in the same tick goes over the burst of 3.
        assert!(lim.check(ip).is_err());
    }

    #[test]
    fn different_ips_have_independent_budgets() {
        let lim = PerIpLimiter::new(nonzero!(1u32), nonzero!(1u32));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        assert!(lim.check(a).is_ok());
        assert!(lim.check(a).is_err());
        assert!(lim.check(b).is_ok());
    }

    // Regression test for the unbounded-map defect: `PerIpLimiter` used to
    // add one `DashMap` entry per distinct client IP and never remove one.
    // A bucket that fully refills while idle must be evicted on sweep, so
    // the map stays bounded to the set of recently active clients.
    #[test]
    fn sweep_evicts_a_fully_refilled_bucket() {
        let lim = PerIpLimiter::new(nonzero!(1_000u32), nonzero!(1u32));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(lim.check(ip).is_ok());
        assert_eq!(lim.len(), 1);
        // A burst of 1 at 1000/s refills in about 1ms. This waits well
        // past that, so the bucket is indistinguishable from fresh.
        std::thread::sleep(Duration::from_millis(20));
        lim.sweep();
        assert_eq!(
            lim.len(),
            0,
            "an idle, fully-refilled bucket must be evicted"
        );
    }

    #[test]
    fn a_still_active_bucket_survives_a_sweep() {
        let lim = PerIpLimiter::new(nonzero!(1u32), nonzero!(1u32));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        // This spends the only token, so the bucket is not yet
        // indistinguishable from fresh.
        assert!(lim.check(ip).is_ok());
        lim.sweep();
        assert_eq!(lim.len(), 1, "a recently spent bucket must survive a sweep");
    }
}
