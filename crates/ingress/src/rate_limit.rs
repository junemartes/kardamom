//! Per-IP token-bucket rate limit. It runs before any costly work, so
//! abusive clients get rejected at near-zero CPU cost.

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use dashmap::DashMap;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};

type DirectLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Marker error returned when an IP's bucket has no tokens left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimited;

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("rate limit exceeded")
    }
}

impl std::error::Error for RateLimited {}

/// Per-IP `governor` token bucket. A new IP gets a fresh limiter on its first
/// call.
pub struct PerIpLimiter {
    quota: Quota,
    buckets: Arc<DashMap<IpAddr, Arc<DirectLimiter>>>,
}

impl PerIpLimiter {
    pub fn new(per_sec: NonZeroU32, burst: NonZeroU32) -> Self {
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        Self {
            quota,
            buckets: Arc::new(DashMap::new()),
        }
    }

    /// Returns `Ok(())` on allow, `Err(RateLimited)` when the IP's bucket
    /// is empty.
    pub fn check(&self, ip: IpAddr) -> Result<(), RateLimited> {
        let limiter = self
            .buckets
            .entry(ip)
            .or_insert_with(|| Arc::new(RateLimiter::direct(self.quota)))
            .clone();
        limiter.check().map(|_| ()).map_err(|_| RateLimited)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

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
}
