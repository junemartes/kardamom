//! The seal latency: the time from the publish of a transaction on
//! `tx_data` to the cluster egress frame that gives it a canonical index.
//!
//! The submit path and the egress tap run on different threads. The
//! submit path holds a [`SealStamps`] and sends the hash and the publish
//! instant of each transaction over a bounded channel. The egress tap
//! owns the [`SealClock`]: it moves the stamps into its own map, and
//! records one histogram sample for each sealed hash that it has a stamp
//! for. A transaction that a different ingress published has no stamp
//! here, so it gives no sample.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

use alloy_primitives::B256;

/// The stamps the channel holds between two seals. Only
/// [`SealClock::sealed`] empties the channel, so the channel fills only
/// while no transaction of this ingress seals. At 6,000 tx/s this
/// capacity holds about 10 s of stamps.
const STAMP_CAPACITY: usize = 64 * 1024;

/// The clock removes a stamp older than this, and counts it in
/// [`crate::metrics::SEAL_STAMPS_EXPIRED_TOTAL`]. Most such stamps are of
/// transactions that the sequencer refused. A seal that comes later gives
/// no sample, so the histogram has no value above this age: a non-zero
/// counter under load means that the histogram undercounts the tail. The
/// age is above the 90 s receipt drain of `kardamom-load`.
const STAMP_TTL: Duration = Duration::from_secs(180);

/// The clock removes old stamps at most this often.
const SWEEP_EVERY: Duration = Duration::from_secs(5);

/// The submit side: it stamps each transaction at its publish.
#[derive(Clone)]
pub struct SealStamps {
    tx: SyncSender<(B256, Instant)>,
}

impl SealStamps {
    /// Stamp `tx_hash` with the current instant. A full channel drops the
    /// stamp and counts it in
    /// [`crate::metrics::SEAL_STAMPS_DROPPED_TOTAL`]: the histogram then
    /// misses one sample, and the submit never waits. A clock that is gone
    /// (no egress tap) drops the stamp with no count.
    pub fn stamp(&self, tx_hash: B256) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send((tx_hash, Instant::now())) {
            metrics::counter!(crate::metrics::SEAL_STAMPS_DROPPED_TOTAL).increment(1);
        }
    }
}

/// The egress side: the open stamps, by hash.
pub struct SealClock {
    rx: Receiver<(B256, Instant)>,
    open: HashMap<B256, Instant>,
    last_sweep: Instant,
}

impl SealClock {
    /// A connected pair of the submit side and the egress side.
    #[must_use]
    pub fn pair() -> (SealStamps, SealClock) {
        let (tx, rx) = sync_channel(STAMP_CAPACITY);
        let clock = SealClock {
            rx,
            open: HashMap::new(),
            last_sweep: Instant::now(),
        };
        (SealStamps { tx }, clock)
    }

    /// Record the seal latency of `tx_hash`, if this ingress stamped it.
    pub fn sealed(&mut self, tx_hash: B256) {
        if let Some(latency) = self.latency_at(tx_hash, Instant::now()) {
            metrics::histogram!(crate::metrics::SEAL_LATENCY_SECONDS).record(latency.as_secs_f64());
        }
    }

    /// The time from the stamp of `tx_hash` to `now`. It first moves the
    /// waiting stamps into the map, so a stamp sent before the seal is
    /// always found.
    fn latency_at(&mut self, tx_hash: B256, now: Instant) -> Option<Duration> {
        self.open.extend(self.rx.try_iter());
        self.sweep(now);
        self.open
            .remove(&tx_hash)
            .map(|stamped| now.saturating_duration_since(stamped))
    }

    /// Remove and count the stamps older than [`STAMP_TTL`], at most
    /// once in each [`SWEEP_EVERY`].
    fn sweep(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_sweep) < SWEEP_EVERY {
            return;
        }
        self.last_sweep = now;
        // The count is at most the length of the map, so it cannot overflow.
        let mut expired = 0u64;
        self.open.retain(|_, stamped| {
            let keep = now.saturating_duration_since(*stamped) < STAMP_TTL;
            expired += u64::from(!keep);
            keep
        });
        metrics::counter!(crate::metrics::SEAL_STAMPS_EXPIRED_TOTAL).increment(expired);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamped_hash_gives_one_latency() {
        let (stamps, mut clock) = SealClock::pair();
        let hash = B256::repeat_byte(1);
        stamps.stamp(hash);
        let later = Instant::now() + Duration::from_millis(40);
        let latency = clock.latency_at(hash, later).expect("the hash has a stamp");
        assert!(latency >= Duration::from_millis(40));
        assert_eq!(clock.latency_at(hash, later), None, "one sample per seal");
    }

    #[test]
    fn a_seal_after_a_minute_still_gives_a_sample() {
        let (stamps, mut clock) = SealClock::pair();
        let hash = B256::repeat_byte(5);
        stamps.stamp(hash);
        let late = Instant::now() + Duration::from_secs(60);
        assert!(clock.latency_at(hash, late).is_some());
    }

    #[test]
    fn a_hash_without_a_stamp_gives_none() {
        let (_stamps, mut clock) = SealClock::pair();
        assert_eq!(clock.latency_at(B256::repeat_byte(2), Instant::now()), None);
    }

    #[test]
    fn the_sweep_removes_old_stamps() {
        let (stamps, mut clock) = SealClock::pair();
        stamps.stamp(B256::repeat_byte(3));
        let late = Instant::now() + STAMP_TTL + SWEEP_EVERY;
        assert_eq!(clock.latency_at(B256::repeat_byte(4), late), None);
        assert!(clock.open.is_empty(), "the old stamp is gone");
    }
}
