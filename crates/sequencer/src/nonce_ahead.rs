//! The senders that the sealer's nonce guard rejects above the chain.
//!
//! The guard counts every ordered record, and a skip uses no nonce on the
//! chain. So after a skip, the guard can expect a nonce of a sender that
//! the chain never reaches. A reject that shows this looks the same as a
//! reject while an ordered ref is not executed yet, for example after a
//! restart of the sequencer, which then holds no ledger entry for the
//! ref. So the watch calls a sender stuck only when the same chain nonce
//! stays for a settle time: by then every ordered ref has its receipt,
//! and a receipt raises the chain nonce.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy_primitives::Address;

/// The bound on watched senders. A sender leaves when its receipt floor
/// rises, and an entry older than ten settle times leaves on an insert
/// into a full map.
const CAPACITY: usize = 4096;

/// What the sequencer answers for a reject above the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The chain nonce has not moved for the settle time: the sender is
    /// stuck.
    Stuck,
    /// The receipts have not settled. An ordered ref can still execute.
    InFlight,
}

/// The first reject above the chain of each watched sender, and the
/// chain nonce at that reject.
#[derive(Debug)]
pub(crate) struct NonceAheadWatch {
    settle: Duration,
    since: HashMap<Address, (u64, Instant)>,
}

impl NonceAheadWatch {
    pub(crate) fn new(settle: Duration) -> Self {
        Self {
            settle,
            since: HashMap::new(),
        }
    }

    /// A reject of `sender` above the chain nonce `chain` at `now`. The
    /// first reject at a chain nonce starts the settle time.
    pub(crate) fn observe(&mut self, sender: Address, chain: u64, now: Instant) -> Verdict {
        let first = match self.since.get(&sender) {
            Some((at_chain, first)) if *at_chain == chain => *first,
            _ => self.start(sender, chain, now),
        };
        if now.duration_since(first) >= self.settle {
            Verdict::Stuck
        } else {
            Verdict::InFlight
        }
    }

    /// The receipt floor of `sender` rose: its old rejects say nothing.
    pub(crate) fn forget(&mut self, sender: Address) {
        self.since.remove(&sender);
    }

    /// Start the settle time of `sender` at `now`. A full map first drops
    /// the entries older than ten settle times. If it is still full, the
    /// sender is not watched, and every reject of it is `InFlight`.
    fn start(&mut self, sender: Address, chain: u64, now: Instant) -> Instant {
        if self.since.len() >= CAPACITY {
            let horizon = self.settle.saturating_mul(10);
            self.since
                .retain(|_, (_, first)| now.duration_since(*first) < horizon);
        }
        if self.since.len() < CAPACITY || self.since.contains_key(&sender) {
            self.since.insert(sender, (chain, now));
        }
        now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SETTLE: Duration = Duration::from_secs(15);

    #[test]
    fn a_sender_is_stuck_only_after_the_settle_time_at_one_chain_nonce() {
        let mut watch = NonceAheadWatch::new(SETTLE);
        let s = Address::repeat_byte(1);
        let t0 = Instant::now();
        assert_eq!(watch.observe(s, 0, t0), Verdict::InFlight);
        assert_eq!(watch.observe(s, 0, t0 + SETTLE), Verdict::Stuck);
        // The chain moved: the settle time starts again.
        assert_eq!(watch.observe(s, 1, t0 + SETTLE), Verdict::InFlight);
        watch.forget(s);
        assert_eq!(watch.observe(s, 1, t0 + SETTLE * 2), Verdict::InFlight);
    }

    #[test]
    fn a_full_map_drops_old_entries_and_never_calls_a_new_sender_stuck() {
        let mut watch = NonceAheadWatch::new(SETTLE);
        let t0 = Instant::now();
        (0..CAPACITY).for_each(|i| {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&u64::try_from(i).unwrap().to_be_bytes());
            watch.observe(Address::from(bytes), 0, t0);
        });
        let late = Address::repeat_byte(0xff);
        let soon = t0 + SETTLE;
        assert_eq!(watch.observe(late, 0, soon), Verdict::InFlight);
        assert_eq!(watch.observe(late, 0, soon + SETTLE), Verdict::InFlight);
        let much_later = t0 + SETTLE * 10;
        assert_eq!(watch.observe(late, 0, much_later), Verdict::InFlight);
        assert_eq!(watch.observe(late, 0, much_later + SETTLE), Verdict::Stuck);
    }
}
