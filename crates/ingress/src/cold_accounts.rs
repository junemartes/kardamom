//! The executor answers for senders that no cache layer holds.
//!
//! A sender with no row in the local layer or in Redis is cold: it never
//! sent a transaction and never received value, or its rows aged out.
//! The admission path asks the executor query once for such a sender and
//! keeps the answer here. So each cold sender costs one query per TTL,
//! not one query per submit. A row in the local layer always wins: the
//! admission path reads this map only after the local layer and Redis
//! missed.
//!
//! The map is bounded, oldest first, like the receipt cache: the insert
//! that fills the ring retires the oldest address.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use kardamom_cache::AccountView;

/// The bound on cold senders the map holds.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

/// How long an answer stays. A cold sender that gets funds or lands a
/// transaction gets a row in the local layer, which wins. So the TTL
/// bounds only a missed receipt frame, and it matches the local layer's
/// default TTL.
const TTL: Duration = Duration::from_secs(30);

/// The executor answers, by address, with the time each one came.
pub(crate) struct ColdAccounts {
    map: DashMap<Address, (AccountView, Instant)>,
    order: ArrayQueue<Address>,
}

impl Default for ColdAccounts {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            order: ArrayQueue::new(CAPACITY.get()),
        }
    }
}

impl ColdAccounts {
    /// The answer for `address`, when one came within the TTL.
    pub(crate) fn get(&self, address: Address) -> Option<AccountView> {
        let entry = self.map.get(&address)?;
        (entry.1.elapsed() <= TTL).then(|| entry.0.clone())
    }

    /// Keep the answer for `address`. A full ring retires the oldest
    /// address. A retired address that came again since then loses its
    /// newer answer too, which costs one more query and nothing else.
    pub(crate) fn insert(&self, address: Address, view: AccountView) {
        self.map.insert(address, (view, Instant::now()));
        if let Some(oldest) = self.order.force_push(address) {
            self.map.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::*;

    fn view(nonce: u64) -> AccountView {
        AccountView {
            nonce,
            balance: U256::ZERO,
            tx_idx: 0,
        }
    }

    #[test]
    fn an_answer_is_kept_and_the_ring_retires_the_oldest() {
        let cold = ColdAccounts::default();
        let first = Address::repeat_byte(0x01);
        cold.insert(first, view(3));
        assert_eq!(cold.get(first).map(|v| v.nonce), Some(3));
        (0..CAPACITY.get()).for_each(|i| {
            let mut bytes = [0xffu8; 20];
            bytes[..8].copy_from_slice(&u64::try_from(i).unwrap().to_be_bytes());
            cold.insert(Address::from(bytes), view(0));
        });
        assert!(cold.get(first).is_none(), "the oldest address retires");
        assert_eq!(cold.map.len(), CAPACITY.get());
    }
}
