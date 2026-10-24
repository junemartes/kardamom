//! [`Drops`]: the transactions that the sequencers dropped, as a status of
//! each hash.
//!
//! Both twins of a lane report their drops on `tx_drops`. The egress keeps
//! the first report of a hash, so the report of the other twin changes
//! nothing. A receipt wins over a drop: the owner forgets the drop of each
//! hash that gets a receipt, and takes no drop of a hash whose receipt it
//! holds. The map keeps the newest `capacity` drops. The oldest goes first.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256};
use kardamom_types::{DropReason, TxDrop};

/// One kept drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dropped {
    pub sender: Address,
    pub nonce: u64,
    pub reason: DropReason,
}

/// The status of a transaction hash, as the egress knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// The egress holds the receipt of the hash.
    Executed,
    /// A sequencer dropped the transaction, and no receipt came.
    Dropped(Dropped),
    /// The egress holds neither a receipt nor a drop of the hash.
    Unknown,
}

/// The kept drops.
pub struct Drops {
    capacity: NonZeroUsize,
    /// The hashes of the kept drops, oldest first, each with its count.
    order: VecDeque<(B256, u64)>,
    /// The drop of each hash, with the count it had when the map took it.
    /// A forgotten hash stays in `order` until it is the oldest: the count
    /// tells that entry from a later drop of the same hash.
    by_hash: HashMap<B256, (u64, Dropped)>,
    /// The count of the next drop taken.
    next: u64,
}

impl Drops {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            by_hash: HashMap::new(),
            next: 0,
        }
    }

    /// Keep the drop of `drop.tx_hash`, unless the map holds a drop of the
    /// hash already. The oldest drop goes when the map is full.
    pub fn take(&mut self, drop: &TxDrop) {
        let Entry::Vacant(slot) = self.by_hash.entry(drop.tx_hash) else {
            return;
        };
        let count = self.next;
        slot.insert((
            count,
            Dropped {
                sender: drop.sender,
                nonce: drop.nonce,
                reason: drop.reason,
            },
        ));
        self.order.push_back((drop.tx_hash, count));
        // The count only tells a kept drop from an older entry of the same
        // hash. Two counts 2^64 drops apart never meet in a map of
        // `capacity` entries, so the count wraps.
        self.next = count.wrapping_add(1);
        if self.order.len() > self.capacity.get() {
            self.evict_oldest();
        }
    }

    /// Forget the drop of `tx_hash`: a receipt of the hash came.
    pub fn forget(&mut self, tx_hash: &B256) {
        self.by_hash.remove(tx_hash);
    }

    /// The drop of `tx_hash`, while the map keeps it.
    #[must_use]
    pub fn dropped(&self, tx_hash: &B256) -> Option<Dropped> {
        self.by_hash.get(tx_hash).map(|(_, dropped)| *dropped)
    }

    /// Remove the oldest entry of `order`, and its drop when the drop is
    /// still the one that the entry names.
    fn evict_oldest(&mut self) {
        let named = self.order.pop_front().filter(|(hash, count)| {
            self.by_hash
                .get(hash)
                .is_some_and(|(kept, _)| kept == count)
        });
        if let Some((hash, _)) = named {
            self.by_hash.remove(&hash);
        }
    }
}

#[cfg(test)]
#[path = "drops_tests.rs"]
mod tests;
