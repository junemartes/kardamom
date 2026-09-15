//! The local layer: the accounts recently written, as seen on the
//! `tx_receipts` batch frames.
//!
//! One writer, the receipt pump, applies rows. Many readers, the RPC
//! tasks, look accounts up. The map is a `DashMap`; the writer keeps the
//! eviction order as its own state, so no lock is held for it.
//!
//! The head is the highest applied batch end position, monotone. A missed
//! frame is not detectable from the receipt stream (positions are not
//! dense, and a marker consumes a slot with no receipt), so the TTL
//! bounds the damage instead: a stale entry expires.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use dashmap::DashMap;
use kardamom_types::{AccountRow, BPosition};

use crate::client::AccountView;
use crate::config::LiveAccountsConfig;

/// One resident account.
struct Entry {
    view: AccountView,
    seen_at: Instant,
}

/// The shared read side. See the module doc.
pub struct LiveAccounts {
    map: DashMap<Address, Entry>,
    /// The highest applied batch end position, as a canonical index.
    head: AtomicU64,
    ttl: Duration,
}

impl LiveAccounts {
    /// The read side and its one writer.
    #[must_use]
    pub fn new(cfg: &LiveAccountsConfig) -> (Arc<Self>, LiveAccountsWriter) {
        let shared = Arc::new(Self {
            map: DashMap::with_capacity(cfg.capacity.get()),
            head: AtomicU64::new(0),
            ttl: cfg.ttl(),
        });
        let writer = LiveAccountsWriter {
            shared: shared.clone(),
            order: VecDeque::new(),
            capacity: cfg.capacity,
        };
        (shared, writer)
    }

    /// The account's latest local state, when it is resident and not
    /// expired.
    #[must_use]
    pub fn get(&self, address: Address) -> Option<AccountView> {
        let entry = self.map.get(&address)?;
        (entry.seen_at.elapsed() <= self.ttl).then(|| entry.view.clone())
    }

    /// The highest applied batch end position, as a canonical index. Zero
    /// before the first batch.
    #[must_use]
    pub fn head(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }

    /// The number of resident entries, expired ones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// The write side: applies a batch's rows and evicts by age and capacity.
pub struct LiveAccountsWriter {
    shared: Arc<LiveAccounts>,
    /// Write order, for eviction. An address repeats once per write.
    order: VecDeque<(Instant, Address)>,
    capacity: NonZeroUsize,
}

impl LiveAccountsWriter {
    /// Apply the rows of a batch that ends at `end`, through the monotone
    /// rule: a row applies only when `end` is beyond the entry's position.
    /// Returns how many rows applied.
    pub fn apply(&mut self, end: BPosition, rows: &[AccountRow]) -> usize {
        let tx_idx = end.as_index();
        let now = Instant::now();
        let applied = rows
            .iter()
            .filter(|row| self.apply_one(row, tx_idx, now))
            .count();
        self.shared.head.fetch_max(tx_idx, Ordering::AcqRel);
        self.evict(now);
        applied
    }

    /// Insert or update one row. Returns whether it applied.
    fn apply_one(&mut self, row: &AccountRow, tx_idx: u64, now: Instant) -> bool {
        let mut entry = self.shared.map.entry(row.address).or_insert_with(|| Entry {
            view: AccountView {
                nonce: row.nonce,
                balance: row.balance,
                tx_idx: 0,
            },
            seen_at: now,
        });
        if entry.view.tx_idx >= tx_idx && entry.view.tx_idx != 0 {
            return false;
        }
        entry.view = AccountView {
            nonce: row.nonce,
            balance: row.balance,
            tx_idx,
        };
        entry.seen_at = now;
        drop(entry);
        self.order.push_back((now, row.address));
        true
    }

    /// Drop expired entries, then the oldest beyond capacity. An address
    /// rewritten since its queue entry keeps its newer value.
    fn evict(&mut self, now: Instant) {
        let ttl = self.shared.ttl;
        while let Some(&(at, address)) = self.order.front()
            && (now.duration_since(at) > ttl || self.order.len() > self.capacity.get())
        {
            self.order.pop_front();
            self.shared
                .map
                .remove_if(&address, |_, entry| entry.seen_at == at);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use alloy_primitives::U256;

    use super::*;

    fn row(byte: u8, nonce: u64) -> AccountRow {
        AccountRow {
            address: Address::repeat_byte(byte),
            nonce,
            balance: U256::from(nonce),
        }
    }

    fn cfg(capacity: usize, ttl_ms: u64) -> LiveAccountsConfig {
        LiveAccountsConfig {
            capacity: NonZeroUsize::new(capacity).unwrap(),
            ttl_ms: NonZeroU64::new(ttl_ms).unwrap(),
        }
    }

    #[test]
    fn rows_insert_update_and_keep_the_newest_position() {
        let (live, mut writer) = LiveAccounts::new(&cfg(16, 60_000));
        assert_eq!(writer.apply(BPosition::from_index(10), &[row(0xaa, 1)]), 1);
        assert_eq!(live.get(Address::repeat_byte(0xaa)).unwrap().nonce, 1);
        // An older batch does not overwrite.
        assert_eq!(writer.apply(BPosition::from_index(5), &[row(0xaa, 9)]), 0);
        assert_eq!(live.get(Address::repeat_byte(0xaa)).unwrap().nonce, 1);
        // A newer one does, and the head is monotone.
        assert_eq!(writer.apply(BPosition::from_index(20), &[row(0xaa, 2)]), 1);
        assert_eq!(live.get(Address::repeat_byte(0xaa)).unwrap().nonce, 2);
        assert_eq!(live.head(), 20);
        writer.apply(BPosition::from_index(15), &[]);
        assert_eq!(live.head(), 20);
    }

    #[test]
    fn capacity_evicts_the_oldest_and_a_rewrite_survives() {
        let (live, mut writer) = LiveAccounts::new(&cfg(2, 60_000));
        writer.apply(BPosition::from_index(1), &[row(0x01, 1)]);
        writer.apply(BPosition::from_index(2), &[row(0x02, 1)]);
        writer.apply(BPosition::from_index(3), &[row(0x01, 2)]);
        writer.apply(BPosition::from_index(4), &[row(0x03, 1)]);
        assert!(live.get(Address::repeat_byte(0x02)).is_none(), "oldest out");
        assert_eq!(live.get(Address::repeat_byte(0x01)).unwrap().nonce, 2);
        assert!(live.len() <= 2);
    }

    #[test]
    fn ttl_expires_an_entry() {
        let (live, mut writer) = LiveAccounts::new(&cfg(16, 1));
        writer.apply(BPosition::from_index(1), &[row(0xaa, 1)]);
        std::thread::sleep(Duration::from_millis(5));
        assert!(live.get(Address::repeat_byte(0xaa)).is_none());
        // The next apply sweeps it out of the map too.
        writer.apply(BPosition::from_index(2), &[row(0xbb, 1)]);
        assert_eq!(live.len(), 1);
    }
}
