//! Verification buffers: a shared, bounded, cursor-pruned core, plus typed
//! wrappers for the BAL (by block number), receipts (by canonical tx_idx),
//! and per-block claim indexes.
//!
//! The binary's Aeron subscriber tasks fill the buffers. The sync exec and
//! commit threads drain them, and wait briefly for the matching data to
//! arrive.
//!
//! # Why a `Condvar`, not a tokio channel
//!
//! The async/sync seam elsewhere in the validator uses tokio primitives
//! (`tokio::sync::mpsc`, `CancellationToken`). This buffer keeps a
//! `Mutex` + `Condvar` on purpose:
//!
//! - The consumer waits on a KEY with a DEADLINE, not on the next item.
//!   A channel is FIFO; the keyed map would still have to exist beside it,
//!   and `tokio::sync::mpsc` has no `recv_timeout` for the sync side.
//! - The wait lives entirely on the std thread. The async producer only
//!   takes the mutex for a short, await-free critical section and calls
//!   `notify_all`, which never blocks. So no tokio task ever parks on a
//!   std primitive, and the exec thread needs no runtime handle.
//! - `Condvar::wait_timeout` gives the deadline semantics `take` depends on
//!   (see the comment in [`KeyedBuffer::take`]) with one primitive.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use kardamom_types::{BPosition, BlockDelta, Receipt};

/// Key of a verification buffer. It maps to the increasing index (block
/// number or canonical record index) that the catch-up and pruning logic
/// uses.
trait BufKey: Ord + Copy {
    fn index(self) -> u64;
}
impl BufKey for u64 {
    fn index(self) -> u64 {
        self
    }
}
impl BufKey for BPosition {
    fn index(self) -> u64 {
        self.as_index()
    }
}

/// Shared core of [`BalBuffer`] and [`ReceiptBuffer`]. The producer task
/// inserts values. The sync consumer thread calls `take` in increasing key
/// order, and waits briefly for matching data. The buffer is bounded and
/// cursor-pruned, so late or stale data can never leak: an entry the
/// consumer's cursor has already passed will never be requested again.
struct KeyedBuffer<K: BufKey, V> {
    inner: Mutex<KeyedInner<K, V>>,
    cv: Condvar,
    /// Max retained entries. On overflow, the oldest entry is evicted. The
    /// consumer treats missing data as "could not verify", never as a
    /// divergence, so eviction can only leave a block or tx unverified.
    cap: usize,
    /// Catch-up skip horizon, in index units. See [`take`](Self::take).
    lookbehind: u64,
}

struct KeyedInner<K: BufKey, V> {
    map: BTreeMap<K, V>,
    /// Index of the latest key requested by `take`. Requests only increase,
    /// so an insert strictly below this index is a late arrival for a key
    /// the consumer already handled (took, skipped, or timed out). The
    /// buffer drops it. This fixes a leak from data that lands just after
    /// its take gave up.
    cursor: Option<u64>,
}

impl<K: BufKey, V> KeyedBuffer<K, V> {
    fn new(cap: usize, lookbehind: u64) -> Self {
        Self {
            inner: Mutex::new(KeyedInner {
                map: BTreeMap::new(),
                cursor: None,
            }),
            cv: Condvar::new(),
            cap,
            lookbehind,
        }
    }

    fn insert(&self, key: K, value: V) {
        let mut g = self.inner.lock().unwrap();
        // This is a late arrival below the consumer's cursor: no future
        // take will request it. Dropping it here, plus the prune in
        // `take`, stops the buffer from growing with dead entries.
        if g.cursor.is_some_and(|c| key.index() < c) {
            return;
        }
        g.map.insert(key, value);
        while g.map.len() > self.cap {
            g.map.pop_first();
        }
        drop(g);
        self.cv.notify_all();
    }

    /// Take the value for `key`, and wait up to `timeout` for it to arrive.
    /// Returns `None` if it never arrives. The caller treats that as "could
    /// not verify", never as a divergence.
    fn take(&self, key: K, timeout: Duration) -> Option<V> {
        // Use a deadline, not a fresh timeout per wakeup. Inserts for other
        // keys call notify_all on every block (about 250ms to 2s on a live
        // chain). A fresh timeout per wakeup would mean a wait for a key
        // that never arrives never times out, and the consumer hangs
        // forever on one lost item while the buffer keeps filling.
        let deadline = std::time::Instant::now() + timeout;
        let mut g = self.inner.lock().unwrap();
        // Requests only increase: everything below `key` is already
        // resolved (taken, skipped, or timed out) and can be pruned.
        // Remember the cursor so late re-arrivals are dropped at insert.
        g.cursor = Some(g.cursor.map_or(key.index(), |c| c.max(key.index())));
        g.map = std::mem::take(&mut g.map).split_off(&key);
        loop {
            if let Some(v) = g.map.remove(&key) {
                return Some(v);
            }
            // Catch-up check: if the live head (the highest buffered key)
            // is far ahead of `key`, this item has aged out of the live
            // stream's buffer and will never arrive. Return None now
            // instead of waiting out the timeout, so the validator catches
            // up fast after a cold start or a lapse longer than the live
            // buffer. A caught-up validator asks for keys near the head, so
            // this check never triggers and verification runs as normal.
            if let Some((&head, _)) = g.map.last_key_value()
                && head.index() > key.index() + self.lookbehind
            {
                return None;
            }
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return g.map.remove(&key);
            };
            let (g2, wait) = self.cv.wait_timeout(g, remaining).unwrap();
            g = g2;
            if wait.timed_out() {
                return g.map.remove(&key);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }
}

/// Buffer of executor-published BALs, keyed by block number. The Aeron
/// `tx_bal` subscriber task calls [`insert`](Self::insert). The sync exec
/// thread calls [`take`](Self::take), and waits briefly for the matching
/// block.
pub struct BalBuffer {
    core: KeyedBuffer<u64, BlockDelta>,
}

impl Default for BalBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(Self::MAX_BUFFERED, Self::BACKLOG_LOOKBEHIND),
        }
    }
}

impl BalBuffer {
    /// How far below the live head (the highest buffered block) a
    /// requested block must be to count as unrecoverable backlog. Its BAL
    /// has aged out of the live `tx_bal` multicast buffer and will never
    /// arrive, so the validator commits it unverified at once instead of
    /// waiting. A caught-up validator asks for blocks near the head (a
    /// smaller lag than this value), so it always waits and verifies.
    /// Only a validator catching up from a cold start, or after a lapse
    /// longer than the multicast buffer, skips the wait.
    pub(crate) const BACKLOG_LOOKBEHIND: u64 = 16;
    /// Bound on buffered BALs (whole `BlockDelta` values, the heavyweight
    /// case). This is about 17 to 35 minutes of chain at a 250ms-to-2s
    /// block rate, well beyond the verify window, so eviction fires only
    /// if the consumer stalls outright.
    pub(crate) const MAX_BUFFERED: usize = 1024;

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[cfg(test)]
    fn with_cap(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            core: KeyedBuffer::new(cap, Self::BACKLOG_LOOKBEHIND),
        })
    }

    pub fn insert(&self, delta: BlockDelta) {
        self.core.insert(delta.block_number, delta);
    }

    /// Take the BAL for `block`, and wait up to `timeout` for it to arrive.
    /// Returns `None` if it never arrives; the caller treats that as
    /// "could not verify", not as a divergence. See [`KeyedBuffer::take`]
    /// for the deadline and catch-up rules.
    pub fn take(&self, block: u64, timeout: Duration) -> Option<BlockDelta> {
        self.core.take(block, timeout)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.core.len()
    }
}

/// Buffer of executor-published receipts, keyed by canonical `tx_idx`. The
/// `tx_receipts` subscriber task fills it; the commit thread drains it.
pub struct ReceiptBuffer {
    core: KeyedBuffer<BPosition, Receipt>,
}

impl Default for ReceiptBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(Self::MAX_BUFFERED, Self::BACKLOG_LOOKBEHIND),
        }
    }
}

impl ReceiptBuffer {
    /// This mirrors [`BalBuffer::BACKLOG_LOOKBEHIND`], but in canonical
    /// records rather than blocks. When the highest buffered `tx_idx` is
    /// this far ahead of the requested one, the executor's receipt for the
    /// requested tx has aged out of the live `tx_receipts` stream and will
    /// never arrive. The buffer skips it at once, marked unverified,
    /// instead of blocking the commit thread for the full wait per
    /// historical tx. 4096 records is about the same reach as the BAL
    /// heuristic's 16 blocks, at a few hundred tx per block. A caught-up
    /// validator's requests trail the head by less than this, so it always
    /// waits and verifies.
    const BACKLOG_LOOKBEHIND: u64 = 4096;
    /// Bound on buffered receipts. Receipts are small structs; this cap is
    /// only a leak guard.
    const MAX_BUFFERED: usize = 1 << 16;

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert(&self, receipt: Receipt) {
        self.core.insert(receipt.tx_idx, receipt);
    }

    /// See [`KeyedBuffer::take`] for the deadline rules and the aged-out
    /// catch-up skip.
    pub fn take(&self, idx: BPosition, timeout: Duration) -> Option<Receipt> {
        self.core.take(idx, timeout)
    }
}

/// Per-block EIP-7928 claim index for parallel validation. This is separate
/// from [`BalBuffer`], so the merged-delta check path stays untouched. A
/// missing claim index falls back to sequential re-execution, never to a
/// gap in verification.
pub struct ClaimBuffer {
    core: KeyedBuffer<u64, (u16, Arc<crate::parallel::ClaimIndex>)>,
}

impl Default for ClaimBuffer {
    fn default() -> Self {
        Self {
            core: KeyedBuffer::new(BalBuffer::MAX_BUFFERED, BalBuffer::BACKLOG_LOOKBEHIND),
        }
    }
}

impl ClaimBuffer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Insert a block's claims with the granularity the frame declared. The
    /// validator's view of the ladder must come from the wire, from what
    /// the executor actually produced, never from local config.
    pub fn insert(&self, block: u64, granularity: u16, claims: crate::parallel::ClaimIndex) {
        self.core.insert(block, (granularity, Arc::new(claims)));
    }

    /// Take `block`'s claims, and wait up to `timeout`. On `None`, the
    /// caller re-executes this block sequentially.
    pub fn take(
        &self,
        block: u64,
        timeout: Duration,
    ) -> Option<(u16, Arc<crate::parallel::ClaimIndex>)> {
        self.core.take(block, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_types::{AccountChange, StorageChange};

    fn delta(block: u64, bal_val: u64) -> BlockDelta {
        BlockDelta {
            block_number: block,
            accounts: vec![AccountChange {
                address: Address::from([0x11; 20]),
                nonce: 1,
                balance: U256::from(bal_val),
                code_hash: B256::ZERO,
            }],
            storage: vec![StorageChange {
                address: Address::from([0x11; 20]),
                key: B256::from(U256::from(1u64)),
                value: U256::from(7u64),
            }],
            code: vec![],
            receipts: vec![],
        }
    }

    fn receipt(idx: u64, status: bool, gas: u64, wsh: u8) -> Receipt {
        Receipt {
            tx_idx: BPosition::from_index(idx),
            status,
            gas_used: gas,
            write_set_hash: B256::from([wsh; 32]),
            ..Default::default()
        }
    }

    // The receipt buffer mirrors the BAL catch-up skip. When the buffered
    // head is far ahead of the requested tx_idx, the receipt has aged out
    // of the live stream, and take() must return None at once instead of
    // blocking the commit thread for the full wait per historical tx.
    #[test]
    fn receipt_take_skips_aged_out_backlog_immediately() {
        let buf = ReceiptBuffer::new();
        buf.insert(receipt(10_000, true, 21_000, 0xab)); // This is the live head, far ahead.
        let start = std::time::Instant::now();
        let got = buf.take(BPosition::from_index(0), Duration::from_secs(5));
        assert!(got.is_none(), "aged-out receipt must be skipped");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "skip must not consume the wait window: {:?}",
            start.elapsed()
        );
    }

    // Data that arrives after its take() gave up, by a skip or a timeout,
    // must not leak in the buffer forever. Inserts below the consumer's
    // cursor are dropped, and stale entries are pruned as the cursor moves.
    #[test]
    fn late_arrival_below_cursor_does_not_leak() {
        let bals = BalBuffer::new();
        // The consumer asked for block 5 and gave up; nothing is buffered.
        assert!(bals.take(5, Duration::from_millis(10)).is_none());
        // BALs for blocks the cursor has passed arrive late, so they are dropped.
        bals.insert(delta(3, 100));
        bals.insert(delta(4, 100));
        assert_eq!(bals.len(), 0, "late below-cursor inserts must be dropped");
        // An in-window insert still works.
        bals.insert(delta(6, 100));
        assert_eq!(bals.len(), 1);
        assert!(bals.take(6, Duration::from_millis(10)).is_some());
        // Entries below a later request are pruned by the take itself.
        bals.insert(delta(7, 100));
        assert!(bals.take(9, Duration::from_millis(10)).is_none());
        assert_eq!(bals.len(), 0, "stale entry below the cursor must be pruned");
    }

    // The buffer is bounded, so a stalled consumer cannot make it hold the
    // whole live stream in RAM. The oldest entry is evicted first, which can
    // only leave a block unverified, never cause a false divergence.
    #[test]
    fn buffer_is_bounded_evicting_oldest() {
        let bals = BalBuffer::with_cap(3);
        for b in 1..=5u64 {
            bals.insert(delta(b, 100));
        }
        assert_eq!(bals.len(), 3);
        // Blocks 1 and 2 are evicted; blocks 3 through 5 are kept.
        assert!(bals.take(3, Duration::from_millis(10)).is_some());
        assert!(bals.take(4, Duration::from_millis(10)).is_some());
        assert!(bals.take(5, Duration::from_millis(10)).is_some());
    }

    #[test]
    fn buffers_block_until_value_arrives() {
        // take() must return promptly once another thread inserts a value.
        // This covers the handoff from the Aeron task to the exec thread.
        let bals = BalBuffer::new();
        let bals2 = bals.clone();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            bals2.insert(delta(7, 5));
        });
        let got = bals.take(7, Duration::from_secs(2)).expect("delta arrives");
        assert_eq!(got.block_number, 7);
        h.join().unwrap();
    }
}
