//! Verification buffers: a shared bounded, cursor-pruned core + the typed
//! wrappers (BAL by block number, receipts by canonical tx_idx, per-block
//! claim indexes).
//!
//! The buffers are filled by the binary's Aeron subscriber tasks and drained
//! by the (sync) exec/commit threads, blocking briefly for the matching
//! artifact to arrive.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use kardamom_types::{BPosition, BlockDelta, Receipt};

/// Key of a verification buffer, mappable to the monotone index (block number
/// / canonical record index) the catch-up + pruning arithmetic runs on.
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

/// Shared core of [`BalBuffer`] / [`ReceiptBuffer`]: producer task inserts,
/// the (sync) consumer thread `take`s in MONOTONE key order, blocking briefly
/// for the matching artifact. Bounded and cursor-pruned so late/stale
/// artifacts can never leak: an entry the consumer's cursor has already
/// passed is dead weight (no future take will request it).
struct KeyedBuffer<K: BufKey, V> {
    inner: Mutex<KeyedInner<K, V>>,
    cv: Condvar,
    /// Max retained entries. On overflow the OLDEST entry is evicted: the
    /// consumer treats a missing artifact as "could not verify" (never a
    /// false divergence), so eviction can only cost an unverified block/tx.
    cap: usize,
    /// Catch-up skip horizon in index units — see [`take`](Self::take).
    lookbehind: u64,
}

struct KeyedInner<K: BufKey, V> {
    map: BTreeMap<K, V>,
    /// Index of the latest key requested by `take`. Requests are monotone, so
    /// inserts strictly below it are late arrivals for keys the consumer has
    /// already passed (taken, skipped or timed out) and are dropped — the
    /// leak fix for artifacts that land just after their take gave up.
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
        // Late arrival below the consumer's cursor: no future take will ever
        // request it — dropping it here (plus the prune in `take`) keeps the
        // buffer from accreting dead entries for the process lifetime.
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

    /// Take the value for `key`, waiting up to `timeout` for it to arrive.
    /// Returns `None` if it never showed (the caller treats that as "could
    /// not verify", never as divergence).
    fn take(&self, key: K, timeout: Duration) -> Option<V> {
        // DEADLINE semantics, not per-wakeup timeout: inserts for OTHER keys
        // notify_all every block (~250ms-2s under a live chain), and a full
        // fresh timeout per wakeup means a wait for a key that never arrives
        // NEVER times out — the consumer then hangs forever on one lost
        // artifact while the buffer keeps filling (observed as a total
        // validator freeze with a healthy chain).
        let deadline = std::time::Instant::now() + timeout;
        let mut g = self.inner.lock().unwrap();
        // Requests are monotone: everything below `key` has already been
        // resolved (taken / skipped / timed out) and can be pruned; remember
        // the cursor so late re-arrivals are dropped at insert.
        g.cursor = Some(g.cursor.map_or(key.index(), |c| c.max(key.index())));
        g.map = std::mem::take(&mut g.map).split_off(&key);
        loop {
            if let Some(v) = g.map.remove(&key) {
                return Some(v);
            }
            // CATCH-UP: if the live head (highest buffered key) is far ahead
            // of `key`, this artifact has aged out of the live stream's term
            // buffer — it will never arrive. Don't waste the wait window;
            // return None now ("commit unverified") so the validator catches
            // up fast (the cold-start backlog, and a lapse gap larger than
            // the term buffer). Keys still inside the buffered window are
            // taken above; a caught-up validator's requests are near the
            // head, so it never trips this and verifies normally.
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

/// Buffer of executor-published BALs keyed by block number. The Aeron `tx_bal`
/// subscriber task calls [`insert`](Self::insert); the (sync) exec thread calls
/// [`take`](Self::take), blocking briefly for the matching block to arrive.
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
    /// How far below the live head (the highest buffered block) a requested
    /// block must be to count as "unrecoverable backlog": its BAL has aged out
    /// of the live `tx_bal` multicast term buffer and will never arrive, so we
    /// commit it unverified immediately instead of waiting. A caught-up
    /// validator asks for blocks at/near the head (lag < this), so it always
    /// waits and verifies; only a validator catching up from a cold start (or
    /// a long lapse whose gap exceeds the term buffer) skips.
    pub(crate) const BACKLOG_LOOKBEHIND: u64 = 16;
    /// Bound on buffered BALs (whole `BlockDelta`s — the heavyweight buffer).
    /// ~17-35 min of chain at the 250ms-2s block cadence; far beyond the
    /// verify window, so eviction only fires if the consumer stalls outright.
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

    /// Take the BAL for `block`, waiting up to `timeout` for it to arrive.
    /// Returns `None` if it never showed (the caller treats that as "could not
    /// verify", not as divergence). See [`KeyedBuffer::take`] for the deadline
    /// + catch-up semantics.
    pub fn take(&self, block: u64, timeout: Duration) -> Option<BlockDelta> {
        self.core.take(block, timeout)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.core.len()
    }
}

/// Buffer of executor-published receipts keyed by canonical `tx_idx`. Filled by
/// the `tx_receipts` subscriber task; drained by the commit thread.
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
    /// Receipt-path mirror of [`BalBuffer::BACKLOG_LOOKBEHIND`], in canonical
    /// RECORDS rather than blocks: when the highest buffered `tx_idx` is this
    /// far ahead of the requested one, the executor's receipt for the
    /// requested tx has aged out of the live `tx_receipts` stream and will
    /// never arrive — skip immediately ("unverified") instead of blocking the
    /// commit thread for the full receipt window per historical tx (which
    /// capped cold-start catch-up behind a loaded chain at ~0.2 tx/s).
    /// 4096 records ≈ the BAL heuristic's 16 blocks at a few hundred tx/block;
    /// a caught-up validator's requests trail the head by less, so it always
    /// waits and verifies.
    const BACKLOG_LOOKBEHIND: u64 = 4096;
    /// Bound on buffered receipts (small structs; the cap is a leak guard).
    const MAX_BUFFERED: usize = 1 << 16;

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert(&self, receipt: Receipt) {
        self.core.insert(receipt.tx_idx, receipt);
    }

    /// See [`KeyedBuffer::take`] — deadline semantics plus the aged-out
    /// catch-up skip.
    pub fn take(&self, idx: BPosition, timeout: Duration) -> Option<Receipt> {
        self.core.take(idx, timeout)
    }
}

/// Per-block EIP-7928 claim index (parallel validation). Separate from
/// [`BalBuffer`] so the merged-delta cross-check path is untouched: a
/// missing claim index degrades to sequential re-execution, never to a
/// verification gap.
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

    /// Insert a block's claims WITH the granularity the frame declared —
    /// the validator's view of the ladder must come from the wire (what the
    /// executor actually produced), never from local config.
    pub fn insert(&self, block: u64, granularity: u16, claims: crate::parallel::ClaimIndex) {
        self.core.insert(block, (granularity, Arc::new(claims)));
    }

    /// [`insert`](Self::insert) for an already-shared index — the BAL pump
    /// decodes each frame once and feeds BOTH the parallel engine's buffer
    /// and the outbox extractor's without cloning the index.
    pub fn insert_arc(
        &self,
        block: u64,
        granularity: u16,
        claims: Arc<crate::parallel::ClaimIndex>,
    ) {
        self.core.insert(block, (granularity, claims));
    }

    /// Take `block`'s claims, waiting up to `timeout`. `None` ⇒ the caller
    /// re-executes sequentially for this block.
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

    // F10.3: the receipt buffer mirrors the BAL catch-up skip — when the
    // buffered head is far ahead of the requested tx_idx, the receipt has
    // aged out of the live stream and take() must return None IMMEDIATELY
    // instead of blocking the commit thread for the full wait per historical
    // tx (the cold-start crawl).
    #[test]
    fn receipt_take_skips_aged_out_backlog_immediately() {
        let buf = ReceiptBuffer::new();
        buf.insert(receipt(10_000, true, 21_000, 0xab)); // live head, far ahead
        let start = std::time::Instant::now();
        let got = buf.take(BPosition::from_index(0), Duration::from_secs(5));
        assert!(got.is_none(), "aged-out receipt must be skipped");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "skip must not consume the wait window: {:?}",
            start.elapsed()
        );
    }

    // F10.6 / F01.4: an artifact arriving AFTER its take() gave up (skip or
    // timeout) must not leak in the buffer forever — inserts below the
    // consumer's cursor are dropped, and stale entries are pruned as the
    // cursor advances.
    #[test]
    fn late_arrival_below_cursor_does_not_leak() {
        let bals = BalBuffer::new();
        // The consumer asked for block 5 and gave up (nothing buffered).
        assert!(bals.take(5, Duration::from_millis(10)).is_none());
        // BALs for blocks the cursor has passed arrive late: dropped.
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

    // F10.6: the buffer is bounded — a stalled consumer cannot make it hold
    // the entire live stream in RAM; the oldest entry is evicted first (it
    // can only become an "unverified" block, never a false divergence).
    #[test]
    fn buffer_is_bounded_evicting_oldest() {
        let bals = BalBuffer::with_cap(3);
        for b in 1..=5u64 {
            bals.insert(delta(b, 100));
        }
        assert_eq!(bals.len(), 3);
        // 1 and 2 were evicted; 3..=5 retained.
        assert!(bals.take(3, Duration::from_millis(10)).is_some());
        assert!(bals.take(4, Duration::from_millis(10)).is_some());
        assert!(bals.take(5, Duration::from_millis(10)).is_some());
    }

    #[test]
    fn buffers_block_until_value_arrives() {
        // take() must return promptly once a value is inserted from another
        // thread (covers the Aeron-task → exec-thread handoff).
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
