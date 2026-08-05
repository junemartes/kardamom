//! Validator node core: cross-check the local re-execution against the
//! sequencer's published artifacts, fail-stop on divergence.
//!
//! A validator reuses the whole [`kardamom_engine`] pipeline — the same reader/
//! join topology and `execute_tx` core the executor runs — but wires two
//! role-specific seams instead of publishing receipts:
//!
//! - [`ValidatorWriterQueue`] wraps the trie-aware state writer's
//!   [`StateWriterQueue`]. At each block close it receives the locally-computed
//!   [`BlockDelta`] (`submit(boundary, delta)`), cross-checks its **write-set**
//!   against the executor's per-block **BAL** (subscribed on `tx_bal`), and
//!   forwards the delta to the trie-aware writer (which advances the MPT state
//!   root). A write-set mismatch is a proven execution divergence → fail-stop.
//! - [`ValidatorReceiptSink`] implements [`TxReceiptsPublication`]. It does not
//!   publish anything; instead it cross-checks each locally-recomputed receipt
//!   against the executor's published receipt (subscribed on `tx_receipts`) for
//!   the same `tx_idx`. A receipt mismatch is also fail-stop.
//!
//! Both seams are the *existing* engine trait seams — no engine change is
//! needed. The buffers ([`BalBuffer`], [`ReceiptBuffer`]) are filled by the
//! binary's Aeron subscriber tasks and drained by the (sync) exec/commit
//! threads, blocking briefly for the matching artifact to arrive.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use kardamom_engine::{CMessage, ExecutorError, StateWriterQueue, TxReceiptsPublication};
use kardamom_types::{BPosition, BlockBoundary, BlockDelta, Receipt};

/// L1 output attester: collects `MessagePassed` leaves from re-executed
/// blocks, builds the per-output withdrawals root, posts to the L1 oracle.
pub mod flight;
pub mod parallel;

pub mod attester;
pub mod epoch_verify;
pub mod metrics;

/// Shared divergence flag. Once tripped, the validator has observed a proven
/// discrepancy between its independent re-execution and the sequencer's output;
/// the surrounding seams return an error so the engine pipeline halts.
#[derive(Debug, Default)]
pub struct Divergence {
    halted: AtomicBool,
    reason: Mutex<Option<String>>,
}

impl Divergence {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a divergence (idempotent — the first reason wins), bump the metric.
    pub fn record(&self, reason: impl Into<String>) {
        if !self.halted.swap(true, Ordering::SeqCst) {
            let reason = reason.into();
            tracing::error!(reason = %reason, "validator divergence detected — halting");
            *self.reason.lock().unwrap() = Some(reason);
            metrics::counter_divergence();
        }
    }

    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().unwrap().clone()
    }
}

/// True when two block deltas have an identical **write-set** (accounts,
/// storage, code). Receipts are cross-checked separately via `tx_receipts`, so
/// they are intentionally excluded here. Both the validator and the executor
/// build these vecs from sorted maps, so equality is order-stable.
pub fn write_set_eq(a: &BlockDelta, b: &BlockDelta) -> bool {
    a.accounts == b.accounts && a.storage == b.storage && a.code == b.code
}

/// A short human summary of the first write-set field that differs.
fn write_set_diff_summary(local: &BlockDelta, bal: &BlockDelta) -> String {
    if local.accounts != bal.accounts {
        format!(
            "accounts differ (local {} vs bal {})",
            local.accounts.len(),
            bal.accounts.len()
        )
    } else if local.storage != bal.storage {
        format!(
            "storage differs (local {} vs bal {})",
            local.storage.len(),
            bal.storage.len()
        )
    } else {
        format!(
            "code differs (local {} vs bal {})",
            local.code.len(),
            bal.code.len()
        )
    }
}

// ---------------------------------------------------------------------------
// Verification buffers: a shared bounded, cursor-pruned core + the two typed
// wrappers (BAL by block number, receipts by canonical tx_idx).
// ---------------------------------------------------------------------------

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

/// Returns `true` when the two receipts agree on the execution-output fields:
/// success status, gas used, the write-set hash (the per-tx determinism
/// witness), and the emitted **logs**. Logs are execution output carried on
/// the wire and consumed downstream — `write_set_hash` covers state writes
/// but not events, so a log-only divergence would otherwise pass silently.
///
/// Deliberately OUT of scope: the RPC enrichment fields (`nonce`, `from`,
/// `to`, `contract_address`, `effective_gas_price`, `block_number`,
/// `transaction_index`, `cumulative_gas_used`). They are derived
/// deterministically from inputs this check already covers (the envelope, the
/// canonical order, and the per-block `gas_used` sums), so a divergence there
/// implies a divergence in a checked field — comparing them would only
/// re-verify arithmetic, not execution.
pub fn receipt_consistent(local: &Receipt, published: &Receipt) -> bool {
    local.status == published.status
        && local.gas_used == published.gas_used
        && local.write_set_hash == published.write_set_hash
        && local.logs == published.logs
}

// ---------------------------------------------------------------------------
// ValidatorWriterQueue: BAL cross-check + forward to the trie-aware writer.
// ---------------------------------------------------------------------------

/// How long the exec thread waits for a block's BAL before giving up the check.
pub const BAL_WAIT: Duration = Duration::from_secs(5);
/// How long the commit thread waits for a tx's published receipt before skipping.
pub const RECEIPT_WAIT: Duration = Duration::from_secs(5);

/// Wraps the trie-aware [`StateWriterQueue`]: cross-checks each block's
/// write-set against the executor's BAL, then forwards the delta to the writer
/// (which advances the MPT state root). Fail-stops on a write-set mismatch.
pub struct ValidatorWriterQueue<Q: StateWriterQueue> {
    inner: Q,
    bals: Arc<BalBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// Highest block already submitted THIS process lifetime. A cluster
    /// SESSION replay (lapse + reconnect, no restart) re-delivers blocks
    /// the validator already executed; re-execution against
    /// already-applied state yields empty deltas that cannot match the
    /// BAL — same false-divergence class as the restart cascade, session
    /// flavor.
    high_water: u64,
    /// Blocks at or below this were DURABLY VERIFIED before a restart.
    /// Crash-recovery replays them for state reconstruction, but
    /// re-execution against already-applied state yields EMPTY deltas
    /// (every tx skips as nonce-too-low) — comparing those against
    /// retained BAL frames produced FALSE divergences that cascaded:
    /// each fail-stop restart re-entered replay and diverged again,
    /// turning any transient stall into a permanent restart loop.
    verify_floor: u64,
}

impl<Q: StateWriterQueue> ValidatorWriterQueue<Q> {
    pub fn new(inner: Q, bals: Arc<BalBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            inner,
            bals,
            divergence,
            wait: BAL_WAIT,
            verify_floor: 0,
            high_water: 0,
        }
    }

    /// Skip BAL verification for blocks at or below `floor` (the recovery
    /// resume point): they were verified before the restart, and replay
    /// re-execution against already-applied state legitimately produces
    /// deltas that cannot match the BAL.
    #[must_use]
    pub fn with_verify_floor(mut self, floor: u64) -> Self {
        self.verify_floor = floor;
        self
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

impl<Q: StateWriterQueue> StateWriterQueue for ValidatorWriterQueue<Q> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        if block.block_number <= self.verify_floor || block.block_number <= self.high_water {
            tracing::debug!(
                block = block.block_number,
                floor = self.verify_floor,
                high_water = self.high_water,
                "replay overlap; BAL verification skipped (already verified)"
            );
            return self.inner.submit(block, delta);
        }
        self.high_water = block.block_number;
        match self.bals.take(block.block_number, self.wait) {
            Some(bal) => {
                if write_set_eq(&delta, &bal) {
                    metrics::counter_block_verified();
                } else {
                    let summary = write_set_diff_summary(&delta, &bal);
                    let reason =
                        format!("block {} write-set != BAL: {summary}", block.block_number);
                    self.divergence.record(reason.clone());
                    // Divergence (not State): fatal + non-retryable, so no
                    // engine retry loop can absorb it (see F10.1).
                    return Err(ExecutorError::Divergence(reason));
                }
            }
            None => {
                // Could not verify this block (BAL never arrived). Not a proven
                // divergence — log + count, then keep following.
                tracing::warn!(
                    block = block.block_number,
                    "no BAL received within timeout; block left unverified"
                );
                metrics::counter_bal_missing();
            }
        }
        // Forward to the trie-aware writer (advances the MPT state root).
        self.inner.submit(block, delta)
    }
}

// ---------------------------------------------------------------------------
// ValidatorReceiptSink: tx_receipts cross-check; never publishes.
// ---------------------------------------------------------------------------

/// Per-block EIP-7928 claim index (parallel validation). Separate from
/// [`BalBuffer`] so the merged-delta cross-check path is untouched: a
/// missing claim index degrades to sequential re-execution, never to a
/// verification gap.
pub struct ClaimBuffer {
    core: KeyedBuffer<u64, (u16, std::sync::Arc<crate::parallel::ClaimIndex>)>,
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
        self.core
            .insert(block, (granularity, std::sync::Arc::new(claims)));
    }

    /// Take `block`'s claims, waiting up to `timeout`. `None` ⇒ the caller
    /// re-executes sequentially for this block.
    pub fn take(
        &self,
        block: u64,
        timeout: Duration,
    ) -> Option<(u16, std::sync::Arc<crate::parallel::ClaimIndex>)> {
        self.core.take(block, timeout)
    }
}

/// Implements [`TxReceiptsPublication`] but verifies instead of publishing: each
/// locally-recomputed receipt is checked against the executor's published
/// receipt for the same `tx_idx`. Fail-stops on a mismatch.
pub struct ValidatorReceiptSink {
    receipts: Arc<ReceiptBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// Recent-block input ring for the receipt-divergence dump — the
    /// mismatch fires after the block's records/claims are gone, so without
    /// this the F3-era wsh incident left one log line and nothing to replay.
    flight: Option<Arc<crate::flight::FlightRing>>,
}

impl ValidatorReceiptSink {
    pub fn new(receipts: Arc<ReceiptBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            receipts,
            divergence,
            wait: RECEIPT_WAIT,
            flight: None,
        }
    }

    /// Attach the flight ring (dump block inputs on a receipt mismatch).
    #[must_use]
    pub fn with_flight(mut self, flight: Arc<crate::flight::FlightRing>) -> Self {
        self.flight = Some(flight);
        self
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

impl TxReceiptsPublication for ValidatorReceiptSink {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        // LATCH: once a divergence is proven the sink keeps failing. The
        // first failing publish consumed the published receipt from the
        // buffer, so without this a caller that retries (the engine's
        // must-deliver loop is belt-and-braces here — it no longer retries
        // Divergence) would find an empty buffer, land in the "unverified"
        // arm and quietly resume committing past a proven mismatch.
        if self.divergence.is_halted() {
            return Err(ExecutorError::Divergence(
                self.divergence
                    .reason()
                    .unwrap_or_else(|| "validator halted on divergence".into()),
            ));
        }
        match msg {
            CMessage::Receipt(local) => match self.receipts.take(local.tx_idx, self.wait) {
                Some(published) => {
                    if receipt_consistent(&local, &published) {
                        Ok(())
                    } else {
                        // Identify the tx, not just the mismatch: #159 took a
                        // multi-day forensic hash inversion to attribute
                        // because this line once carried only status/gas/wsh.
                        let reason = format!(
                            "receipt mismatch at tx_idx {:?}: local(status={}, gas={}, wsh={}, \
                             logs={}) vs published(status={}, gas={}, wsh={}, logs={}) \
                             [tx_hash={} from={} to={:?} block={} tx_index={}]",
                            local.tx_idx,
                            local.status,
                            local.gas_used,
                            local.write_set_hash,
                            local.logs.len(),
                            published.status,
                            published.gas_used,
                            published.write_set_hash,
                            published.logs.len(),
                            local.tx_hash,
                            local.from,
                            local.to,
                            local.block_number,
                            local.transaction_index,
                        );
                        // FLIGHT RECORDER first (best-effort): the fail-stop
                        // is permanent, so this is the only chance to
                        // capture the block inputs behind the mismatch.
                        if let Some(f) = self.flight.as_ref() {
                            f.dump_receipt_divergence(&local, &published);
                        }
                        self.divergence.record(reason.clone());
                        Err(ExecutorError::Divergence(reason))
                    }
                }
                None => {
                    // No published receipt to compare against — cannot verify.
                    metrics::counter_receipt_missing();
                    Ok(())
                }
            },
            // Block boundaries carry no per-tx info to verify; nothing to do.
            CMessage::BlockBoundary(_) => Ok(()),
        }
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

    fn boundary(block: u64) -> BlockBoundary {
        BlockBoundary {
            block_number: block,
            end_tx_idx: BPosition::from_index(block),
            l2_timestamp: 1_700_000_000 + block,
            l1_origin: 0,
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

    /// Recording fake inner writer queue.
    #[derive(Default)]
    struct RecordingQueue {
        submitted: Arc<Mutex<Vec<u64>>>,
    }
    impl StateWriterQueue for RecordingQueue {
        fn submit(
            &mut self,
            block: BlockBoundary,
            _delta: BlockDelta,
        ) -> Result<(), ExecutorError> {
            self.submitted.lock().unwrap().push(block.block_number);
            Ok(())
        }
    }

    #[test]
    fn matching_bal_forwards_and_does_not_diverge() {
        let bals = BalBuffer::new();
        let div = Divergence::new();
        let submitted = Arc::new(Mutex::new(Vec::new()));
        let inner = RecordingQueue {
            submitted: submitted.clone(),
        };
        let mut q = ValidatorWriterQueue::new(inner, bals.clone(), div.clone());

        bals.insert(delta(1, 100));
        q.submit(boundary(1), delta(1, 100)).unwrap();

        assert!(!div.is_halted());
        assert_eq!(*submitted.lock().unwrap(), vec![1]);
    }

    #[test]
    fn mismatched_bal_fail_stops() {
        let bals = BalBuffer::new();
        let div = Divergence::new();
        let inner = RecordingQueue::default();
        let mut q = ValidatorWriterQueue::new(inner, bals.clone(), div.clone());

        bals.insert(delta(1, 100)); // BAL says balance 100
        let err = q.submit(boundary(1), delta(1, 999)).unwrap_err(); // local says 999

        assert!(matches!(err, ExecutorError::Divergence(_)));
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("write-set != BAL"));
    }

    #[test]
    fn missing_bal_does_not_diverge() {
        let bals = BalBuffer::new();
        let div = Divergence::new();
        let submitted = Arc::new(Mutex::new(Vec::new()));
        let inner = RecordingQueue {
            submitted: submitted.clone(),
        };
        let mut q = ValidatorWriterQueue::new(inner, bals.clone(), div.clone())
            .with_wait(Duration::from_millis(50));

        // No BAL inserted: submit must still forward and NOT flag divergence.
        q.submit(boundary(1), delta(1, 100)).unwrap();
        assert!(!div.is_halted());
        assert_eq!(*submitted.lock().unwrap(), vec![1]);
    }

    #[test]
    fn consistent_receipt_passes_inconsistent_fails() {
        let buf = ReceiptBuffer::new();
        let div = Divergence::new();
        let mut sink = ValidatorReceiptSink::new(buf.clone(), div.clone())
            .with_wait(Duration::from_millis(50));

        buf.insert(receipt(0, true, 21_000, 0xab));
        // Same execution-correctness fields → passes.
        sink.publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
            .unwrap();
        assert!(!div.is_halted());

        // Divergent write_set_hash → fail-stop.
        buf.insert(receipt(1, true, 21_000, 0xab));
        let err = sink
            .publish(CMessage::Receipt(receipt(1, true, 21_000, 0xff)))
            .unwrap_err();
        assert!(matches!(err, ExecutorError::Divergence(_)));
        assert!(div.is_halted());

        // F10.1 regression: a RETRY of the same publish (the engine's
        // must-deliver loop) finds the buffer empty — it must KEEP failing
        // via the divergence latch, not slide into the "unverified" Ok arm.
        let err2 = sink
            .publish(CMessage::Receipt(receipt(1, true, 21_000, 0xff)))
            .unwrap_err();
        assert!(matches!(err2, ExecutorError::Divergence(_)));
    }

    /// A receipt mismatch with the flight ring attached must leave a
    /// replayable artifact: both receipts + the ring's recent block inputs.
    /// The F3-era wsh incident left ONE LOG LINE — this is the regression
    /// test for "never again undiagnosable".
    #[test]
    fn receipt_mismatch_dumps_flight_ring() {
        use kardamom_engine::actor::BufferedRecord;
        use kardamom_engine::exec_types::TxIndex;

        let dir = std::env::temp_dir().join(format!("kardamom-flight-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // SAFETY: test-local env var; tests in this file don't race on it.
        unsafe { std::env::set_var("KARDAMOM_FLIGHT_DIR", &dir) };

        let ring = crate::flight::FlightRing::new();
        ring.push(
            7,
            20,
            &[BufferedRecord::Tx {
                tx_idx: TxIndex(0),
                position: BPosition::from_index(0),
                envelope: kardamom_types::TxEnvelope {
                    correlation_id: 1,
                    raw_tx: vec![0xde, 0xad].into(),
                    sender: Address::from([0x11; 20]),
                    tx_hash: B256::from([0x22; 32]),
                },
            }],
            None,
        );

        let buf = ReceiptBuffer::new();
        let div = Divergence::new();
        let mut sink = ValidatorReceiptSink::new(buf.clone(), div.clone())
            .with_wait(Duration::from_millis(50))
            .with_flight(ring);

        buf.insert(receipt(3, true, 21_000, 0xab));
        let err = sink
            .publish(CMessage::Receipt(receipt(3, true, 21_000, 0xff)))
            .unwrap_err();
        assert!(matches!(err, ExecutorError::Divergence(_)));

        let dump = dir.join("receipt-divergence-0-3.json");
        let body = std::fs::read_to_string(&dump).expect("dump file must exist");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        // Both receipts, field-level.
        assert_eq!(
            v["local"]["write_set_hash"],
            format!("{:?}", B256::from([0xff; 32]))
        );
        assert_eq!(
            v["published"]["write_set_hash"],
            format!("{:?}", B256::from([0xab; 32]))
        );
        // The ring's block inputs are replayable.
        assert_eq!(v["ring"][0]["block"], 7);
        assert_eq!(v["ring"][0]["granularity"], 20);
        assert_eq!(v["ring"][0]["records"][0]["kind"], "tx");
        assert_eq!(v["ring"][0]["records"][0]["raw"], "dead");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // F10.5: a log-only divergence (same status/gas/write-set hash) must trip
    // the cross-check — logs are published execution output, not enrichment.
    #[test]
    fn log_only_divergence_fail_stops() {
        use kardamom_types::WireLog;
        let buf = ReceiptBuffer::new();
        let div = Divergence::new();
        let mut sink = ValidatorReceiptSink::new(buf.clone(), div.clone())
            .with_wait(Duration::from_millis(50));

        let log = |topic: u8| WireLog {
            address: Address::from([0x22; 20]),
            topics: vec![B256::repeat_byte(topic)],
            data: Default::default(),
        };
        let mut published = receipt(0, true, 21_000, 0xab);
        published.logs = vec![log(0x01)];
        let mut local = receipt(0, true, 21_000, 0xab);
        local.logs = vec![log(0x02)];

        buf.insert(published);
        let err = sink.publish(CMessage::Receipt(local)).unwrap_err();
        assert!(matches!(err, ExecutorError::Divergence(_)));
        assert!(div.is_halted());
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
