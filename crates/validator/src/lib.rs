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
pub mod attester;
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
// BAL buffer: block_number -> executor BlockDelta, filled by the tx_bal task.
// ---------------------------------------------------------------------------

/// Buffer of executor-published BALs keyed by block number. The Aeron `tx_bal`
/// subscriber task calls [`insert`](Self::insert); the (sync) exec thread calls
/// [`take`](Self::take), blocking briefly for the matching block to arrive.
#[derive(Default)]
pub struct BalBuffer {
    inner: Mutex<BTreeMap<u64, BlockDelta>>,
    cv: Condvar,
}

impl BalBuffer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert(&self, delta: BlockDelta) {
        let block = delta.block_number;
        self.inner.lock().unwrap().insert(block, delta);
        self.cv.notify_all();
    }

    /// Take the BAL for `block`, waiting up to `timeout` for it to arrive.
    /// Returns `None` if it never showed (the caller treats that as "could not
    /// verify", not as divergence).
    pub fn take(&self, block: u64, timeout: Duration) -> Option<BlockDelta> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(d) = guard.remove(&block) {
                return Some(d);
            }
            let (g, wait) = self.cv.wait_timeout(guard, timeout).unwrap();
            guard = g;
            if wait.timed_out() {
                return guard.remove(&block);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Receipt buffer: tx_idx -> executor Receipt, filled by the tx_receipts task.
// ---------------------------------------------------------------------------

/// Buffer of executor-published receipts keyed by canonical `tx_idx`. Filled by
/// the `tx_receipts` subscriber task; drained by the commit thread.
#[derive(Default)]
pub struct ReceiptBuffer {
    inner: Mutex<BTreeMap<BPosition, Receipt>>,
    cv: Condvar,
}

impl ReceiptBuffer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert(&self, receipt: Receipt) {
        let idx = receipt.tx_idx;
        self.inner.lock().unwrap().insert(idx, receipt);
        self.cv.notify_all();
    }

    pub fn take(&self, idx: BPosition, timeout: Duration) -> Option<Receipt> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(r) = guard.remove(&idx) {
                return Some(r);
            }
            let (g, wait) = self.cv.wait_timeout(guard, timeout).unwrap();
            guard = g;
            if wait.timed_out() {
                return guard.remove(&idx);
            }
        }
    }
}

/// Returns `true` when the two receipts agree on the execution-correctness
/// fields: success status, gas used, and the write-set hash (the per-tx
/// determinism witness). Enrichment fields (block number, tx index) are derived
/// identically by both nodes, so the core three are the meaningful check.
pub fn receipt_consistent(local: &Receipt, published: &Receipt) -> bool {
    local.status == published.status
        && local.gas_used == published.gas_used
        && local.write_set_hash == published.write_set_hash
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
}

impl<Q: StateWriterQueue> ValidatorWriterQueue<Q> {
    pub fn new(inner: Q, bals: Arc<BalBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            inner,
            bals,
            divergence,
            wait: BAL_WAIT,
        }
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

impl<Q: StateWriterQueue> StateWriterQueue for ValidatorWriterQueue<Q> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        match self.bals.take(block.block_number, self.wait) {
            Some(bal) => {
                if write_set_eq(&delta, &bal) {
                    metrics::counter_block_verified();
                } else {
                    let summary = write_set_diff_summary(&delta, &bal);
                    let reason =
                        format!("block {} write-set != BAL: {summary}", block.block_number);
                    self.divergence.record(reason.clone());
                    return Err(ExecutorError::State(reason));
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

/// Implements [`TxReceiptsPublication`] but verifies instead of publishing: each
/// locally-recomputed receipt is checked against the executor's published
/// receipt for the same `tx_idx`. Fail-stops on a mismatch.
pub struct ValidatorReceiptSink {
    receipts: Arc<ReceiptBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
}

impl ValidatorReceiptSink {
    pub fn new(receipts: Arc<ReceiptBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            receipts,
            divergence,
            wait: RECEIPT_WAIT,
        }
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

impl TxReceiptsPublication for ValidatorReceiptSink {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(local) => match self.receipts.take(local.tx_idx, self.wait) {
                Some(published) => {
                    if receipt_consistent(&local, &published) {
                        Ok(())
                    } else {
                        let reason = format!(
                            "receipt mismatch at tx_idx {:?}: local(status={}, gas={}, wsh={}) \
                             vs published(status={}, gas={}, wsh={})",
                            local.tx_idx,
                            local.status,
                            local.gas_used,
                            local.write_set_hash,
                            published.status,
                            published.gas_used,
                            published.write_set_hash,
                        );
                        self.divergence.record(reason.clone());
                        Err(ExecutorError::State(reason))
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

        assert!(matches!(err, ExecutorError::State(_)));
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
        assert!(matches!(err, ExecutorError::State(_)));
        assert!(div.is_halted());
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
