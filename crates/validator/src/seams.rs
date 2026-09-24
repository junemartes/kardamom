//! The validator's two engine seams: the block-close BAL write-set check
//! ([`ValidatorWriterQueue`]) and the per-tx receipt check with the
//! account-row check ([`ValidatorReceiptSink`]). Both use existing engine
//! trait seams ([`StateWriterQueue`], [`TxReceiptsPublication`]) and stop
//! the process via [`Divergence`] on a proven mismatch.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_engine::{
    CMessage, ExecutorError, StateWriterQueue, TxReceiptsPublication, publish_each,
};
use kardamom_types::{AccountRow, BPosition, BlockBoundary, BlockDelta, Receipt, ReceiptRows};

use crate::buffers::{BalBuffer, ReceiptBuffer};
use crate::{Divergence, metrics};

/// How long the exec thread waits for a block's BAL before it skips the check.
const BAL_WAIT: Duration = Duration::from_secs(5);
/// How long the commit thread waits for a tx's published receipt before it skips.
const RECEIPT_WAIT: Duration = Duration::from_secs(5);

/// Returns `true` when two block deltas have the same write-set (accounts,
/// storage, code). Receipts are checked separately via `tx_receipts`, so this
/// function skips them on purpose. The validator and the executor both build
/// these vectors from sorted maps, so equal content means equal order.
fn write_set_eq(a: &BlockDelta, b: &BlockDelta) -> bool {
    a.accounts == b.accounts && a.storage == b.storage && a.code == b.code
}

/// Return a short summary of the first write-set field that differs.
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

/// Returns `true` when the two receipts agree on the execution-output
/// fields: success status, gas used, the write-set hash (the per-tx
/// determinism witness), and the emitted logs. The check includes logs
/// because `write_set_hash` covers state writes but not events. Without the
/// log check, a log-only divergence would pass silently.
///
/// The check skips the RPC enrichment fields on purpose (`nonce`, `from`,
/// `to`, `contract_address`, `effective_gas_price`, `block_number`,
/// `transaction_index`, `cumulative_gas_used`). These fields derive
/// deterministically from inputs the check already covers: the envelope,
/// the canonical order, and the per-block `gas_used` sums. A divergence in
/// one of them implies a divergence in a checked field, so comparing them
/// would only re-verify arithmetic, not execution.
fn receipt_consistent(local: &Receipt, published: &Receipt) -> bool {
    local.status == published.status
        && local.gas_used == published.gas_used
        && local.write_set_hash == published.write_set_hash
        && local.logs == published.logs
        // The typed skip cause is part of the deterministic transition:
        // same input, same reason on every replica.
        && local.skip_reason == published.skip_reason
}

// ---------------------------------------------------------------------------
// ValidatorWriterQueue: checks the BAL, then forwards to the trie-aware writer.
// ---------------------------------------------------------------------------

/// Wraps the trie-aware [`StateWriterQueue`]. It checks each block's
/// write-set against the executor's BAL, then sends the delta to the writer,
/// which advances the MPT state root. It stops the process on a mismatch.
pub struct ValidatorWriterQueue<Q: StateWriterQueue> {
    inner: Q,
    bals: Arc<BalBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// Highest block already submitted in this process's lifetime. A
    /// cluster session replay (lapse and reconnect, no restart) re-delivers
    /// blocks the validator already ran. Re-execution against already-
    /// applied state gives empty deltas that cannot match the BAL. This
    /// field catches that case, the session form of the restart cascade
    /// below.
    high_water: u64,
    /// Blocks at or below this value were verified before a restart.
    /// Crash recovery replays them to rebuild state, but re-execution
    /// against already-applied state gives empty deltas (every tx skips as
    /// nonce-too-low). Comparing those against retained BAL frames would
    /// report a false divergence on every replay of these blocks.
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

    /// Skip BAL verification for blocks at or below `floor`, the recovery
    /// resume point. These blocks were verified before the restart. Their
    /// replay re-execution correctly produces deltas that do not match the
    /// BAL, because the state is already applied.
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
        if let Some(bal) = self.bals.take(block.block_number, self.wait) {
            if write_set_eq(&delta, &bal) {
                metrics::counter_block_verified();
            } else {
                let summary = write_set_diff_summary(&delta, &bal);
                let reason = format!("block {} write-set != BAL: {summary}", block.block_number);
                // `Divergence` is fatal; the engine does not retry it.
                return Err(ExecutorError::Divergence(self.divergence.halt(reason)));
            }
        } else {
            // The BAL never arrived, so this block stays unverified. This
            // is not a proven divergence: log it and count it.
            tracing::warn!(
                block = block.block_number,
                "no BAL received within timeout; block left unverified"
            );
            metrics::counter_bal_missing();
        }
        // Send the delta to the trie-aware writer; this advances the MPT state root.
        self.inner.submit(block, delta)
    }
}

// ---------------------------------------------------------------------------
// ValidatorReceiptSink: checks tx_receipts; never publishes.
// ---------------------------------------------------------------------------

/// How far behind the newest local receipt a recorded account value stays
/// available for a row check, in canonical positions. A published batch
/// spans at most 64 receipts, so its rows reference writes within that
/// reach; the margin covers a validator that lags the executors.
const RECENT_LOOKBEHIND: u64 = 4096;

/// The latest local post-state of recently written accounts. The sink
/// records every local receipt's rows in canonical order, so at the moment
/// it processes the receipt at position P, an entry is the account's state
/// after P. That is what a published batch row tagged P claims.
#[derive(Default)]
struct RecentAccounts {
    latest: HashMap<Address, (u64, AccountRow)>,
    /// Write order, for eviction. An address repeats once per write.
    order: VecDeque<(u64, Address)>,
}

impl RecentAccounts {
    fn record(&mut self, at: BPosition, rows: &[AccountRow]) {
        let idx = at.as_index();
        for row in rows {
            self.latest.insert(row.address, (idx, row.clone()));
            self.order.push_back((idx, row.address));
        }
        self.evict_below(idx.saturating_sub(RECENT_LOOKBEHIND));
    }

    /// Drop entries last written before `below`. An address rewritten
    /// since keeps its newer value.
    fn evict_below(&mut self, below: u64) {
        while let Some(&(idx, address)) = self.order.front()
            && idx < below
        {
            self.order.pop_front();
            if self.latest.get(&address).is_some_and(|(at, _)| *at == idx) {
                self.latest.remove(&address);
            }
        }
    }

    /// The local value of `row`'s account, when one is recorded.
    fn local(&self, row: &AccountRow) -> Option<&AccountRow> {
        self.latest.get(&row.address).map(|(_, local)| local)
    }
}

/// Implements [`TxReceiptsPublication`], but checks receipts instead of
/// publishing them. It compares each recomputed receipt against the
/// executor's published receipt for the same `tx_idx`, and the published
/// account rows of a batch that ends there against the local state, and
/// stops the process on a mismatch.
pub struct ValidatorReceiptSink {
    receipts: Arc<ReceiptBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// Recent-block input ring for the receipt-divergence dump. The mismatch
    /// fires after the block's records and claims are gone, so without this
    /// ring a mismatch leaves only a log line, with nothing to replay.
    flight: Option<Arc<crate::flight::FlightRing>>,
    /// The local account state at the current position, for the row check.
    recent: RecentAccounts,
}

impl ValidatorReceiptSink {
    pub fn new(receipts: Arc<ReceiptBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            receipts,
            divergence,
            wait: RECEIPT_WAIT,
            flight: None,
            recent: RecentAccounts::default(),
        }
    }

    /// Attach the flight ring. It dumps block inputs on a receipt mismatch.
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
        match msg {
            CMessage::Receipt(local) => self.check_item(&ReceiptRows::bare(local)),
            // A block boundary carries no per-tx data to check.
            CMessage::BlockBoundary(_) => self.latched(),
        }
    }

    /// The engine's commit thread delivers receipts through this path,
    /// each with the rows the local execution wrote. Every item is checked
    /// in order, and the first failure stops the batch.
    fn publish_receipts(&mut self, items: &[ReceiptRows]) -> (usize, Option<ExecutorError>) {
        publish_each(items, |item| self.check_item(item))
    }
}

impl ValidatorReceiptSink {
    /// The divergence latch: once a divergence is proven, the sink keeps
    /// failing. The first failing publish took the published receipt out
    /// of the buffer. Without this latch, a retrying caller would find an
    /// empty buffer, land in the "unverified" arm, and commit past a
    /// proven mismatch.
    fn latched(&self) -> Result<(), ExecutorError> {
        match self
            .divergence
            .halt_reason("validator halted on divergence")
        {
            Some(reason) => Err(ExecutorError::Divergence(reason)),
            None => Ok(()),
        }
    }

    /// Check one local receipt, then the published account rows of a
    /// batch that ends at its position, when a frame ending there has
    /// arrived.
    fn check_item(&mut self, item: &ReceiptRows) -> Result<(), ExecutorError> {
        self.latched()?;
        self.check_receipt(&item.receipt)?;
        self.recent.record(item.receipt.tx_idx, &item.accounts);
        let Some(published) = self.receipts.take_rows(item.receipt.tx_idx) else {
            return Ok(());
        };
        // Under parallel validation only a block's last receipt carries
        // rows. A receipt with no rows is not at a position where the
        // local state is known per position, so the rows stay unverified.
        if item.accounts.is_empty() {
            metrics::counter_rows_unverified();
            return Ok(());
        }
        self.check_rows(&item.receipt, &published)
    }

    /// Compare published rows with the local state at the same position.
    /// A row for an account with no recorded local value is unverified,
    /// not a divergence: a cold start can begin inside a batch.
    fn check_rows(&self, local: &Receipt, published: &[AccountRow]) -> Result<(), ExecutorError> {
        let mismatch = published.iter().find_map(|row| {
            self.recent
                .local(row)
                .filter(|l| *l != row)
                .map(|l| (row, l))
        });
        let Some((row, local_row)) = mismatch else {
            self.count_rows(published);
            return Ok(());
        };
        let reason = format!(
            "account row mismatch at tx_idx {:?}: {} local(nonce={}, balance={}) vs \
             published(nonce={}, balance={}) [tx_hash={} block={}]",
            local.tx_idx,
            row.address,
            local_row.nonce,
            local_row.balance,
            row.nonce,
            row.balance,
            local.tx_hash,
            local.block_number,
        );
        Err(ExecutorError::Divergence(self.divergence.halt(reason)))
    }

    /// Count a batch with no mismatch: verified when every row had a
    /// local value to compare with, unverified otherwise.
    fn count_rows(&self, published: &[AccountRow]) {
        if published.iter().all(|row| self.recent.local(row).is_some()) {
            metrics::counter_rows_verified();
        } else {
            metrics::counter_rows_unverified();
        }
    }
    /// Cross-checks one local receipt against the executor's published
    /// receipt. `Ok` when they match, or when no published receipt
    /// turned up in time to compare (counted as `receipt_missing`).
    /// `Err` halts the validator on a proven mismatch, after a
    /// best-effort flight dump.
    fn check_receipt(&self, local: &Receipt) -> Result<(), ExecutorError> {
        let Some(published) = self.receipts.take(local.tx_idx, self.wait) else {
            // No published receipt to compare against, so skip the check.
            metrics::counter_receipt_missing();
            return Ok(());
        };
        if receipt_consistent(local, &published) {
            return Ok(());
        }
        // Include the tx identity, not only the mismatch, so
        // responders can find the transaction without a separate
        // hash lookup.
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
        // Dump the flight ring first, on a best-effort basis. The
        // stop is permanent, so this is the only chance to capture
        // the block inputs behind the mismatch.
        if let Some(f) = self.flight.as_ref() {
            f.dump_receipt_divergence(local, &published);
        }
        Err(ExecutorError::Divergence(self.divergence.halt(reason)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_types::{AccountChange, BPosition, StorageChange};
    use std::sync::Mutex;

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

    fn row(byte: u8, nonce: u64) -> AccountRow {
        AccountRow {
            address: Address::from([byte; 20]),
            nonce,
            balance: U256::from(nonce),
        }
    }

    fn item(idx: u64, rows: Vec<AccountRow>) -> ReceiptRows {
        ReceiptRows {
            receipt: receipt(idx, true, 21_000, 0xab),
            accounts: rows,
        }
    }

    /// A sink whose buffer already holds the published receipts 0 and 1.
    fn sink_with_two_receipts() -> (Arc<ReceiptBuffer>, Arc<Divergence>, ValidatorReceiptSink) {
        let buf = ReceiptBuffer::new();
        let div = Divergence::new();
        buf.insert(receipt(0, true, 21_000, 0xab));
        buf.insert(receipt(1, true, 21_000, 0xab));
        let sink = ValidatorReceiptSink::new(buf.clone(), div.clone())
            .with_wait(Duration::from_millis(50));
        (buf, div, sink)
    }

    #[test]
    fn matching_rows_pass_and_a_forged_row_halts() {
        let (buf, div, mut sink) = sink_with_two_receipts();
        // The batch [0, 1] ends at 1. Account 0x11 was last written at 0,
        // so its row is checked against the value recorded there.
        buf.insert_rows(BPosition::from_index(1), vec![row(0x11, 1), row(0x22, 2)]);
        let (n, err) =
            sink.publish_receipts(&[item(0, vec![row(0x11, 1)]), item(1, vec![row(0x22, 2)])]);
        assert_eq!((n, err.is_none()), (2, true));
        assert!(!div.is_halted());

        // A published row that disagrees with the local state at its
        // position is a proven divergence.
        buf.insert(receipt(2, true, 21_000, 0xab));
        buf.insert_rows(BPosition::from_index(2), vec![row(0x11, 9)]);
        let (n, err) = sink.publish_receipts(&[item(2, vec![row(0x22, 3)])]);
        assert_eq!(n, 0);
        assert!(matches!(err, Some(ExecutorError::Divergence(_))));
        assert!(div.reason().unwrap().contains("account row mismatch"));
    }

    #[test]
    fn unknown_account_and_bare_receipt_leave_rows_unverified() {
        let (buf, div, mut sink) = sink_with_two_receipts();
        // A row for an account this validator never wrote: unverified.
        buf.insert_rows(BPosition::from_index(0), vec![row(0x33, 5)]);
        // Rows at a position whose local receipt carries none (the
        // parallel-validation shape): unverified, even when they differ.
        buf.insert_rows(BPosition::from_index(1), vec![row(0x11, 9)]);
        let (n, err) = sink.publish_receipts(&[item(0, vec![row(0x11, 1)]), item(1, Vec::new())]);
        assert_eq!((n, err.is_none()), (2, true));
        assert!(!div.is_halted());
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

        bals.insert(delta(1, 100)); // The BAL has balance 100.
        let err = q.submit(boundary(1), delta(1, 999)).unwrap_err(); // The local delta has 999.

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

        // No BAL is inserted: submit must still forward, and must not flag a divergence.
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
        // The execution-correctness fields match, so the check passes.
        sink.publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
            .unwrap();
        assert!(!div.is_halted());

        // The write_set_hash values differ, so the process must stop.
        buf.insert(receipt(1, true, 21_000, 0xab));
        let err = sink
            .publish(CMessage::Receipt(receipt(1, true, 21_000, 0xff)))
            .unwrap_err();
        assert!(matches!(err, ExecutorError::Divergence(_)));
        assert!(div.is_halted());

        // Regression test: a retry of the same publish finds the buffer
        // empty. It must keep failing through the divergence latch, and
        // must not fall into the "unverified" Ok arm.
        let err2 = sink
            .publish(CMessage::Receipt(receipt(1, true, 21_000, 0xff)))
            .unwrap_err();
        assert!(matches!(err2, ExecutorError::Divergence(_)));
    }

    /// A receipt mismatch with the flight ring attached must leave a
    /// replayable record: both receipts, plus the ring's recent block
    /// inputs.
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
            std::num::NonZeroU16::new(20).expect("fixture granularity"),
            kardamom_engine::block_env::ExecEnv {
                chain_id: 1,
                block_number: 7,
                l2_timestamp: 1_700_000_000,
            },
            &[BufferedRecord::Tx {
                tx_idx: TxIndex(0),
                position: BPosition::from_index(0),
                envelope: kardamom_types::TxEnvelope {
                    correlation_id: 1,
                    raw_tx: vec![0xde, 0xad].into(),
                    sender: Address::from([0x11; 20]),
                    tx_hash: B256::from([0x22; 32]),
                    max_inclusion_block: u64::MAX,
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
        // Check both receipts, field by field.
        assert_eq!(
            v["local"]["write_set_hash"],
            format!("{:?}", B256::from([0xff; 32]))
        );
        assert_eq!(
            v["published"]["write_set_hash"],
            format!("{:?}", B256::from([0xab; 32]))
        );
        // The ring's block inputs can be replayed.
        assert_eq!(v["ring"][0]["block"], 7);
        assert_eq!(v["ring"][0]["granularity"], 20);
        assert_eq!(v["ring"][0]["records"][0]["kind"], "tx");
        assert_eq!(v["ring"][0]["records"][0]["raw"], "dead");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A log-only divergence (same status, gas, and write-set hash) must
    // trip the check. Logs are published execution output, not enrichment.
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
            data: bytes::Bytes::default(),
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
}
