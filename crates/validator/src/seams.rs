//! The validator's two engine seams: the block-close BAL write-set check
//! ([`ValidatorWriterQueue`]) and the per-tx receipt check
//! ([`ValidatorReceiptSink`]). Both use existing engine trait seams
//! ([`StateWriterQueue`], [`TxReceiptsPublication`]) and stop the process via
//! [`Divergence`] on a proven mismatch.

use std::sync::Arc;
use std::time::Duration;

use kardamom_engine::{CMessage, ExecutorError, StateWriterQueue, TxReceiptsPublication};
use kardamom_types::{BlockBoundary, BlockDelta, Receipt};

use crate::buffers::{BalBuffer, ReceiptBuffer};
use crate::{Divergence, metrics};

/// How long the exec thread waits for a block's BAL before it skips the check.
pub const BAL_WAIT: Duration = Duration::from_secs(5);
/// How long the commit thread waits for a tx's published receipt before it skips.
pub const RECEIPT_WAIT: Duration = Duration::from_secs(5);

/// Returns `true` when two block deltas have the same write-set (accounts,
/// storage, code). Receipts are checked separately via `tx_receipts`, so this
/// function skips them on purpose. The validator and the executor both build
/// these vectors from sorted maps, so equal content means equal order.
pub fn write_set_eq(a: &BlockDelta, b: &BlockDelta) -> bool {
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
pub fn receipt_consistent(local: &Receipt, published: &Receipt) -> bool {
    local.status == published.status
        && local.gas_used == published.gas_used
        && local.write_set_hash == published.write_set_hash
        && local.logs == published.logs
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
    /// nonce-too-low). Comparing those against retained BAL frames caused
    /// false divergences: each stop-and-restart replayed again and
    /// diverged again, turning a short stall into a permanent restart
    /// loop.
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
        match self.bals.take(block.block_number, self.wait) {
            Some(bal) => {
                if write_set_eq(&delta, &bal) {
                    metrics::counter_block_verified();
                } else {
                    let summary = write_set_diff_summary(&delta, &bal);
                    let reason =
                        format!("block {} write-set != BAL: {summary}", block.block_number);
                    self.divergence.record(reason.clone());
                    // Use `Divergence`, not `State`: it is fatal and the
                    // engine does not retry it.
                    return Err(ExecutorError::Divergence(reason));
                }
            }
            None => {
                // The BAL never arrived, so this block stays unverified.
                // This is not a proven divergence: log it and count it.
                tracing::warn!(
                    block = block.block_number,
                    "no BAL received within timeout; block left unverified"
                );
                metrics::counter_bal_missing();
            }
        }
        // Send the delta to the trie-aware writer; this advances the MPT state root.
        self.inner.submit(block, delta)
    }
}

// ---------------------------------------------------------------------------
// ValidatorReceiptSink: checks tx_receipts; never publishes.
// ---------------------------------------------------------------------------

/// Implements [`TxReceiptsPublication`], but checks receipts instead of
/// publishing them. It compares each recomputed receipt against the
/// executor's published receipt for the same `tx_idx`, and stops the
/// process on a mismatch.
pub struct ValidatorReceiptSink {
    receipts: Arc<ReceiptBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// Recent-block input ring for the receipt-divergence dump. The mismatch
    /// fires after the block's records and claims are gone, so without this
    /// ring a mismatch leaves only a log line, with nothing to replay.
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
        // Latch: once a divergence is proven, the sink keeps failing. The
        // first failing publish took the published receipt out of the
        // buffer. Without this latch, a retrying caller would find an
        // empty buffer, land in the "unverified" arm, and commit past a
        // proven mismatch.
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
                        // Include the tx identity, not only the mismatch, so
                        // responders can find the transaction without a
                        // separate hash lookup.
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
                        // Dump the flight ring first, on a best-effort basis.
                        // The stop is permanent, so this is the only chance
                        // to capture the block inputs behind the mismatch.
                        if let Some(f) = self.flight.as_ref() {
                            f.dump_receipt_divergence(&local, &published);
                        }
                        self.divergence.record(reason.clone());
                        Err(ExecutorError::Divergence(reason))
                    }
                }
                None => {
                    // No published receipt to compare against, so skip the check.
                    metrics::counter_receipt_missing();
                    Ok(())
                }
            },
            // A block boundary carries no per-tx data to check.
            CMessage::BlockBoundary(_) => Ok(()),
        }
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
            20,
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
}
