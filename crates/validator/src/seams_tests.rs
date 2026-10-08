use super::*;
use crate::replica::{Attribution, Check, ReplicaId};
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountChange, BPosition, StorageChange};
use std::sync::Mutex;

const R1: ReplicaId = ReplicaId::from_session(11);
const R2: ReplicaId = ReplicaId::from_session(22);
const R3: ReplicaId = ReplicaId::from_session(33);

/// Publish `replica`'s BAL for `block` with balance `bal_val`.
fn bal(bals: &BalBuffer, replica: ReplicaId, block: u64, bal_val: u64) {
    bals.insert(block, replica, delta(block, bal_val));
}

/// Publish `replica`'s receipt.
fn publish(buf: &ReceiptBuffer, replica: ReplicaId, receipt: Receipt) {
    buf.receipts.insert(receipt.tx_idx, replica, receipt);
}

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
        base_fee: 0,
        gas_used: 0,
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
    publish(&buf, R1, receipt(0, true, 21_000, 0xab));
    publish(&buf, R1, receipt(1, true, 21_000, 0xab));
    let sink =
        ValidatorReceiptSink::new(buf.clone(), div.clone()).with_wait(Duration::from_millis(50));
    (buf, div, sink)
}

#[test]
fn matching_rows_pass_and_a_forged_row_halts() {
    let (buf, div, mut sink) = sink_with_two_receipts();
    // The batch [0, 1] ends at 1. Account 0x11 was last written at 0,
    // so its row is checked against the value recorded there.
    buf.rows.insert(
        BPosition::from_index(1),
        R1,
        vec![row(0x11, 1), row(0x22, 2)],
    );
    let (n, err) =
        sink.publish_receipts(&[item(0, vec![row(0x11, 1)]), item(1, vec![row(0x22, 2)])]);
    assert_eq!((n, err.is_none()), (2, true));
    assert!(!div.is_halted());

    // A published row that disagrees with the local state at its
    // position is a proven divergence.
    publish(&buf, R1, receipt(2, true, 21_000, 0xab));
    buf.rows
        .insert(BPosition::from_index(2), R1, vec![row(0x11, 9)]);
    let (n, err) = sink.publish_receipts(&[item(2, vec![row(0x22, 3)])]);
    assert_eq!(n, 0);
    assert!(matches!(err, Some(ExecutorError::Divergence(_))));
    assert!(div.reason().unwrap().contains("account row mismatch"));
}

#[test]
fn unknown_account_and_bare_receipt_leave_rows_unverified() {
    let (buf, div, mut sink) = sink_with_two_receipts();
    // A row for an account this validator never wrote: unverified.
    buf.rows
        .insert(BPosition::from_index(0), R1, vec![row(0x33, 5)]);
    // Rows at a position whose local receipt carries none (the
    // parallel-validation shape): unverified, even when they differ.
    buf.rows
        .insert(BPosition::from_index(1), R1, vec![row(0x11, 9)]);
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
        _refs: Vec<TxRef>,
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

    bal(&bals, R1, 1, 100);
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();

    assert!(!div.is_halted());
    assert_eq!(*submitted.lock().unwrap(), vec![1]);
}

#[test]
fn mismatched_bal_fail_stops() {
    let bals = BalBuffer::new();
    let div = Divergence::new();
    let inner = RecordingQueue::default();
    let mut q = ValidatorWriterQueue::new(inner, bals.clone(), div.clone());

    bal(&bals, R1, 1, 100); // The BAL has balance 100.
    // The local delta has 999.
    let err = q
        .submit(boundary(1), delta(1, 999), Vec::new())
        .unwrap_err();

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
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    assert!(!div.is_halted());
    assert_eq!(*submitted.lock().unwrap(), vec![1]);
}

#[test]
fn consistent_receipt_passes_inconsistent_fails() {
    let buf = ReceiptBuffer::new();
    let div = Divergence::new();
    let mut sink =
        ValidatorReceiptSink::new(buf.clone(), div.clone()).with_wait(Duration::from_millis(50));

    publish(&buf, R1, receipt(0, true, 21_000, 0xab));
    // The execution-correctness fields match, so the check passes.
    sink.publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
        .unwrap();
    assert!(!div.is_halted());

    // The write_set_hash values differ, so the process must stop.
    publish(&buf, R1, receipt(1, true, 21_000, 0xab));
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
            fees: kardamom_types::BlockFees::NONE,
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

    publish(&buf, R1, receipt(3, true, 21_000, 0xab));
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
    let mut sink =
        ValidatorReceiptSink::new(buf.clone(), div.clone()).with_wait(Duration::from_millis(50));

    let log = |topic: u8| WireLog {
        address: Address::from([0x22; 20]),
        topics: vec![B256::repeat_byte(topic)],
        data: bytes::Bytes::default(),
    };
    let mut published = receipt(0, true, 21_000, 0xab);
    published.logs = vec![log(0x01)];
    let mut local = receipt(0, true, 21_000, 0xab);
    local.logs = vec![log(0x02)];

    publish(&buf, R1, published);
    let err = sink.publish(CMessage::Receipt(local)).unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert!(div.is_halted());
}

/// A writer queue over a fresh BAL buffer, with a short BAL wait.
fn bal_queue() -> (
    Arc<BalBuffer>,
    Arc<Divergence>,
    ValidatorWriterQueue<RecordingQueue>,
) {
    let bals = BalBuffer::new();
    let div = Divergence::new();
    let q = ValidatorWriterQueue::new(RecordingQueue::default(), bals.clone(), div.clone())
        .with_wait(Duration::from_millis(50));
    (bals, div, q)
}

/// A receipt sink over a fresh buffer, with a short receipt wait.
fn receipt_sink() -> (Arc<ReceiptBuffer>, Arc<Divergence>, ValidatorReceiptSink) {
    let buf = ReceiptBuffer::new();
    let div = Divergence::new();
    let sink =
        ValidatorReceiptSink::new(buf.clone(), div.clone()).with_wait(Duration::from_millis(50));
    (buf, div, sink)
}

/// Assert that the standing divergence names exactly the sessions
/// `differ` on `check`, in the attribution and at the end of the reason.
fn assert_names(div: &Divergence, differ: &[ReplicaId], check: Check) -> Attribution {
    assert!(div.is_halted());
    let a = div.replica().expect("a replica attribution");
    assert_eq!((a.check, a.differ.as_slice()), (check, differ));
    let reason = div.reason().unwrap();
    assert!(reason.ends_with(&format!("[{a}]")), "{reason}");
    a
}

// Three replicas, and the wrong BAL arrives first. A buffer that keeps
// only the last BAL of a block lets it pass unchecked.
#[test]
fn a_wrong_bal_that_arrives_first_halts() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 999);
    bal(&bals, R2, 1, 100);
    bal(&bals, R3, 1, 100);
    let err = q
        .submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert_names(&div, &[R1], Check::Bal);
}

#[test]
fn a_wrong_bal_that_arrives_last_halts() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 100);
    bal(&bals, R2, 1, 100);
    bal(&bals, R3, 1, 999);
    let err = q
        .submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert_names(&div, &[R3], Check::Bal);
}

// A wrong BAL that arrives after its block's check is compared with the
// checked write-set at the next block, and the reason names its block.
#[test]
fn a_wrong_bal_after_the_check_halts_at_the_next_block() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 100);
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    bal(&bals, R2, 1, 100);
    bal(&bals, R3, 1, 999);
    bal(&bals, R1, 2, 100);
    let err = q
        .submit(boundary(2), delta(2, 100), Vec::new())
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert_names(&div, &[R3], Check::Bal);
    assert!(
        div.reason()
            .unwrap()
            .starts_with("block 1 write-set != BAL")
    );
}

// A BAL that misses the wait leaves its block unverified, but it is
// still compared when it arrives.
#[test]
fn a_bal_after_the_wait_is_still_checked() {
    let (bals, div, mut q) = bal_queue();
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    assert!(!div.is_halted());
    bal(&bals, R1, 1, 999);
    bal(&bals, R1, 2, 100);
    q.submit(boundary(2), delta(2, 100), Vec::new())
        .unwrap_err();
    assert_names(&div, &[R1], Check::Bal);
}

#[test]
fn three_agreeing_replicas_pass() {
    let (bals, div, mut q) = bal_queue();
    for r in [R1, R2, R3] {
        bal(&bals, r, 1, 100);
    }
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    let (buf, _, mut sink) = receipt_sink();
    for r in [R1, R2, R3] {
        publish(&buf, r, receipt(0, true, 21_000, 0xab));
    }
    sink.publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
        .unwrap();
    // The next block passes too: block 1 left no late result behind.
    for r in [R1, R2, R3] {
        bal(&bals, r, 2, 100);
    }
    q.submit(boundary(2), delta(2, 100), Vec::new()).unwrap();
    assert!(!div.is_halted());
}

// Three replicas, and the wrong receipt arrives first: the receipt form
// of the hole.
#[test]
fn a_wrong_receipt_that_arrives_first_halts() {
    let (buf, div, mut sink) = receipt_sink();
    publish(&buf, R1, receipt(0, true, 21_000, 0xff));
    publish(&buf, R2, receipt(0, true, 21_000, 0xab));
    publish(&buf, R3, receipt(0, true, 21_000, 0xab));
    let err = sink
        .publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert_names(&div, &[R1], Check::Receipt);
}

// Two replicas disagree with each other. The validator's re-execution
// decides: the replica that differs from it is the one named, whatever
// the arrival order.
#[test]
fn replicas_that_disagree_name_the_wrong_one() {
    let (buf, div, mut sink) = receipt_sink();
    publish(&buf, R1, receipt(0, true, 21_000, 0xab));
    publish(&buf, R2, receipt(0, true, 21_000, 0xff));
    let err = sink
        .publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
        .unwrap_err();
    assert!(matches!(err, ExecutorError::Divergence(_)));
    assert_names(&div, &[R2], Check::Receipt);
    assert!(div.reason().unwrap().contains("receipt mismatch at tx_idx"));
}

#[test]
fn wrong_rows_name_the_replica() {
    let (buf, div, mut sink) = sink_with_two_receipts();
    buf.rows.insert(
        BPosition::from_index(1),
        R1,
        vec![row(0x11, 1), row(0x22, 2)],
    );
    buf.rows
        .insert(BPosition::from_index(1), R2, vec![row(0x11, 9)]);
    let (n, err) =
        sink.publish_receipts(&[item(0, vec![row(0x11, 1)]), item(1, vec![row(0x22, 2)])]);
    assert_eq!(n, 1);
    assert!(matches!(err, Some(ExecutorError::Divergence(_))));
    assert_names(&div, &[R2], Check::Rows);
}

// The validator keeps the checked write-sets of the check window only.
// A BAL below the window has nothing to compare with, so it is never
// compared, and it cannot halt.
#[test]
fn checked_results_stay_inside_the_window() {
    let (bals, div, mut q) = bal_queue();
    let last = BalBuffer::CHECK_WINDOW * 3;
    (1..=last).for_each(|b| {
        bal(&bals, R1, b, 100);
        q.submit(boundary(b), delta(b, 100), Vec::new()).unwrap();
    });
    assert_eq!(
        q.checked.len(),
        usize::try_from(BalBuffer::CHECK_WINDOW + 1).unwrap()
    );
    bal(&bals, R2, 1, 999);
    bal(&bals, R1, last + 1, 100);
    q.submit(boundary(last + 1), delta(last + 1, 100), Vec::new())
        .unwrap();
    assert!(!div.is_halted());
}

// A replica that restarts and replays a block publishes its result again
// under a new session. The late result agrees, so it passes.
#[test]
fn a_late_result_from_a_new_session_that_agrees_passes() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 100);
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    bal(&bals, ReplicaId::from_session(44), 1, 100);
    bal(&bals, R1, 2, 100);
    q.submit(boundary(2), delta(2, 100), Vec::new()).unwrap();
    assert!(!div.is_halted());
}

// A correct replica that restarts in a loop publishes the same bytes
// under ever new sessions. They share one entry, so they never push out
// the wrong result of another replica.
#[test]
fn a_restart_storm_does_not_push_out_a_wrong_result() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 999);
    (100..300i32).for_each(|s| bal(&bals, ReplicaId::from_session(s), 1, 100));
    q.submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    let a = assert_names(&div, &[R1], Check::Bal);
    assert!(!a.validator_suspect());
}

// Two replicas on one media driver publish under one session. A wrong
// result under the shared session is compared, not dropped as a repeat.
#[test]
fn two_replicas_sharing_one_session_are_both_compared() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 100);
    bal(&bals, R1, 1, 999);
    q.submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    let a = assert_names(&div, &[R1], Check::Bal);
    assert_eq!(a.agree, vec![R1]);
}

// A session that already agreed and then publishes a different result
// for the same key (a late repeat) is compared too.
#[test]
fn a_late_repeat_with_a_different_value_is_compared() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 100);
    q.submit(boundary(1), delta(1, 100), Vec::new()).unwrap();
    bal(&bals, R1, 1, 999);
    bal(&bals, R1, 2, 100);
    q.submit(boundary(2), delta(2, 100), Vec::new())
        .unwrap_err();
    assert_names(&div, &[R1], Check::Bal);
}

// Every differing session of the key is named, with the count of those
// that agree.
#[test]
fn every_differing_replica_is_named() {
    let (buf, div, mut sink) = receipt_sink();
    publish(&buf, R1, receipt(0, true, 21_000, 0xff));
    publish(&buf, R2, receipt(0, true, 21_000, 0xab));
    publish(&buf, R3, receipt(0, true, 21_000, 0xee));
    sink.publish(CMessage::Receipt(receipt(0, true, 21_000, 0xab)))
        .unwrap_err();
    let a = assert_names(&div, &[R1, R3], Check::Receipt);
    assert_eq!(a.agree, vec![R2]);
    assert!(
        div.reason()
            .unwrap()
            .contains("[2 of 3 replica sessions differ")
    );
}

// Every replica differs from the validator, and the replicas agree with
// each other: the verdict says that the validator is the suspect.
#[test]
fn all_replicas_differ_and_agree_makes_the_validator_the_suspect() {
    let (bals, div, mut q) = bal_queue();
    for r in [R1, R2, R3] {
        bal(&bals, r, 1, 999);
    }
    q.submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    let a = assert_names(&div, &[R1, R2, R3], Check::Bal);
    assert!(a.validator_suspect());
    assert!(
        div.reason()
            .unwrap()
            .contains("so the validator is the suspect")
    );
}

// Every replica differs, but the replicas also differ from each other:
// no side is the suspect from the counts alone.
#[test]
fn all_replicas_differ_from_each_other_names_them_all() {
    let (bals, div, mut q) = bal_queue();
    bal(&bals, R1, 1, 997);
    bal(&bals, R2, 1, 998);
    q.submit(boundary(1), delta(1, 100), Vec::new())
        .unwrap_err();
    let a = assert_names(&div, &[R1, R2], Check::Bal);
    assert!(!a.validator_suspect());
}
