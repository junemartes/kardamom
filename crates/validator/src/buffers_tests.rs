use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use ::metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountChange, StorageChange};

const R1: ReplicaId = ReplicaId::from_session(11);
const R2: ReplicaId = ReplicaId::from_session(22);

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

/// Insert `replica`'s BAL for `block` with balance `bal_val`.
fn insert(bals: &BalBuffer, replica: ReplicaId, block: u64, bal_val: u64) {
    bals.insert(block, replica, delta(block, bal_val));
}

/// The distinct balances in `taken`, with their sessions, in arrival
/// order.
fn balances(taken: &Taken<u64, BlockDelta>) -> Vec<(U256, Vec<ReplicaId>)> {
    taken
        .current
        .iter()
        .map(|d| (d.value.accounts[0].balance, d.replicas.clone()))
        .collect()
}

/// A recorder that counts `validator_replica_results_unchecked_total`
/// for one `reason`, and ignores every other metric.
struct UncheckedCount {
    reason: &'static str,
    count: Arc<AtomicU64>,
}

impl UncheckedCount {
    /// The count of `reason` while `run` runs.
    fn of(reason: &'static str, run: impl FnOnce()) -> u64 {
        let recorder = Self {
            reason,
            count: Arc::default(),
        };
        ::metrics::with_local_recorder(&recorder, run);
        recorder.count.load(Ordering::Relaxed)
    }
}

impl Recorder for UncheckedCount {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let ours = key.name() == "validator_replica_results_unchecked_total"
            && key
                .labels()
                .any(|l| l.key() == "reason" && l.value() == self.reason);
        if ours {
            Counter::from_arc(Arc::clone(&self.count))
        } else {
            Counter::noop()
        }
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

// The receipt buffer mirrors the BAL catch-up skip. When a run of
// buffered keys is far ahead of the requested tx_idx, the receipt has
// aged out of the live stream, and take() must return at once instead
// of blocking the commit thread for the full wait per historical tx.
#[test]
fn receipt_take_skips_aged_out_backlog_immediately() {
    let buf = ReceiptBuffer::new();
    for idx in 10_000..10_004 {
        buf.receipts.insert(
            BPosition::from_index(idx),
            R1,
            receipt(idx, true, 21_000, 0xab),
        );
    }
    let start = std::time::Instant::now();
    let got = buf
        .receipts
        .take(BPosition::from_index(0), Duration::from_secs(5));
    assert!(got.current.is_empty(), "aged-out receipt must be skipped");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "skip must not consume the wait window: {:?}",
        start.elapsed()
    );
}

// A restart on an idle chain has no live head, so the catch-up skip
// never fires. After one take waits its whole timeout with no insert,
// the buffer is quiet, and a take for a missing key returns at once.
#[test]
fn a_quiet_buffer_does_not_wait_for_a_missing_key() {
    let buf = ReceiptBuffer::new();
    let got = buf
        .receipts
        .take(BPosition::from_index(0), Duration::from_millis(20));
    assert!(got.current.is_empty());
    let start = std::time::Instant::now();
    let got = buf
        .receipts
        .take(BPosition::from_index(1), Duration::from_secs(5));
    assert!(got.current.is_empty());
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "a quiet take must not wait: {:?}",
        start.elapsed()
    );
}

// An insert ends the quiet: the next take waits again for its key.
#[test]
fn an_insert_ends_the_quiet() {
    let bals = BalBuffer::new();
    assert!(bals.take(1, Duration::from_millis(20)).current.is_empty());
    insert(&bals, R1, 2, 100);
    let bals2 = bals.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        insert(&bals2, R1, 3, 100);
    });
    let got = bals.take(3, Duration::from_secs(2));
    assert_eq!(got.current.len(), 1, "the take must wait after an insert");
    h.join().unwrap();
}

// A take that times out while inserts of other keys come does not turn
// the buffer quiet: the producer is live, and the next key can come.
#[test]
fn a_timeout_with_inserts_keeps_the_wait() {
    let bals = BalBuffer::new();
    let bals2 = bals.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(5));
        insert(&bals2, R1, 2, 100);
        std::thread::sleep(Duration::from_millis(40));
        insert(&bals2, R1, 3, 100);
    });
    assert!(bals.take(1, Duration::from_millis(20)).current.is_empty());
    let got = bals.take(3, Duration::from_secs(2));
    assert_eq!(
        got.current.len(),
        1,
        "the take must wait while inserts come"
    );
    h.join().unwrap();
}

// One wrong key far ahead is not a live head. It does not turn the
// catch-up skip on, so the consumer still waits for its own key.
#[test]
fn one_key_far_ahead_does_not_skip_the_wait() {
    let bals = BalBuffer::new();
    insert(&bals, R1, 5_000, 100);
    let bals2 = bals.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        insert(&bals2, R1, 1, 100);
    });
    let got = bals.take(1, Duration::from_secs(2));
    assert_eq!(got.current.len(), 1, "the wait must not be skipped");
    h.join().unwrap();
}

// A second replica's result for a key joins the first one. It never
// replaces it, so a wrong result that arrives first is still handed to
// the consumer. Equal results of two sessions share one entry.
#[test]
fn distinct_results_are_kept_in_arrival_order() {
    let bals = BalBuffer::new();
    insert(&bals, R1, 1, 999);
    insert(&bals, R2, 1, 100);
    insert(&bals, ReplicaId::from_session(33), 1, 100);
    let taken = bals.take(1, Duration::from_millis(10));
    assert_eq!(
        balances(&taken),
        vec![
            (U256::from(999), vec![R1]),
            (U256::from(100), vec![R2, ReplicaId::from_session(33)])
        ]
    );
}

// A session that repeats the same result adds nothing, and the repeat
// counts. A session that publishes a different result under the same
// session id (two replicas on one driver, or a restart that attaches to
// a live publication) is kept, so it is compared.
#[test]
fn a_repeat_is_counted_and_a_new_value_under_one_session_is_kept() {
    let bals = BalBuffer::new();
    let repeats = UncheckedCount::of("repeat", || {
        insert(&bals, R1, 1, 100);
        insert(&bals, R1, 1, 100);
        insert(&bals, R1, 1, 999);
    });
    assert_eq!(repeats, 1);
    let taken = bals.take(1, Duration::from_millis(10));
    assert_eq!(
        balances(&taken),
        vec![(U256::from(100), vec![R1]), (U256::from(999), vec![R1])]
    );
}

// A result that arrives after the take of its key, but inside the check
// window, goes to the consumer with the next take. A result below the
// window is refused and counted, so it cannot leak.
#[test]
fn late_arrivals_are_handed_over_or_refused() {
    let bals = BalBuffer::new();
    assert!(bals.take(100, Duration::from_millis(10)).current.is_empty());
    let late = UncheckedCount::of("late", || insert(&bals, R1, 3, 100));
    assert_eq!(late, 1);
    assert_eq!(bals.len(), 0, "an insert below the window must be refused");
    insert(&bals, R2, 99, 100);
    insert(&bals, R1, 101, 100);
    let taken = bals.take(101, Duration::from_millis(10));
    assert_eq!(taken.current.len(), 1);
    assert_eq!(
        taken
            .late
            .iter()
            .map(|(block, d)| (*block, d[0].replicas.clone()))
            .collect::<Vec<_>>(),
        vec![(99, vec![R2])]
    );
    assert_eq!(bals.len(), 0, "a take leaves nothing below its key");
}

// A key beyond the reach above the cursor is refused and counted. A key
// that arrived before the first take, and lies beyond the reach, goes
// at the first take.
#[test]
fn keys_beyond_the_reach_are_refused() {
    let bals = BalBuffer::new();
    insert(&bals, R1, u64::MAX, 100);
    let ahead = UncheckedCount::of("ahead", || {
        assert!(bals.take(1, Duration::from_millis(10)).current.is_empty());
        insert(&bals, R1, 2 + BalBuffer::REACH, 100);
    });
    assert_eq!(ahead, 2);
    assert_eq!(bals.len(), 0);
}

// The buffer is bounded in keys. On overflow the highest key goes, so a
// key far ahead never pushes out a key near the consumer, and the
// eviction counts. Each key holds at most `MAX_DISTINCT` distinct
// results, and each result names at most `MAX_SESSIONS` sessions.
#[test]
fn buffer_is_bounded_and_counts_evictions() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(3).expect("fixture cap"));
    let evicted = UncheckedCount::of("evicted", || {
        (1..=5u64).for_each(|b| insert(&bals, R1, b, 100));
    });
    assert_eq!(evicted, 2);
    assert_eq!(bals.len(), 3);
    assert_eq!(bals.take(1, Duration::from_millis(10)).current.len(), 1);
    assert_eq!(bals.take(2, Duration::from_millis(10)).current.len(), 1);
    let bound = UncheckedCount::of("bound", || {
        (0..20u64).for_each(|v| insert(&bals, R2, 3, 1_000 + v));
        (0..40i32).for_each(|s| insert(&bals, ReplicaId::from_session(s), 3, 100));
    });
    // Block 3 already holds one value, so 7 of the 20 new values fit.
    // Session 11 is R1, a repeat; 31 of the other 39 sessions fit.
    assert_eq!(bound, 13 + 8);
    let taken = bals.take(3, Duration::from_millis(10));
    assert_eq!(taken.current.len(), MAX_DISTINCT);
    assert_eq!(taken.current[0].replicas.len(), MAX_SESSIONS);
}

#[test]
fn buffers_block_until_value_arrives() {
    // take() must return promptly once another thread inserts a value.
    // This covers the handoff from the Aeron task to the exec thread.
    let bals = BalBuffer::new();
    let bals2 = bals.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        insert(&bals2, R1, 7, 5);
    });
    let got = bals.take(7, Duration::from_secs(2));
    assert_eq!(got.current[0].value.block_number, 7);
    h.join().unwrap();
}

/// A receipt at `idx` in `block`.
fn receipt_in(idx: u64, block: u64, wsh: u8) -> Receipt {
    Receipt {
        block_number: block,
        ..receipt(idx, true, 21_000, wsh)
    }
}

/// A rollback withdraws the receipts of every session above its block,
/// and the rows at the positions of those receipts. The receipts and rows
/// at or below the block stay.
#[test]
fn a_rollback_withdraws_every_session_above_its_block() {
    let buf = ReceiptBuffer::new();
    let rows = vec![AccountRow::default()];
    for (replica, wsh) in [(R1, 0xaa), (R2, 0xbb)] {
        buf.receipts
            .insert(BPosition::from_index(4), replica, receipt_in(4, 2, wsh));
        buf.receipts
            .insert(BPosition::from_index(5), replica, receipt_in(5, 3, wsh));
        buf.rows
            .insert(BPosition::from_index(4), replica, rows.clone());
        buf.rows
            .insert(BPosition::from_index(5), replica, rows.clone());
    }

    buf.roll_back(2);

    let sessions = |taken: &[Distinct<Receipt>]| -> Vec<ReplicaId> {
        taken.iter().flat_map(|d| d.replicas.clone()).collect()
    };
    let wait = Duration::ZERO;
    let at4 = buf.receipts.take(BPosition::from_index(4), wait);
    assert_eq!(sessions(&at4.current), [R1, R2], "block 2 stays");
    let at5 = buf.receipts.take(BPosition::from_index(5), wait);
    assert!(
        at5.current.is_empty(),
        "every session is withdrawn above block 2"
    );
    let rows4 = buf.rows.take(BPosition::from_index(4), wait);
    assert_eq!(Distinct::results(&rows4.current), 2);
    let rows5 = buf.rows.take(BPosition::from_index(5), wait);
    assert!(rows5.current.is_empty());
}

/// A rollback withdraws the BALs of every session above its block: a
/// BAL names no receipt session.
#[test]
fn a_rollback_withdraws_every_bal_above_its_block() {
    let bals = BalBuffer::new();
    insert(&bals, R1, 2, 10);
    insert(&bals, R2, 3, 20);
    insert(&bals, R1, 3, 30);
    bals.roll_back(2);
    assert_eq!(
        balances(&bals.take(2, Duration::ZERO)),
        [(U256::from(10u64), vec![R1])]
    );
    assert_eq!(bals.len(), 0, "block 3 is withdrawn");
}

/// A discarded tail is unavailable even while unrelated live results arrive.
#[test]
fn an_evicted_result_near_the_live_head_does_not_wait_again() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(8).unwrap());
    (0..=12).for_each(|block| insert(&bals, R1, block, block));
    (0..8).for_each(|block| {
        assert_eq!(bals.take(block, Duration::ZERO).current.len(), 1);
    });
    let producer = Arc::clone(&bals);
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        insert(&producer, R1, 13, 13);
        std::thread::sleep(Duration::from_millis(10));
        insert(&producer, R1, 14, 14);
    });
    let started = std::time::Instant::now();
    let taken = bals.take(12, Duration::from_secs(2));
    assert!(
        taken.current.is_empty(),
        "the discarded result stays unverified"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "known eviction must not consume its wait window"
    );
    thread.join().unwrap();
}

#[test]
fn a_reinserted_evicted_result_is_still_checked() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(2).unwrap());
    (1..=3).for_each(|block| insert(&bals, R1, block, block));
    assert_eq!(bals.take(1, Duration::ZERO).current.len(), 1);
    insert(&bals, R2, 3, 42);
    assert_eq!(
        balances(&bals.take(3, Duration::ZERO)),
        [(U256::from(42), vec![R2])]
    );
}

#[test]
fn eviction_evidence_is_bounded_and_still_proves_an_older_backlog() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(8).unwrap());
    (0..=100).for_each(|block| insert(&bals, R1, block, block));
    {
        let inner = bals.core.inner.lock().unwrap();
        assert_eq!(inner.map.len(), 8);
        assert_eq!(inner.evicted.len(), 8);
    }
    (0..8).for_each(|block| {
        assert_eq!(bals.take(block, Duration::ZERO).current.len(), 1);
    });
    let started = std::time::Instant::now();
    assert!(bals.take(20, Duration::from_secs(2)).current.is_empty());
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_withdrawal_forgets_evictions_and_waits_for_replacement_data() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(2).unwrap());
    (1..=3).for_each(|block| insert(&bals, R1, block, block));
    bals.take(1, Duration::ZERO);
    bals.withdraw(|_, _, _| false);
    let producer = Arc::clone(&bals);
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        insert(&producer, R2, 3, 42);
    });
    assert_eq!(
        balances(&bals.take(3, Duration::from_secs(2))),
        [(U256::from(42), vec![R2])]
    );
    thread.join().unwrap();
}

#[test]
fn one_evicted_key_far_ahead_does_not_skip_a_live_result() {
    let bals = BalBuffer::with_cap(NonZeroUsize::MIN);
    insert(&bals, R1, 0, 0);
    insert(&bals, R1, 10_000, 10_000);
    let producer = Arc::clone(&bals);
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        insert(&producer, R2, 1, 42);
    });
    assert_eq!(
        balances(&bals.take(1, Duration::from_secs(2))),
        [(U256::from(42), vec![R2])]
    );
    thread.join().unwrap();
}
