use super::*;
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

/// The balances of the BALs in `taken`, in arrival order.
fn balances(taken: &Taken<u64, BlockDelta>) -> Vec<U256> {
    taken
        .current
        .iter()
        .map(|a| a.value.accounts[0].balance)
        .collect()
}

// The receipt buffer mirrors the BAL catch-up skip. When the buffered
// head is far ahead of the requested tx_idx, the receipt has aged out
// of the live stream, and take() must return at once instead of
// blocking the commit thread for the full wait per historical tx.
#[test]
fn receipt_take_skips_aged_out_backlog_immediately() {
    let buf = ReceiptBuffer::new();
    buf.insert(R1, receipt(10_000, true, 21_000, 0xab)); // This is the live head, far ahead.
    let start = std::time::Instant::now();
    let got = buf.take(BPosition::from_index(0), Duration::from_secs(5));
    assert!(got.current.is_empty(), "aged-out receipt must be skipped");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "skip must not consume the wait window: {:?}",
        start.elapsed()
    );
}

// A second replica's result for a key joins the first one. It never
// replaces it, so a wrong result that arrives first is still handed to
// the consumer.
#[test]
fn every_replica_result_is_kept_in_arrival_order() {
    let bals = BalBuffer::new();
    bals.insert(R1, delta(1, 999));
    bals.insert(R2, delta(1, 100));
    let taken = bals.take(1, Duration::from_millis(10));
    assert_eq!(balances(&taken), vec![U256::from(999), U256::from(100)]);
    assert_eq!(
        taken.current.iter().map(|a| a.replica).collect::<Vec<_>>(),
        vec![R1, R2]
    );
}

// Each replica's result is kept once. A repeat of the same replica for
// the key is dropped.
#[test]
fn a_repeat_of_one_replica_is_dropped() {
    let bals = BalBuffer::new();
    bals.insert(R1, delta(1, 100));
    bals.insert(R1, delta(1, 999));
    let taken = bals.take(1, Duration::from_millis(10));
    assert_eq!(balances(&taken), vec![U256::from(100)]);
}

// A result that arrives after the take of its key, but inside the check
// window, goes to the consumer with the next take. A result below the
// window is dropped, so it cannot leak.
#[test]
fn late_arrivals_are_handed_over_or_dropped() {
    let bals = BalBuffer::new();
    assert!(bals.take(100, Duration::from_millis(10)).current.is_empty());
    // Below the window (100 - 64): dropped at once.
    bals.insert(R1, delta(3, 100));
    assert_eq!(bals.len(), 0, "an insert below the window must be dropped");
    // Inside the window: kept, and handed over as late.
    bals.insert(R2, delta(99, 100));
    bals.insert(R1, delta(101, 100));
    let taken = bals.take(101, Duration::from_millis(10));
    assert_eq!(taken.current.len(), 1);
    assert_eq!(
        taken
            .late
            .iter()
            .map(|(block, a)| (*block, a.replica))
            .collect::<Vec<_>>(),
        vec![(99, R2)]
    );
    assert_eq!(bals.len(), 0, "a take leaves nothing below its key");
}

// The buffer is bounded, so a stalled consumer cannot make it hold the
// whole live stream in RAM. The oldest key is evicted first, which can
// only leave a block unverified, never cause a false divergence. Each
// key holds at most `MAX_REPLICAS` results.
#[test]
fn buffer_is_bounded_in_keys_and_replicas() {
    let bals = BalBuffer::with_cap(NonZeroUsize::new(3).expect("fixture cap"));
    (1..=5u64).for_each(|b| bals.insert(R1, delta(b, 100)));
    assert_eq!(bals.len(), 3);
    // Blocks 1 and 2 are evicted; blocks 3 through 5 are kept.
    assert_eq!(bals.take(3, Duration::from_millis(10)).current.len(), 1);
    assert_eq!(bals.take(4, Duration::from_millis(10)).current.len(), 1);
    (0..20i32).for_each(|s| bals.insert(ReplicaId::from_session(s), delta(5, 100)));
    // R1 arrived first; seven more replicas fit, the rest are dropped.
    assert_eq!(
        bals.take(5, Duration::from_millis(10)).current.len(),
        MAX_REPLICAS
    );
}

#[test]
fn buffers_block_until_value_arrives() {
    // take() must return promptly once another thread inserts a value.
    // This covers the handoff from the Aeron task to the exec thread.
    let bals = BalBuffer::new();
    let bals2 = bals.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        bals2.insert(R1, delta(7, 5));
    });
    let got = bals.take(7, Duration::from_secs(2));
    assert_eq!(got.current[0].value.block_number, 7);
    h.join().unwrap();
}
