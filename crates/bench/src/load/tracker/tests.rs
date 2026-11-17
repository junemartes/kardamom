use super::*;

use crate::signers::SignerSet;

/// Every accessor reads through the poison-tolerant `lock()` helper,
/// so a poisoned mutex still records the sample.
#[test]
fn confirm_with_gas_records_through_a_poisoned_mutex() {
    let tracker = Tracker::new().expect("tracker");
    // Poison the latency mutex, the way a panicking submit task would.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = tracker.lat_us.lock().unwrap();
        panic!("simulated submit-task panic while holding the lock");
    }));
    assert!(tracker.lat_us.is_poisoned());

    tracker.confirm(
        B256::ZERO,
        &ReceiptStatus::ok(21_000),
        Duration::from_micros(500),
    );

    let lat = tracker.latency_us();
    assert!(lat.p50 > 0, "sample was dropped: p50 is 0");
    assert!(lat.max > 0, "sample was dropped: max is 0");
}

/// A tracker that expects two senders with two nonces each, and
/// the hash of `(sender, nonce)`. Sender 0 signs swaps, and sender 1
/// signs transfers.
fn expecting() -> (Tracker, SignerSet, impl Fn(usize, u64) -> B256) {
    let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 2).expect("signers");
    let hash = |sender: usize, nonce: u64| {
        let slot = u8::try_from(sender).expect("small") * 10 + u8::try_from(nonce).expect("small");
        B256::with_last_byte(slot)
    };
    let planned: Vec<PlannedTx> = (0..2)
        .flat_map(|s| (0..2).map(move |n| (s, n)))
        .map(|(sender, nonce)| PlannedTx {
            raw: alloy_primitives::Bytes::new(),
            hash: hash(sender, nonce),
            sender,
            nonce,
            class: if sender == 0 {
                TxClass::Swap
            } else {
                TxClass::Transfer
            },
        })
        .collect();
    let mut tracker = Tracker::new().expect("tracker");
    tracker.expect(&signers, planned.iter());
    (tracker, signers, hash)
}

fn mined(from: Address, block: u64) -> ReceiptStatus {
    ReceiptStatus {
        status: 1,
        gas: 21_000,
        from: Some(from),
        block: Some(block),
    }
}

#[test]
fn a_receipt_of_another_sender_or_without_a_block_is_a_bad_receipt() {
    let (tracker, signers, hash) = expecting();
    let latency = Duration::from_millis(1);
    tracker.confirm(hash(0, 0), &mined(signers[1].address, 5), latency);
    tracker.confirm(hash(0, 1), &ReceiptStatus::ok(21_000), latency);
    tracker.confirm(hash(1, 0), &mined(signers[1].address, 5), latency);
    let c = tracker.counts();
    assert_eq!((c.receipted, c.bad_status, c.bad_receipt), (3, 0, 2));
}

#[test]
fn a_lower_nonce_in_a_later_block_is_a_bad_receipt_whichever_confirms_first() {
    let (tracker, signers, hash) = expecting();
    let latency = Duration::from_millis(1);
    let me = signers[0].address;
    tracker.confirm(hash(0, 1), &mined(me, 7), latency);
    tracker.confirm(hash(0, 0), &mined(me, 9), latency);
    assert_eq!(tracker.counts().bad_receipt, 1, "nonce 0 after nonce 1");
    let other = signers[1].address;
    tracker.confirm(hash(1, 0), &mined(other, 9), latency);
    tracker.confirm(hash(1, 1), &mined(other, 9), latency);
    assert_eq!(
        tracker.counts().bad_receipt,
        1,
        "one block holds both nonces"
    );
}

#[test]
fn a_skip_receipt_takes_no_place_in_the_nonce_order() {
    let (tracker, signers, hash) = expecting();
    let latency = Duration::from_millis(1);
    let me = signers[0].address;
    let skip = ReceiptStatus {
        status: 0,
        gas: 0,
        ..mined(me, 9)
    };
    tracker.confirm(hash(0, 0), &skip, latency);
    tracker.confirm(hash(0, 1), &mined(me, 7), latency);
    let c = tracker.counts();
    assert_eq!((c.bad_status, c.bad_receipt), (1, 0));
}

#[test]
fn a_hash_without_an_expectation_is_never_checked() {
    let (tracker, _, _) = expecting();
    tracker.confirm(
        B256::repeat_byte(0xEE),
        &ReceiptStatus::ok(0),
        Duration::ZERO,
    );
    assert_eq!(tracker.counts().bad_receipt, 0);
}

#[test]
fn the_feed_path_checks_the_receipt_on_both_sides_of_the_race() {
    let (tracker, signers, hash) = expecting();
    let wrong = mined(signers[1].address, 3);
    // The feed first, then the submit task registers.
    tracker.confirm_from_feed(hash(0, 0), &wrong);
    tracker.await_feed(hash(0, 0), Instant::now());
    // The submit task first, then the feed.
    tracker.await_feed(hash(0, 1), Instant::now());
    tracker.confirm_from_feed(hash(0, 1), &wrong);
    let c = tracker.counts();
    assert_eq!((c.receipted, c.bad_receipt), (2, 2));
    assert_eq!(tracker.pending_len(), 0);
}

#[test]
fn each_class_counts_its_own_receipts_failures_and_latency() {
    let (tracker, signers, hash) = expecting();
    let swapper = signers[0].address;
    let payer = signers[1].address;
    let failed = ReceiptStatus {
        status: 0,
        ..mined(swapper, 4)
    };
    tracker.confirm(hash(0, 0), &mined(swapper, 3), Duration::from_micros(900));
    tracker.confirm(hash(0, 1), &failed, Duration::from_micros(700));
    tracker.confirm(hash(1, 0), &mined(payer, 3), Duration::from_micros(300));
    tracker.confirm(
        B256::repeat_byte(0xEE),
        &ReceiptStatus::ok(0),
        Duration::from_micros(5),
    );
    let classes = tracker.class_report();
    assert_eq!(
        classes.keys().copied().collect::<Vec<_>>(),
        [TxClass::Transfer, TxClass::Swap],
        "a class with no receipt and a hash with no expectation add no entry"
    );
    let swap = classes[&TxClass::Swap];
    assert_eq!((swap.receipted, swap.failed), (2, 1));
    assert!((700..=901).contains(&swap.lat_max_us), "{swap:?}");
    let transfer = classes[&TxClass::Transfer];
    assert_eq!((transfer.receipted, transfer.failed), (1, 0));
    assert!((299..=301).contains(&transfer.lat_p99_us), "{transfer:?}");
    assert_eq!(tracker.counts().bad_status, 1);
}
