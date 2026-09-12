//! Integration tests for the L1 watcher's `process_once` pass, driven
//! through the crate's public API with the `testing`-feature fakes.

use kardamom_da_watcher::publisher::fakes::InMemoryEpochPublisher;
use kardamom_da_watcher::source::fakes::MockL1Source;
use kardamom_da_watcher::{L1SourceError, LockboxLog, MonitorError, process_once};

use alloy_primitives::U256;
use alloy_primitives::{Address, B256, address};
use kardamom_types::epoch::{
    DepositLog, UpgradeLog, alias_l1_address, source_hash, source_hash_system,
};

fn lockbox() -> Address {
    address!("0000000000000000000000000000000000C0DE01")
}

/// A log in L1 block `number`, whose hash is the mock's filler for that
/// number. This is the hash the watcher will fetch on its own.
fn dep_log(number: u64, log_index: u64, mint: u128) -> LockboxLog {
    LockboxLog::Deposit(DepositLog {
        block_number: number,
        block_hash: MockL1Source::filler_hash(number),
        log_index,
        from: Address::repeat_byte(0x11),
        to: Address::repeat_byte(0x22),
        mint,
        gas_limit: 200_000,
        data: alloy_primitives::Bytes::new(),
    })
}

/// An upgrade-transaction log in L1 block `number`.
fn upg_log(number: u64, log_index: u64, feature: u64, activation: u64) -> LockboxLog {
    LockboxLog::Upgrade(UpgradeLog {
        block_number: number,
        block_hash: MockL1Source::filler_hash(number),
        log_index,
        feature_id: alloy_primitives::U256::from(feature),
        activation_timestamp: activation,
    })
}

#[tokio::test]
async fn seed_call_returns_zero_and_advances_cursor() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(100));
    let mut cursor = None;
    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(cursor, Some(100));
    assert!(pub_.published.lock().unwrap().is_empty());
}

/// The upgrade transaction rides the deposit path end to end: the same
/// query, the same epoch, the same publish. If it ever needed its own
/// plumbing, the sealer and slot accounting would need changes too.
#[tokio::test]
async fn an_upgrade_log_becomes_a_system_deposit_in_its_epoch() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(201));
    src.push_logs(Ok(vec![upg_log(201, 0, 1, 0)]));
    let mut cursor = Some(200);

    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();

    assert_eq!(n, 1);
    let v = pub_.published.lock().unwrap();
    assert_eq!(v[0].deposits.len(), 1);
    let d = &v[0].deposits[0];
    assert!(d.is_system_transaction);
    assert_eq!(d.from, kardamom_types::upgrades::SYSTEM_UPGRADER);
    assert_eq!(d.to, Some(kardamom_types::upgrades::CHAIN_STATE));
    assert_eq!(
        d.source_hash,
        source_hash_system(MockL1Source::filler_hash(201), 0)
    );
    assert_eq!(d.mint, 0);
}

/// Deposits and upgrades arrive on one topic-filtered query, so they
/// must stay in L1 log order within the epoch. An upgrade must not
/// overtake a deposit that L1 sequenced first.
#[tokio::test]
async fn deposits_and_upgrades_share_one_epoch_in_log_order() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(301));
    // Pushed out of order on purpose. The rule sorts by log_index.
    src.push_logs(Ok(vec![
        dep_log(301, 2, 500),
        upg_log(301, 1, 7, 1_700_000_000_250),
        dep_log(301, 0, 100),
    ]));
    let mut cursor = Some(300);

    process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();

    let v = pub_.published.lock().unwrap();
    let kinds: Vec<bool> = v[0]
        .deposits
        .iter()
        .map(|d| d.is_system_transaction)
        .collect();
    assert_eq!(
        kinds,
        vec![false, true, false],
        "epoch must follow L1 log order, not arrival order"
    );
    assert_eq!(v[0].deposits[0].mint, 100);
    assert_eq!(v[0].deposits[2].mint, 500);
}

#[tokio::test]
async fn one_epoch_per_l1_block_with_deposits_grouped_by_block() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(153));
    // A single range query spans three blocks: two deposits in 151,
    // none in 152, one in 153.
    src.push_logs(Ok(vec![
        dep_log(151, 0, 100),
        dep_log(151, 1, 200),
        dep_log(153, 5, 300),
    ]));
    let mut cursor = Some(150);

    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();

    assert_eq!(n, 3, "one epoch per block in (150, 153]");
    assert_eq!(cursor, Some(153));
    let v = pub_.published.lock().unwrap();
    assert_eq!(
        v.iter().map(|e| e.l1_number).collect::<Vec<_>>(),
        vec![151, 152, 153],
        "epochs are emitted in L1 order with no gaps"
    );
    assert_eq!(v[0].deposits.len(), 2);
    assert!(v[1].deposits.is_empty(), "block 152 had no deposits");
    assert_eq!(v[2].deposits.len(), 1);

    // Deposit content is derived, not passed through: the sender is
    // aliased, and source_hash comes from the L1 (block_hash, log_index).
    assert_eq!(
        v[0].deposits[0].from,
        alias_l1_address(Address::repeat_byte(0x11))
    );
    assert_eq!(
        v[0].deposits[0].source_hash,
        source_hash(MockL1Source::filler_hash(151), 0)
    );
    assert_eq!(v[0].deposits[0].to, Some(Address::repeat_byte(0x22)));
    assert_eq!(v[0].deposits[0].mint, 100);
    assert_eq!(v[0].deposits[0].value, U256::from(100u64));
}

/// The no-skipping rule in miniature: a stretch of L1 with no deposits
/// at all must still produce one epoch per block. Otherwise, the origin
/// sequence gets a hole that a verifier would reject.
#[tokio::test]
async fn depositless_range_still_emits_every_epoch() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(105));
    src.push_logs(Ok(vec![]));
    let mut cursor = Some(100);

    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();

    assert_eq!(n, 5);
    let v = pub_.published.lock().unwrap();
    assert_eq!(
        v.iter().map(|e| e.l1_number).collect::<Vec<_>>(),
        vec![101, 102, 103, 104, 105]
    );
    assert!(v.iter().all(|e| e.deposits.is_empty()));
}

#[tokio::test]
async fn not_finalized_surfaces_distinct_error_no_cursor_advance() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Err(L1SourceError::NotFinalized));
    let mut cursor = None;
    let err = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::NotFinalized));
    assert!(cursor.is_none());
}

#[tokio::test]
async fn tip_below_cursor_is_noop() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(50));
    let mut cursor = Some(100);
    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(cursor, Some(100));
}

#[tokio::test]
async fn backpressure_holds_cursor_so_next_tick_retries() {
    let pub_ = InMemoryEpochPublisher::default();
    *pub_.fail_with_backpressure.lock().unwrap() = true;
    let src = MockL1Source::new();
    src.push_tip(Ok(200));
    src.push_logs(Ok(vec![dep_log(200, 0, 100)]));
    let mut cursor = Some(150);
    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 0); // The first publish was backpressured, so the loop returned early.
    assert_eq!(cursor, Some(150)); // The cursor did not advance.
}

/// Once `published` reaches 2 entries, trip `flag` and report done.
fn trip_backpressure_once_published(
    published: &std::sync::Arc<std::sync::Mutex<Vec<kardamom_types::EpochRecord>>>,
    flag: &std::sync::Arc<std::sync::Mutex<bool>>,
) -> bool {
    if published.lock().unwrap().len() < 2 {
        return false;
    }
    *flag.lock().unwrap() = true;
    true
}

/// A partially published range must resume at the first block that did
/// not publish. It must not re-emit the blocks that did publish, and
/// must not skip past them.
#[tokio::test]
async fn partial_range_resumes_at_the_first_unpublished_block() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(104));
    src.push_logs(Ok(vec![]));
    let mut cursor = Some(100);

    // Let 101 and 102 through, then jam the transport.
    let flag = pub_.fail_with_backpressure.clone();
    let published = pub_.published.clone();
    std::thread::spawn(move || {
        let _ = kardamom_obs::testkit::poll_sync(
            "backpressure trip after 2 published",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_millis(1),
            || Ok(trip_backpressure_once_published(&published, &flag).then_some(())),
        );
    });
    let n = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap();

    assert!((1..=4).contains(&n));
    // Whatever it managed, the cursor names the last block it published.
    assert_eq!(cursor, Some(100 + n as u64));
}

/// A log whose block hash disagrees with the hash fetched for that block
/// number means the two reads saw different L1s. Publishing that epoch
/// would bake a wrong `source_hash` into the chain.
#[tokio::test]
async fn log_disagreeing_with_the_block_hash_is_rejected() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(151));
    src.push_logs(Ok(vec![LockboxLog::Deposit(DepositLog {
        block_number: 151,
        block_hash: B256::repeat_byte(0xFF), // not the filler for 151
        log_index: 0,
        from: Address::repeat_byte(0x11),
        to: Address::repeat_byte(0x22),
        mint: 1,
        gas_limit: 100,
        data: alloy_primitives::Bytes::new(),
    })]));
    let mut cursor = Some(150);

    let err = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap_err();

    assert!(matches!(err, MonitorError::Derive(_)));
    assert_eq!(cursor, Some(150), "cursor must not pass a bad epoch");
    assert!(pub_.published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn block_hash_failure_stops_the_range() {
    let pub_ = InMemoryEpochPublisher::default();
    let src = MockL1Source::new();
    src.push_tip(Ok(151));
    src.push_logs(Ok(vec![]));
    *src.block_hash_fails.lock().unwrap() = true;
    let mut cursor = Some(150);

    let err = process_once(&pub_, &src, lockbox(), &mut cursor)
        .await
        .unwrap_err();

    assert!(matches!(err, MonitorError::BlockHash(_)));
    assert_eq!(cursor, Some(150));
}
