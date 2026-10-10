//! The L1 watcher's pass over the `l1_blocks` records, driven through the
//! crate's public API with the `testing`-feature fakes: the anchor, the
//! publish of each record's epoch, the copies of the second follower
//! instance, a gap, a broken parent link, a second hash for one block,
//! and a record no archive holds.
//!
//! No test here raises the process halt.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_da_watcher::feed::fakes::{ScriptedFeed, ScriptedStream};
use kardamom_da_watcher::publisher::fakes::{InMemoryEpochPublisher, PublisherTap};
use kardamom_da_watcher::{
    DaWatcherConfig, L1ResumeAfter, L1Watcher, MonitorError, ResumeAfterError,
};
use kardamom_obs::halt::{Clears, HaltCause};
use kardamom_types::epoch::{DepositLog, LockboxLog, UpgradeLog, source_hash};

const TICK: Duration = Duration::from_millis(5);

/// A watcher over `stream`, resumed after `after` when given, with no
/// cursor file. The test keeps the publisher's tap.
fn watcher(
    stream: &ScriptedStream,
    after: Option<u64>,
) -> (
    L1Watcher<ScriptedFeed, InMemoryEpochPublisher>,
    PublisherTap,
) {
    let (publisher, tap) = InMemoryEpochPublisher::new();
    let config = DaWatcherConfig {
        tick: TICK,
        silence: Duration::from_secs(3600),
        resume_after: after
            .map(|b| L1ResumeAfter::from(NonZeroU64::new(b).expect("a test block is not 0"))),
    };
    (L1Watcher::new(publisher, stream.feed(), config, None), tap)
}

fn numbers(tap: &PublisherTap) -> Vec<u64> {
    tap.epochs().iter().map(|e| e.l1_number).collect()
}

fn deposit(number: u64, log_index: u64) -> LockboxLog {
    LockboxLog::Deposit(DepositLog {
        block_number: number,
        block_hash: ScriptedStream::hash(number),
        log_index,
        from: Address::repeat_byte(0x11),
        to: Address::repeat_byte(0x22),
        mint: 1_000,
        gas_limit: 200_000,
        data: alloy_primitives::Bytes::new(),
    })
}

fn upgrade(number: u64, log_index: u64) -> LockboxLog {
    LockboxLog::Upgrade(UpgradeLog {
        block_number: number,
        block_hash: ScriptedStream::hash(number),
        log_index,
        feature_id: alloy_primitives::U256::from(3u64),
        activation_timestamp: 9,
    })
}

/// The first record of a watcher with no position anchors it: nothing
/// before it is published, and its own epoch is not either.
#[tokio::test]
async fn the_first_record_anchors_the_watcher() {
    let stream = ScriptedStream::default();
    stream.tips(&[100]);
    let (mut w, tap) = watcher(&stream, None);
    assert_eq!(w.process_once().await.unwrap(), 0);
    assert_eq!(w.cursor(), Some(100));
    assert!(tap.epochs().is_empty());
}

/// The resume block is parsed once, at the flag. Block 0 names no epoch,
/// so it is refused, and the operator omits the flag instead.
#[test]
fn a_resume_block_is_a_nonzero_l1_block_number() {
    let parsed: L1ResumeAfter = "41".parse().unwrap();
    assert_eq!(parsed.block(), 41);
    assert_eq!("0".parse::<L1ResumeAfter>(), Err(ResumeAfterError::Zero));
    assert!(matches!(
        "-1".parse::<L1ResumeAfter>(),
        Err(ResumeAfterError::NotANumber(_))
    ));
}

/// A watcher that resumes after a block takes the block's hash from its
/// record and publishes every block after it, from the archives and then
/// live, with no gap and no repeat.
#[tokio::test]
async fn a_resumed_watcher_publishes_every_block_after_the_resume_block() {
    let stream = ScriptedStream::default();
    stream.archive_up_to(108);
    stream.tips(&[110]);
    let (mut w, tap) = watcher(&stream, Some(100));
    assert_eq!(w.process_once().await.unwrap(), 10);
    assert_eq!(numbers(&tap), (101..=110).collect::<Vec<_>>());
}

/// The watcher publishes each record's epoch unchanged: the follower
/// derived it, with its deposits and upgrades in log order. A block with
/// no deposit still has its epoch.
#[tokio::test]
async fn each_record_s_epoch_is_published_unchanged() {
    let stream = ScriptedStream::default();
    stream.set_logs(102, vec![deposit(102, 0), upgrade(102, 1), deposit(102, 2)]);
    stream.tips(&[100, 104]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    assert_eq!(w.process_once().await.unwrap(), 4);
    let epochs = tap.epochs();
    let expected: Vec<_> = (101..=104).map(|n| stream.epoch(n)).collect();
    assert_eq!(epochs, expected);
    assert_eq!(epochs[1].deposits.len(), 3);
    assert_eq!(
        epochs[1].deposits[0].source_hash,
        source_hash(ScriptedStream::hash(102), 0)
    );
    assert!(epochs[1].deposits[1].is_system_transaction);
    assert!(epochs[0].deposits.is_empty() && epochs[3].deposits.is_empty());
}

/// The second follower instance publishes the same records: the copies
/// are dropped, each epoch is published once.
#[tokio::test]
async fn the_second_instance_s_copies_are_dropped() {
    let stream = ScriptedStream::default();
    stream.tips(&[100, 103]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    w.process_once().await.unwrap();
    (101..=103).for_each(|n| stream.inject(stream.record(n)));
    stream.tips(&[104]);
    assert_eq!(w.process_once().await.unwrap(), 1);
    assert_eq!(numbers(&tap), [101, 102, 103, 104]);
}

/// A record past the next block is a gap on the subscription: the watcher
/// reads the missing records from the archives, in order.
#[tokio::test]
async fn a_gap_on_the_live_stream_is_filled_from_the_archives() {
    let stream = ScriptedStream::default();
    stream.tips(&[100]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    stream.archive_up_to(120);
    stream.inject(stream.record(121));
    assert_eq!(w.process_once().await.unwrap(), 0, "the gap is seen");
    tokio::time::sleep(TICK * 2).await;
    stream.archive_up_to(121);
    assert_eq!(w.process_once().await.unwrap(), 21);
    assert_eq!(numbers(&tap), (101..=121).collect::<Vec<_>>());
}

/// A record whose parent is not the head is a chain break: the watcher
/// publishes nothing, keeps its head, and halts on `l1_chain_break`,
/// which clears by itself once a record that descends arrives.
#[tokio::test]
async fn a_record_that_does_not_descend_from_the_head_is_a_chain_break() {
    let stream = ScriptedStream::default();
    stream.tips(&[100]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    let mut lie = stream.record(101);
    lie.parent_hash = B256::repeat_byte(0xEE);
    stream.inject(lie);
    let err = w.process_once().await.unwrap_err();
    assert!(
        matches!(err, MonitorError::ChainBreak { number: 101, parent, .. } if parent == B256::repeat_byte(0xEE)),
        "got {err}"
    );
    let halt = err.halt().unwrap();
    assert_eq!(halt.cause, HaltCause::L1ChainBreak);
    assert_eq!(halt.clears, Clears::Auto);
    assert!(tap.epochs().is_empty());
    assert_eq!(w.cursor(), Some(100));

    tokio::time::sleep(TICK * 2).await;
    stream.archive_up_to(101);
    assert_eq!(
        w.process_once().await.unwrap(),
        1,
        "the archive's record descends"
    );
}

/// Two records of one block with different hashes halt the watcher on
/// `l1_follower_disagreement`, which an operator clears. The first record
/// was published; the watcher takes no record after the second.
#[tokio::test]
async fn a_second_hash_for_one_block_halts_until_an_operator_clears_it() {
    let stream = ScriptedStream::default();
    stream.tips(&[100, 101]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    w.process_once().await.unwrap();
    let mut lie = stream.record(101);
    lie.hash = B256::repeat_byte(0xAB);
    stream.inject(lie);
    let err = w.process_once().await.unwrap_err();
    assert!(
        matches!(err, MonitorError::Disagreement { number: 101, second, .. } if second == B256::repeat_byte(0xAB)),
        "got {err}"
    );
    let halt = err.halt().unwrap();
    assert_eq!(halt.cause, HaltCause::L1FollowerDisagreement);
    assert_eq!(halt.clears, Clears::Operator);
    stream.tips(&[103]);
    assert_eq!(w.process_once().await.unwrap(), 0, "held until the clear");
    assert_eq!(numbers(&tap), [101]);
}

/// A second record of a published block with the same hash and another
/// epoch is the same halt: the follower instances disagree on the content.
#[tokio::test]
async fn a_second_epoch_for_one_block_halts_too() {
    let stream = ScriptedStream::default();
    stream.tips(&[100, 101]);
    let (mut w, _tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    w.process_once().await.unwrap();
    let mut lie = stream.record(101);
    lie.epoch.deposits.push(kardamom_types::Deposit::default());
    stream.inject(lie);
    let err = w.process_once().await.unwrap_err();
    assert!(
        matches!(err, MonitorError::ContentDisagreement { number: 101 }),
        "got {err}"
    );
    assert_eq!(err.halt().unwrap().cause, HaltCause::L1FollowerDisagreement);
}

/// With no record of the resume block on the stream or in an archive, the
/// watcher waits: it publishes nothing and stays before the block. It
/// resumes once an archive holds the record.
#[tokio::test]
async fn a_resume_block_no_archive_holds_is_waited_for() {
    let stream = ScriptedStream::default();
    stream.archive_down(true);
    stream.tips(&[105]);
    let (mut w, tap) = watcher(&stream, Some(100));
    assert_eq!(w.process_once().await.unwrap(), 0);
    assert_eq!(w.cursor(), Some(100));
    assert!(w.confirmed().is_none(), "not anchored without the record");

    stream.archive_down(false);
    tokio::time::sleep(TICK * 2).await;
    assert_eq!(w.process_once().await.unwrap(), 5);
    assert_eq!(numbers(&tap), (101..=105).collect::<Vec<_>>());
}

/// A publish the transport refuses holds the record; the next pass after
/// a tick publishes it, with no gap and no repeat.
#[tokio::test]
async fn a_refused_publish_holds_the_record_and_retries_after_a_tick() {
    let stream = ScriptedStream::default();
    stream.tips(&[100, 103]);
    let (mut w, tap) = watcher(&stream, None);
    w.process_once().await.unwrap();
    tap.set_backpressure(true);
    assert_eq!(w.process_once().await.unwrap(), 0);
    assert_eq!(w.cursor(), Some(100));
    tap.set_backpressure(false);
    assert_eq!(w.process_once().await.unwrap(), 0, "not before the retry");
    tokio::time::sleep(TICK * 2).await;
    assert_eq!(w.process_once().await.unwrap(), 3);
    assert_eq!(numbers(&tap), [101, 102, 103]);
}
