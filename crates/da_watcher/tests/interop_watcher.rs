//! Integration tests for the interop watcher's stream-driven loop, driven
//! through the crate's public API with the `test-support`-feature fakes.

use std::num::NonZeroU32;
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes};
use kardamom_types::xchain::{
    Anchor, OutboxMessage, RemoteEpochRecord, XChainError, derive_remote_epoch, remote_source_hash,
};

use kardamom_da_watcher::interop::mock::MockInteropFeed;
use kardamom_da_watcher::interop::publisher::fakes::InMemoryRemoteEpochPublisher;
use kardamom_da_watcher::interop::source::WsRemoteChainSource;
use kardamom_da_watcher::interop::{
    CursorFile, InteropError, InteropWatcherConfig, RemoteChainSource, process_once, spawn,
};
use kardamom_da_watcher::watcher::WatcherHandle;

const SELF: u64 = 412_347;
const ORIGIN: u64 = 412_346;

fn msg(seq: u64, block: u64) -> OutboxMessage {
    OutboxMessage {
        origin_block_number: block,
        origin_block_hash: Anchor {
            origin_chain_id: ORIGIN,
            block_number: block,
        }
        .hash(),
        dest_chain_id: SELF,
        seq,
        sender: Address::repeat_byte(0xA1),
        target: Address::repeat_byte(0xB2),
        value: 0,
        gas_limit: 200_000,
        data: Bytes::from_static(&[0xCA, 0xFE]),
        callback: None,
    }
}

/// The watcher under test, wired to a real WebSocket against the scripted
/// feed — the transport in the test is the transport that ships.
fn spawn_against(feed: &MockInteropFeed) -> (InMemoryRemoteEpochPublisher, WatcherHandle) {
    let publisher = InMemoryRemoteEpochPublisher::default();
    let handle = spawn_resuming(feed, publisher.clone(), 0, None);
    (publisher, handle)
}

/// [`spawn_against`] with an explicit resume position and (optionally) a
/// durable cursor — the restart-shaped variant.
fn spawn_resuming(
    feed: &MockInteropFeed,
    publisher: InMemoryRemoteEpochPublisher,
    start_seq: u64,
    cursor_file: Option<CursorFile>,
) -> WatcherHandle {
    let source = WsRemoteChainSource::new(ORIGIN, SELF, feed.url())
        .with_reconnect(Duration::from_millis(20), NonZeroU32::new(50).unwrap());
    spawn(
        publisher,
        source,
        InteropWatcherConfig {
            self_chain_id: SELF,
            start_seq,
            retry_interval: Duration::from_millis(20),
        },
        cursor_file,
    )
}

async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
    kardamom_obs::testkit::poll_until(
        what,
        Duration::from_secs(10),
        Duration::from_millis(5),
        async || Ok(cond().then_some(())),
    )
    .await
    .unwrap();
}

/// The watcher halted of its own accord (fail-stop) rather than being
/// asked to. The shutdown sender is held alive across the await on
/// purpose: dropping it IS a shutdown request, which would make this
/// assertion pass for the wrong reason.
async fn assert_halted(handle: WatcherHandle) {
    let WatcherHandle { task, shutdown } = handle;
    let outcome = tokio::time::timeout(Duration::from_secs(10), task).await;
    drop(shutdown);
    outcome
        .expect("watcher should have halted on the derivation fault")
        .expect("watcher task must not panic");
}

/// A record the sink would not take must not advance the cursor: the next
/// pass re-derives the same batch, and re-derivation being byte-identical
/// is what makes the retry safe even if the first attempt did land.
#[tokio::test]
async fn a_declined_publish_holds_the_cursor() {
    use kardamom_da_watcher::interop::source::fakes::ScriptedRemoteSource;

    let publisher = InMemoryRemoteEpochPublisher::default();
    *publisher.fail_with_backpressure.lock().unwrap() = true;
    let mut source = ScriptedRemoteSource::new(ORIGIN);
    source.push_batch(Ok(vec![msg(0, 100)]));
    source.push_batch(Ok(vec![msg(0, 100)]));
    let mut cursor = 0u64;

    let n = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(cursor, 0, "cursor must not pass an unpublished record");

    *publisher.fail_with_backpressure.lock().unwrap() = false;
    let n = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(cursor, 1);
    assert_eq!(source.cursors, vec![0, 0], "the retry re-reads from 0");
}

/// A feed failure stalls the pair; it does not fault it. The distinction
/// is the whole per-pair posture: an unreachable peer recovers by itself,
/// a broken sequence does not.
#[tokio::test]
async fn a_feed_failure_is_retryable_not_a_fault() {
    use kardamom_da_watcher::interop::source::RemoteSourceError;
    use kardamom_da_watcher::interop::source::fakes::ScriptedRemoteSource;

    let publisher = InMemoryRemoteEpochPublisher::default();
    let mut source = ScriptedRemoteSource::new(ORIGIN);
    source.push_batch(Err(RemoteSourceError::Transport("peer down".into())));
    let mut cursor = 7u64;

    let err = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap_err();
    assert!(matches!(err, InteropError::Source(_)), "got {err:?}");
    assert_eq!(cursor, 7);
}

/// Messages group by ORIGIN BLOCK, not by arrival: three messages across
/// two blocks are two records, cut where the origin cut them.
#[tokio::test]
async fn one_record_per_origin_block() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    feed.push_message(msg(0, 100));
    feed.push_message(msg(1, 100));
    feed.push_message(msg(2, 101));
    // A block cannot be known complete until a later one appears, so the
    // sentinel is what closes block 101 — the tail-latency property the
    // source's module docs call out.
    feed.push_message(msg(3, 102));

    wait_until(|| publisher.records().len() >= 2, "two records").await;
    let _ = handle.shutdown.send(());

    let records = publisher.records();
    assert_eq!(records.len(), 2, "one record per origin block, no more");

    assert_eq!(records[0].anchor_number, 100);
    assert_eq!(records[0].first_seq, 0);
    assert_eq!(records[0].last_seq(), 1);
    assert_eq!(
        records[0]
            .messages
            .iter()
            .map(|m| m.source_hash)
            .collect::<Vec<_>>(),
        vec![remote_source_hash(ORIGIN, 0), remote_source_hash(ORIGIN, 1)],
    );

    assert_eq!(records[1].anchor_number, 101);
    assert_eq!(records[1].first_seq, 2);
    assert_eq!(records[1].last_seq(), 2);
    assert_eq!(
        records[1].messages.first().source_hash,
        remote_source_hash(ORIGIN, 2)
    );

    // The cursor is not directly observable; the record sequence is its
    // shadow — dense, contiguous, and starting where the last one ended.
    assert_eq!(records[0].last_seq() + 1, records[1].first_seq);
}

/// A feed that drops an item must stall the pair, not step over the hole.
#[tokio::test]
async fn a_feed_gap_halts_the_pair() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    feed.push_message(msg(0, 100));
    feed.gap_next(1);
    feed.push_message(msg(1, 100)); // swallowed by the feed
    feed.push_message(msg(2, 101));
    feed.push_message(msg(3, 102));

    assert_halted(handle).await;
    let records = publisher.records();
    assert_eq!(records.len(), 1, "only the pre-gap record may exist");
    assert_eq!(records[0].last_seq(), 0);

    // Nothing lands after the halt, even though the feed keeps serving.
    feed.push_message(msg(4, 103));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(publisher.records().len(), 1);
}

/// Two messages at one seq: an equivocating origin, not a retry.
#[tokio::test]
async fn a_duplicate_seq_halts_the_pair() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    feed.push_message(msg(0, 100));
    let mut twin = msg(0, 100);
    twin.target = Address::repeat_byte(0xEE);
    feed.push_message(twin);
    feed.push_message(msg(1, 101));

    assert_halted(handle).await;
    assert!(
        publisher.records().is_empty(),
        "the faulty batch must not be published"
    );
}

/// A message addressed to another chain is a fault of the feed, and
/// silently dropping it would hide that.
#[tokio::test]
async fn a_foreign_destination_halts_the_pair() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    let mut stray = msg(0, 100);
    stray.dest_chain_id = SELF + 1;
    feed.push_message(stray);
    feed.push_message(msg(1, 101));

    assert_halted(handle).await;
    assert!(publisher.records().is_empty());
}

/// A feed that serves its own anchor is a fault of the feed. The watcher
/// recomputes the anchor and stops the pair.
#[tokio::test]
async fn a_feed_chosen_anchor_halts_the_pair() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    let mut forged = msg(0, 100);
    forged.origin_block_hash = B256::repeat_byte(0xEE);
    feed.push_message(forged);
    feed.push_message(msg(1, 101));

    assert_halted(handle).await;
    assert!(
        publisher.records().is_empty(),
        "a batch with a forged anchor must not be published"
    );
}

/// A batch that spans two origin blocks is rejected by the shared rule.
/// The scripted source hands over such a batch directly.
#[tokio::test]
async fn a_multi_block_batch_is_a_fault() {
    use kardamom_da_watcher::interop::source::fakes::ScriptedRemoteSource;

    let publisher = InMemoryRemoteEpochPublisher::default();
    let mut source = ScriptedRemoteSource::new(ORIGIN);
    source.push_batch(Ok(vec![msg(0, 100), msg(1, 101)]));
    let mut cursor = 0u64;

    let err = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            InteropError::Derive(XChainError::MultiBlockBatch {
                first_block: 100,
                found_block: 101
            })
        ),
        "got {err:?}"
    );
    assert_eq!(cursor, 0);
    assert!(publisher.records().is_empty());
}

/// A dropped session must re-subscribe from the watcher's own cursor and
/// produce records the racing/uninterrupted run would also have produced —
/// equal `canonical_id`s, which is what lets cluster dedup collapse them.
#[tokio::test]
async fn a_reconnect_reproduces_byte_identical_records() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    feed.push_message(msg(0, 100));
    feed.push_message(msg(1, 100));
    wait_until(|| feed.subscription_count() >= 1, "the first subscription").await;

    // Drop the session mid-block: block 100 is open, its messages are
    // already streamed but not yet a record.
    feed.close_sessions();
    wait_until(|| feed.subscription_count() >= 2, "a re-subscription").await;

    feed.push_message(msg(2, 101));
    feed.push_message(msg(3, 102));
    wait_until(|| publisher.records().len() >= 2, "two records").await;
    let _ = handle.shutdown.send(());

    let records = publisher.records();
    assert_eq!(records.len(), 2, "the replay must not duplicate a record");

    let expected: Vec<RemoteEpochRecord> = vec![
        derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, 100), msg(1, 100)]).unwrap(),
        derive_remote_epoch(SELF, ORIGIN, 2, &[msg(2, 101)]).unwrap(),
    ];
    assert_eq!(records, expected, "records must be byte-identical");
    assert_eq!(
        records
            .iter()
            .map(kardamom_types::xchain::RemoteEpochRecord::canonical_id)
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(kardamom_types::xchain::RemoteEpochRecord::canonical_id)
            .collect::<Vec<_>>(),
    );
}

/// Restart continuity, through the DURABLE cursor: a watcher that
/// persisted its position and died resumes exactly where it stopped —
/// same records, no repeat, no hole — with the resume seq coming from
/// the file, not from the CLI seed (which deliberately lies here).
#[tokio::test]
async fn a_restart_resumes_exactly_from_the_persisted_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let cursor_path = dir.path().join("pair.cursor");

    let feed = MockInteropFeed::new(ORIGIN).await;
    let publisher = InMemoryRemoteEpochPublisher::default();
    let handle = spawn_resuming(
        &feed,
        publisher.clone(),
        0,
        Some(CursorFile::open(&cursor_path).unwrap()),
    );

    feed.push_message(msg(0, 100));
    feed.push_message(msg(1, 100));
    feed.push_message(msg(2, 101));
    feed.push_message(msg(3, 102)); // sentinel: closes 101
    wait_until(|| publisher.records().len() >= 2, "two records").await;
    let _ = handle.shutdown.send(());
    handle.task.await.unwrap();
    // The watcher's task has exited, so its `CursorFile` (and the lock it
    // holds) has dropped; reopening now is safe. The block scope ends
    // this reopened handle before the next spawn reopens the file.
    let resumed = {
        let cursor_file = CursorFile::open(&cursor_path).unwrap();
        let loaded = cursor_file.load().unwrap();
        assert_eq!(
            loaded,
            Some(3),
            "the persisted cursor must be one past the last PUBLISHED seq \
             (seq 3's block is still open, so it is not published yet)"
        );
        loaded.expect("cursor persisted")
    };

    // "Restart": a fresh watcher over the same publisher, seeded with a
    // deliberately wrong CLI value — the file must win.
    let handle = spawn_resuming(
        &feed,
        publisher.clone(),
        resumed,
        Some(CursorFile::open(&cursor_path).unwrap()),
    );
    feed.push_message(msg(4, 103)); // closes 102
    feed.push_message(msg(5, 104)); // closes 103
    wait_until(|| publisher.records().len() >= 4, "four records").await;
    let _ = handle.shutdown.send(());
    handle.task.await.unwrap();

    let seqs: Vec<u64> = publisher
        .records()
        .iter()
        .flat_map(|r| r.messages.iter().map(|m| m.seq))
        .collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2, 3, 4],
        "no loss, no repeat across the restart"
    );
    assert_eq!(
        publisher.deduped_count(),
        0,
        "a clean restart re-publishes nothing"
    );
    assert_eq!(
        CursorFile::open(&cursor_path).unwrap().load().unwrap(),
        Some(5)
    );
}

/// The crash window the write ordering exists for: die AFTER the publish,
/// BEFORE the persist. The restart resumes STALE, re-derives the same
/// batch, re-publishes it — and the duplicate is absorbed by dedup on
/// `canonical_id`, proving end to end that the stale side of the
/// asymmetry really is harmless.
#[tokio::test]
async fn a_crash_between_publish_and_persist_resumes_stale_and_dedup_absorbs() {
    use kardamom_da_watcher::interop::source::fakes::ScriptedRemoteSource;

    let dir = tempfile::tempdir().unwrap();
    let cursor_file = CursorFile::open(dir.path().join("pair.cursor")).unwrap();
    let publisher = InMemoryRemoteEpochPublisher::default();

    // First life: publish succeeds, then the process dies before the
    // persist. The block scope end is the crash: `source` drops with no
    // persist ever called.
    {
        let mut source = ScriptedRemoteSource::new(ORIGIN);
        source.push_batch(Ok(vec![msg(0, 100), msg(1, 100)]));
        let mut cursor = cursor_file.load().unwrap().unwrap_or(0);
        let n = process_once(&publisher, &mut source, SELF, &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(cursor, 2, "in-memory cursor advanced past the publish");
    }
    assert_eq!(cursor_file.load().unwrap(), None, "nothing durable yet");

    // Second life: resume from the (stale) durable state, which replays
    // the SAME feed prefix — byte-identical derivation by construction.
    let mut source = ScriptedRemoteSource::new(ORIGIN);
    source.push_batch(Ok(vec![msg(0, 100), msg(1, 100)]));
    source.push_batch(Ok(vec![msg(2, 101)]));
    let mut cursor = cursor_file.load().unwrap().unwrap_or(0);
    assert_eq!(cursor, 0, "resumed stale — the harmless side");
    let n = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap();
    assert_eq!(
        n, 1,
        "the re-publish is reported successful to the producer"
    );
    cursor_file.persist(cursor).unwrap();
    let n = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap();
    assert_eq!(n, 1);
    cursor_file.persist(cursor).unwrap();

    // The duplicate was absorbed, not executed twice: one copy of each
    // record, one dedup hit, and the lane is dense.
    let records = publisher.records();
    assert_eq!(
        records.len(),
        2,
        "the replayed record must not appear twice"
    );
    assert_eq!(
        publisher.deduped_count(),
        1,
        "the replay was absorbed by canonical_id dedup"
    );
    assert_eq!(records[0].first_seq, 0);
    assert_eq!(records[0].last_seq(), 1);
    assert_eq!(records[1].first_seq, 2);
    assert_eq!(cursor_file.load().unwrap(), Some(3));
}

/// `Lagged` says the feed cannot serve our cursor. Reading on would
/// skip, and a re-subscribe gets the same answer: the pair halts, and
/// nothing past the floor is published.
#[tokio::test]
async fn a_lagged_marker_halts_the_pair() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    feed.push_message(msg(0, 100));
    feed.push_message(msg(1, 100));
    feed.push_message(msg(2, 101));
    feed.push_message(msg(3, 102));
    // Seqs 0-1 aged out of the feed before the watcher started.
    feed.set_floor(2);
    let (publisher, handle) = spawn_against(&feed);

    assert_halted(handle).await;
    assert!(
        publisher.records().is_empty(),
        "nothing past the floor may be published"
    );
    assert_eq!(
        feed.subscription_count(),
        1,
        "a lag marker must not trigger a re-subscribe loop"
    );
}

/// The bound itself: `next_batch` returns the terminal error in bounded
/// time, from one subscription, instead of looping on re-subscribe.
#[tokio::test]
async fn a_lagged_marker_terminates_next_batch() {
    use kardamom_da_watcher::interop::source::RemoteSourceError;

    let feed = MockInteropFeed::new(ORIGIN).await;
    feed.push_message(msg(3, 102));
    feed.set_floor(3);
    let mut source = WsRemoteChainSource::new(ORIGIN, SELF, feed.url())
        .with_reconnect(Duration::from_millis(20), NonZeroU32::new(50).unwrap());

    let err = tokio::time::timeout(Duration::from_secs(5), source.next_batch(0))
        .await
        .expect("next_batch must return, not loop")
        .unwrap_err();
    assert!(
        matches!(
            err,
            RemoteSourceError::Lagged {
                cursor: 0,
                floor: 3,
                floor_block: Some(102)
            }
        ),
        "got {err:?}"
    );
    assert_eq!(feed.subscription_count(), 1);

    // Through `process_once`: the terminal variant, cursor unchanged.
    let publisher = InMemoryRemoteEpochPublisher::default();
    let mut cursor = 0u64;
    let err = process_once(&publisher, &mut source, SELF, &mut cursor)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            InteropError::Lagged {
                cursor: 0,
                floor: 3
            }
        ),
        "got {err:?}"
    );
    assert_eq!(cursor, 0);
    // A cursor at the floor is served; the head closes the block.
    feed.push_head(103);
    let batch = tokio::time::timeout(Duration::from_secs(5), source.next_batch(3))
        .await
        .expect("the batch must close on the head")
        .unwrap();
    assert_eq!(batch.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![3]);
}

/// A lane with one message delivers: the origin's `head` event closes
/// the block, no later message on the lane is needed.
#[tokio::test]
async fn a_head_event_closes_a_single_message_block() {
    let feed = MockInteropFeed::new(ORIGIN).await;
    let (publisher, handle) = spawn_against(&feed);

    feed.push_message(msg(0, 100));
    feed.push_head(100); // at the open block: not a close
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(publisher.records().is_empty(), "block 100 is still open");
    feed.push_head(101);
    wait_until(|| !publisher.records().is_empty(), "one record").await;

    // The next message starts a new block; a later head closes it too.
    feed.push_message(msg(1, 105));
    feed.push_head(106);
    wait_until(|| publisher.records().len() >= 2, "two records").await;
    let _ = handle.shutdown.send(());

    let records = publisher.records();
    assert_eq!(records[0].anchor_number, 100);
    assert_eq!(records[0].first_seq, 0);
    assert_eq!(records[0].last_seq(), 0);
    assert_eq!(records[1].anchor_number, 105);
    assert_eq!(records[1].first_seq, 1);
    assert_eq!(
        publisher.deduped_count(),
        0,
        "a head close must not replay the batch"
    );
}
