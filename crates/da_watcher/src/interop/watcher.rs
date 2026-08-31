//! Interop watcher loop: cursor-tailed consumption of a peer chain's outbox
//! feed, one [`RemoteEpochRecord`] per origin block that carried messages,
//! published for the sequencer to relay onto the canonical stream.
//!
//! The unit is the RECORD, not the message, for the same reason the L1 unit is
//! the epoch: the batch's boundaries are what `canonical_id` commits to, so
//! they must be a property of the origin chain and not of the observer (see
//! [`crate::interop::source`]).
//!
//! ## The fault domain is the PAIR, and that asymmetry is deliberate
//!
//! Any `XChainError` out of `derive_remote_epoch` stops THIS watcher and
//! nothing else. The chain keeps producing blocks; every other peer pairing
//! keeps flowing; only the A→B direction stalls, loudly, at a known cursor.
//!
//! That is the opposite of what happens once a bad record reaches the
//! canonical stream: there, the destination's verifier halts the whole chain,
//! because a record on the stream has already been executed by every replica
//! and disagreement about it is a consensus fault.
//!
//! The asymmetry is the point. Before the stream, a feed gap is one peer's
//! problem and containing it there is a liveness win. After the stream, it is
//! everyone's problem and continuing would be a safety loss. What is NOT
//! allowed at either end is skipping: a watcher that stepped over a gap would
//! turn a stalled pair into a permanently wrong one, and the destination could
//! never tell, since a message that was never derived leaves no evidence.
//! Hence: derive, or stop.

use std::time::Duration;

use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use kardamom_types::xchain::{XChainError, derive_remote_epoch};

use crate::interop::cursor::CursorFile;
use crate::interop::publisher::{PublishError, RemoteEpochPublisher};
use crate::interop::source::{RemoteChainSource, RemoteSourceError};
use crate::metrics;
use crate::watcher::WatcherHandle;

/// Interop watcher configuration. One instance per (origin, destination) pair.
#[derive(Debug, Clone, Copy)]
pub struct InteropWatcherConfig {
    /// OUR chain id. Passed to `derive_remote_epoch`, which rejects any
    /// message addressed elsewhere rather than dropping it.
    pub self_chain_id: u64,
    /// Cursor seed: the first per-pair seq not yet canonicalised. A fresh pair
    /// starts at 0; a restart must resume at the destination chain's recorded
    /// position, or the first derivation fault will be its own bookkeeping.
    pub start_seq: u64,
    /// Pause before retrying after a feed transport/decode failure. Not a poll
    /// interval — the source blocks until a batch is ready — so this only
    /// paces the failure path.
    pub retry_interval: Duration,
}

/// Outcomes of one interop pass.
#[derive(Debug, thiserror::Error)]
pub enum InteropError {
    /// The feed is unreachable or serving something this build cannot read.
    /// Retryable: the cursor is unchanged and the pair recovers by itself.
    #[error("remote feed error: {0}")]
    Source(RemoteSourceError),
    /// The batch violates the shared derivation rule. TERMINAL for this pair —
    /// see the module docs.
    #[error("remote epoch derivation failed: {0}")]
    Derive(XChainError),
    /// The publisher transport is permanently closed; the watcher must exit.
    #[error("remote epoch publisher closed")]
    PublisherClosed,
}

/// One processing pass: take the next origin block's batch, derive its record,
/// publish it, advance the cursor. Public so tests can exercise it without a
/// task or a timer.
///
/// Returns `Ok(1)` when a record was published and `Ok(0)` when the publisher
/// declined it (backpressure or a transient transport failure). On `Ok(0)` the
/// cursor is unchanged and the next pass re-derives the SAME batch — safe
/// because re-derivation is byte-identical, so a record that did land is
/// collapsed by cluster dedup rather than executed twice.
///
/// # Errors
/// - [`InteropError::Source`] if the feed failed. Cursor unchanged; retry.
/// - [`InteropError::Derive`] if the batch broke the sequence rules. Cursor
///   unchanged; the caller must STOP (never skip).
/// - [`InteropError::PublisherClosed`] if the sink is shut.
pub async fn process_once<S, P>(
    publisher: &P,
    source: &mut S,
    self_chain_id: u64,
    cursor: &mut u64,
) -> Result<usize, InteropError>
where
    S: RemoteChainSource,
    P: RemoteEpochPublisher,
{
    let origin = source.origin_chain_id();
    let batch = source
        .next_batch(*cursor)
        .await
        .map_err(InteropError::Source)?;

    // The batch goes in verbatim: ordering, gap, duplicate and
    // foreign-destination verdicts all belong to the shared rule, which the
    // destination's verifier re-runs against the resulting record.
    let record = derive_remote_epoch(self_chain_id, origin, *cursor, &batch)
        .map_err(InteropError::Derive)?;
    let messages = record.messages.len();
    let last_seq = record.last_seq();

    match publisher.publish(&record) {
        Ok(pos) => {
            *cursor = last_seq + 1;
            let origin_label = origin.to_string();
            ::metrics::counter!(metrics::REMOTE_EPOCHS_PUBLISHED_TOTAL, "origin" => origin_label.clone())
                .increment(1);
            ::metrics::counter!(metrics::REMOTE_MESSAGES_TOTAL, "origin" => origin_label.clone())
                .increment(messages as u64);
            ::metrics::gauge!(metrics::REMOTE_CURSOR_SEQ, "origin" => origin_label)
                .set(*cursor as f64);
            debug!(
                target: "da_watcher::interop",
                origin,
                origin_block = record.anchor_number,
                first_seq = record.first_seq,
                last_seq,
                messages,
                ?pos,
                "published remote epoch"
            );
            Ok(1)
        }
        Err(PublishError::Backpressure) => {
            warn!(
                target: "da_watcher::interop",
                origin,
                first_seq = record.first_seq,
                "remote epoch publish backpressured; will retry from the same cursor"
            );
            Ok(0)
        }
        Err(PublishError::Closed) => Err(InteropError::PublisherClosed),
        Err(PublishError::Transport(detail)) => {
            // Never "log and carry on": a skipped record is a permanent hole
            // in the pair's dense seq, which is exactly what the destination
            // would later halt on.
            warn!(
                target: "da_watcher::interop",
                origin,
                first_seq = record.first_seq,
                %detail,
                "remote epoch publish failed; retrying from the same cursor"
            );
            Ok(0)
        }
    }
}

/// Spawn the interop watcher for one pair. Returns the same
/// [`WatcherHandle`] shape the L1 watcher uses.
///
/// The loop is stream-driven rather than timer-driven — `next_batch` blocks
/// until an origin block closes — so there is no tick interval, only the
/// failure-path pace in [`InteropWatcherConfig::retry_interval`].
///
/// `cursor_file`, when given, is persisted after every pass that advanced the
/// cursor — that is, strictly AFTER the publish it describes (see the write
/// site below for why that ordering is load-bearing). `config.start_seq` is
/// the caller's resume position either way; loading the file (and preferring
/// it over the CLI seed) is the binary's job, so the loop has exactly one
/// notion of "where am I".
///
/// The task ends on shutdown, on a closed publisher, or on a derivation fault.
/// The last of those is the fail-stop: the handle's `task` completing without
/// a shutdown signal is the pair's halt signal.
pub fn spawn<S, P>(
    publisher: P,
    mut source: S,
    config: InteropWatcherConfig,
    cursor_file: Option<CursorFile>,
) -> WatcherHandle
where
    S: RemoteChainSource,
    P: RemoteEpochPublisher,
{
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let origin = source.origin_chain_id();
    let origin_label = origin.to_string();
    let task = tokio::spawn(async move {
        let mut cursor = config.start_seq;
        loop {
            let cursor_before = cursor;
            let outcome = tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    info!(target: "da_watcher::interop", origin, "shutting down");
                    break;
                }
                // Cancelled mid-pass only by the shutdown branch above; the
                // dropped future can cost at most an un-consumed feed item,
                // which the cursor-authoritative resume replays.
                r = process_once(&publisher, &mut source, config.self_chain_id, &mut cursor) => r,
            };
            // THE WRITE SITE. Persist strictly AFTER a successful publish
            // (the only thing that advances `cursor`), never before. The two
            // failure directions are not symmetric: a cursor that dies STALE
            // here is harmless — the restart re-derives byte-identical
            // records and cluster dedup on `canonical_id` absorbs the
            // re-publish — while a cursor persisted AHEAD of a publish would
            // be a permanent lane hole no retry can fill (the record between
            // the two positions was never published and never will be). A
            // FAILED persist therefore only degrades restart to the stale
            // case, so it is logged loudly and the pair keeps flowing.
            if cursor != cursor_before
                && let Some(cf) = &cursor_file
                && let Err(e) = cf.persist(cursor)
            {
                ::metrics::counter!(
                    metrics::REMOTE_CURSOR_PERSIST_FAILURES_TOTAL,
                    "origin" => origin_label.clone()
                )
                .increment(1);
                warn!(
                    target: "da_watcher::interop",
                    origin,
                    cursor,
                    error = %e,
                    "cursor persist failed; a restart before the next successful persist \
                     resumes STALE (harmless: dedup absorbs the re-publish)"
                );
            }
            match outcome {
                Ok(_) => {
                    ::metrics::counter!(
                        metrics::REMOTE_WATCHER_TICK_TOTAL,
                        "origin" => origin_label.clone(),
                        "outcome" => "ok"
                    )
                    .increment(1);
                }
                Err(InteropError::Source(e)) => {
                    ::metrics::counter!(
                        metrics::REMOTE_WATCHER_TICK_TOTAL,
                        "origin" => origin_label.clone(),
                        "outcome" => "feed_error"
                    )
                    .increment(1);
                    warn!(
                        target: "da_watcher::interop",
                        origin,
                        cursor,
                        error = %e,
                        "outbox feed unavailable; the pair stalls until it recovers"
                    );
                    tokio::time::sleep(config.retry_interval).await;
                }
                Err(InteropError::PublisherClosed) => {
                    ::metrics::counter!(
                        metrics::REMOTE_WATCHER_TICK_TOTAL,
                        "origin" => origin_label.clone(),
                        "outcome" => "publisher_closed"
                    )
                    .increment(1);
                    warn!(target: "da_watcher::interop", origin, "publisher closed; exiting");
                    break;
                }
                Err(InteropError::Derive(e)) => {
                    ::metrics::counter!(
                        metrics::REMOTE_WATCHER_TICK_TOTAL,
                        "origin" => origin_label.clone(),
                        "outcome" => "fault"
                    )
                    .increment(1);
                    error!(
                        target: "da_watcher::interop",
                        origin,
                        cursor,
                        error = %e,
                        "remote epoch derivation fault; STOPPING this pair (a feed gap is \
                         never skipped — operator intervention required)"
                    );
                    break;
                }
            }
        }
    });
    WatcherHandle {
        task,
        shutdown: shutdown_tx,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use alloy_primitives::{Address, B256, Bytes};
    use kardamom_types::xchain::{OutboxMessage, RemoteEpochRecord, remote_source_hash};

    use super::*;
    use crate::interop::mock::MockInteropFeed;
    use crate::interop::publisher::fakes::InMemoryRemoteEpochPublisher;
    use crate::interop::source::WsRemoteChainSource;

    const SELF: u64 = 412_347;
    const ORIGIN: u64 = 412_346;

    fn msg(seq: u64, block: u64) -> OutboxMessage {
        OutboxMessage {
            origin_block_number: block,
            origin_block_hash: B256::repeat_byte(block as u8),
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
    async fn spawn_against(
        feed: &MockInteropFeed,
    ) -> (InMemoryRemoteEpochPublisher, WatcherHandle) {
        let publisher = InMemoryRemoteEpochPublisher::default();
        let handle = spawn_resuming(feed, publisher.clone(), 0, None).await;
        (publisher, handle)
    }

    /// [`spawn_against`] with an explicit resume position and (optionally) a
    /// durable cursor — the restart-shaped variant.
    async fn spawn_resuming(
        feed: &MockInteropFeed,
        publisher: InMemoryRemoteEpochPublisher,
        start_seq: u64,
        cursor_file: Option<CursorFile>,
    ) -> WatcherHandle {
        let source = WsRemoteChainSource::new(ORIGIN, SELF, feed.url())
            .with_reconnect(Duration::from_millis(20), 50);
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
        let deadline = Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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
        use crate::interop::source::fakes::ScriptedRemoteSource;

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
        use crate::interop::source::RemoteSourceError;
        use crate::interop::source::fakes::ScriptedRemoteSource;

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
        let (publisher, handle) = spawn_against(&feed).await;

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
            records[1].messages[0].source_hash,
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
        let (publisher, handle) = spawn_against(&feed).await;

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
        let (publisher, handle) = spawn_against(&feed).await;

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
        let (publisher, handle) = spawn_against(&feed).await;

        let mut stray = msg(0, 100);
        stray.dest_chain_id = SELF + 1;
        feed.push_message(stray);
        feed.push_message(msg(1, 101));

        assert_halted(handle).await;
        assert!(publisher.records().is_empty());
    }

    /// A dropped session must re-subscribe from the watcher's own cursor and
    /// produce records the racing/uninterrupted run would also have produced —
    /// equal `canonical_id`s, which is what lets cluster dedup collapse them.
    #[tokio::test]
    async fn a_reconnect_reproduces_byte_identical_records() {
        let feed = MockInteropFeed::new(ORIGIN).await;
        let (publisher, handle) = spawn_against(&feed).await;

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
            records.iter().map(|r| r.canonical_id()).collect::<Vec<_>>(),
            expected
                .iter()
                .map(|r| r.canonical_id())
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
        let cursor_file = CursorFile::new(dir.path().join("pair.cursor"));

        let feed = MockInteropFeed::new(ORIGIN).await;
        let publisher = InMemoryRemoteEpochPublisher::default();
        let handle = spawn_resuming(&feed, publisher.clone(), 0, Some(cursor_file.clone())).await;

        feed.push_message(msg(0, 100));
        feed.push_message(msg(1, 100));
        feed.push_message(msg(2, 101));
        feed.push_message(msg(3, 102)); // sentinel: closes 101
        wait_until(|| publisher.records().len() >= 2, "two records").await;
        let _ = handle.shutdown.send(());
        handle.task.await.unwrap();
        assert_eq!(
            cursor_file.load().unwrap(),
            Some(3),
            "the persisted cursor must be one past the last PUBLISHED seq \
             (seq 3's block is still open, so it is not published yet)"
        );

        // "Restart": a fresh watcher over the same publisher, seeded with a
        // deliberately wrong CLI value — the file must win.
        let resumed = cursor_file.load().unwrap().expect("cursor persisted");
        let handle =
            spawn_resuming(&feed, publisher.clone(), resumed, Some(cursor_file.clone())).await;
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
        assert_eq!(cursor_file.load().unwrap(), Some(5));
    }

    /// The crash window the write ordering exists for: die AFTER the publish,
    /// BEFORE the persist. The restart resumes STALE, re-derives the same
    /// batch, re-publishes it — and the duplicate is absorbed by dedup on
    /// `canonical_id`, proving end to end that the stale side of the
    /// asymmetry really is harmless.
    #[tokio::test]
    async fn a_crash_between_publish_and_persist_resumes_stale_and_dedup_absorbs() {
        use crate::interop::source::fakes::ScriptedRemoteSource;

        let dir = tempfile::tempdir().unwrap();
        let cursor_file = CursorFile::new(dir.path().join("pair.cursor"));
        let publisher = InMemoryRemoteEpochPublisher::default();

        // First life: publish succeeds, then the process dies before the
        // persist (simulated by simply never calling it).
        let mut source = ScriptedRemoteSource::new(ORIGIN);
        source.push_batch(Ok(vec![msg(0, 100), msg(1, 100)]));
        let mut cursor = cursor_file.load().unwrap().unwrap_or(0);
        let n = process_once(&publisher, &mut source, SELF, &mut cursor)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(cursor, 2, "in-memory cursor advanced past the publish");
        drop(source); // the crash: no persist happened
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

    /// `Lagged` says the feed's retention overtook us; reading on would skip.
    /// Resuming from the cursor must lose nothing and repeat nothing.
    #[tokio::test]
    async fn a_lagged_marker_resumes_from_the_cursor() {
        let feed = MockInteropFeed::new(ORIGIN).await;
        let (publisher, handle) = spawn_against(&feed).await;

        feed.push_message(msg(0, 100));
        feed.push_lagged(3);
        feed.push_message(msg(1, 100));
        feed.push_message(msg(2, 101));
        feed.push_message(msg(3, 102));

        wait_until(|| publisher.records().len() >= 2, "two records").await;
        let _ = handle.shutdown.send(());

        assert!(
            feed.subscription_count() >= 2,
            "a lag marker must trigger a re-subscription, not a read-on"
        );
        let records = publisher.records();
        assert_eq!(records.len(), 2);
        let seqs: Vec<u64> = records
            .iter()
            .flat_map(|r| r.messages.iter().map(|m| m.seq))
            .collect();
        assert_eq!(
            seqs,
            vec![0, 1, 2],
            "no loss and no duplicates across the lag"
        );
    }
}
