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

use std::ops::ControlFlow;
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
    /// The feed cannot serve our cursor: its floor is above it. TERMINAL for
    /// this pair. Reading on would skip; re-subscribing gets the same
    /// answer. An operator resets the cursor, or backfills from DA.
    #[error("remote feed lagged: cursor {cursor} is below the feed floor {floor}")]
    Lagged { cursor: u64, floor: u64 },
    /// The publisher transport is permanently closed; the watcher must exit.
    #[error("remote epoch publisher closed")]
    PublisherClosed,
    /// The batch's last seq is `u64::MAX`: advancing the cursor past it
    /// would wrap to 0, which would look like a fresh pair and rewind the
    /// lane's dedup and reorder checks. Effectively unreachable; a real
    /// peer feed never produces this many messages.
    #[error("interop cursor overflowed u64 after publishing seq {last_seq}")]
    CursorOverflow { last_seq: u64 },
}

/// The interop watcher's state for one pair: the cursor file, the
/// origin chain id (for logging), the retryable-error backoff, the
/// publisher, the source, our chain id, and the per-pair cursor (the
/// first seq not yet canonicalised).
pub struct InteropWatcher<S, P> {
    cursor_file: Option<CursorFile>,
    origin: u64,
    origin_label: String,
    retry_interval: Duration,
    publisher: P,
    source: S,
    self_chain_id: u64,
    cursor: u64,
}

/// What [`InteropWatcher::report`] decided for the next pass.
enum Pace {
    /// Pass again at once.
    Continue,
    /// Wait out the retry interval, then pass again.
    Retry,
    /// Stop the pair.
    Stop,
}

impl<S: RemoteChainSource, P: RemoteEpochPublisher> InteropWatcher<S, P> {
    /// `cursor_file`, when given, is persisted after every pass that
    /// advanced the cursor — that is, strictly AFTER the publish it
    /// describes (see [`Self::persist_cursor`] for why that ordering is
    /// load-bearing). `config.start_seq` is the caller's resume position
    /// either way; loading the file (and preferring it over the CLI seed)
    /// is the binary's job, so the loop has exactly one notion of "where
    /// am I".
    pub fn new(
        publisher: P,
        source: S,
        config: InteropWatcherConfig,
        cursor_file: Option<CursorFile>,
    ) -> Self {
        let origin = source.origin_chain_id();
        Self {
            cursor_file,
            origin,
            origin_label: origin.to_string(),
            retry_interval: config.retry_interval,
            publisher,
            source,
            self_chain_id: config.self_chain_id,
            cursor: config.start_seq,
        }
    }

    /// The first per-pair seq not yet canonicalised.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The remote source. Unit tests inspect the scripted fake through
    /// it.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// The remote source, mutably. Unit tests script the next batch
    /// through it.
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    /// Spawn the interop watcher for one pair. Returns the same
    /// [`WatcherHandle`] shape the L1 watcher uses.
    ///
    /// The loop is stream-driven rather than timer-driven — `next_batch`
    /// blocks until an origin block closes — so there is no tick interval,
    /// only the failure-path pace in [`InteropWatcherConfig::retry_interval`].
    ///
    /// The task ends on shutdown, on a closed publisher, on a derivation
    /// fault, or on a feed lag. The last two are the fail-stop: the
    /// handle's `task` completing without a shutdown signal is the pair's
    /// halt signal.
    pub fn spawn(
        publisher: P,
        source: S,
        config: InteropWatcherConfig,
        cursor_file: Option<CursorFile>,
    ) -> WatcherHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(Self::new(publisher, source, config, cursor_file).run(shutdown_rx));
        WatcherHandle {
            task,
            shutdown: shutdown_tx,
        }
    }

    /// The pass loop. `shutdown` stays outside the state, so the select
    /// in [`Self::step`] can wait on it while the pass borrows `self`.
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        while let ControlFlow::Continue(()) = self.step(&mut shutdown).await {}
    }

    /// One pass: run [`Self::process_once`] (or handle shutdown), persist
    /// the cursor when it advanced, then apply the outcome. `Break` ends
    /// the task: shutdown, or a fail-stop outcome.
    async fn step(&mut self, shutdown: &mut oneshot::Receiver<()>) -> ControlFlow<()> {
        let cursor_before = self.cursor;
        let outcome = tokio::select! {
            biased;
            _ = shutdown => {
                info!(target: "da_watcher::interop", origin = self.origin, "shutting down");
                return ControlFlow::Break(());
            }
            // Cancelled mid-pass only by the shutdown branch above; the
            // dropped future can cost at most an un-consumed feed item,
            // which the cursor-authoritative resume replays.
            r = self.process_once() => r,
        };
        self.persist_cursor(cursor_before);
        match self.report(outcome) {
            Pace::Continue => ControlFlow::Continue(()),
            Pace::Retry => {
                tokio::time::sleep(self.retry_interval).await;
                ControlFlow::Continue(())
            }
            Pace::Stop => ControlFlow::Break(()),
        }
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
    /// - [`InteropError::Lagged`] if the feed floor is above the cursor. Cursor
    ///   unchanged; the caller must STOP.
    /// - [`InteropError::PublisherClosed`] if the sink is shut.
    /// - [`InteropError::CursorOverflow`] if advancing the cursor past the
    ///   published batch would wrap `u64`.
    pub async fn process_once(&mut self) -> Result<usize, InteropError> {
        let origin = self.origin;
        let cursor = self.cursor;
        let batch = self.source.next_batch(cursor).await.map_err(|e| match e {
            RemoteSourceError::Lagged { cursor, floor, .. } => {
                InteropError::Lagged { cursor, floor }
            }
            e => InteropError::Source(e),
        })?;

        // The anchor is a pure function of (origin, block). The feed must
        // not choose it, so recompute it here and reject a message that
        // differs. Terminal for the pair, like every derivation fault.
        batch
            .iter()
            .try_for_each(|m| m.check_anchor(origin))
            .map_err(InteropError::Derive)?;

        // The batch goes in verbatim: ordering, gap, duplicate, multi-block
        // and foreign-destination verdicts all belong to the shared rule,
        // which the destination's verifier re-runs against the resulting
        // record.
        let record = derive_remote_epoch(self.self_chain_id, origin, cursor, &batch)
            .map_err(InteropError::Derive)?;
        let messages = record.messages.len().get();
        let last_seq = record.last_seq();

        match self.publisher.publish(&record) {
            Ok(pos) => {
                self.cursor = last_seq
                    .checked_add(1)
                    .ok_or(InteropError::CursorOverflow { last_seq })?;
                ::metrics::counter!(metrics::REMOTE_EPOCHS_PUBLISHED_TOTAL, "origin" => self.origin_label.clone())
                    .increment(1);
                ::metrics::counter!(metrics::REMOTE_MESSAGES_TOTAL, "origin" => self.origin_label.clone())
                    .increment(messages as u64);
                // Metric value; f64 precision loss only above 2^52, never
                // reached by a per-pair cursor.
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "metric value; never nears 2^52 for a per-pair cursor"
                )]
                ::metrics::gauge!(metrics::REMOTE_CURSOR_SEQ, "origin" => self.origin_label.clone())
                    .set(self.cursor as f64);
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
                // Never "log and carry on": a skipped record is a permanent
                // hole in the pair's dense seq, which is exactly what the
                // destination would later halt on.
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

    /// Persist the cursor after a pass that advanced it. Never called
    /// before the publish it records: a cursor that dies STALE (crash
    /// before this call) is harmless, because the restart re-derives a
    /// byte-identical record and cluster dedup on `canonical_id` absorbs
    /// the re-publish. A cursor persisted AHEAD of its publish would be a
    /// permanent lane hole instead, so this only runs when the cursor
    /// actually moved past `cursor_before`.
    fn persist_cursor(&self, cursor_before: u64) {
        let cursor = self.cursor;
        if cursor == cursor_before {
            return;
        }
        let Some(cf) = self.cursor_file.as_ref() else {
            return;
        };
        if let Err(e) = cf.persist(cursor) {
            ::metrics::counter!(
                metrics::REMOTE_CURSOR_PERSIST_FAILURES_TOTAL,
                "origin" => self.origin_label.clone()
            )
            .increment(1);
            warn!(
                target: "da_watcher::interop",
                origin = self.origin,
                cursor,
                error = %e,
                "cursor persist failed; a restart before the next successful persist \
                 resumes STALE (harmless: dedup absorbs the re-publish)"
            );
        }
    }

    /// Record the pass outcome and decide the pace of the next one:
    /// [`Pace::Retry`] for a retryable feed error, [`Pace::Stop`] when
    /// the publisher closed or the batch broke the derivation rule (a
    /// fail-stop fault).
    fn report(&self, outcome: Result<usize, InteropError>) -> Pace {
        let origin = self.origin;
        let cursor = self.cursor;
        match outcome {
            Ok(_) => {
                self.record_tick("ok");
                Pace::Continue
            }
            Err(InteropError::Source(e)) => {
                self.record_tick("feed_error");
                warn!(
                    target: "da_watcher::interop",
                    origin,
                    cursor,
                    error = %e,
                    "outbox feed unavailable; the pair stalls until it recovers"
                );
                Pace::Retry
            }
            Err(InteropError::PublisherClosed) => {
                self.record_tick("publisher_closed");
                warn!(target: "da_watcher::interop", origin, "publisher closed; exiting");
                Pace::Stop
            }
            Err(InteropError::Lagged { cursor: at, floor }) => {
                self.record_tick("fault");
                error!(
                    target: "da_watcher::interop",
                    origin,
                    cursor = at,
                    floor,
                    "remote feed lagged; STOPPING this pair (the feed cannot serve the \
                     cursor; reset the cursor or backfill from DA — operator intervention \
                     required)"
                );
                Pace::Stop
            }
            Err(e @ (InteropError::Derive(_) | InteropError::CursorOverflow { .. })) => {
                self.record_tick("fault");
                error!(
                    target: "da_watcher::interop",
                    origin,
                    cursor,
                    error = %e,
                    "remote epoch derivation fault; STOPPING this pair (a feed gap is never \
                     skipped; operator intervention required)"
                );
                Pace::Stop
            }
        }
    }

    /// Count one pass under its outcome label.
    fn record_tick(&self, outcome: &'static str) {
        ::metrics::counter!(
            metrics::REMOTE_WATCHER_TICK_TOTAL,
            "origin" => self.origin_label.clone(),
            "outcome" => outcome
        )
        .increment(1);
    }
}
