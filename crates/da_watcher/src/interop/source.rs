//! Remote-chain source trait — the seam between the interop watcher and the
//! origin chain's feed, mirroring [`crate::source::L1Source`].
//!
//! One read: `next_batch(cursor_seq)` — the next contiguous run of messages
//! addressed to us, starting at `cursor_seq`.
//!
//! # THE BATCHING RULE: one batch = exactly one origin block
//!
//! A batch must be **all of the messages to us from exactly one origin block**,
//! never a prefix of one and never a span of two. This is a correctness
//! constraint, not a style preference.
//!
//! `RemoteEpochRecord::canonical_id` keys on `(origin_chain_id, anchor_hash,
//! first_seq, last_seq)` — the batch's boundaries. Cluster dedup collapses
//! racing relayers only because they all derive the SAME id for the same
//! messages. Two watchers that cut the stream differently (one after a
//! restart, one racing it live) would produce records with different
//! `first_seq`/`last_seq` over overlapping seq ranges: different ids, dedup
//! sees two distinct records, and the destination executes the overlap twice.
//! The origin block is the only cut point every observer agrees on without
//! coordination, because it is a property of the origin chain rather than of
//! the observer's timing.
//!
//! Two consequences worth stating out loud:
//!
//!   * A block closes when the feed proves it is complete: a message from a
//!     LATER block on the same lane, or a `head` event whose block number
//!     is past the open block. The origin validator sends one `head` after
//!     the last message once a later block closes, so a lane with a single
//!     message still delivers within one origin block time.
//!   * A batch is what the watcher hands to `derive_remote_epoch` verbatim.
//!     The source never filters, reorders, de-duplicates or repairs: every one
//!     of those is a verdict the shared derivation rule owns, and a source that
//!     quietly fixed a gap would hide exactly the fault the no-skip rule
//!     exists to catch.

use std::future::Future;
use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::time::Duration;

use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use kardamom_types::xchain::OutboxMessage;
use tracing::{debug, error, warn};

use kardamom_interop_feed::{
    Lag, OutboxCursor, OutboxEventDto, OutboxMessageDto, SUBSCRIBE_OUTBOX_METHOD,
    UNSUBSCRIBE_OUTBOX_METHOD,
};

use crate::metrics;

/// Errors a [`RemoteChainSource`] can surface.
///
/// `Transport` and `Decode` are RETRYABLE at the watcher: neither is a
/// statement about the derived chain. A feed that is down, or serving a
/// shape this build cannot read, stalls the pair loudly and recovers by
/// itself when the peer is fixed. `Lagged` is TERMINAL: the feed cannot
/// serve the cursor, and reading on would skip messages.
#[derive(Debug, thiserror::Error)]
pub enum RemoteSourceError {
    /// Connect/subscribe/stream failure against the peer's feed.
    #[error("remote feed transport error: {0}")]
    Transport(String),
    /// A feed item this build cannot interpret (see
    /// [`kardamom_interop_feed::FeedDecodeError`]).
    #[error("remote feed decode error: {0}")]
    Decode(String),
    /// The feed's floor is above our cursor: the messages between are gone
    /// from the peer (retention, or a peer restart). The pair must stop.
    /// Recovery is an operator cursor reset, or a DA backfill.
    #[error(
        "remote feed lagged: cursor {cursor} is below the feed floor {floor} \
         (first served block {floor_block:?})"
    )]
    Lagged {
        cursor: u64,
        floor: u64,
        floor_block: Option<u64>,
    },
}

/// The origin-chain view the interop watcher needs.
///
/// `&mut self` (unlike [`crate::source::L1Source`]'s `&self`) because a
/// streaming source is stateful: it holds the live subscription and the
/// partially-accumulated block. That also makes exclusive ownership the
/// contract, so `Sync` is not required.
pub trait RemoteChainSource: Send + 'static {
    /// The peer chain this source observes. Configured, not inferred: it is an
    /// input to every derived message's `source_hash` and sender alias.
    fn origin_chain_id(&self) -> u64;

    /// Next batch of messages, resuming at `cursor_seq` (the first seq not yet
    /// canonicalised by the caller).
    ///
    /// Blocks until a batch is complete. **One batch is one origin block** —
    /// see the module docs.
    ///
    /// `cursor_seq` is authoritative on every call: a caller that did not
    /// advance (a publish it could not complete) gets the same batch again,
    /// which is safe precisely because re-derivation is byte-identical.
    fn next_batch(
        &mut self,
        cursor_seq: u64,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RemoteSourceError>> + Send;
}

/// [`WsRemoteChainSource::new`]'s default reconnect budget.
const DEFAULT_MAX_RECONNECT_ATTEMPTS: NonZeroU32 = NonZeroU32::new(8).unwrap();

/// Production [`RemoteChainSource`]: a jsonrpsee WebSocket subscription to a
/// peer validator's `kardamom_subscribeOutbox`.
///
/// Recovery posture: a dropped connection or an ended subscription take one
/// path — discard the partially-accumulated block and re-subscribe from the
/// source's own cursor. Discarding rather than resuming mid-block is what
/// keeps the batch cut on the origin block boundary after a reconnect, which
/// is what keeps the derived records byte-identical to the un-interrupted
/// run. A [`OutboxEventDto::Lagged`] marker is different: the peer says it
/// cannot serve the cursor, and a re-subscribe from the same cursor gets
/// the same answer. It ends `next_batch` with
/// [`RemoteSourceError::Lagged`], which the watcher treats as a pair fault.
pub struct WsRemoteChainSource {
    origin_chain_id: u64,
    dest_chain_id: u64,
    url: String,
    reconnect_backoff: Duration,
    max_reconnect_attempts: NonZeroU32,
    // Held for its lifetime, not its API: dropping the client closes the
    // WebSocket out from under the subscription.
    client: Option<WsClient>,
    subscription: Option<Subscription<OutboxEventDto>>,
    /// First seq of the batch under construction; `None` before the first
    /// call. Compared against the caller's cursor to detect a rewind.
    cursor: Option<u64>,
    /// Messages of the origin block currently being accumulated.
    pending: Vec<OutboxMessage>,
}

impl WsRemoteChainSource {
    /// `url` is the peer validator's WebSocket endpoint (`ws://host:port`).
    /// `dest_chain_id` is OUR chain id — the feed's server-side filter.
    pub fn new(origin_chain_id: u64, dest_chain_id: u64, url: impl Into<String>) -> Self {
        Self {
            origin_chain_id,
            dest_chain_id,
            url: url.into(),
            reconnect_backoff: Duration::from_millis(250),
            max_reconnect_attempts: DEFAULT_MAX_RECONNECT_ATTEMPTS,
            client: None,
            subscription: None,
            cursor: None,
            pending: Vec::new(),
        }
    }

    /// Pause between reconnect attempts, and how many to make before handing
    /// the failure back to the watcher. Bounded rather than infinite so a dead
    /// peer surfaces on the watcher's tick metric instead of inside a silent
    /// retry loop.
    #[must_use]
    pub fn with_reconnect(mut self, backoff: Duration, max_attempts: NonZeroU32) -> Self {
        self.reconnect_backoff = backoff;
        self.max_reconnect_attempts = max_attempts;
        self
    }

    /// Drop the session and the partial block. The next `next_batch` pass
    /// re-subscribes from `self.cursor` and replays the block from its start.
    fn drop_session(&mut self) {
        self.subscription = None;
        self.client = None;
        self.pending.clear();
    }

    async fn connect(
        &mut self,
        from: u64,
    ) -> Result<Subscription<OutboxEventDto>, RemoteSourceError> {
        let client = WsClientBuilder::default()
            .build(&self.url)
            .await
            .map_err(|e| RemoteSourceError::Transport(format!("connect {}: {e}", self.url)))?;
        let subscription = client
            .subscribe::<OutboxEventDto, _>(
                SUBSCRIBE_OUTBOX_METHOD,
                rpc_params![self.dest_chain_id, OutboxCursor::new(from)],
                UNSUBSCRIBE_OUTBOX_METHOD,
            )
            .await
            .map_err(|e| RemoteSourceError::Transport(format!("subscribe: {e}")))?;
        self.client = Some(client);
        Ok(subscription)
    }

    /// Ensure a live subscription at `from`, reconnecting with retry if
    /// none is open, then return it. The borrow comes from this call, not
    /// from re-reading the `Option` field, so a caller never needs its own
    /// "just connected" assertion.
    async fn ensure_subscribed(
        &mut self,
        from: u64,
    ) -> Result<&mut Subscription<OutboxEventDto>, RemoteSourceError> {
        let sub = match self.subscription.take() {
            Some(sub) => sub,
            None => self.connect_with_retry(from).await?,
        };
        Ok(self.subscription.insert(sub))
    }

    /// Retry [`try_connect`] up to `max_reconnect_attempts` times, pausing
    /// `reconnect_backoff` between attempts. Only the last attempt's error
    /// is returned: earlier ones are logged, since only the final attempt
    /// decides whether the caller gives up.
    async fn connect_with_retry(
        &mut self,
        from: u64,
    ) -> Result<Subscription<OutboxEventDto>, RemoteSourceError> {
        let last_attempt = self.max_reconnect_attempts.get() - 1;
        for attempt in 0..last_attempt {
            if let ControlFlow::Break(sub) = self.try_connect(from, attempt).await {
                return Ok(sub);
            }
        }
        match self.try_connect(from, last_attempt).await {
            ControlFlow::Break(sub) => Ok(sub),
            ControlFlow::Continue(e) => Err(e),
        }
    }

    /// One `ensure_subscribed` attempt: success breaks out of the retry
    /// loop with the new subscription, failure logs, sleeps the reconnect
    /// backoff, and hands back its error for the caller's final error if
    /// every attempt fails.
    async fn try_connect(
        &mut self,
        from: u64,
        attempt: u32,
    ) -> ControlFlow<Subscription<OutboxEventDto>, RemoteSourceError> {
        match self.connect(from).await {
            Ok(sub) => {
                debug!(
                    target: "da_watcher::interop",
                    origin = self.origin_chain_id,
                    cursor = from,
                    "subscribed to outbox feed"
                );
                ControlFlow::Break(sub)
            }
            Err(e) => {
                warn!(
                    target: "da_watcher::interop",
                    origin = self.origin_chain_id,
                    attempt,
                    error = %e,
                    "outbox feed subscribe failed; retrying"
                );
                tokio::time::sleep(self.reconnect_backoff).await;
                ControlFlow::Continue(e)
            }
        }
    }

    /// Fold one message into the block under construction, returning the
    /// previous block's batch when this message proves that block is over.
    ///
    /// Any CHANGE of block number closes the batch, not just an increase: a
    /// feed that walks backwards is broken, and letting the batch close makes
    /// the derivation rule the one that says so.
    fn absorb(&mut self, msg: OutboxMessage) -> Option<Vec<OutboxMessage>> {
        let closes = self
            .pending
            .first()
            .is_some_and(|f| f.origin_block_number != msg.origin_block_number);
        if !closes {
            self.pending.push(msg);
            return None;
        }
        let batch = std::mem::replace(&mut self.pending, vec![msg]);
        Some(self.close(batch))
    }

    /// Fold a `head` event in: the origin closed every block through
    /// `head_block`. Returns the open batch when its block is below the
    /// head. A head at or below the open block, or with nothing open, is
    /// ignored.
    fn absorb_head(&mut self, head_block: u64) -> Option<Vec<OutboxMessage>> {
        let closes = self
            .pending
            .first()
            .is_some_and(|f| head_block > f.origin_block_number);
        if !closes {
            return None;
        }
        let batch = std::mem::take(&mut self.pending);
        Some(self.close(batch))
    }

    /// Hand a closed batch back and record the cursor the caller is
    /// expected to advance to, so an un-advanced caller (backpressure) is
    /// detected as a rewind and replayed. `checked_add` guards a
    /// `seq == u64::MAX` message (unreachable in practice) from wrapping to
    /// 0; falling back to `None` instead just forces the next call onto the
    /// same resubscribe path an actually-rewound caller already takes.
    fn close(&mut self, batch: Vec<OutboxMessage>) -> Vec<OutboxMessage> {
        self.cursor = batch
            .iter()
            .map(|m| m.seq)
            .max()
            .and_then(|s| s.checked_add(1));
        batch
    }
}

impl WsRemoteChainSource {
    /// One subscription event, decoded and folded into the open batch.
    /// `Some` means a batch closed and the caller returns it; `None` means
    /// the caller loops and reads the next event.
    async fn step(&mut self, from: u64) -> Result<Option<Vec<OutboxMessage>>, RemoteSourceError> {
        let subscription = self.ensure_subscribed(from).await?;
        match subscription.next().await {
            Some(Ok(OutboxEventDto::Message(dto))) => self.on_message(*dto),
            Some(Ok(OutboxEventDto::Head { block_number })) => Ok(self.absorb_head(block_number)),
            Some(Ok(OutboxEventDto::Lagged {
                skipped,
                floor_seq,
                floor_block,
            })) => self.on_lagged(from, skipped, floor_seq, floor_block),
            Some(Err(e)) => {
                self.on_stream_error(e);
                Ok(None)
            }
            None => {
                self.on_closed(from);
                Ok(None)
            }
        }
    }

    /// Decode one message DTO, count it, and fold it into the open batch.
    fn on_message(
        &mut self,
        dto: OutboxMessageDto,
    ) -> Result<Option<Vec<OutboxMessage>>, RemoteSourceError> {
        let msg = dto
            .into_outbox_message(self.origin_chain_id)
            .map_err(|e| RemoteSourceError::Decode(e.to_string()))?;
        ::metrics::counter!(
            metrics::REMOTE_MESSAGES_RECEIVED_TOTAL,
            "origin" => self.origin_chain_id.to_string()
        )
        .increment(1);
        Ok(self.absorb(msg))
    }

    /// Handle a lag marker: terminal for the pair if it is above the
    /// cursor (reading on would skip messages, which the no-skip rule
    /// forbids); otherwise a peer bug the watcher tolerates by
    /// re-subscribing.
    fn on_lagged(
        &mut self,
        from: u64,
        skipped: u64,
        floor_seq: Option<u64>,
        floor_block: Option<u64>,
    ) -> Result<Option<Vec<OutboxMessage>>, RemoteSourceError> {
        let Lag { floor, floor_block } = Lag::resolve(from, skipped, floor_seq, floor_block);
        if floor > from {
            error!(
                target: "da_watcher::interop",
                origin = self.origin_chain_id,
                skipped,
                cursor = from,
                floor,
                ?floor_block,
                "outbox feed lagged: the peer cannot serve the cursor; \
                 the pair must stop"
            );
            self.drop_session();
            return Err(RemoteSourceError::Lagged {
                cursor: from,
                floor,
                floor_block,
            });
        }
        // A lag marker at or below the cursor is a peer bug, not a loss.
        // Re-subscribe, paced by the watcher.
        warn!(
            target: "da_watcher::interop",
            origin = self.origin_chain_id,
            skipped,
            cursor = from,
            "outbox feed sent a lag marker below the cursor; re-subscribing"
        );
        ::metrics::counter!(
            metrics::REMOTE_FEED_RESUBSCRIBE_TOTAL,
            "origin" => self.origin_chain_id.to_string(),
            "cause" => "lagged"
        )
        .increment(1);
        self.drop_session();
        Ok(None)
    }

    /// An undecodable stream item: log, count, and re-subscribe from the
    /// cursor.
    fn on_stream_error(&mut self, e: impl std::fmt::Display) {
        warn!(
            target: "da_watcher::interop",
            origin = self.origin_chain_id,
            error = %e,
            "outbox feed item undecodable; re-subscribing from cursor"
        );
        ::metrics::counter!(
            metrics::REMOTE_FEED_RESUBSCRIBE_TOTAL,
            "origin" => self.origin_chain_id.to_string(),
            "cause" => "stream_error"
        )
        .increment(1);
        self.drop_session();
    }

    /// The subscription ended: log, count, and re-subscribe from the
    /// cursor.
    fn on_closed(&mut self, from: u64) {
        warn!(
            target: "da_watcher::interop",
            origin = self.origin_chain_id,
            cursor = from,
            "outbox feed closed; re-subscribing from cursor"
        );
        ::metrics::counter!(
            metrics::REMOTE_FEED_RESUBSCRIBE_TOTAL,
            "origin" => self.origin_chain_id.to_string(),
            "cause" => "closed"
        )
        .increment(1);
        self.drop_session();
    }
}

impl RemoteChainSource for WsRemoteChainSource {
    fn origin_chain_id(&self) -> u64 {
        self.origin_chain_id
    }

    async fn next_batch(
        &mut self,
        cursor_seq: u64,
    ) -> Result<Vec<OutboxMessage>, RemoteSourceError> {
        if self.cursor != Some(cursor_seq) {
            // The caller rewound (first call, or a batch it could not publish).
            // Its cursor wins: re-subscribe there and replay.
            self.drop_session();
            self.cursor = Some(cursor_seq);
        }
        let from = cursor_seq;

        loop {
            if let Some(batch) = self.step(from).await? {
                return Ok(batch);
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::{OutboxMessage, RemoteChainSource, RemoteSourceError};

    /// In-memory [`RemoteChainSource`] driven by a scripted queue of batches —
    /// the interop counterpart of [`crate::source::fakes::MockL1Source`], for
    /// watcher-logic tests that should not pay for a WebSocket.
    ///
    /// Deliberately dumb: it hands back whatever was scripted regardless of
    /// `cursor_seq`, so tests can script the malformed batches the derivation
    /// rule must reject.
    pub struct ScriptedRemoteSource {
        origin_chain_id: u64,
        batches: VecDeque<Result<Vec<OutboxMessage>, RemoteSourceError>>,
        /// Cursor values `next_batch` was called with, in order.
        pub cursors: Vec<u64>,
    }

    impl ScriptedRemoteSource {
        #[must_use]
        pub fn new(origin_chain_id: u64) -> Self {
            Self {
                origin_chain_id,
                batches: VecDeque::new(),
                cursors: Vec::new(),
            }
        }

        pub fn push_batch(&mut self, batch: Result<Vec<OutboxMessage>, RemoteSourceError>) {
            self.batches.push_back(batch);
        }
    }

    impl RemoteChainSource for ScriptedRemoteSource {
        fn origin_chain_id(&self) -> u64 {
            self.origin_chain_id
        }

        async fn next_batch(
            &mut self,
            cursor_seq: u64,
        ) -> Result<Vec<OutboxMessage>, RemoteSourceError> {
            self.cursors.push(cursor_seq);
            match self.batches.pop_front() {
                Some(b) => b,
                // Script exhausted: a real source would block for the next
                // origin block, so do the same rather than inventing an end.
                None => std::future::pending().await,
            }
        }
    }
}
