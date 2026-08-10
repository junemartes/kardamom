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
//!   * The tail block cannot close until a message from a LATER block arrives —
//!     an implementation can only know a block is done by seeing the next one.
//!     Idle-pair tail latency is therefore unbounded until the feed grows
//!     block-completion (anchoring / heartbeat) events, a later slice.
//!   * A batch is what the watcher hands to `derive_remote_epoch` verbatim.
//!     The source never filters, reorders, de-duplicates or repairs: every one
//!     of those is a verdict the shared derivation rule owns, and a source that
//!     quietly fixed a gap would hide exactly the fault the no-skip rule
//!     exists to catch.

use std::time::Duration;

use async_trait::async_trait;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use kardamom_types::xchain::OutboxMessage;
use tracing::{debug, warn};

use crate::interop::feed::{
    OutboxCursor, OutboxEventDto, SUBSCRIBE_OUTBOX_METHOD, UNSUBSCRIBE_OUTBOX_METHOD,
};
use crate::metrics;

/// Errors a [`RemoteChainSource`] can surface.
///
/// All of them are RETRYABLE at the watcher: none is a statement about the
/// derived chain, which is the only thing that halts a pair. A feed that is
/// down, or serving a shape this build cannot read, stalls the pair loudly and
/// recovers by itself when the peer is fixed.
#[derive(Debug, thiserror::Error)]
pub enum RemoteSourceError {
    /// Connect/subscribe/stream failure against the peer's feed.
    #[error("remote feed transport error: {0}")]
    Transport(String),
    /// A feed item this build cannot interpret (see
    /// [`crate::interop::feed::FeedDecodeError`]).
    #[error("remote feed decode error: {0}")]
    Decode(String),
}

/// The origin-chain view the interop watcher needs.
///
/// `&mut self` (unlike [`crate::source::L1Source`]'s `&self`) because a
/// streaming source is stateful: it holds the live subscription and the
/// partially-accumulated block. That also makes exclusive ownership the
/// contract, so `Sync` is not required.
#[async_trait]
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
    async fn next_batch(
        &mut self,
        cursor_seq: u64,
    ) -> Result<Vec<OutboxMessage>, RemoteSourceError>;
}

/// Production [`RemoteChainSource`]: a jsonrpsee WebSocket subscription to a
/// peer validator's `kardamom_subscribeOutbox`.
///
/// Recovery posture: a dropped connection, an ended subscription, or a
/// [`OutboxEventDto::Lagged`] marker all take the same path — discard the
/// partially-accumulated block and re-subscribe from the source's own cursor.
/// Discarding rather than resuming mid-block is what keeps the batch cut on
/// the origin block boundary after a reconnect, which is what keeps the
/// derived records byte-identical to the un-interrupted run.
pub struct WsRemoteChainSource {
    origin_chain_id: u64,
    dest_chain_id: u64,
    url: String,
    reconnect_backoff: Duration,
    max_reconnect_attempts: u32,
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
            max_reconnect_attempts: 8,
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
    pub fn with_reconnect(mut self, backoff: Duration, max_attempts: u32) -> Self {
        self.reconnect_backoff = backoff;
        self.max_reconnect_attempts = max_attempts.max(1);
        self
    }

    /// Drop the session and the partial block. The next `next_batch` pass
    /// re-subscribes from `self.cursor` and replays the block from its start.
    fn drop_session(&mut self) {
        self.subscription = None;
        self.client = None;
        self.pending.clear();
    }

    async fn connect(&mut self, from: u64) -> Result<(), RemoteSourceError> {
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
        self.subscription = Some(subscription);
        Ok(())
    }

    async fn ensure_subscribed(&mut self, from: u64) -> Result<(), RemoteSourceError> {
        if self.subscription.is_some() {
            return Ok(());
        }
        let mut last = None;
        for attempt in 0..self.max_reconnect_attempts {
            match self.connect(from).await {
                Ok(()) => {
                    debug!(
                        target: "da_watcher::interop",
                        origin = self.origin_chain_id,
                        cursor = from,
                        "subscribed to outbox feed"
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        target: "da_watcher::interop",
                        origin = self.origin_chain_id,
                        attempt,
                        error = %e,
                        "outbox feed subscribe failed; retrying"
                    );
                    last = Some(e);
                    tokio::time::sleep(self.reconnect_backoff).await;
                }
            }
        }
        Err(last.expect("at least one attempt"))
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
        // Track the caller's expected next cursor so an un-advanced caller
        // (backpressure) is detected as a rewind and replayed.
        self.cursor = batch.iter().map(|m| m.seq).max().map(|s| s + 1);
        Some(batch)
    }
}

#[async_trait]
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
            self.ensure_subscribed(from).await?;
            let subscription = self.subscription.as_mut().expect("subscribed above");
            match subscription.next().await {
                Some(Ok(OutboxEventDto::Message(dto))) => {
                    let msg = dto
                        .into_outbox_message(self.origin_chain_id)
                        .map_err(|e| RemoteSourceError::Decode(e.to_string()))?;
                    ::metrics::counter!(
                        metrics::REMOTE_MESSAGES_RECEIVED_TOTAL,
                        "origin" => self.origin_chain_id.to_string()
                    )
                    .increment(1);
                    if let Some(batch) = self.absorb(msg) {
                        return Ok(batch);
                    }
                }
                Some(Ok(OutboxEventDto::Lagged { skipped })) => {
                    // Reading on would skip messages, which the no-skip rule
                    // forbids; the cursor is the only sound resume point.
                    warn!(
                        target: "da_watcher::interop",
                        origin = self.origin_chain_id,
                        skipped,
                        cursor = from,
                        "outbox feed lagged; re-subscribing from cursor"
                    );
                    ::metrics::counter!(
                        metrics::REMOTE_FEED_RESUBSCRIBE_TOTAL,
                        "origin" => self.origin_chain_id.to_string(),
                        "cause" => "lagged"
                    )
                    .increment(1);
                    self.drop_session();
                }
                Some(Err(e)) => {
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
                None => {
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
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::*;

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

    #[async_trait]
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
