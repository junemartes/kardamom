//! The validator's serving surfaces: a jsonrpsee WS server implementing
//! the shared wire contract (`kardamom-interop-feed`):
//! `kardamom_subscribeOutbox` from the
//! [`FeedStore`] and `kardamom_subscribeAttestations` from the
//! [`AttestationStore`]. OFF by default; enabled by `--serve-feed`.
//!
//! Serving discipline per subscription (the `MockInteropFeed` /
//! `subscribeReceipts` pattern): tap the store's wake channel BEFORE
//! accepting, then scan-and-wait — cursor-honoring backfill first, live
//! items as the store appends. A cursor below the lane floor gets a
//! `Lagged {skipped, floorSeq, floorBlock}` frame, then the retained
//! suffix. The floor is the retention cutoff, or the block the validator
//! resumed at after a restart (the earlier blocks never reached the
//! extractor). A subscriber must not read on past `Lagged`: the watcher
//! stops the pair and an operator resets its cursor.
//!
//! Head events: after the last message a subscriber received, the server
//! sends one `Head {blockNumber}` as soon as a later block closes. The
//! subscriber can then close its open batch. One head per message-bearing
//! block, so an idle lane costs nothing.
//!
//! Caps: [`FeedServerLimits`] bounds live subscriptions per destination
//! and in total. A subscribe over a cap is rejected with an RPC error.
//!
//! Fail-stop coupling: serving and verifying share this process, so a
//! divergence halt exits the process and the sockets die with it — the
//! egress-spec's "a validator whose verification halts must stop serving".

mod slots;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle, SubscriptionSink};
use kardamom_interop_feed::{
    AttestationCursor, AttestationDto, AttestationEventDto, AttestationFeedApiServer, OutboxCursor,
    OutboxEventDto, OutboxFeedApiServer, OutboxMessageDto,
};
use kardamom_types::xchain::OutboxMessage;

use self::slots::{Accepted, Rejected, Slots};
use crate::interop::store::{AttestationStore, FeedStore, LaneFloor, LaneScan};

/// Default total live-subscription cap. The validator binary's
/// `--feed-max-subscriptions` / `KARDAMOM_FEED_MAX_SUBSCRIPTIONS` reads
/// this same constant, so the default has one definition.
pub const DEFAULT_FEED_MAX_SUBSCRIPTIONS: NonZeroUsize =
    NonZeroUsize::new(256).expect("compile-time constant");
/// Default per-destination outbox-subscription cap. The validator
/// binary's `--feed-max-subscriptions-per-dest` /
/// `KARDAMOM_FEED_MAX_SUBSCRIPTIONS_PER_DEST` reads this same constant.
pub const DEFAULT_FEED_MAX_SUBSCRIPTIONS_PER_DEST: NonZeroUsize =
    NonZeroUsize::new(8).expect("compile-time constant");

/// Live-subscription caps for the feed server.
///
/// The validator binary reads the defaults below from
/// `--feed-max-subscriptions` / `KARDAMOM_FEED_MAX_SUBSCRIPTIONS` and
/// `--feed-max-subscriptions-per-dest` /
/// `KARDAMOM_FEED_MAX_SUBSCRIPTIONS_PER_DEST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedServerLimits {
    /// Cap on live subscriptions of both kinds.
    pub max_subscriptions: NonZeroUsize,
    /// Cap on live outbox subscriptions for one destination chain.
    pub max_subscriptions_per_dest: NonZeroUsize,
}

impl Default for FeedServerLimits {
    fn default() -> Self {
        Self {
            max_subscriptions: DEFAULT_FEED_MAX_SUBSCRIPTIONS,
            max_subscriptions_per_dest: DEFAULT_FEED_MAX_SUBSCRIPTIONS_PER_DEST,
        }
    }
}

/// Everything the serving surfaces need; cheap to clone into the server.
pub struct FeedServerState {
    pub chain_id: u64,
    pub validator_id: String,
    pub store: Arc<FeedStore>,
    pub attestations: Arc<AttestationStore>,
    pub limits: FeedServerLimits,
    /// The validator's committed state, for `eth_getStorageAt` (see
    /// [`crate::interop::state_rpc`]). `None` leaves that method off.
    pub state_env: Option<kardamom_state::StateEnv>,
}

struct Handler {
    state: Arc<FeedServerState>,
    slots: Arc<Slots>,
}

/// JSON-RPC error code for a rejected subscription (a cap was hit). Read
/// only by [`slots`], a child module — no `pub` needed for that, and
/// nothing outside this crate names it.
const SUBSCRIPTION_CAP_ERROR_CODE: i32 = -32010;

async fn send_event<T: serde::Serialize>(sink: &SubscriptionSink, event: &T) -> Result<(), ()> {
    let msg = serde_json::value::to_raw_value(event).map_err(|_| ())?;
    sink.send(msg).await.map_err(|_| ())
}

/// One outbox subscriber's live cursor state: the next seq to serve, and
/// the block awaiting a `Head` frame once a later one closes it. `None`
/// means no block is owed a head: before any message, or once the head for
/// the awaited block has gone out. `Some(block)` means a later block still
/// owes that block its head.
struct LaneSession {
    next: u64,
    awaiting_head: Option<u64>,
}

impl LaneSession {
    fn new(start: u64) -> Self {
        Self {
            next: start,
            awaiting_head: None,
        }
    }

    /// The `Lagged` frame, when the cursor is below the lane's floor. Also
    /// advances the cursor to the floor: the subscriber must not read on
    /// past `Lagged`, but the loop still needs a cursor to scan from next.
    fn lag_frame(&mut self, scan: &LaneScan) -> Option<OutboxEventDto> {
        let LaneFloor::Known(floor) = scan.floor_seq else {
            return None;
        };
        if self.next >= floor {
            return None;
        }
        // The guard above proves `floor > self.next`; `saturating_sub`
        // states that bound instead of leaving it to a bare `-`.
        let skipped = floor.saturating_sub(self.next);
        let ev = OutboxEventDto::Lagged {
            skipped,
            floor_seq: Some(floor),
            floor_block: Some(scan.floor_block),
        };
        self.next = floor;
        Some(ev)
    }

    /// One message frame: advances the cursor past it and arms the head
    /// for its block.
    fn message_frame(&mut self, origin: u64, m: &OutboxMessage) -> OutboxEventDto {
        // A wrap here (`m.seq == u64::MAX`) would stall the subscriber's
        // cursor instead of advancing it.
        self.next = m.seq.saturating_add(1);
        self.awaiting_head = Some(m.origin_block_number);
        OutboxEventDto::Message(Box::new(OutboxMessageDto::from_outbox_message(origin, m)))
    }

    /// The `Head` frame, once a later block closes the one a message
    /// armed. The scan's head and messages come from one lock, so a head
    /// past the armed block proves that block is complete.
    fn head_frame(&mut self, head_block: u64) -> Option<OutboxEventDto> {
        let armed = self.awaiting_head?;
        if head_block <= armed {
            return None;
        }
        self.awaiting_head = None;
        Some(OutboxEventDto::Head {
            block_number: head_block,
        })
    }

    /// Send every message in `msgs` in order, advancing this session's
    /// cursor and head-arm state as each one goes out. Stops at the first
    /// send failure (the sink closed), so the caller ends the
    /// subscription there.
    async fn send_messages(
        &mut self,
        sink: &SubscriptionSink,
        origin: u64,
        msgs: &[OutboxMessage],
    ) -> Result<(), ()> {
        for m in msgs {
            let ev = self.message_frame(origin, m);
            send_event(sink, &ev).await?;
        }
        Ok(())
    }
}

/// One `subscribe_outbox` connection: sink, wake channel, the store, and
/// this lane's session state. Concrete, not generic — this shape has
/// exactly one caller.
struct OutboxFeed<'a> {
    sink: &'a SubscriptionSink,
    wake: tokio::sync::watch::Receiver<u64>,
    store: Arc<FeedStore>,
    dest_chain_id: u64,
    origin: u64,
    session: LaneSession,
}

impl OutboxFeed<'_> {
    /// Run the subscription until the sink closes.
    async fn serve(mut self) -> SubscriptionResult {
        while self.step().await.is_some() {}
        Ok(())
    }

    /// One step of the scan-and-wait loop: scan, send a `Lagged` frame if
    /// the cursor aged out of retention, else send retained messages and
    /// any due head frame, then wait for new data. Returns `None` once
    /// the sink closes.
    async fn step(&mut self) -> Option<()> {
        let scan = self.store.from_seq(self.dest_chain_id, self.session.next);
        if let Some(ev) = self.session.lag_frame(&scan) {
            send_event(self.sink, &ev).await.ok()?;
            return Some(());
        }
        self.session
            .send_messages(self.sink, self.origin, &scan.msgs)
            .await
            .ok()?;
        if let Some(ev) = self.session.head_frame(scan.head_block) {
            send_event(self.sink, &ev).await.ok()?;
        }
        tokio::select! {
            () = self.sink.closed() => None,
            r = self.wake.changed() => r.ok(),
        }
    }
}

#[async_trait::async_trait]
impl OutboxFeedApiServer for Handler {
    async fn subscribe_outbox(
        &self,
        pending: PendingSubscriptionSink,
        dest_chain_id: u64,
        cursor: OutboxCursor,
    ) -> SubscriptionResult {
        let Accepted { pending, _guard } = match self.take_slot(pending, Some(dest_chain_id)).await
        {
            Ok(accepted) => accepted,
            Err(Rejected) => return Ok(()),
        };
        // Tap before accept: nothing appended in between may be missed.
        let wake = self.state.store.subscribe();
        let sink = pending.accept().await?;
        let origin = self.state.store.origin_chain_id();
        OutboxFeed {
            sink: &sink,
            wake,
            store: self.state.store.clone(),
            dest_chain_id,
            origin,
            session: LaneSession::new(cursor.seq),
        }
        .serve()
        .await
    }
}

/// One `subscribe_attestations` connection: sink, wake channel, and the
/// server state the attestation frames read from. Concrete, not generic —
/// this shape has exactly one caller.
struct AttestationFeed<'a> {
    sink: &'a SubscriptionSink,
    wake: tokio::sync::watch::Receiver<u64>,
    state: Arc<FeedServerState>,
}

impl AttestationFeed<'_> {
    /// Run the subscription from `start` until the sink closes.
    async fn serve(mut self, start: u64) -> SubscriptionResult {
        let mut next = start;
        while let Some(after) = self.step(next).await {
            next = after;
        }
        Ok(())
    }

    /// One step of the scan-and-wait loop: scan from `next`, send a
    /// `Lagged` frame if the cursor aged out of retention, else send each
    /// retained attestation and wait for new data. Returns the next
    /// cursor to scan from, or `None` once the sink closes.
    async fn step(&mut self, next: u64) -> Option<u64> {
        let scan = self.state.attestations.from_block(next);
        if next < scan.floor {
            // The guard above proves `scan.floor > next`; `saturating_sub`
            // states that bound instead of leaving it to a bare `-`.
            let skipped = scan.floor.saturating_sub(next);
            send_event(self.sink, &AttestationEventDto::Lagged { skipped })
                .await
                .ok()?;
            return Some(scan.floor);
        }
        let mut next = next;
        for a in &scan.items {
            let ev = AttestationEventDto::Attestation(Box::new(AttestationDto {
                chain_id: self.state.chain_id,
                block_number: a.block_number,
                state_root: a.state_root,
                validator_id: self.state.validator_id.clone(),
                // Attestations are unsigned; the wire field is already
                // optional (see the DTO docs).
                signature: None,
            }));
            send_event(self.sink, &ev).await.ok()?;
            // A wrap here (`block_number == u64::MAX`) would stall the
            // subscriber's cursor instead of advancing it.
            next = a.block_number.saturating_add(1);
        }
        tokio::select! {
            () = self.sink.closed() => None,
            r = self.wake.changed() => r.ok().map(|()| next),
        }
    }
}

#[async_trait::async_trait]
impl AttestationFeedApiServer for Handler {
    async fn subscribe_attestations(
        &self,
        pending: PendingSubscriptionSink,
        cursor: AttestationCursor,
    ) -> SubscriptionResult {
        let Accepted { pending, _guard } = match self.take_slot(pending, None).await {
            Ok(accepted) => accepted,
            Err(Rejected) => return Ok(()),
        };
        let wake = self.state.attestations.subscribe();
        let sink = pending.accept().await?;
        AttestationFeed {
            sink: &sink,
            wake,
            state: self.state.clone(),
        }
        .serve(cursor.block_number)
        .await
    }
}

/// Start the feed server. Returns the bound address and the handle whose
/// drop shuts the server down (hold it for the process lifetime).
///
/// # Errors
///
/// Returns an error if the server cannot bind `addr`.
pub async fn start_feed_server(
    addr: SocketAddr,
    state: FeedServerState,
) -> anyhow::Result<(SocketAddr, ServerHandle)> {
    let state = Arc::new(state);
    let slots = Arc::new(Slots::default());
    let server = Server::builder()
        .build(addr)
        .await
        .map_err(|e| anyhow::anyhow!("feed server bind {addr}: {e}"))?;
    let local = server
        .local_addr()
        .map_err(|e| anyhow::anyhow!("feed server local_addr: {e}"))?;
    let mut module = OutboxFeedApiServer::into_rpc(Handler {
        state: state.clone(),
        slots: slots.clone(),
    });
    module.merge(AttestationFeedApiServer::into_rpc(Handler {
        state: state.clone(),
        slots,
    }))?;
    if let Some(env) = &state.state_env {
        use crate::interop::state_rpc::{StateReadApiServer, StateReadHandler};
        module.merge(StateReadApiServer::into_rpc(StateReadHandler::new(
            env.clone(),
        )))?;
    }
    Ok((local, server.start(module)))
}
