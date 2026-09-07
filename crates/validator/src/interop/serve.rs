//! The validator's serving surfaces (spec §5, egress spec E1): a jsonrpsee
//! WS server — the validator's first; until now it exposed only Prometheus
//! metrics — implementing the shared wire contract
//! (`kardamom-interop-feed`): `kardamom_subscribeOutbox` from the
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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle, SubscriptionSink};
use jsonrpsee::types::ErrorObject;
use kardamom_interop_feed::{
    AttestationCursor, AttestationDto, AttestationEventDto, AttestationFeedApiServer, OutboxCursor,
    OutboxEventDto, OutboxFeedApiServer, OutboxMessageDto,
};

use crate::interop::store::{AttestationStore, FeedStore};

/// Live-subscription caps for the feed server.
///
/// Defaults: 256 subscriptions in total (outbox and attestation streams
/// together), 8 outbox subscriptions per destination chain. The validator
/// binary reads them from `--feed-max-subscriptions` /
/// `KARDAMOM_FEED_MAX_SUBSCRIPTIONS` and
/// `--feed-max-subscriptions-per-dest` /
/// `KARDAMOM_FEED_MAX_SUBSCRIPTIONS_PER_DEST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedServerLimits {
    /// Cap on live subscriptions of both kinds.
    pub max_subscriptions: usize,
    /// Cap on live outbox subscriptions for one destination chain.
    pub max_subscriptions_per_dest: usize,
}

impl Default for FeedServerLimits {
    fn default() -> Self {
        Self {
            max_subscriptions: 256,
            max_subscriptions_per_dest: 8,
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
}

/// Live-subscription counters, shared by both handlers.
#[derive(Default)]
struct Slots {
    inner: Mutex<SlotCounts>,
}

#[derive(Default)]
struct SlotCounts {
    total: usize,
    per_dest: BTreeMap<u64, usize>,
}

/// Holds one subscription slot; drop releases it. Every return path of a
/// handler drops it, so a closed socket always frees its slot.
struct SlotGuard {
    slots: Arc<Slots>,
    dest: Option<u64>,
}

impl Slots {
    /// Take a slot for a subscription, `dest` for outbox subscriptions.
    /// Returns the cap that was hit.
    fn take(
        self: &Arc<Self>,
        limits: FeedServerLimits,
        dest: Option<u64>,
    ) -> Result<SlotGuard, &'static str> {
        let mut g = self.inner.lock().unwrap();
        if g.total >= limits.max_subscriptions {
            return Err("total subscription cap reached");
        }
        if let Some(d) = dest {
            let n = g.per_dest.entry(d).or_default();
            if *n >= limits.max_subscriptions_per_dest {
                return Err("per-destination subscription cap reached");
            }
            *n += 1;
        }
        g.total += 1;
        Ok(SlotGuard {
            slots: self.clone(),
            dest,
        })
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let mut g = self.slots.inner.lock().unwrap();
        g.total = g.total.saturating_sub(1);
        if let Some(d) = self.dest
            && let Some(n) = g.per_dest.get_mut(&d)
        {
            *n = n.saturating_sub(1);
            // Drop the entry at zero, so a client that churns destination
            // ids cannot grow the map without bound.
            if *n == 0 {
                g.per_dest.remove(&d);
            }
        }
    }
}

struct Handler {
    state: Arc<FeedServerState>,
    slots: Arc<Slots>,
}

/// JSON-RPC error code for a rejected subscription (a cap was hit).
pub const SUBSCRIPTION_CAP_ERROR_CODE: i32 = -32010;

async fn send_event<T: serde::Serialize>(sink: &SubscriptionSink, event: &T) -> Result<(), ()> {
    let msg = serde_json::value::to_raw_value(event).map_err(|_| ())?;
    sink.send(msg).await.map_err(|_| ())
}

/// Take a slot, or reject the pending subscription. `None` means rejected
/// (the caller returns `Ok(())`: the client already got the error).
async fn take_slot_or_reject(
    handler: &Handler,
    pending: PendingSubscriptionSink,
    dest: Option<u64>,
) -> Option<(PendingSubscriptionSink, SlotGuard)> {
    match handler.slots.take(handler.state.limits, dest) {
        Ok(guard) => Some((pending, guard)),
        Err(cap) => {
            tracing::warn!(
                target: "validator::interop::serve",
                dest,
                cap,
                "feed subscription rejected"
            );
            crate::metrics::counter_feed_subscription_rejected();
            pending
                .reject(ErrorObject::owned(
                    SUBSCRIPTION_CAP_ERROR_CODE,
                    cap,
                    None::<()>,
                ))
                .await;
            None
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
        let Some((pending, _slot)) = take_slot_or_reject(self, pending, Some(dest_chain_id)).await
        else {
            return Ok(());
        };
        // Tap before accept: nothing appended in between may be missed.
        let mut wake = self.state.store.subscribe();
        let sink = pending.accept().await?;
        let origin = self.state.store.origin_chain_id();
        let mut next = cursor.seq;
        // Block of the last message sent, and whether a head for a later
        // block went out since. One head closes the subscriber's open
        // batch; more would be noise.
        let mut last_msg_block: Option<u64> = None;
        let mut head_sent = false;
        loop {
            let scan = self.state.store.from_seq(dest_chain_id, next);
            if let Some(floor) = scan.floor_seq
                && next < floor
            {
                // The cursor is below what this store can serve: name the
                // loss and the floor, then serve what is retained. The
                // subscriber must not read on; the watcher stops the pair.
                let ev = OutboxEventDto::Lagged {
                    skipped: floor - next,
                    floor_seq: Some(floor),
                    floor_block: Some(scan.floor_block),
                };
                if send_event(&sink, &ev).await.is_err() {
                    return Ok(());
                }
                next = floor;
                continue;
            }
            for m in &scan.msgs {
                let ev = OutboxEventDto::Message(Box::new(OutboxMessageDto::from_outbox_message(
                    origin, m,
                )));
                if send_event(&sink, &ev).await.is_err() {
                    return Ok(());
                }
                next = m.seq + 1;
                last_msg_block = Some(m.origin_block_number);
                head_sent = false;
            }
            // The scan's head and messages come from one lock, so a head
            // past the last message's block proves that block is complete.
            if let Some(b) = last_msg_block
                && !head_sent
                && scan.head_block > b
            {
                let ev = OutboxEventDto::Head {
                    block_number: scan.head_block,
                };
                if send_event(&sink, &ev).await.is_err() {
                    return Ok(());
                }
                head_sent = true;
            }
            tokio::select! {
                () = sink.closed() => return Ok(()),
                r = wake.changed() => {
                    if r.is_err() {
                        return Ok(()); // store gone — shutdown
                    }
                }
            }
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
        let Some((pending, _slot)) = take_slot_or_reject(self, pending, None).await else {
            return Ok(());
        };
        let mut wake = self.state.attestations.subscribe();
        let sink = pending.accept().await?;
        let mut next = cursor.block_number;
        loop {
            let (atts, floor) = self.state.attestations.from_block(next);
            if next < floor {
                let ev = AttestationEventDto::Lagged {
                    skipped: floor - next,
                };
                if send_event(&sink, &ev).await.is_err() {
                    return Ok(());
                }
                next = floor;
                continue;
            }
            for (block, root) in &atts {
                let ev = AttestationEventDto::Attestation(Box::new(AttestationDto {
                    chain_id: self.state.chain_id,
                    block_number: *block,
                    state_root: *root,
                    validator_id: self.state.validator_id.clone(),
                    // UNSIGNED in E1 — E2 adds the per-validator key; the
                    // wire field is already optional (see the DTO docs).
                    signature: None,
                }));
                if send_event(&sink, &ev).await.is_err() {
                    return Ok(());
                }
                next = block + 1;
            }
            tokio::select! {
                () = sink.closed() => return Ok(()),
                r = wake.changed() => {
                    if r.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Start the feed server. Returns the bound address and the handle whose
/// drop shuts the server down (hold it for the process lifetime).
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
    module.merge(AttestationFeedApiServer::into_rpc(Handler { state, slots }))?;
    Ok((local, server.start(module)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};
    use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
    use jsonrpsee::rpc_params;
    use jsonrpsee::ws_client::WsClientBuilder;
    use kardamom_interop_feed::{
        SUBSCRIBE_ATTESTATIONS_METHOD, SUBSCRIBE_OUTBOX_METHOD, UNSUBSCRIBE_ATTESTATIONS_METHOD,
        UNSUBSCRIBE_OUTBOX_METHOD,
    };
    use kardamom_types::xchain::OutboxMessage;

    const CHAIN: u64 = 412_346;
    const DEST: u64 = 412_347;

    fn msg(seq: u64, block: u64) -> OutboxMessage {
        OutboxMessage {
            origin_block_number: block,
            origin_block_hash: B256::repeat_byte(block as u8),
            dest_chain_id: DEST,
            seq,
            sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 100_000,
            data: Default::default(),
            callback: None,
        }
    }

    async fn spawn(
        store: Arc<FeedStore>,
        attestations: Arc<AttestationStore>,
    ) -> (SocketAddr, ServerHandle) {
        spawn_with_limits(store, attestations, FeedServerLimits::default()).await
    }

    async fn spawn_with_limits(
        store: Arc<FeedStore>,
        attestations: Arc<AttestationStore>,
        limits: FeedServerLimits,
    ) -> (SocketAddr, ServerHandle) {
        start_feed_server(
            "127.0.0.1:0".parse().unwrap(),
            FeedServerState {
                chain_id: CHAIN,
                validator_id: "test-validator".into(),
                store,
                attestations,
                limits,
            },
        )
        .await
        .unwrap()
    }

    async fn connect(addr: SocketAddr) -> jsonrpsee::ws_client::WsClient {
        WsClientBuilder::default()
            .build(format!("ws://{addr}"))
            .await
            .unwrap()
    }

    async fn subscribe_outbox(
        client: &jsonrpsee::ws_client::WsClient,
        cursor: u64,
    ) -> Result<Subscription<OutboxEventDto>, jsonrpsee::core::ClientError> {
        client
            .subscribe(
                SUBSCRIBE_OUTBOX_METHOD,
                rpc_params![DEST, OutboxCursor::new(cursor)],
                UNSUBSCRIBE_OUTBOX_METHOD,
            )
            .await
    }

    /// Nothing arrives on `sub` within 200 ms.
    async fn assert_quiet(sub: &mut Subscription<OutboxEventDto>) {
        let r = tokio::time::timeout(std::time::Duration::from_millis(200), sub.next()).await;
        assert!(r.is_err(), "expected no frame, got {r:?}");
    }

    /// Backfill from the cursor, then live items — over a REAL WS client,
    /// speaking exactly the watcher's wire protocol.
    #[tokio::test]
    async fn outbox_subscription_backfills_then_streams() {
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let atts = Arc::new(AttestationStore::new(100));
        store.append_block(1, vec![msg(0, 1), msg(1, 1)]);
        let (addr, _handle) = spawn(store.clone(), atts).await;

        let client = WsClientBuilder::default()
            .build(format!("ws://{addr}"))
            .await
            .unwrap();
        let mut sub: Subscription<OutboxEventDto> = client
            .subscribe(
                SUBSCRIBE_OUTBOX_METHOD,
                rpc_params![DEST, OutboxCursor::new(1)],
                UNSUBSCRIBE_OUTBOX_METHOD,
            )
            .await
            .unwrap();

        // Cursor 1: seq 0 is skipped, seq 1 backfills.
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(m.seq, 1);
        assert_eq!(m.origin_chain_id, CHAIN);

        // Live append reaches the open subscription.
        store.append_block(2, vec![msg(2, 2)]);
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(m.seq, 2);
    }

    /// A cursor below the retention floor gets a Lagged frame naming the
    /// loss, then the retained suffix.
    #[tokio::test]
    async fn outbox_cursor_below_retention_is_lagged() {
        let store = Arc::new(FeedStore::new(CHAIN, 2));
        let atts = Arc::new(AttestationStore::new(100));
        store.append_block(1, vec![msg(0, 1), msg(1, 1)]);
        store.append_block(5, vec![msg(2, 5)]);
        // head 5, retention 2 -> block-1 messages pruned, floor = 2.
        let (addr, _handle) = spawn(store, atts).await;

        let client = WsClientBuilder::default()
            .build(format!("ws://{addr}"))
            .await
            .unwrap();
        let mut sub: Subscription<OutboxEventDto> = client
            .subscribe(
                SUBSCRIBE_OUTBOX_METHOD,
                rpc_params![DEST, OutboxCursor::new(0)],
                UNSUBSCRIBE_OUTBOX_METHOD,
            )
            .await
            .unwrap();

        assert_eq!(
            sub.next().await.unwrap().unwrap(),
            OutboxEventDto::Lagged {
                skipped: 2,
                floor_seq: Some(2),
                floor_block: Some(3),
            }
        );
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected the retained suffix after Lagged");
        };
        assert_eq!(m.seq, 2);
    }

    #[tokio::test]
    async fn attestations_stream_unsigned_with_cursor() {
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let atts = Arc::new(AttestationStore::new(100));
        atts.push(1, B256::repeat_byte(0x01));
        atts.push(2, B256::repeat_byte(0x02));
        let (addr, _handle) = spawn(store, atts.clone()).await;

        let client = WsClientBuilder::default()
            .build(format!("ws://{addr}"))
            .await
            .unwrap();
        let mut sub: Subscription<AttestationEventDto> = client
            .subscribe(
                SUBSCRIBE_ATTESTATIONS_METHOD,
                rpc_params![AttestationCursor::new(2)],
                UNSUBSCRIBE_ATTESTATIONS_METHOD,
            )
            .await
            .unwrap();

        let AttestationEventDto::Attestation(a) = sub.next().await.unwrap().unwrap() else {
            panic!("expected an attestation");
        };
        assert_eq!(a.block_number, 2);
        assert_eq!(a.chain_id, CHAIN);
        assert_eq!(a.state_root, B256::repeat_byte(0x02));
        assert_eq!(a.validator_id, "test-validator");
        assert!(a.signature.is_none(), "E1 attestations are unsigned");

        // Live root reaches the open subscription.
        atts.push(3, B256::repeat_byte(0x03));
        let AttestationEventDto::Attestation(a) = sub.next().await.unwrap().unwrap() else {
            panic!("expected an attestation");
        };
        assert_eq!(a.block_number, 3);
    }

    /// H8: after a restart the store did not see the earlier blocks. The
    /// first message names the lane floor. A stale cursor gets `Lagged`
    /// with the floor seq and the resume block, then the retained suffix.
    #[tokio::test]
    async fn a_resumed_store_lags_a_stale_cursor_with_the_resume_block() {
        let store = Arc::new(FeedStore::new(CHAIN, 100).with_resume_block(Some(40)));
        let atts = Arc::new(AttestationStore::new(100));
        let (addr, _handle) = spawn(store.clone(), atts).await;
        let client = connect(addr).await;
        let mut sub = subscribe_outbox(&client, 0).await.unwrap();

        // The floor is unknown until the first post-resume message: silence.
        store.append_block(40, vec![]);
        assert_quiet(&mut sub).await;

        store.append_block(41, vec![msg(5, 41)]);
        assert_eq!(
            sub.next().await.unwrap().unwrap(),
            OutboxEventDto::Lagged {
                skipped: 5,
                floor_seq: Some(5),
                floor_block: Some(40),
            }
        );
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected the retained suffix after Lagged");
        };
        assert_eq!(m.seq, 5);
    }

    /// M7: a lane with one message still delivers. A later block boundary
    /// produces one `Head` frame; further empty boundaries produce none.
    #[tokio::test]
    async fn a_head_event_follows_the_last_message_once() {
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let atts = Arc::new(AttestationStore::new(100));
        store.append_block(1, vec![msg(0, 1)]);
        let (addr, _handle) = spawn(store.clone(), atts).await;
        let client = connect(addr).await;
        let mut sub = subscribe_outbox(&client, 0).await.unwrap();

        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(m.seq, 0);
        // Block 1 is still the head: nothing closes it yet.
        assert_quiet(&mut sub).await;

        store.append_block(2, vec![]);
        assert_eq!(
            sub.next().await.unwrap().unwrap(),
            OutboxEventDto::Head { block_number: 2 }
        );
        // Idle boundaries after the head are silent.
        store.append_block(3, vec![]);
        assert_quiet(&mut sub).await;

        // The next message re-arms the head.
        store.append_block(4, vec![msg(1, 4)]);
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(m.seq, 1);
        store.append_block(5, vec![]);
        assert_eq!(
            sub.next().await.unwrap().unwrap(),
            OutboxEventDto::Head { block_number: 5 }
        );
    }

    /// A subscriber that connects after the block closed gets the head
    /// right after the backfill.
    #[tokio::test]
    async fn backfill_ends_with_a_head_when_the_block_is_closed() {
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let atts = Arc::new(AttestationStore::new(100));
        store.append_block(1, vec![msg(0, 1)]);
        store.append_block(7, vec![]);
        let (addr, _handle) = spawn(store, atts).await;
        let client = connect(addr).await;
        let mut sub = subscribe_outbox(&client, 0).await.unwrap();
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(m.seq, 0);
        assert_eq!(
            sub.next().await.unwrap().unwrap(),
            OutboxEventDto::Head { block_number: 7 }
        );
    }

    /// M9: the per-destination cap and the total cap reject the excess,
    /// and a closed subscription frees its slot.
    #[tokio::test]
    async fn subscription_caps_reject_the_excess() {
        let store = Arc::new(FeedStore::new(CHAIN, 100));
        let atts = Arc::new(AttestationStore::new(100));
        let (addr, _handle) = spawn_with_limits(
            store,
            atts,
            FeedServerLimits {
                max_subscriptions: 2,
                max_subscriptions_per_dest: 1,
            },
        )
        .await;
        let client = connect(addr).await;

        let first = subscribe_outbox(&client, 0).await.unwrap();
        // Second outbox subscription on the same destination: over the cap.
        let err = subscribe_outbox(&client, 0).await.unwrap_err();
        assert!(
            err.to_string().contains("per-destination"),
            "expected the per-destination cap, got {err}"
        );
        // An attestation subscription fills the total cap.
        let _atts: Subscription<AttestationEventDto> = client
            .subscribe(
                SUBSCRIBE_ATTESTATIONS_METHOD,
                rpc_params![AttestationCursor::new(0)],
                UNSUBSCRIBE_ATTESTATIONS_METHOD,
            )
            .await
            .unwrap();
        let err = client
            .subscribe::<AttestationEventDto, _>(
                SUBSCRIBE_ATTESTATIONS_METHOD,
                rpc_params![AttestationCursor::new(0)],
                UNSUBSCRIBE_ATTESTATIONS_METHOD,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("total"),
            "expected the total cap, got {err}"
        );

        // Closing the outbox subscription frees its slot (asynchronously).
        first.unsubscribe().await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if subscribe_outbox(&client, 0).await.is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "slot never freed");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}
