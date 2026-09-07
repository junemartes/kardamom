//! The validator's serving surfaces: a jsonrpsee WS server implementing
//! the shared wire contract (`kardamom-interop-feed`):
//! `kardamom_subscribeOutbox` from the
//! [`FeedStore`] and `kardamom_subscribeAttestations` from the
//! [`AttestationStore`]. OFF by default; enabled by `--serve-feed`.
//!
//! Serving discipline per subscription (the `MockInteropFeed` /
//! `subscribeReceipts` pattern): tap the store's wake channel BEFORE
//! accepting, then scan-and-wait — cursor-honoring backfill first, live
//! items as the store appends. A cursor below the retention floor gets a
//! `Lagged {skipped}` frame; recovery is re-subscribing from the
//! subscriber's own cursor (and if that cursor is still below retention the
//! subscriber has genuinely lost the window — deeper backfill is v2, the
//! data is in DA).
//!
//! Fail-stop coupling: serving and verifying share this process, so a
//! divergence halt exits the process and the sockets die with it — the
//! egress-spec's "a validator whose verification halts must stop serving".

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::B256;
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle, SubscriptionSink};
use kardamom_interop_feed::{
    AttestationCursor, AttestationDto, AttestationEventDto, AttestationFeedApiServer, OutboxCursor,
    OutboxEventDto, OutboxFeedApiServer, OutboxMessageDto,
};

use crate::interop::store::{AttestationStore, FeedStore};

/// Everything the serving surfaces need; cheap to clone into the server.
pub struct FeedServerState {
    pub chain_id: u64,
    pub validator_id: String,
    pub store: Arc<FeedStore>,
    pub attestations: Arc<AttestationStore>,
}

struct Handler {
    state: Arc<FeedServerState>,
}

async fn send_event<T: serde::Serialize>(sink: &SubscriptionSink, event: &T) -> Result<(), ()> {
    let msg = serde_json::value::to_raw_value(event).map_err(|_| ())?;
    sink.send(msg).await.map_err(|_| ())
}

/// The subscription loop shared by both feeds: cursor-honoring backfill
/// first, then live items as the store appends, until the sink closes or
/// the store shuts down. `scan(next)` returns the items retained from
/// `next` and the lane's retention floor. `lagged(skipped)` builds the
/// wire frame for a cursor that aged out of retention. `item(&I)` builds
/// one item's wire frame plus the cursor value to advance to after
/// sending it. Sync callbacks only: the store reads are lock-and-return,
/// no I/O, so only this function's own send/wait steps are async — the one
/// place either feed's per-item loop needs to nest inside the outer
/// scan-and-wait loop.
async fn serve_cursor_feed<I, E: serde::Serialize>(
    sink: &SubscriptionSink,
    mut wake: tokio::sync::watch::Receiver<u64>,
    start: u64,
    scan: impl Fn(u64) -> (Vec<I>, u64),
    lagged: impl Fn(u64) -> E,
    item: impl Fn(&I) -> (E, u64),
) -> SubscriptionResult {
    let mut next = start;
    loop {
        let (items, floor) = scan(next);
        if next < floor {
            // The cursor aged out of retention: name the loss, then serve
            // what is retained (the subscriber re-subscribes from its own
            // cursor regardless — Lagged is not a read-on). Bounded: the
            // guarding `if` proves `floor > next`.
            if send_event(sink, &lagged(floor - next)).await.is_err() {
                return Ok(());
            }
            next = floor;
            continue;
        }
        for it in &items {
            let (ev, advance_to) = item(it);
            if send_event(sink, &ev).await.is_err() {
                return Ok(());
            }
            next = advance_to;
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

#[async_trait::async_trait]
impl OutboxFeedApiServer for Handler {
    async fn subscribe_outbox(
        &self,
        pending: PendingSubscriptionSink,
        dest_chain_id: u64,
        cursor: OutboxCursor,
    ) -> SubscriptionResult {
        // Tap before accept: nothing appended in between may be missed.
        let wake = self.state.store.subscribe();
        let sink = pending.accept().await?;
        let origin = self.state.store.origin_chain_id();
        serve_cursor_feed(
            &sink,
            wake,
            cursor.seq,
            |next| self.state.store.from_seq(dest_chain_id, next),
            |skipped| OutboxEventDto::Lagged { skipped },
            |m: &kardamom_types::xchain::OutboxMessage| {
                let ev = OutboxEventDto::Message(Box::new(OutboxMessageDto::from_outbox_message(
                    origin, m,
                )));
                // A wrap here (`m.seq == u64::MAX`) would stall the
                // subscriber's cursor instead of advancing it.
                (ev, m.seq.saturating_add(1))
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl AttestationFeedApiServer for Handler {
    async fn subscribe_attestations(
        &self,
        pending: PendingSubscriptionSink,
        cursor: AttestationCursor,
    ) -> SubscriptionResult {
        let wake = self.state.attestations.subscribe();
        let sink = pending.accept().await?;
        serve_cursor_feed(
            &sink,
            wake,
            cursor.block_number,
            |next| self.state.attestations.from_block(next),
            |skipped| AttestationEventDto::Lagged { skipped },
            |(block, root): &(u64, B256)| {
                let ev = AttestationEventDto::Attestation(Box::new(AttestationDto {
                    chain_id: self.state.chain_id,
                    block_number: *block,
                    state_root: *root,
                    validator_id: self.state.validator_id.clone(),
                    // Attestations are unsigned; the wire field is
                    // already optional (see the DTO docs).
                    signature: None,
                }));
                // A wrap here (`block == u64::MAX`) would stall the
                // subscriber's cursor instead of advancing it.
                (ev, block.saturating_add(1))
            },
        )
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
    let server = Server::builder()
        .build(addr)
        .await
        .map_err(|e| anyhow::anyhow!("feed server bind {addr}: {e}"))?;
    let local = server
        .local_addr()
        .map_err(|e| anyhow::anyhow!("feed server local_addr: {e}"))?;
    let mut module = OutboxFeedApiServer::into_rpc(Handler {
        state: state.clone(),
    });
    module.merge(AttestationFeedApiServer::into_rpc(Handler { state }))?;
    Ok((local, server.start(module)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
    use jsonrpsee::rpc_params;
    use jsonrpsee::ws_client::WsClientBuilder;
    use kardamom_interop_feed::{
        SUBSCRIBE_ATTESTATIONS_METHOD, SUBSCRIBE_OUTBOX_METHOD, UNSUBSCRIBE_ATTESTATIONS_METHOD,
        UNSUBSCRIBE_OUTBOX_METHOD,
    };
    use kardamom_types::xchain::OutboxMessage;

    fn nz(n: u64) -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(n).expect("fixture retention")
    }

    const CHAIN: u64 = 412_346;
    const DEST: u64 = 412_347;

    fn msg(seq: u64, block: u64) -> OutboxMessage {
        crate::interop::outbox_msg(DEST, seq, block)
    }

    async fn spawn(
        store: Arc<FeedStore>,
        attestations: Arc<AttestationStore>,
    ) -> (SocketAddr, ServerHandle) {
        start_feed_server(
            "127.0.0.1:0".parse().unwrap(),
            FeedServerState {
                chain_id: CHAIN,
                validator_id: "test-validator".into(),
                store,
                attestations,
            },
        )
        .await
        .unwrap()
    }

    /// Backfill from the cursor, then live items — over a REAL WS client,
    /// speaking exactly the watcher's wire protocol.
    #[tokio::test]
    async fn outbox_subscription_backfills_then_streams() {
        let store = Arc::new(FeedStore::new(CHAIN, nz(100)));
        let atts = Arc::new(AttestationStore::new(nz(100)));
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
        let store = Arc::new(FeedStore::new(CHAIN, nz(2)));
        let atts = Arc::new(AttestationStore::new(nz(100)));
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
            OutboxEventDto::Lagged { skipped: 2 }
        );
        let OutboxEventDto::Message(m) = sub.next().await.unwrap().unwrap() else {
            panic!("expected the retained suffix after Lagged");
        };
        assert_eq!(m.seq, 2);
    }

    #[tokio::test]
    async fn attestations_stream_unsigned_with_cursor() {
        let store = Arc::new(FeedStore::new(CHAIN, nz(100)));
        let atts = Arc::new(AttestationStore::new(nz(100)));
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
}
