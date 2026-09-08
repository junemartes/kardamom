use std::num::NonZeroUsize;

use alloy_primitives::B256;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use kardamom_interop_feed::{
    SUBSCRIBE_ATTESTATIONS_METHOD, SUBSCRIBE_OUTBOX_METHOD, UNSUBSCRIBE_ATTESTATIONS_METHOD,
    UNSUBSCRIBE_OUTBOX_METHOD,
};
use kardamom_types::xchain::OutboxMessage;

use super::*;

fn nz(n: u64) -> crate::interop::store::RetentionBlocks {
    crate::interop::store::RetentionBlocks::new(
        std::num::NonZeroU64::new(n).expect("fixture retention"),
    )
}

fn nzu(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("fixture limit")
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
            state_env: None,
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

/// After a restart the store did not see the earlier blocks. The first
/// message names the lane floor. A stale cursor gets `Lagged` with the
/// floor seq and the resume block, then the retained suffix.
#[tokio::test]
async fn a_resumed_store_lags_a_stale_cursor_with_the_resume_block() {
    let store = Arc::new(FeedStore::new(CHAIN, nz(100)).with_resume_block(Some(40)));
    let atts = Arc::new(AttestationStore::new(nz(100)));
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

/// A lane with one message still delivers. A later block boundary produces
/// one `Head` frame; further empty boundaries produce none.
#[tokio::test]
async fn a_head_event_follows_the_last_message_once() {
    let store = Arc::new(FeedStore::new(CHAIN, nz(100)));
    let atts = Arc::new(AttestationStore::new(nz(100)));
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
    let store = Arc::new(FeedStore::new(CHAIN, nz(100)));
    let atts = Arc::new(AttestationStore::new(nz(100)));
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

/// The per-destination cap and the total cap reject the excess, and a
/// closed subscription frees its slot.
#[tokio::test]
async fn subscription_caps_reject_the_excess() {
    let store = Arc::new(FeedStore::new(CHAIN, nz(100)));
    let atts = Arc::new(AttestationStore::new(nz(100)));
    let (addr, _handle) = spawn_with_limits(
        store,
        atts,
        FeedServerLimits {
            max_subscriptions: nzu(2),
            max_subscriptions_per_dest: nzu(1),
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
    while subscribe_outbox(&client, 0).await.is_err() {
        assert!(std::time::Instant::now() < deadline, "slot never freed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
