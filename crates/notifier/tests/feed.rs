//! The notifier's client surface without Aeron: events pushed straight
//! into the hub, and a WebSocket subscriber that replays then follows.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use kardamom_notifier::dto::{FeedItem, Stage, StatusFilter, TxStatusEvent};
use kardamom_notifier::feed::{Feed, SUBSCRIBE_METHOD, UNSUBSCRIBE_METHOD};
use kardamom_notifier::hub::{Hub, HubConfig, HubHandle, Observed};
use kardamom_notifier::ring::RingConfig;
use kardamom_notifier::server::{ListenConfig, Running, serve};
use kardamom_types::{Receipt, TxStatus};
use tokio_util::sync::CancellationToken;

struct Stack {
    hub: HubHandle,
    running: Running,
    shutdown: CancellationToken,
}

impl Stack {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    /// A stack with the events path on, with a stage table of `entries`,
    /// or off with `None`.
    async fn start_with(stage_table: Option<NonZeroUsize>) -> Self {
        let shutdown = CancellationToken::new();
        let (hub, handle) = Hub::new(
            HubConfig {
                ring: RingConfig {
                    max_age: Duration::from_secs(600),
                    max_events: NonZeroUsize::new(10_000).unwrap(),
                },
                feed_buffer: NonZeroUsize::new(1024).unwrap(),
                ingest_buffer: NonZeroUsize::new(1024).unwrap(),
                stage_table,
            },
            shutdown.clone(),
        );
        tokio::spawn(hub.run());
        let running = serve(
            ListenConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                max_connections: 100,
            },
            Feed::new(handle.clone(), NonZeroUsize::new(2).unwrap()),
            shutdown.clone(),
        )
        .await
        .unwrap();
        Self {
            hub: handle,
            running,
            shutdown,
        }
    }

    fn url(&self, scheme: &str) -> String {
        format!("{scheme}://{}", self.running.addr)
    }

    async fn push(&self, status: TxStatus) {
        self.observe(Observed::Status(status)).await;
    }

    async fn observe(&self, observed: Observed) {
        self.hub.ingest().send(observed).await.unwrap();
    }

    /// Wait until the ring holds `n` events.
    async fn settle(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.hub.replay(StatusFilter::all(), 0, 1 << 20).await.len() < n {
            assert!(
                Instant::now() < deadline,
                "the ring never reached {n} events"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.running.stop().await;
    }
}

fn receipt(hash: B256, sender: Address, nonce: u64) -> Receipt {
    Receipt {
        tx_hash: hash,
        from: sender,
        nonce,
        status: true,
        ..Receipt::default()
    }
}

async fn next_status(sub: &mut Subscription<FeedItem>) -> TxStatusEvent {
    let item = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("an item within 5 s")
        .expect("the stream is open")
        .expect("the item parses");
    match item {
        FeedItem::Status(event) => event,
        FeedItem::Lagged(l) => panic!("unexpected lag marker: {l:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_replays_the_ring_then_follows_live() {
    let stack = Stack::start().await;
    let alice = Address::repeat_byte(0xA1);
    let bob = Address::repeat_byte(0xB0);
    let h1 = B256::repeat_byte(1);
    let h2 = B256::repeat_byte(2);
    stack.push(TxStatus::offered(h1, alice, 0)).await;
    stack.push(TxStatus::sealed(h1)).await;
    stack.push(TxStatus::offered(h2, bob, 0)).await;
    // Both ingress replicas publish a sealed: the second copy is dropped.
    stack.push(TxStatus::sealed(h1)).await;
    stack.settle(3).await;

    let client = WsClientBuilder::default()
        .build(stack.url("ws"))
        .await
        .unwrap();
    let mut sub: Subscription<FeedItem> = client
        .subscribe(
            SUBSCRIBE_METHOD,
            rpc_params![StatusFilter::Sender { sender: alice }],
            UNSUBSCRIBE_METHOD,
        )
        .await
        .unwrap();
    let offered = next_status(&mut sub).await;
    assert_eq!((offered.tx_hash, offered.stage), (h1, Stage::Offered));
    let sealed = next_status(&mut sub).await;
    assert_eq!((sealed.tx_hash, sealed.stage), (h1, Stage::Sealed));
    assert_eq!(sealed.sender, Some(alice), "the ring resolves the sender");

    // Live: bob's receipt is filtered out, alice's executed comes through.
    stack.push(TxStatus::executed(&receipt(h2, bob, 0))).await;
    stack.push(TxStatus::executed(&receipt(h1, alice, 0))).await;
    let executed = next_status(&mut sub).await;
    assert_eq!((executed.tx_hash, executed.stage), (h1, Stage::Executed));
    assert_eq!(executed.status, Some(1));

    // The full feed, subscribed later, replays all five in order.
    let mut all: Subscription<FeedItem> = client
        .subscribe(
            SUBSCRIBE_METHOD,
            rpc_params![StatusFilter::all()],
            UNSUBSCRIBE_METHOD,
        )
        .await
        .unwrap();
    let mut stages = Vec::new();
    for _ in 0..5 {
        let e = next_status(&mut all).await;
        stages.push((e.tx_hash, e.stage));
    }
    assert_eq!(
        stages,
        vec![
            (h1, Stage::Offered),
            (h1, Stage::Sealed),
            (h2, Stage::Offered),
            (h2, Stage::Executed),
            (h1, Stage::Executed),
        ]
    );
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_events_path_sends_the_latest_state_of_each_hash() {
    use kardamom_log::aeron_live::ReceiptFrame;
    use kardamom_types::ReceiptBatch;
    use kardamom_types::tx_events::{TxEventKind, TxEventsFrame};

    let stack = Stack::start_with(NonZeroUsize::new(1024)).await;
    let client = WsClientBuilder::default()
        .build(stack.url("ws"))
        .await
        .unwrap();
    let mut sub: Subscription<FeedItem> = client
        .subscribe(
            SUBSCRIBE_METHOD,
            rpc_params![StatusFilter::all()],
            UNSUBSCRIBE_METHOD,
        )
        .await
        .unwrap();
    let hash = B256::repeat_byte(0x31);
    let sender = Address::repeat_byte(0x32);
    // A `tx_status` stage that the path replaces is not stored.
    stack.push(TxStatus::offered(hash, sender, 1)).await;
    let mut accepted = TxEventsFrame::new(TxEventKind::Accepted, 1);
    accepted.push_accepted(&hash);
    stack
        .observe(Observed::Events(accepted.bytes().to_vec()))
        .await;
    let mut merged = receipt(hash, sender, 1);
    merged.block_number = 3;
    let frame = ReceiptFrame::Batch(ReceiptBatch {
        receipts: vec![merged],
        accounts: Vec::new(),
    });
    stack.observe(Observed::Merged(frame.clone())).await;
    // A copy from the other egress is not sent.
    stack.observe(Observed::Merged(frame)).await;
    let first = next_status(&mut sub).await;
    assert_eq!((first.tx_hash, first.stage), (hash, Stage::Accepted));
    let second = next_status(&mut sub).await;
    assert_eq!((second.tx_hash, second.stage), (hash, Stage::Receipt));
    assert_eq!(second.sender, Some(sender));
    stack.settle(2).await;
    let ring = stack.hub.replay(StatusFilter::all(), 0, 100).await;
    assert_eq!(
        ring.len(),
        2,
        "the offered record and the copy are not stored"
    );
    stack.stop().await;
}
