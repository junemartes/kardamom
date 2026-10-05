//! The notifier's client surfaces without Aeron: events pushed straight
//! into the hub, a WebSocket subscriber that replays then follows, and a
//! webhook subscriber that is down when its events arrive and gets every
//! one of them once when it comes up.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use jsonrpsee::ws_client::WsClientBuilder;
use kardamom_notifier::dto::{
    FeedItem, Stage, StatusFilter, TxStatusEvent, WebhookAck, WebhookRequest,
};
use kardamom_notifier::feed::{Feed, SUBSCRIBE_METHOD, UNSUBSCRIBE_METHOD};
use kardamom_notifier::hub::{Hub, HubConfig, HubHandle, Observed};
use kardamom_notifier::ring::RingConfig;
use kardamom_notifier::server::{Hooks, ListenConfig, Running, serve};
use kardamom_notifier::shard::InstanceSet;
use kardamom_notifier::webhooks::{IDEMPOTENCY_HEADER, SIGNATURE_HEADER, Webhooks, WebhooksConfig};
use kardamom_types::{Receipt, TxStatus};
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Stack {
    hub: HubHandle,
    running: Running,
    shutdown: CancellationToken,
    _dir: tempfile::TempDir,
}

impl Stack {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        let (hub, handle) = Hub::new(
            HubConfig {
                ring: RingConfig {
                    max_age: Duration::from_secs(600),
                    max_events: NonZeroUsize::new(10_000).unwrap(),
                },
                feed_buffer: NonZeroUsize::new(1024).unwrap(),
                ingest_buffer: NonZeroUsize::new(1024).unwrap(),
            },
            shutdown.clone(),
        );
        let (webhooks, registrar) = Webhooks::start(
            WebhooksConfig {
                dir: dir.path().join("webhooks"),
                instances: InstanceSet::new(0, std::num::NonZeroU32::MIN).unwrap(),
                retain_bytes: 1 << 30,
                request_timeout: Duration::from_secs(2),
                queue: NonZeroUsize::new(1024).unwrap(),
            },
            &handle,
            shutdown.clone(),
        )
        .unwrap();
        tokio::spawn(hub.run());
        tokio::spawn(webhooks.run());
        let running = serve(
            ListenConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                max_connections: 100,
            },
            Feed::new(handle.clone(), NonZeroUsize::new(2).unwrap()),
            Hooks::new(registrar, Vec::new()).unwrap(),
            shutdown.clone(),
        )
        .await
        .unwrap();
        Self {
            hub: handle,
            running,
            shutdown,
            _dir: dir,
        }
    }

    fn url(&self, scheme: &str) -> String {
        format!("{scheme}://{}", self.running.addr)
    }

    async fn push(&self, status: TxStatus) {
        self.hub
            .ingest()
            .send(Observed::Status(status))
            .await
            .unwrap();
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

/// One delivery the receiver saw.
struct Delivery {
    key: String,
    signature: String,
    body: Vec<u8>,
}

async fn record(
    req: HttpRequest<hyper::body::Incoming>,
    tx: mpsc::UnboundedSender<Delivery>,
) -> Result<HttpResponse<HttpBody>, tower::BoxError> {
    let header = |name: &str| {
        req.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    let key = header(IDEMPOTENCY_HEADER);
    let signature = header(SIGNATURE_HEADER);
    let body = req.into_body().collect().await?.to_bytes().to_vec();
    let _ = tx.send(Delivery {
        key,
        signature,
        body,
    });
    Ok(HttpResponse::builder()
        .status(200)
        .body(HttpBody::from("ok"))?)
}

/// Accept connections on `listener` and record every POST.
async fn receiver(listener: TcpListener, tx: mpsc::UnboundedSender<Delivery>) {
    while let Ok((sock, _)) = listener.accept().await {
        let tx = tx.clone();
        let svc = tower::service_fn(move |req| record(req, tx.clone()));
        tokio::spawn(jsonrpsee::server::serve(sock, svc));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_subscriber_down_at_the_event_gets_it_once_when_up() {
    let stack = Stack::start().await;
    // The receiver's port is known, but nothing listens on it yet.
    let addr: SocketAddr = kardamom_obs::testkit::free_port();
    let secret = "s3cret".to_string();
    let request = WebhookRequest {
        url: format!("http://{addr}/hook"),
        filter: StatusFilter::all(),
        secret: secret.clone(),
    };
    let http = reqwest::Client::new();
    let ack: WebhookAck = http
        .post(format!("{}/webhooks", stack.url("http")))
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ack.owner, 0);
    // The same registration names the same subscription.
    let again: WebhookAck = http
        .post(format!("{}/webhooks", stack.url("http")))
        .json(&request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again.id, ack.id);

    let alice = Address::repeat_byte(0xA1);
    let h = B256::repeat_byte(7);
    stack.push(TxStatus::offered(h, alice, 3)).await;
    stack.push(TxStatus::sealed(h)).await;
    stack.push(TxStatus::executed(&receipt(h, alice, 3))).await;
    stack.settle(3).await;
    // The first attempt fails against the closed port and the loop backs
    // off. The receiver comes up in the meantime.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let listener = TcpListener::bind(addr).await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(receiver(listener, tx));

    let mut seen = Vec::new();
    let deadline = Duration::from_secs(20);
    while seen.len() < 3 {
        let d = tokio::time::timeout(deadline, rx.recv())
            .await
            .expect("a delivery within the retry window")
            .expect("the receiver is up");
        seen.push(d);
    }
    let keys: Vec<&str> = seen.iter().map(|d| d.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            format!("{h:#x}:offered"),
            format!("{h:#x}:sealed"),
            format!("{h:#x}:executed")
        ]
    );
    for d in &seen {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&d.body);
        let expected = format!(
            "sha256={}",
            alloy_primitives::hex::encode(mac.finalize().into_bytes())
        );
        assert_eq!(d.signature, expected, "the signature covers the body");
        let event: TxStatusEvent = serde_json::from_slice(&d.body).unwrap();
        assert_eq!(event.tx_hash, h);
        assert_eq!(event.sender, Some(alice));
    }
    // Nothing is delivered twice.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .is_err()
    );
    stack.stop().await;
}
