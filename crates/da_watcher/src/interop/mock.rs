//! `MockInteropFeed` — a scripted stand-in for an origin chain's validator
//! feed, and the simulated external validator the two-chain e2e harness drives.
//!
//! It is a REAL jsonrpsee WebSocket server implementing the real
//! [`OutboxFeedApi`](crate::interop::feed::OutboxFeedApi), because the parts of
//! the destination side most likely to be wrong are the transport ones:
//! resume-from-cursor, reconnect, and lag recovery. An in-process channel would
//! test the derivation rule (already covered in `kardamom_types::xchain`) and
//! nothing else.
//!
//! ## What it deliberately does NOT do
//!
//! * **It does not filter by destination.** `dest_chain_id` is recorded for
//!   assertions and otherwise ignored, so tests can script the malicious- or
//!   buggy-feed case that `derive_remote_epoch` must fail-stop on. A feed that
//!   filtered correctly could never produce that case.
//! * **It does not repair its script.** [`MockInteropFeed::gap_next`] makes it
//!   swallow messages, producing exactly the hole in the dense seq that the
//!   no-skip rule exists to catch.
//! * **It carries no finality stamps or anchor proofs.** v1 is feed-trust mode
//!   (spec §10 tier T0); see [`crate::interop::feed`].
//!
//! ## Retention model
//!
//! Everything pushed is retained and served from `cursor.seq` onward, which
//! is the contract a real feed offers within its retention window.
//! [`MockInteropFeed::set_floor`] models the window's edge the way the
//! validator's store does: messages below the floor are gone, and EVERY
//! subscribe with a cursor below the floor gets a `Lagged` frame naming the
//! floor, then the retained suffix. A subscriber that re-subscribes from
//! the same cursor gets the same answer, exactly like the real server.
//! [`MockInteropFeed::push_head`] scripts the origin's `head` event.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle};
use kardamom_types::xchain::OutboxMessage;
use tokio::sync::watch;

use crate::interop::feed::{OutboxCursor, OutboxEventDto, OutboxFeedApiServer, OutboxMessageDto};

/// One entry of the mock's retained script.
#[derive(Clone, Debug)]
enum FeedItem {
    Message(Box<OutboxMessage>),
    /// The origin closed every block through this one.
    Head(u64),
}

struct FeedState {
    origin_chain_id: u64,
    script: Mutex<Vec<FeedItem>>,
    /// First seq the feed serves. A subscribe below it gets `Lagged`.
    floor: Mutex<u64>,
    /// Messages the feed will swallow before retaining any more.
    swallow: Mutex<u64>,
    /// `dest_chain_id` of every subscription received, in order. Its length is
    /// the re-subscription count the reconnect/lag tests assert on.
    subscribed_dests: Mutex<Vec<u64>>,
    /// Script length; bumped on every push so serving handlers wake.
    items: watch::Sender<usize>,
    /// Bumped by `close_sessions` to end every in-flight subscription.
    close_epoch: watch::Sender<u64>,
}

/// A scripted origin-chain feed. Drop stops the server.
pub struct MockInteropFeed {
    state: Arc<FeedState>,
    addr: SocketAddr,
    // Held for its Drop: it is what shuts the server down with the fixture.
    _server: ServerHandle,
}

impl MockInteropFeed {
    /// Bind a feed for `origin_chain_id` on an ephemeral loopback port.
    ///
    /// Panics on bind failure: a test environment that cannot bind loopback is
    /// broken in a way no caller can handle.
    pub async fn new(origin_chain_id: u64) -> Self {
        let state = Arc::new(FeedState {
            origin_chain_id,
            script: Mutex::new(Vec::new()),
            floor: Mutex::new(0),
            swallow: Mutex::new(0),
            subscribed_dests: Mutex::new(Vec::new()),
            items: watch::channel(0usize).0,
            close_epoch: watch::channel(0u64).0,
        });
        let server = Server::builder()
            .build("127.0.0.1:0".parse::<SocketAddr>().expect("loopback addr"))
            .await
            .expect("bind mock interop feed");
        let addr = server.local_addr().expect("mock feed local_addr");
        let handle = server.start(OutboxFeedApiServer::into_rpc(FeedHandler {
            state: state.clone(),
        }));
        Self {
            state,
            addr,
            _server: handle,
        }
    }

    /// The peer chain this feed claims to be.
    pub fn origin_chain_id(&self) -> u64 {
        self.state.origin_chain_id
    }

    /// WebSocket endpoint to point a [`crate::interop::WsRemoteChainSource`] at.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Retain a message and serve it to every subscriber whose cursor is at or
    /// below its seq — unless [`Self::gap_next`] armed a swallow, in which case
    /// the message is dropped on the floor as a lossy feed would drop it.
    pub fn push_message(&self, msg: OutboxMessage) {
        {
            let mut swallow = self.state.swallow.lock().unwrap();
            if *swallow > 0 {
                *swallow -= 1;
                return;
            }
        }
        let len = {
            let mut script = self.state.script.lock().unwrap();
            script.push(FeedItem::Message(Box::new(msg)));
            script.len()
        };
        self.state.items.send_replace(len);
    }

    /// Raise the feed floor: messages below `seq` are gone, and every
    /// subscribe with a cursor below `seq` gets a `Lagged` frame first —
    /// the real server's retention edge.
    pub fn set_floor(&self, seq: u64) {
        let len = {
            let mut floor = self.state.floor.lock().unwrap();
            *floor = (*floor).max(seq);
            let mut script = self.state.script.lock().unwrap();
            script.retain(|item| match item {
                FeedItem::Message(m) => m.seq >= *floor,
                FeedItem::Head(_) => true,
            });
            script.len()
        };
        self.state.items.send_replace(len);
    }

    /// Script a `head` event: the origin closed every block through
    /// `block`. A subscriber with an open batch from an earlier block
    /// closes it.
    pub fn push_head(&self, block: u64) {
        let len = {
            let mut script = self.state.script.lock().unwrap();
            script.push(FeedItem::Head(block));
            script.len()
        };
        self.state.items.send_replace(len);
    }

    /// Make the feed swallow the next `n` pushed messages — a hole in the
    /// dense per-pair seq, which the destination must halt on rather than
    /// step over.
    pub fn gap_next(&self, n: u64) {
        *self.state.swallow.lock().unwrap() += n;
    }

    /// End every in-flight subscription, forcing subscribers through their
    /// reconnect path. The listener stays bound, so [`Self::url`] keeps
    /// working and the reconnect is the client's own.
    pub fn close_sessions(&self) {
        self.state
            .close_epoch
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    /// How many subscriptions this feed has served since it started. Greater
    /// than one means a subscriber went through its resume path.
    pub fn subscription_count(&self) -> usize {
        self.state.subscribed_dests.lock().unwrap().len()
    }

    /// The `dest_chain_id` each subscription asked for, in order.
    pub fn subscribed_dests(&self) -> Vec<u64> {
        self.state.subscribed_dests.lock().unwrap().clone()
    }
}

struct FeedHandler {
    state: Arc<FeedState>,
}

#[async_trait::async_trait]
impl OutboxFeedApiServer for FeedHandler {
    async fn subscribe_outbox(
        &self,
        pending: PendingSubscriptionSink,
        dest_chain_id: u64,
        cursor: OutboxCursor,
    ) -> SubscriptionResult {
        // Watch both signals before accepting so nothing scripted in between
        // is missed — the `subscribeReceipts` tap-before-accept discipline.
        let mut items = self.state.items.subscribe();
        let mut closed = self.state.close_epoch.subscribe();
        let sink = pending.accept().await?;
        self.state
            .subscribed_dests
            .lock()
            .unwrap()
            .push(dest_chain_id);

        // The retention edge, judged on EVERY subscribe like the real
        // server: a cursor below the floor is told so, then served from
        // the floor.
        let floor = *self.state.floor.lock().unwrap();
        let mut from = cursor.seq;
        if from < floor {
            let floor_block = self
                .state
                .script
                .lock()
                .unwrap()
                .iter()
                .find_map(|i| match i {
                    FeedItem::Message(m) => Some(m.origin_block_number),
                    FeedItem::Head(_) => None,
                });
            let event = OutboxEventDto::Lagged {
                skipped: floor - from,
                floor_seq: Some(floor),
                floor_block,
            };
            let msg = serde_json::value::to_raw_value(&event)
                .map_err(|e| format!("serialize feed event: {e}"))?;
            if sink.send(msg).await.is_err() {
                return Ok(());
            }
            from = floor;
        }

        let mut next = 0usize;
        loop {
            loop {
                let Some(item) = self.state.script.lock().unwrap().get(next).cloned() else {
                    break;
                };
                next += 1;
                let event = match item {
                    // Honouring the cursor is the whole contract: a resumed
                    // subscriber must see exactly the same suffix a fresh one
                    // at that cursor would.
                    FeedItem::Message(m) if m.seq < from => continue,
                    FeedItem::Message(m) => OutboxEventDto::Message(Box::new(
                        OutboxMessageDto::from_outbox_message(self.state.origin_chain_id, &m),
                    )),
                    FeedItem::Head(block) => OutboxEventDto::Head {
                        block_number: block,
                    },
                };
                let msg = serde_json::value::to_raw_value(&event)
                    .map_err(|e| format!("serialize feed event: {e}"))?;
                if sink.send(msg).await.is_err() {
                    return Ok(());
                }
            }
            tokio::select! {
                () = sink.closed() => return Ok(()),
                // An `Err` return, not `Ok(())`: jsonrpsee treats a clean
                // return as "no further message" and leaves the subscriber
                // waiting forever, which is a hang rather than the session
                // drop this is meant to simulate.
                _ = closed.changed() => return Err("feed session closed".into()),
                r = items.changed() => {
                    if r.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
}
