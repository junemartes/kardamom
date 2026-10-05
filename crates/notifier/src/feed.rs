//! The WebSocket feed: `kardamom_subscribeTxStatus(filter)`.
//!
//! On subscribe, the session taps the live feed first, then replays the
//! ring page by page, then follows the live feed from the last replayed
//! sequence number. A slow session gets a `lagged` marker and continues
//! from the present. A session that goes away ends quietly.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::{PendingSubscriptionSink, SubscriptionSink};
use tokio::sync::broadcast;

use crate::dto::{FeedItem, Lagged, StatusFilter};
use crate::hub::HubHandle;
use crate::metrics as m;
use crate::ring::Stamped;

/// The method name a client subscribes with.
pub const SUBSCRIBE_METHOD: &str = "kardamom_subscribeTxStatus";
/// The method name a client unsubscribes with.
pub const UNSUBSCRIBE_METHOD: &str = "kardamom_unsubscribeTxStatus";

#[rpc(server, namespace = "kardamom")]
pub trait TxStatusFeedApi {
    /// Stream the status events `filter` selects: the ring first, then
    /// live.
    #[subscription(name = "subscribeTxStatus", item = FeedItem)]
    async fn subscribe_tx_status(&self, filter: StatusFilter) -> SubscriptionResult;
}

/// The feed's server side: the hub, and the replay page size.
#[derive(Clone)]
pub struct Feed {
    hub: HubHandle,
    page: NonZeroUsize,
}

impl Feed {
    #[must_use]
    pub fn new(hub: HubHandle, page: NonZeroUsize) -> Self {
        Self { hub, page }
    }
}

#[async_trait::async_trait]
impl TxStatusFeedApiServer for Feed {
    async fn subscribe_tx_status(
        &self,
        pending: PendingSubscriptionSink,
        filter: StatusFilter,
    ) -> SubscriptionResult {
        // The live tap precedes the accept and the replay, so nothing
        // stored in between is missed: the replay ends at a sequence
        // number, and the live loop skips everything at or below it.
        let live = self.hub.subscribe();
        let sink = pending.accept().await?;
        StatusSubscription {
            hub: self.hub.clone(),
            live,
            filter,
            sink,
            last_seq: 0,
            page: self.page.get(),
        }
        .run()
        .await?;
        Ok(())
    }
}

/// One live-feed poll's outcome.
enum Live {
    Item(FeedItem),
    Skip,
    End,
}

/// One subscription session end to end.
struct StatusSubscription {
    hub: HubHandle,
    live: broadcast::Receiver<Stamped>,
    filter: StatusFilter,
    sink: SubscriptionSink,
    /// The newest sequence number sent. The live loop skips everything
    /// at or below it.
    last_seq: u64,
    page: usize,
}

impl StatusSubscription {
    async fn run(mut self) -> Result<(), String> {
        metrics::gauge!(m::WS_SUBSCRIPTIONS).increment(1.0);
        let result = self.serve().await;
        metrics::gauge!(m::WS_SUBSCRIPTIONS).decrement(1.0);
        result
    }

    /// Replay, then live, until the subscriber goes away or the hub
    /// ends.
    async fn serve(&mut self) -> Result<(), String> {
        while self.replay_page().await?.is_continue() {}
        while let Some(item) = self.next_live().await
            && self.send(&item).await?
        {}
        Ok(())
    }

    /// One replay page. `Break` once the page was short, or the
    /// subscriber went away.
    async fn replay_page(&mut self) -> Result<ControlFlow<()>, String> {
        let page = self.hub.replay(self.filter, self.last_seq, self.page).await;
        let full = page.len() >= self.page;
        let mut events = page.into_iter();
        while let Some(stamped) = events.next()
            && self.send_stamped(&stamped).await?
        {}
        let done = !full || self.sink.is_closed();
        Ok(if done {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    }

    async fn send_stamped(&mut self, stamped: &Stamped) -> Result<bool, String> {
        self.last_seq = stamped.seq;
        self.send(&FeedItem::Status(stamped.event.clone())).await
    }

    /// Serialize and send one item. `Ok(false)` means the subscriber
    /// went away, so the caller ends the session instead of treating a
    /// closed sink as an error.
    async fn send(&self, item: &FeedItem) -> Result<bool, String> {
        let msg = serde_json::value::to_raw_value(item)
            .map_err(|e| format!("serialize subscription item: {e}"))?;
        metrics::counter!(m::WS_EVENTS_TOTAL).increment(1);
        Ok(self.sink.send(msg).await.is_ok())
    }

    /// The next live item that passes the filter and follows the replay,
    /// or a lag marker. `None` once the sink closes or the hub ends.
    async fn next_live(&mut self) -> Option<FeedItem> {
        loop {
            match self.poll_live().await {
                Live::Item(item) => return Some(item),
                Live::Skip => (),
                Live::End => return None,
            }
        }
    }

    async fn poll_live(&mut self) -> Live {
        tokio::select! {
            () = self.sink.closed() => Live::End,
            r = self.live.recv() => self.on_live(r),
        }
    }

    fn on_live(&mut self, r: Result<Stamped, broadcast::error::RecvError>) -> Live {
        match r {
            Ok(s) if s.seq <= self.last_seq || !self.filter.matches(&s.event) => Live::Skip,
            Ok(s) => {
                self.last_seq = s.seq;
                Live::Item(FeedItem::Status(s.event))
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                metrics::counter!(m::WS_LAGGED_TOTAL).increment(1);
                Live::Item(FeedItem::Lagged(Lagged::skipped(n)))
            }
            Err(broadcast::error::RecvError::Closed) => Live::End,
        }
    }
}
