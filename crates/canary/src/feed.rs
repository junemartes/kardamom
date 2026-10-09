//! The notifier's status feed. One WebSocket session holds one
//! subscription per ring account (the sender filter), and every event
//! goes to the board. A probe reads the stages of its transaction from
//! the board. The board is an actor: the feed task and the probe tasks
//! reach it through a channel, so no lock guards the map.

use std::collections::HashMap;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use futures::StreamExt;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::metrics;

/// The board drops a transaction this long after its first event.
const MAX_AGE: Duration = Duration::from_secs(600);
/// The board prunes by age once it holds this many transactions.
const PRUNE_AT: usize = 4096;
/// The wait before a new session after a session ends.
const RECONNECT: Duration = Duration::from_secs(1);

/// The stages of one transaction, each with the time its event arrived.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stages {
    pub offered: Option<Instant>,
    pub sealed: Option<Instant>,
    pub executed: Option<Instant>,
    /// The reason of a rejection.
    pub rejected: Option<String>,
}

/// One status event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub hash: B256,
    pub stage: Stage,
    pub at: Instant,
}

/// A stage as the feed names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    Offered,
    Sealed,
    Executed,
    Rejected(String),
}

impl Event {
    /// The event of one feed item, or `None` for a lag marker or an item
    /// that does not parse.
    fn parse(item: &serde_json::Value, at: Instant) -> Option<Self> {
        let hash = item["tx_hash"].as_str()?.parse().ok()?;
        let stage = match item["stage"].as_str()? {
            "offered" => Stage::Offered,
            "sealed" => Stage::Sealed,
            "executed" => Stage::Executed,
            "rejected" => Stage::Rejected(item["reason"].as_str().unwrap_or("unknown").to_string()),
            _ => return None,
        };
        Some(Self { hash, stage, at })
    }
}

enum Message {
    Event(Event),
    Query {
        hash: B256,
        reply: oneshot::Sender<Stages>,
    },
}

/// The stages of recent canary transactions.
pub struct Board {
    rx: mpsc::Receiver<Message>,
    entries: HashMap<B256, (Instant, Stages)>,
}

/// The way to the board.
#[derive(Debug, Clone)]
pub struct BoardHandle {
    tx: mpsc::Sender<Message>,
}

impl Board {
    #[must_use]
    pub fn new() -> (Self, BoardHandle) {
        let (tx, rx) = mpsc::channel(1024);
        (
            Self {
                rx,
                entries: HashMap::new(),
            },
            BoardHandle { tx },
        )
    }

    /// Serve until every handle drops.
    pub async fn run(mut self) {
        while let Some(message) = self.rx.recv().await {
            self.apply(message);
        }
    }

    fn apply(&mut self, message: Message) {
        match message {
            Message::Event(event) => self.record(event),
            Message::Query { hash, reply } => {
                let stages = self.entries.get(&hash).map(|(_, s)| s.clone());
                let _ = reply.send(stages.unwrap_or_default());
            }
        }
    }

    /// Keep the first arrival of each stage.
    fn record(&mut self, event: Event) {
        if self.entries.len() >= PRUNE_AT {
            let now = event.at;
            self.entries
                .retain(|_, (first, _)| now.saturating_duration_since(*first) < MAX_AGE);
        }
        let (_, stages) = self
            .entries
            .entry(event.hash)
            .or_insert_with(|| (event.at, Stages::default()));
        let at = event.at;
        match event.stage {
            Stage::Offered => stages.offered = stages.offered.or(Some(at)),
            Stage::Sealed => stages.sealed = stages.sealed.or(Some(at)),
            Stage::Executed => stages.executed = stages.executed.or(Some(at)),
            Stage::Rejected(reason) => stages.rejected = stages.rejected.take().or(Some(reason)),
        }
    }
}

impl BoardHandle {
    /// The stages the board holds for `hash`. A board that ended answers
    /// none.
    pub async fn stages(&self, hash: B256) -> Stages {
        let (reply, answer) = oneshot::channel();
        if self.tx.send(Message::Query { hash, reply }).await.is_err() {
            return Stages::default();
        }
        answer.await.unwrap_or_default()
    }

    /// Hand one event to the board.
    pub async fn event(&self, event: Event) {
        let _ = self.tx.send(Message::Event(event)).await;
    }
}

/// The subscriber of the status feed.
#[derive(Debug, Clone)]
pub struct Feed {
    pub url: String,
    pub senders: Vec<Address>,
    pub board: BoardHandle,
}

impl Feed {
    /// Hold a session open, and open a new one after it ends, for ever.
    pub async fn run(self) {
        loop {
            self.session().await;
            metrics::feed_gap("disconnect");
            ::metrics::gauge!(metrics::FEED_CONNECTED).set(0.0);
            tokio::time::sleep(RECONNECT).await;
        }
    }

    /// One session: connect, subscribe for every sender, and pass each
    /// item to the board until the session ends.
    async fn session(&self) {
        let client = match WsClientBuilder::default().build(&self.url).await {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(url = %self.url, error = %e, "status feed: connect failed");
                return;
            }
        };
        let subscriptions = futures::future::try_join_all(self.senders.iter().map(|sender| {
            client.subscribe::<serde_json::Value, _>(
                "kardamom_subscribeTxStatus",
                rpc_params![serde_json::json!({ "sender": sender })],
                "kardamom_unsubscribeTxStatus",
            )
        }))
        .await;
        let subscriptions: Vec<Subscription<serde_json::Value>> = match subscriptions {
            Ok(subscriptions) => subscriptions,
            Err(e) => {
                tracing::warn!(url = %self.url, error = %e, "status feed: subscribe failed");
                return;
            }
        };
        ::metrics::gauge!(metrics::FEED_CONNECTED).set(1.0);
        let mut items = futures::stream::select_all(subscriptions);
        while let Some(item) = items.next().await {
            self.pass(item).await;
        }
    }

    async fn pass(&self, item: Result<serde_json::Value, serde_json::Error>) {
        let Ok(item) = item else {
            return;
        };
        match Event::parse(&item, Instant::now()) {
            Some(event) => self.board.event(event).await,
            None if item.get("skipped").is_some() => metrics::feed_gap("lagged"),
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(hash: B256, stage: &str) -> serde_json::Value {
        serde_json::json!({ "tx_hash": hash, "stage": stage, "reason": "da-lag" })
    }

    #[tokio::test]
    async fn a_late_stage_after_the_query_still_reaches_the_board() {
        let (board, handle) = Board::new();
        tokio::spawn(board.run());
        let hash = B256::repeat_byte(3);
        let now = Instant::now();
        handle
            .event(Event::parse(&item(hash, "sealed"), now).unwrap())
            .await;
        let early = handle.stages(hash).await;
        assert!(early.sealed.is_some());
        assert_eq!(early.executed, None, "the receipt can come first");
        handle
            .event(Event::parse(&item(hash, "executed"), now).unwrap())
            .await;
        handle
            .event(Event::parse(&item(hash, "offered"), now).unwrap())
            .await;
        let late = handle.stages(hash).await;
        assert!(late.executed.is_some() && late.offered.is_some());
    }

    #[tokio::test]
    async fn a_rejection_keeps_its_reason_and_a_marker_is_no_event() {
        let (board, handle) = Board::new();
        tokio::spawn(board.run());
        let hash = B256::repeat_byte(4);
        handle
            .event(Event::parse(&item(hash, "rejected"), Instant::now()).unwrap())
            .await;
        assert_eq!(
            handle.stages(hash).await.rejected.as_deref(),
            Some("da-lag")
        );
        let marker = serde_json::json!({ "stage": "lagged", "skipped": 3 });
        assert_eq!(Event::parse(&marker, Instant::now()), None);
    }
}
