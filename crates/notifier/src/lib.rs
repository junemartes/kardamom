//! `kardamom-notifier`: transaction status events for clients.
//!
//! The service taps three streams, `tx_status`, `tx_receipts` and
//! `tx_errors`, and derives one feed of status events: `offered`,
//! `sealed`, `executed`, `rejected`, one per step a transaction takes.
//! It keeps the last minutes of events in a ring, indexed by hash and by
//! sender, and serves them through the WebSocket method
//! `kardamom_subscribeTxStatus(filter)`, which replays the ring for the
//! filter and then streams. The service holds no state on disk: a
//! restarted instance starts with an empty ring. The webhooks are gone
//! (2026-10-25). Offline delivery comes later through a task queue.
//!
//! Every instance taps the streams itself: the streams are the queue,
//! and no broker sits between the pipeline and the fan-out. Nothing here
//! is on the hot path. A slow client costs the client, never the chain.
//!
//! Module map:
//!
//! - [`dto`]: the JSON shapes.
//! - [`ring`]: the bounded event store and its indexes.
//! - [`hub`]: the task that owns the ring and fans events out.
//! - [`taps`]: the three Aeron subscriptions.
//! - [`feed`]: the WebSocket subscription.
//! - [`server`]: the listener of the feed.

// The `#[rpc]` subscription methods expand to functions that carry a bare
// `#[must_use]` and return a pinned boxed future, which is `#[must_use]` on
// its own. The lint fires in the macro's output, so it is allowed here.
#![allow(clippy::double_must_use)]

pub mod dto;
pub mod events;
pub mod feed;
pub mod hub;
pub mod metrics;
pub mod ring;
pub mod server;
pub mod stages;
pub mod taps;
