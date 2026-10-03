//! `kardamom-notifier`: transaction status events for clients.
//!
//! The service taps three streams, `tx_status`, `tx_receipts` and
//! `tx_errors`, and derives one feed of status events: `offered`,
//! `sealed`, `executed`, `rejected`, one per step a transaction takes.
//! It keeps the last minutes of events in a ring, indexed by hash and by
//! sender, and serves them two ways:
//!
//! - the WebSocket method `kardamom_subscribeTxStatus(filter)`, which
//!   replays the ring for the filter and then streams;
//! - webhook subscriptions, `POST /webhooks`, delivered at least once
//!   from a per-subscription outbox on disk.
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
//! - [`webhooks`], [`delivery`], [`outbox`], [`shard`]: webhook
//!   registration, the appender and the delivery loop, the on-disk
//!   outbox, and the instance sharding.
//! - [`server`]: the one listener for both client surfaces.

// The `#[rpc]` subscription methods expand to functions that carry a bare
// `#[must_use]` and return a pinned boxed future, which is `#[must_use]` on
// its own. The lint fires in the macro's output, so it is allowed here.
#![allow(clippy::double_must_use)]

pub mod delivery;
pub mod dto;
pub mod feed;
pub mod hub;
pub mod metrics;
pub mod outbox;
pub mod ring;
pub mod server;
pub mod shard;
pub mod taps;
pub mod webhooks;
