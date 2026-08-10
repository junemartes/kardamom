//! Interop: the cross-chain source adapter, where the origin is a PEER
//! KARDAMOM CHAIN instead of L1 (`docs/specs/interop-outbox-messaging-spec.md`
//! §5–§6).
//!
//! The shape is the L1 pipeline's, seam for seam, because the guarantee is the
//! same one: an externally-sourced transaction stream must be derived
//! identically by every replica or the chain halts.
//!
//! | L1 (deposits) | interop (cross-chain messages) |
//! |---|---|
//! | [`crate::source::L1Source`] | [`source::RemoteChainSource`] |
//! | [`crate::publisher::EpochPublisher`] | [`publisher::RemoteEpochPublisher`] |
//! | [`crate::watcher::process_once`] | [`watcher::process_once`] |
//! | `kardamom_types::epoch::derive_epoch` | `kardamom_types::xchain::derive_remote_epoch` |
//! | one epoch per finalized L1 block | one record per ORIGIN BLOCK that carried messages |
//!
//! What is deliberately NOT shared: the derivation rule itself lives in
//! `kardamom_types::xchain` so the destination's verifier runs the same copy
//! the watcher does — a second copy would verify nothing.
//!
//! ## Scope of this slice
//!
//! v1 is **feed-trust** (spec §10 tier T0): the watcher executes what the feed
//! says. Finality stamps, `AnchorProof`s, and the L1-anchored gate that spec
//! §10 makes mandatory before value moves are a later slice; they ADD to the
//! wire contract in [`feed`] rather than reshaping it.

pub mod feed;
pub mod publisher;
pub mod source;
pub mod watcher;

// The scripted mock validator feed: a REAL jsonrpsee WS server, so the
// transport under test is the transport that ships. Gated behind
// `test-support` so the two-chain e2e harness can drive an origin chain that
// does not exist yet.
#[cfg(any(test, feature = "test-support"))]
pub mod mock;

pub use feed::{
    OutboxCursor, OutboxEventDto, OutboxFeedApiServer, OutboxMessageDto, SUBSCRIBE_OUTBOX_METHOD,
    UNSUBSCRIBE_OUTBOX_METHOD,
};
pub use publisher::RemoteEpochPublisher;
pub use source::{RemoteChainSource, RemoteSourceError, WsRemoteChainSource};
pub use watcher::{InteropError, InteropWatcherConfig, process_once, spawn};
