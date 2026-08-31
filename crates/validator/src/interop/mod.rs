//! The validator's interop role (`docs/specs/egress-node-spec.md` v2,
//! `docs/specs/interop-outbox-messaging-spec.md` §5/§10): one binary, roles
//! by config.
//!
//! - [`verify`] — destination side: the [`RemoteEpochVerifier`] wired on the
//!   engine's `RemoteEpochObserver` seam (inline pair-sequence checks;
//!   content-vs-origin is a later phase — see the module docs).
//! - [`extract`] — origin side: decode `MessageSent` from the re-executed
//!   receipts with the recompute-and-reject discipline, cross-check the
//!   `sentMessages` BAL claim.
//! - [`sink`] — the engine seam feeding extraction per block boundary.
//! - [`store`] — the retained serving stores (outbox lanes, attestations).
//! - [`serve`] — the config-gated jsonrpsee WS surfaces
//!   (`kardamom_subscribeOutbox`, `kardamom_subscribeAttestations`).

pub mod extract;
pub mod serve;
pub mod sink;
pub mod store;
pub mod verify;

pub use extract::{OutboxExtractError, collect_outbox_messages, sent_messages_slot};
pub use serve::{FeedServerState, start_feed_server};
pub use sink::ExtractingReceiptSink;
pub use store::{AttestationStore, FeedStore};
pub use verify::{RemoteEpochFault, RemoteEpochVerifier, check_remote_epoch};
