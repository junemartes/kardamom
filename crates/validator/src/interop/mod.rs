//! The validator's interop role (`docs/specs/egress-node-spec.md` v2,
//! `docs/specs/interop-outbox-messaging-spec.md` §5/§10): one binary, roles
//! by config.
//!
//! - [`verify`] — destination side: the [`RemoteEpochVerifier`] wired on the
//!   engine's `RemoteEpochObserver` seam (inline pair-sequence checks;
//!   content-vs-origin is a later phase — see the module docs).
//!
//! Origin-side extraction + the serving feed store/WS surfaces land in the
//! sibling modules as E1 progresses.

pub mod verify;

pub use verify::{RemoteEpochFault, RemoteEpochVerifier, check_remote_epoch};
