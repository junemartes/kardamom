//! Kardamom canonical log: Aeron-backed channels B and C, the receipt-cache
//! channel, and the quorum watermark aggregator that give tx_ordering its
//! durability guarantee.
//!
//! Durability model: the Aeron Archive daemon is configured with
//! `fileSyncLevel=1` so it fdatasyncs each recorded frame inline. The
//! recorder's `get_recording_position()` therefore returns a position that
//! is byte-durable on local storage. Per-recorder positions are aggregated
//! into a Q-of-N quorum watermark — surviving correlated power loss requires
//! Q nodes to have fsynced past the watermarked position.
//!
//! This crate owns the **transport implementation** only. Wire data types live
//! in [`types`] (re-exported from there). Do not add new wire types
//! here — extend `kardamom-types` instead, per.
//!
//! ## Feature gates
//!
//! - `aeron-live` — pulls in the real `rusteron-client` / `rusteron-archive`
//!   crates and exposes the Aeron-backed publishers, subscribers, recorder,
//!   and supervisor. Off by default so the crate compiles in environments
//!   without the Aeron C library installed.
//! - `testing` — exposes in-memory pub/sub fakes that mirror the Aeron-backed
//!   channel surface for downstream crates' unit tests.
//! - `docker-e2e` — gates the testcontainers-driven Aeron e2e harness; implies
//!   `testing`.

pub mod codec;
pub mod config;
pub mod error;
pub mod receipt_cache;
pub mod supervisor;
pub mod watermark;

#[cfg(feature = "aeron-live")]
pub mod aeron_live;
#[cfg(feature = "aeron-live")]
pub mod publisher;
#[cfg(feature = "aeron-live")]
pub mod recorder;
#[cfg(feature = "aeron-live")]
pub mod subscriber;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::LogError;

// Re-export the shared types so existing call sites can `use log::types::*`
// transparently (they import from kardamom-types under the hood).
pub mod types {
    pub use types::*;
}
