//! Kardamom canonical log: Aeron-backed channels B and C, the receipt-cache
//! channel, the io_uring fsync sidecar, and the per-recorder fsync-watermark
//! streams that give channel B its durability guarantee.
//!
//! Per D-Sh13 there is no quorum aggregator; the ack-path consumer (the
//! ingress proxy) subscribes to a single recorder's FsyncWatermark stream.
//!
//! This crate owns the **transport implementation** only. Wire data types live
//! in [`kardamom_types`] (re-exported from there). Do not add new wire types
//! here — extend `kardamom-types` instead, per D-Sh1.
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
pub mod fsync_sidecar;
pub mod receipt_cache;
pub mod supervisor;

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

// Re-export the shared types so existing call sites can `use kardamom_log::types::*`
// transparently (they import from kardamom-types under the hood).
pub mod types {
    pub use kardamom_types::*;
}
