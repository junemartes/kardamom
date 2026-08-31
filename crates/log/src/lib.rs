//! Kardamom canonical log: Aeron-backed channels B and C, and the
//! archive-at-the-sealer durability path for tx_ordering.
//!
//! Durability model: the Aeron Archive daemon uses `fileSyncLevel=1`, so it
//! runs fdatasync on each recorded frame inline. A single Aeron Archive next
//! to the sealer records the sealer's tx_ordering MDC publication. Its
//! `get_recording_position()` returns a position that is byte-durable on
//! local storage. The sealer publishes that position as the durable
//! watermark that ingress gates its must-deliver ack on, through the
//! ingress cluster-egress observer. The previous N-way Q-of-N
//! quorum aggregator and custom recorders no longer exist.
//!
//! This crate owns the transport implementation only. Wire data types live
//! in [`types`] (re-exported from there). Do not add new wire types
//! here. Extend `kardamom-types` instead.
//!
//! ## Feature gates
//!
//! `rusteron-client` and `rusteron-archive` are unconditional dependencies.
//! There is no `aeron-live` feature. A plain build already produces the
//! Aeron-backed publishers, subscribers, recorder, and supervisor.
//!
//! - `testing`: exposes in-memory pub/sub fakes that mirror the Aeron-backed
//!   channel surface, for downstream crates' unit tests.
//! - `docker-e2e`: gates the testcontainers-driven Aeron e2e harness. Implies
//!   `testing`.

pub mod aeron_live;
pub mod codec;
pub mod config;
pub mod error;
mod offer_retry;
pub mod publisher;
pub mod recorder;
pub mod refetch;
pub mod replay;
pub mod subscriber;
pub mod supervisor;
pub mod watermark;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::LogError;

// Re-export the shared types. Existing call sites can `use kardamom_log::types::*`
// without change; the types come from kardamom-types under the hood.
pub mod types {
    pub use kardamom_types::*;
}
