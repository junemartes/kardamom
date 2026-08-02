//! Kardamom canonical log: Aeron-backed channels B and C and the
//! archive-at-the-sealer durability path for tx_ordering.
//!
//! Durability model: the Aeron Archive daemon is configured with
//! `fileSyncLevel=1` so it fdatasyncs each recorded frame inline. A single
//! Aeron Archive co-located with the **sealer** records the sealer's
//! tx_ordering MDC publication; its `get_recording_position()` returns a
//! position that is byte-durable on local storage, and the sealer publishes
//! that one position as the durable watermark ingress gates its must-deliver
//! ack on (see [`recorder::run_durable_watermark_loop`]). The previous N-way
//! Q-of-N quorum aggregator + custom recorders have been removed.
//!
//! This crate owns the **transport implementation** only. Wire data types live
//! in [`types`] (re-exported from there). Do not add new wire types
//! here — extend `kardamom-types` instead, per.
//!
//! ## Feature gates
//!
//! `rusteron-client` / `rusteron-archive` are **unconditional** dependencies —
//! there is no `aeron-live` feature; a plain build already produces the
//! Aeron-backed publishers, subscribers, recorder, and supervisor.
//!
//! - `testing` — exposes in-memory pub/sub fakes that mirror the Aeron-backed
//!   channel surface for downstream crates' unit tests.
//! - `docker-e2e` — gates the testcontainers-driven Aeron e2e harness; implies
//!   `testing`.

pub mod aeron_live;
pub mod codec;

/// The tx_data wire frame: an owned, validated, zero-copy view of a
/// [`kardamom_types::TxEnvelope`]. One aligned copy per frame; field reads
/// never allocate; clones are refcount bumps.
pub type TxFrame = codec::ArchivedFrame<kardamom_types::TxEnvelope>;
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

// Re-export the shared types so existing call sites can `use kardamom_log::types::*`
// transparently (they import from kardamom-types under the hood).
pub mod types {
    pub use kardamom_types::*;
}
