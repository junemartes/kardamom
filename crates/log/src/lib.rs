//! Kardamom canonical log: the Aeron-backed application channels, the
//! archive recorders, and the archive refetch client.
//!
//! Durability model: the Aeron Archive daemon uses `fileSyncLevel=1`, so it
//! runs fdatasync on each recorded frame inline. The ingress archives
//! record the `tx_data` lanes and the DA watcher's archive records
//! `tx_deposits`. `get_recording_position()` returns a position that is
//! byte-durable on local storage. The canonical order itself is durable in
//! the Aeron Cluster; the ingress gates its ack on cluster egress progress.
//!
//! This crate owns the transport implementation only. Wire data types live
//! in [`types`] (re-exported from there). Do not add new wire types
//! here. Extend `kardamom-types` instead.
//!
//! ## Feature gates
//!
//! `rusteron-client` and `rusteron-archive` are unconditional dependencies.
//! There is no `aeron-live` feature. A plain build already produces the
//! Aeron-backed transport and the recorder.
//!
//! - `testing`: exposes in-memory pub/sub fakes that mirror the Aeron-backed
//!   channel surface, for downstream crates' unit tests.
//! - `docker-e2e`: gates the testcontainers-driven Aeron e2e harness. Implies
//!   `testing`.

pub mod aeron_live;
mod archive_catalog;
pub mod codec;
pub mod config;
pub mod discovery;
pub mod error;
mod ffi;
mod offer_retry;
pub mod recorder;
pub mod refetch;
mod term_layout;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::LogError;

// Re-export the shared types. Existing call sites can `use kardamom_log::types::*`
// without change; the types come from kardamom-types under the hood.
pub mod types {
    pub use kardamom_types::*;
}
