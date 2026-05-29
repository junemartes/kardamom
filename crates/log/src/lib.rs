//! Kardamom canonical log: Aeron-backed channels B and C and the quorum
//! watermark aggregator that give tx_ordering its durability guarantee.
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

pub mod aeron_live;
pub mod bootstrap;
pub mod codec;
pub mod config;
pub mod error;
pub mod publisher;
pub mod recorder;
pub mod subscriber;
pub mod supervisor;
pub mod tx_ordering_archive;
pub mod watermark;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use bootstrap::{
    ArchiveSource, BootstrapDriver, BootstrapError, BootstrapPolicy, BootstrappedSubscriberHandle,
    LiveImage, ReplaySource, ReplayWindow, StreamPosition,
};
pub use error::LogError;

/// Construct a fresh Aeron Archive client connection.
///
/// The returned `AeronArchive` is `!Send + !Sync` (it wraps the C archive
/// client via raw pointers). The caller **must** keep it on a single dedicated
/// thread — typically the recorder thread that owns the [`recorder::Recorder`]
/// it will be handed to.
///
/// `aeron_dir` is the Aeron Media Driver directory (`aeron.dir`). When
/// provided it is applied to the archive context so the archive client finds
/// the same Media Driver instance as the Aeron runtime. Pass `None` to use
/// the default (typically `/dev/shm/aeron-dev`, or whatever is set in the
/// `AERON_DIR` environment variable).
///
/// The control channels default to `aeron:udp?endpoint=localhost:8010` /
/// `localhost:8011` — the fixed well-known ports the Aeron Archive daemon
/// listens on.
pub fn connect_archive_client(
    aeron_dir: Option<&str>,
) -> Result<rusteron_archive::AeronArchive, LogError> {
    aeron_live::build_archive(aeron_dir)
}

// Re-export the shared types so existing call sites can `use kardamom_log::types::*`
// transparently (they import from kardamom-types under the hood).
pub mod types {
    pub use kardamom_types::*;
}
