//! In-memory pub/sub fakes and the Docker-driven real-Aeron test cluster,
//! used by other crates' unit and integration tests.
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]`. Importers add
//! this crate as `kardamom-log = { workspace = true, features = ["testing"] }`
//! to `[dev-dependencies]`.
//!
//! - [`fakes`]: no-Aeron in-memory stand-ins for unit tests. Re-exported
//!   here, so existing `kardamom_log::testing::FakeBus` (and similar)
//!   paths are unchanged.
//! - `cluster` ([`AeronTestCluster`]): a real Aeron Media Driver and
//!   Archive in a Docker container, for integration tests that must run
//!   against the genuine transport. Requires the `docker-e2e` feature.

mod fakes;
pub use fakes::*;

#[cfg(feature = "docker-e2e")]
mod cluster;
#[cfg(feature = "docker-e2e")]
pub use cluster::{AeronTestCluster, SingleNodeRig, recv_within, require_docker};
