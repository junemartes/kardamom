//! Publisher discovery for the dynamic MDC transport.
//!
//! Consul is the discovery control plane: a publication registers its
//! control endpoint, and a consumer watches the catalog and attaches one
//! destination per publisher to its multi-destination subscription.
//! Messages travel directly between Aeron media drivers. Consul never
//! relays a message, answers a per-message lookup, or orders anything.
//!
//! - [`record`]: the service records of discovery version 1.
//! - [`catalog`]: the operations, behind the Consul and in-memory
//!   backends ([`consul`], [`memory`]).
//! - [`registration`]: bind, register, heartbeat, deregister.
//! - [`watch`]: the blocking-query loop and its membership snapshots.
//! - [`reconcile`]: membership snapshots to attach and detach calls.
//! - [`endpoint`]: the advertised address, ports, and URIs.

pub mod catalog;
pub mod consul;
pub mod endpoint;
pub mod memory;
pub mod plane;
pub mod reconcile;
pub mod record;
pub mod recording;
pub mod registration;
pub mod watch;

pub use catalog::{Catalog, Query, QueryResult, RegistrationSpec};
pub use endpoint::{
    MANUAL_SUBSCRIPTION_URI, PortAllocator, PortRange, advertise_ip, destination_uri,
    publication_uri,
};
pub use plane::{DiscoveredPublisher, DiscoveredSubscriber, StreamPlane};
pub use reconcile::{DestinationPort, Plan, Reconciler};
pub use record::{
    ARCHIVE_SERVICE, ArchiveRecord, CLUSTER_MEMBER_SERVICE, ClusterMemberRecord, DISCOVERY_VERSION,
    PUBLISHER_SERVICE, PublisherRecord, Scope, ServiceEntry, ServiceId, Topic,
};
pub use recording::{DiscoveredRecorder, RecorderProgress};
pub use registration::Registration;
pub use watch::{CatalogHealth, Membership, MembershipWatch, WatchTiming};

use crate::config::DiscoveryConfig;
use crate::error::LogError;

/// The allocation-scoped identity Nomad supplies: the instance id that
/// makes every service id unique per process incarnation, and the UDP
/// port range the publications bind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub id: String,
    pub ports: Option<PortRange>,
}

impl Instance {
    /// Read `NOMAD_ALLOC_ID` and `KARDAMOM_MDC_PORTS`. Without an
    /// allocation id (a local run), the id derives from the process id and
    /// the clock, which is unique enough for one host.
    ///
    /// # Errors
    ///
    /// Returns an error if `KARDAMOM_MDC_PORTS` is set and malformed.
    pub fn from_env() -> Result<Self, LogError> {
        let id = std::env::var("NOMAD_ALLOC_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(Self::local_id);
        let ports = std::env::var("KARDAMOM_MDC_PORTS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<PortRange>())
            .transpose()
            .map_err(|e| LogError::Discovery(format!("KARDAMOM_MDC_PORTS: {e}")))?;
        Ok(Self { id, ports })
    }

    fn local_id() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        format!("local-{}-{nanos:x}", std::process::id())
    }

    /// The service id of one publication of this instance.
    #[must_use]
    pub fn service_id(&self, topic: Topic, stream_id: i32) -> ServiceId {
        ServiceId::new(format!("{}:{topic}:{stream_id}", self.id))
    }
}

/// The catalog a service uses under `cfg`: the Consul agent when
/// discovery is enabled.
///
/// # Errors
///
/// Returns an error if the Consul client fails to build.
pub fn catalog_from_config(cfg: &DiscoveryConfig) -> Result<Catalog, LogError> {
    consul::ConsulClient::new(cfg).map(Catalog::Consul)
}

/// The discovery scope of `cfg`.
#[must_use]
pub fn scope_from_config(cfg: &DiscoveryConfig) -> Scope {
    Scope {
        cluster_id: cfg.cluster_id.clone(),
        chain_id: cfg.chain_id,
    }
}
