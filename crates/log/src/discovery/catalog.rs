//! The catalog operations the discovery runtime needs, behind one closed
//! set of backends: the Consul agent in a deployment, an in-memory
//! catalog in tests.

use std::collections::BTreeMap;
use std::time::Duration;

use super::consul::ConsulClient;
use super::memory::MemoryCatalog;
use super::record::{ServiceEntry, ServiceId};
use crate::error::LogError;

/// A catalog read: the entries of one service whose metadata carries
/// every `meta_equals` pair, with only passing health checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    pub service: String,
    pub meta_equals: BTreeMap<String, String>,
    /// The blocking-query index: the result index of the last successful
    /// read, or 0 for an immediate read.
    pub index: u64,
    /// How long a blocking query waits for a change past `index`.
    pub wait: Duration,
}

/// One catalog read's outcome. `index` feeds the next blocking query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub index: u64,
    pub entries: Vec<ServiceEntry>,
}

/// A service registration with a TTL health check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationSpec {
    pub entry: ServiceEntry,
    /// The check turns critical when no pass arrives within this window.
    pub ttl: Duration,
    /// The catalog deletes the service once its check has stayed critical
    /// this long.
    pub deregister_after: Duration,
}

impl RegistrationSpec {
    /// The check id that [`Catalog::pass`] passes.
    #[must_use]
    pub fn check_id(&self) -> String {
        check_id(&self.entry.id)
    }
}

/// The TTL check id of a registration: one check per service.
#[must_use]
pub fn check_id(service: &ServiceId) -> String {
    format!("{service}:ttl")
}

/// The closed set of catalog backends.
#[derive(Clone)]
pub enum Catalog {
    Consul(ConsulClient),
    Memory(MemoryCatalog),
}

impl Catalog {
    /// Register `spec`, replacing any existing registration of the same
    /// id. The new check starts critical until the first pass.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend rejects the registration or is
    /// unreachable.
    pub async fn register(&self, spec: &RegistrationSpec) -> Result<(), LogError> {
        match self {
            Self::Consul(c) => c.register(spec).await,
            Self::Memory(m) => m.register(spec),
        }
    }

    /// Pass the TTL check of `service`.
    ///
    /// # Errors
    ///
    /// Returns an error if the check is unknown to the backend (the
    /// registration is gone) or the backend is unreachable.
    pub async fn pass(&self, service: &ServiceId) -> Result<(), LogError> {
        match self {
            Self::Consul(c) => c.pass(service).await,
            Self::Memory(m) => m.pass(service),
        }
    }

    /// Delete the registration of `service`.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend is unreachable.
    pub async fn deregister(&self, service: &ServiceId) -> Result<(), LogError> {
        match self {
            Self::Consul(c) => c.deregister(service).await,
            Self::Memory(m) => m.deregister(service),
        }
    }

    /// Read the passing entries matching `query`, blocking up to
    /// `query.wait` for a change past `query.index`.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend is unreachable, denies the read, or
    /// answers with a malformed body. An error is never an empty set.
    pub async fn query(&self, query: &Query) -> Result<QueryResult, LogError> {
        match self {
            Self::Consul(c) => c.query(query).await,
            Self::Memory(m) => m.query(query).await,
        }
    }
}
