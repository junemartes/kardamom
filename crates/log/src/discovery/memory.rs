//! The in-memory catalog backend for unit tests: the same operations as
//! the Consul agent, plus test controls that make a check critical, fail
//! the next reads, or reset the index the way a rebuilt Consul cluster
//! does.
//!
//! The state sits behind one mutex because a test drives several
//! simulated processes from several tasks against one catalog. A `Notify`
//! wakes blocking queries on every write.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::catalog::{Query, QueryResult, RegistrationSpec};
use super::record::{ServiceEntry, ServiceId};
use crate::error::LogError;

#[derive(Default)]
struct Registered {
    entry: ServiceEntry,
    passing: bool,
}

impl Default for ServiceEntry {
    fn default() -> Self {
        Self {
            id: ServiceId::new(String::new()),
            name: String::new(),
            address: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            port: 0,
            meta: BTreeMap::new(),
        }
    }
}

struct State {
    services: BTreeMap<ServiceId, Registered>,
    /// Starts at 1 like a Consul catalog: index 0 is a reader's "read
    /// now" request, never a catalog state.
    index: u64,
    /// Reads left to fail before reads succeed again.
    failing_reads: u32,
}

impl Default for State {
    fn default() -> Self {
        Self {
            services: BTreeMap::new(),
            index: 1,
            failing_reads: 0,
        }
    }
}

impl State {
    fn bump(&mut self) {
        self.index += 1;
    }

    fn matches(entry: &ServiceEntry, query: &Query) -> bool {
        entry.name == query.service
            && query
                .meta_equals
                .iter()
                .all(|(k, v)| entry.meta.get(k) == Some(v))
    }

    fn snapshot(&self, query: &Query) -> QueryResult {
        QueryResult {
            index: self.index,
            entries: self
                .services
                .values()
                .filter(|r| r.passing && Self::matches(&r.entry, query))
                .map(|r| r.entry.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryCatalog {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}

impl MemoryCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read or update the state without waking blocking queries.
    fn with_state<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut guard)
    }

    /// Update the state, then wake every blocking query. A `Notified`
    /// future created before this call completes, so a query must not
    /// call this on its own read.
    fn mutate<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let out = self.with_state(f);
        self.changed.notify_waiters();
        out
    }

    /// # Errors
    ///
    /// Never fails; the signature matches the Consul backend.
    pub fn register(&self, spec: &RegistrationSpec) -> Result<(), LogError> {
        self.mutate(|s| {
            s.services.insert(
                spec.entry.id.clone(),
                Registered {
                    entry: spec.entry.clone(),
                    passing: false,
                },
            );
            s.bump();
        });
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error if `service` is not registered.
    pub fn pass(&self, service: &ServiceId) -> Result<(), LogError> {
        self.mutate(|s| {
            let r = s.services.get_mut(service).ok_or_else(|| {
                LogError::Discovery(format!("pass check of {service}: unknown check"))
            })?;
            if !r.passing {
                r.passing = true;
                s.bump();
            }
            Ok(())
        })
    }

    /// # Errors
    ///
    /// Never fails; the signature matches the Consul backend.
    pub fn deregister(&self, service: &ServiceId) -> Result<(), LogError> {
        self.mutate(|s| {
            if s.services.remove(service).is_some() {
                s.bump();
            }
        });
        Ok(())
    }

    /// Test control: the TTL of `service` expired, so its check is
    /// critical until the next pass.
    pub fn expire(&self, service: &ServiceId) {
        self.mutate(|s| {
            if let Some(r) = s.services.get_mut(service) {
                r.passing = false;
                s.bump();
            }
        });
    }

    /// Test control: the next `n` reads fail as an unreachable agent
    /// would.
    pub fn fail_next_reads(&self, n: u32) {
        self.mutate(|s| s.failing_reads = n);
    }

    /// Test control: the catalog was rebuilt, so its index restarts below
    /// every index a reader has seen.
    pub fn reset_index(&self) {
        self.mutate(|s| s.index = 1);
    }

    /// The current index, for a test to wait against.
    #[must_use]
    pub fn index(&self) -> u64 {
        self.with_state(|s| s.index)
    }

    /// # Errors
    ///
    /// Returns an error while a test has reads failing.
    pub async fn query(&self, query: &Query) -> Result<QueryResult, LogError> {
        let deadline = tokio::time::Instant::now() + query.wait;
        loop {
            if let Some(result) = self.query_step(query, deadline).await? {
                return Ok(result);
            }
        }
    }

    /// One blocking-query step: answer when the index moved past
    /// `query.index` or the wait ran out, otherwise wait for a change.
    async fn query_step(
        &self,
        query: &Query,
        deadline: tokio::time::Instant,
    ) -> Result<Option<QueryResult>, LogError> {
        let notified = self.changed.notified();
        let ready = self.with_state(|s| {
            if s.failing_reads > 0 {
                s.failing_reads -= 1;
                return Err(LogError::Discovery("catalog unreachable".into()));
            }
            Ok((s.index != query.index || query.index == 0).then(|| s.snapshot(query)))
        })?;
        if ready.is_some() {
            return Ok(ready);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(Some(self.with_state(|s| s.snapshot(query))));
        }
        let _ = tokio::time::timeout(deadline - now, notified).await;
        Ok(None)
    }
}

/// The wait used in tests that never want a query to block.
pub const NO_WAIT: Duration = Duration::from_millis(0);
