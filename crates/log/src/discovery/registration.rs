//! A publication's catalog registration: registered after its endpoint is
//! bound, kept passing by a heartbeat, and deregistered on graceful exit.
//! After a crash the TTL check turns critical, consumers detach after
//! their grace, and the catalog deletes the record after the configured
//! critical window.

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::catalog::{Catalog, RegistrationSpec};
use super::record::ServiceId;
use crate::error::LogError;

pub struct Registration {
    catalog: Catalog,
    id: ServiceId,
    cancel: CancellationToken,
    heartbeat: JoinHandle<()>,
}

/// The heartbeat's per-tick state.
struct Heartbeat {
    catalog: Catalog,
    spec: RegistrationSpec,
    cancel: CancellationToken,
    interval: Duration,
}

impl Heartbeat {
    async fn run(self) {
        while self.tick().await {}
    }

    /// Wait one interval, then pass the check. A failed pass means the
    /// agent forgot the check (an agent restart), so the registration is
    /// repeated. `false` ends the loop.
    async fn tick(&self) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => return false,
            () = tokio::time::sleep(self.interval) => {}
        }
        if let Err(e) = self.catalog.pass(&self.spec.entry.id).await {
            warn!(service = %self.spec.entry.id, error = %e, "discovery: check pass failed; re-registering");
            self.re_register().await;
        }
        true
    }

    async fn re_register(&self) {
        let registered = self.catalog.register(&self.spec).await;
        let passed = match registered {
            Ok(()) => self.catalog.pass(&self.spec.entry.id).await,
            Err(e) => Err(e),
        };
        if let Err(e) = passed {
            warn!(service = %self.spec.entry.id, error = %e, "discovery: re-registration failed; retrying next tick");
        }
    }
}

impl Registration {
    /// Register `spec`, pass its check once, and start the heartbeat. The
    /// caller binds the endpoint first: a record must never advertise an
    /// endpoint that cannot be reached.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration or the first pass fails.
    pub async fn register(catalog: Catalog, spec: RegistrationSpec) -> Result<Self, LogError> {
        catalog.register(&spec).await?;
        catalog.pass(&spec.entry.id).await?;
        info!(
            service = %spec.entry.id,
            name = %spec.entry.name,
            endpoint = %spec.entry.socket_addr(),
            "discovery: registered"
        );
        let cancel = CancellationToken::new();
        let heartbeat = tokio::spawn(
            Heartbeat {
                catalog: catalog.clone(),
                interval: spec.ttl / 3,
                spec: spec.clone(),
                cancel: cancel.clone(),
            }
            .run(),
        );
        Ok(Self {
            catalog,
            id: spec.entry.id,
            cancel,
            heartbeat,
        })
    }

    #[must_use]
    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    /// Stop the heartbeat and delete the record. This is the graceful
    /// exit path; a dropped `Registration` only stops the heartbeat and
    /// leaves the TTL to expire.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog is unreachable. The heartbeat is
    /// stopped either way, so the TTL still expires.
    pub async fn deregister(mut self) -> Result<(), LogError> {
        self.cancel.cancel();
        let _ = (&mut self.heartbeat).await;
        self.catalog.deregister(&self.id).await?;
        info!(service = %self.id, "discovery: deregistered");
        Ok(())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
