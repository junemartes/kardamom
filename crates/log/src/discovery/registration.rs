//! A publication's catalog registration: registered after its endpoint is
//! bound, kept passing by a heartbeat, and deregistered on graceful exit.
//! After a crash the TTL check turns critical, consumers detach after
//! their grace, and the catalog deletes the record after the configured
//! critical window. A record moves to a new entry through the heartbeat
//! ([`RecordMover`]), so every write of the id goes through one task and
//! a later re-registration writes the moved entry.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::catalog::{Catalog, RegistrationSpec};
use super::record::{ServiceEntry, ServiceId};
use crate::error::LogError;

pub struct Registration {
    catalog: Catalog,
    id: ServiceId,
    cancel: CancellationToken,
    heartbeat: JoinHandle<()>,
    moves: mpsc::UnboundedSender<RecordMove>,
}

/// One move of the record to `entry`, with the reply of the heartbeat.
struct RecordMove {
    entry: ServiceEntry,
    ack: oneshot::Sender<Result<(), LogError>>,
}

/// Moves the record of a registration to a new entry of the same id. The
/// heartbeat registers the entry and keeps it as the spec of every later
/// pass, so a re-registration after a lost check writes the moved entry,
/// not the first one.
#[derive(Clone)]
pub struct RecordMover {
    moves: mpsc::UnboundedSender<RecordMove>,
}

impl RecordMover {
    /// Register `entry` under the id of the registration and pass its
    /// check. Returns once the catalog holds the entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog refuses the entry, or if the
    /// registration is gone.
    pub async fn relocate(&self, entry: ServiceEntry) -> Result<(), LogError> {
        let (ack, done) = oneshot::channel();
        self.moves
            .send(RecordMove { entry, ack })
            .map_err(|_| LogError::Discovery("record move: the registration is gone".into()))?;
        done.await.map_err(|_| {
            LogError::Discovery("record move: the registration ended during the move".into())
        })?
    }
}

/// The heartbeat's per-tick state.
struct Heartbeat {
    catalog: Catalog,
    spec: RegistrationSpec,
    cancel: CancellationToken,
    interval: Duration,
    moves: mpsc::UnboundedReceiver<RecordMove>,
}

impl Heartbeat {
    async fn run(mut self) {
        while self.tick().await {}
    }

    /// Wait for a move or one interval. A move registers the new entry. An
    /// interval passes the check. A failed pass means the agent forgot the
    /// check (an agent restart), so the registration is repeated. `false`
    /// ends the loop.
    async fn tick(&mut self) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => false,
            Some(mv) = self.moves.recv() => {
                self.relocate(mv).await;
                true
            }
            () = tokio::time::sleep(self.interval) => {
                self.pass().await;
                true
            }
        }
    }

    async fn pass(&self) {
        if let Err(e) = self.catalog.pass(&self.spec.entry.id).await {
            warn!(service = %self.spec.entry.id, error = %e, "discovery: check pass failed; re-registering");
            self.re_register().await;
        }
    }

    /// Take `mv.entry` as the spec of every later pass, register it, and
    /// answer the mover.
    async fn relocate(&mut self, mv: RecordMove) {
        self.spec.entry = mv.entry;
        let outcome = self.register_and_pass().await;
        if outcome.is_ok() {
            info!(
                service = %self.spec.entry.id,
                endpoint = %self.spec.entry.socket_addr(),
                "discovery: record moved"
            );
        }
        let _ = mv.ack.send(outcome);
    }

    async fn register_and_pass(&self) -> Result<(), LogError> {
        self.catalog.register(&self.spec).await?;
        self.catalog.pass(&self.spec.entry.id).await
    }

    async fn re_register(&self) {
        if let Err(e) = self.register_and_pass().await {
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
        let (moves, move_rx) = mpsc::unbounded_channel();
        let heartbeat = tokio::spawn(
            Heartbeat {
                catalog: catalog.clone(),
                interval: spec.ttl / 3,
                spec: spec.clone(),
                cancel: cancel.clone(),
                moves: move_rx,
            }
            .run(),
        );
        Ok(Self {
            catalog,
            id: spec.entry.id,
            cancel,
            heartbeat,
            moves,
        })
    }

    #[must_use]
    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    /// The mover of this record. See [`RecordMover`].
    #[must_use]
    pub fn mover(&self) -> RecordMover {
        RecordMover {
            moves: self.moves.clone(),
        }
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

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
