//! The discovery-driven archive recorder: one thread that records every
//! publisher of a topic the catalog lists, on the local archive, through
//! the same dynamic MDC join a subscriber uses.
//!
//! Each publisher becomes its own recording, keyed by its control
//! endpoint and matched by its session id, so a lookup never adopts
//! another publisher's recording, nor an earlier incarnation's on the
//! same port. A publisher that leaves the catalog has its recording
//! subscription stopped; the recording itself stays in the catalog, so
//! refetch can still serve it.
//!
//! Readiness is per instance: the thread reports ready once every one of
//! this process's own publications (the ingress's lanes) has a live
//! recording, which is the barrier the ingress serves behind.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::absence::Absence;
use super::endpoint::destination_uri;
use super::record::PublisherRecord;
use super::watch::{CatalogHealth, Membership};
use crate::config::AeronConfig;
use crate::error::LogError;
use crate::recorder::connect_archive;

mod archive;
use archive::RecorderArchive;

#[cfg(test)]
mod tests;

/// How often the thread re-checks the catalog for recordings that have
/// not appeared yet, between membership changes.
const POLL: Duration = Duration::from_millis(500);

/// What the recorder reports through its readiness callback.
#[derive(Debug, PartialEq, Eq)]
pub enum RecorderProgress {
    /// Every own publication has a live recording.
    Ready { own_recordings: usize },
    /// The thread stopped or failed before that.
    Failed(String),
}

/// One recording started for a publisher.
struct Started {
    stream_id: i32,
    /// The `control=<ip>:<port>` fragment that names this publisher's
    /// recording in the catalog.
    fragment: String,
    /// The archive's recording subscription, when this thread started it.
    /// `None` when the archive already records the channel (a restart
    /// whose earlier subscription is still live).
    subscription_id: Option<i64>,
    /// The publisher's advertised session id, when the record carries
    /// one.
    session_id: Option<i32>,
    /// The recording id, once the image has formed and the catalog lists
    /// it.
    recording_id: Option<i64>,
    /// Whether this process registered the publisher.
    own: bool,
    absence: Absence,
}

pub struct DiscoveredRecorder {
    pub aeron_dir: Option<PathBuf>,
    pub aeron_cfg: AeronConfig,
    pub local_ip: Ipv4Addr,
    /// The instance id whose publications gate readiness.
    pub own_instance: String,
    /// How many own publications must record before ready.
    pub expected_own: usize,
    /// How long a confirmed missing publisher keeps its recording.
    pub removal_grace: Duration,
    pub membership: watch::Receiver<Membership>,
    pub stop: CancellationToken,
    /// The tokio runtime that drives the waits; the thread itself runs
    /// outside every runtime because the archive session is `!Send`.
    pub runtime: tokio::runtime::Handle,
}

/// The thread's whole state once the archive session is connected.
struct Recording<A: RecorderArchive> {
    spec: DiscoveredRecorder,
    archive: A,
    started: BTreeMap<String, Started>,
}

/// What ended one wait.
enum Wake {
    Stop,
    Changed,
    Tick,
}

impl DiscoveredRecorder {
    /// The thread body: connect the archive, then follow the membership
    /// until `stop`. `ready` fires exactly once, with `Ready` when every
    /// own publication records, or `Failed` on any earlier exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the archive control session fails to connect.
    pub fn run(self, ready: impl FnOnce(RecorderProgress)) -> Result<(), LogError> {
        let session = match connect_archive(self.aeron_dir.as_deref(), &self.aeron_cfg) {
            Ok(s) => s,
            Err(e) => {
                ready(RecorderProgress::Failed(format!("connect archive: {e}")));
                return Err(e);
            }
        };
        let mut state = Recording {
            spec: self,
            archive: session.archive,
            started: BTreeMap::new(),
        };
        let mut ready = Some(ready);
        while state.step(&mut ready) {}
        if let Some(ready) = ready {
            ready(RecorderProgress::Failed(
                "stopped before every own recording was live".into(),
            ));
        }
        Ok(())
    }
}

impl<A: RecorderArchive> Recording<A> {
    /// One pass: wait for a membership change, a tick, or stop; then
    /// start recordings for new publishers, stop them for departed ones,
    /// resolve pending recording ids, and report readiness. `false` ends
    /// the loop.
    fn step(&mut self, ready: &mut Option<impl FnOnce(RecorderProgress)>) -> bool {
        if matches!(self.wait(), Wake::Stop) {
            return false;
        }
        let snapshot = self.spec.membership.borrow_and_update().clone();
        if snapshot.is_known() {
            self.reconcile(&snapshot, Instant::now());
        }
        self.resolve_pending();
        self.report(ready);
        true
    }

    fn wait(&mut self) -> Wake {
        let stop = self.spec.stop.clone();
        let membership = &mut self.spec.membership;
        self.spec.runtime.block_on(async {
            tokio::select! {
                () = stop.cancelled() => Wake::Stop,
                changed = membership.changed() => match changed {
                    Ok(()) => Wake::Changed,
                    Err(_) => Wake::Stop,
                },
                () = tokio::time::sleep(POLL) => Wake::Tick,
            }
        })
    }

    /// Start missing recordings and stop only publishers whose confirmed
    /// absence lasts for the removal grace. An outage resets that proof.
    fn reconcile(&mut self, membership: &Membership, now: Instant) {
        if !matches!(membership.health, CatalogHealth::Fresh) {
            self.started.values_mut().for_each(|s| s.absence.clear());
            return;
        }
        let publishers = membership.publishers();
        let desired: BTreeMap<String, &PublisherRecord> = publishers
            .values()
            .map(|p| {
                (
                    destination_uri(IpAddr::V4(self.spec.local_ip), p.control),
                    p,
                )
            })
            .collect();
        let departed: Vec<String> = self
            .started
            .iter_mut()
            .filter_map(|(uri, started)| {
                started
                    .absence
                    .expired(desired.contains_key(uri), now, self.spec.removal_grace)
                    .then(|| uri.clone())
            })
            .collect();
        for uri in &departed {
            self.stop_one(uri);
        }
        let new: Vec<(String, PublisherRecord)> = desired
            .into_iter()
            .filter(|(uri, _)| !self.started.contains_key(uri))
            .map(|(uri, record)| (uri, record.clone()))
            .collect();
        for (uri, record) in &new {
            self.start_one(uri, record);
        }
    }

    fn start_one(&mut self, uri: &str, record: &PublisherRecord) {
        let subscription_id = match self.archive.start(uri, record.stream_id) {
            Ok(id) => Some(id),
            Err(e) => {
                info!(%uri, error = %e, "discovered recorder: start_recording rejected; adopting the existing recording");
                None
            }
        };
        info!(
            %uri,
            stream_id = record.stream_id,
            publisher = %record.publisher_id,
            "discovered recorder: recording publisher"
        );
        self.started.insert(
            uri.to_string(),
            Started {
                stream_id: record.stream_id,
                fragment: format!("control={}", record.control),
                subscription_id,
                session_id: record.session_id,
                recording_id: None,
                own: record.belongs_to(&self.spec.own_instance),
                absence: Absence::default(),
            },
        );
    }

    fn stop_one(&mut self, uri: &str) {
        let Some(started) = self.started.remove(uri) else {
            return;
        };
        if let Some(sub) = started.subscription_id
            && let Err(e) = self.archive.stop(sub)
        {
            warn!(%uri, error = %e, "discovered recorder: stop_recording_subscription failed");
        }
        info!(%uri, recording_id = ?started.recording_id, "discovered recorder: publisher left; recording subscription stopped");
    }

    /// Look up the recording id of every started entry that has none
    /// yet. A recording appears once the publisher's image has formed.
    fn resolve_pending(&mut self) {
        let archive = &self.archive;
        self.started
            .values_mut()
            .filter(|s| s.recording_id.is_none())
            .for_each(|s| s.recording_id = archive.latest(s));
    }

    /// Fire `ready` once every own publication has a recording id.
    fn report(&self, ready: &mut Option<impl FnOnce(RecorderProgress)>) {
        if ready.is_none() {
            return;
        }
        let own_live = self
            .started
            .values()
            .filter(|s| s.own && s.recording_id.is_some())
            .count();
        if own_live >= self.spec.expected_own
            && let Some(ready) = ready.take()
        {
            info!(
                own_recordings = own_live,
                "discovered recorder: every own publication is recorded"
            );
            ready(RecorderProgress::Ready {
                own_recordings: own_live,
            });
        }
    }
}
