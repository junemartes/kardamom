//! The reconciler: turns membership snapshots into idempotent attach and
//! detach calls on one multi-destination subscription.
//!
//! Attachments are keyed by destination URI, the key the Aeron driver
//! uses. A replacement incarnation that reuses an endpoint keeps the
//! destination attached; the driver forms a new image with a new session
//! id, which is how the consumer tells the incarnations apart. The
//! service ids behind each URI are tracked for the log lines.
//!
//! A member that vanishes from a successful read is detached only after
//! it has stayed missing for the removal grace. A degraded read (an
//! error) changes nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use super::endpoint::destination_uri;
use super::record::{PublisherRecord, ServiceId};
use super::watch::Membership;
use crate::error::LogError;

/// The driver-side operations the reconciler drives. The Aeron runtime's
/// destination handle implements it; a test port records the calls.
pub trait DestinationPort {
    /// Attach `uri`. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver rejects or times out the attach.
    fn attach(&self, uri: &str) -> Result<(), LogError>;

    /// Detach `uri`.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver rejects or times out the detach.
    fn detach(&self, uri: &str) -> Result<(), LogError>;
}

/// The attach and detach calls one snapshot asks for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub attach: Vec<String>,
    pub detach: Vec<String>,
}

/// One attached destination and the incarnations behind it.
#[derive(Clone, Debug, Default)]
struct Attached {
    ids: BTreeSet<ServiceId>,
    /// Set while the last successful read no longer lists any of `ids`.
    missing_since: Option<Instant>,
}

pub struct Reconciler {
    /// The local address every destination receives on.
    local_ip: IpAddr,
    grace: Duration,
    attached: BTreeMap<String, Attached>,
}

impl Reconciler {
    #[must_use]
    pub fn new(local_ip: IpAddr, grace: Duration) -> Self {
        Self {
            local_ip,
            grace,
            attached: BTreeMap::new(),
        }
    }

    /// The destination URIs currently attached.
    #[must_use]
    pub fn attached(&self) -> Vec<String> {
        self.attached.keys().cloned().collect()
    }

    /// Compute the calls `membership` asks for at `now`, without applying
    /// them. A membership before the first successful read asks for
    /// nothing.
    #[must_use]
    pub fn plan(&mut self, membership: &Membership, now: Instant) -> Plan {
        if !membership.is_known() {
            return Plan::default();
        }
        let desired = self.ids_by_uri(membership);
        let attach = desired
            .keys()
            .filter(|uri| !self.attached.contains_key(*uri))
            .cloned()
            .collect();
        let detach = self
            .attached
            .iter_mut()
            .filter_map(|(uri, att)| Self::detach_due(uri, att, desired.get(uri), now, self.grace))
            .collect();
        self.note_incarnations(&desired);
        Plan { attach, detach }
    }

    /// Apply `plan` through `port`, recording each success. A failed
    /// attach stays unattached, so the next snapshot retries it. A failed
    /// detach stays attached for the same reason.
    pub fn apply<P: DestinationPort>(&mut self, plan: &Plan, port: &P, membership: &Membership) {
        let ids_by_uri = self.ids_by_uri(membership);
        plan.attach
            .iter()
            .for_each(|uri| self.attach_one(port, uri, ids_by_uri.get(uri)));
        plan.detach
            .iter()
            .for_each(|uri| self.detach_one(port, uri));
    }

    fn attach_one<P: DestinationPort>(
        &mut self,
        port: &P,
        uri: &str,
        ids: Option<&BTreeSet<ServiceId>>,
    ) {
        match port.attach(uri) {
            Ok(()) => {
                info!(%uri, "discovery: attached publisher destination");
                self.attached.insert(
                    uri.to_string(),
                    Attached {
                        ids: ids.cloned().unwrap_or_default(),
                        missing_since: None,
                    },
                );
            }
            Err(e) => warn!(%uri, error = %e, "discovery: attach failed; will retry"),
        }
    }

    fn detach_one<P: DestinationPort>(&mut self, port: &P, uri: &str) {
        match port.detach(uri) {
            Ok(()) => {
                info!(%uri, "discovery: detached publisher destination");
                self.attached.remove(uri);
            }
            Err(e) => warn!(%uri, error = %e, "discovery: detach failed; will retry"),
        }
    }

    fn uri_of(&self, record: &PublisherRecord) -> String {
        destination_uri(self.local_ip, record.control)
    }

    fn ids_by_uri(&self, membership: &Membership) -> BTreeMap<String, BTreeSet<ServiceId>> {
        membership
            .publishers()
            .into_values()
            .fold(BTreeMap::new(), |mut acc, record| {
                acc.entry(self.uri_of(&record))
                    .or_default()
                    .insert(record.id);
                acc
            })
    }

    /// Whether `uri` is due for detach: missing from the read for longer
    /// than `grace`. Updates the missing timer as a side effect, and
    /// clears it when the member is back.
    fn detach_due(
        uri: &str,
        att: &mut Attached,
        desired: Option<&BTreeSet<ServiceId>>,
        now: Instant,
        grace: Duration,
    ) -> Option<String> {
        if desired.is_some() {
            att.missing_since = None;
            return None;
        }
        let since = *att.missing_since.get_or_insert(now);
        (now.duration_since(since) >= grace).then(|| uri.to_string())
    }

    /// Log a replacement incarnation behind an attached endpoint, and
    /// adopt its id set.
    fn note_incarnations(&mut self, desired: &BTreeMap<String, BTreeSet<ServiceId>>) {
        self.attached
            .iter_mut()
            .filter_map(|(uri, att)| desired.get(uri).map(|ids| (uri, att, ids)))
            .for_each(|(uri, att, ids)| att.adopt(uri, ids));
    }
}

impl Attached {
    /// Take over `ids` as the incarnations behind this endpoint, logging
    /// a change.
    fn adopt(&mut self, uri: &str, ids: &BTreeSet<ServiceId>) {
        if *ids != self.ids && !self.ids.is_empty() {
            info!(
                %uri,
                previous = ?self.ids,
                current = ?ids,
                "discovery: publisher incarnation changed behind an attached endpoint"
            );
        }
        self.ids.clone_from(ids);
    }
}

#[cfg(test)]
mod tests;
