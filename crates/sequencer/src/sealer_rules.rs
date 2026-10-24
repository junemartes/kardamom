//! The decision version that the sealer reports for the current cluster
//! session (egress kind 17), as the egress feed thread reads it.
//!
//! The sealer sends kind 17 to each session when the session opens, and to
//! each open session at each new term. A session with no report has not
//! got its report yet, or its sealer is of a release that sends none. The
//! session thread puts a session-start marker on the egress channel when a
//! session opens, and the two can arrive in either order, so each carries
//! the cluster session id: a kind 17 that arrives before its marker waits
//! for it, and a kind 17 of an older session never counts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use kardamom_cluster_adapter::wire::{SEALER_V4_DECISION_VERSION, SEALER_V5_DECISION_VERSION};

use crate::metrics;

/// The decision version that the sealer reported for the current session,
/// shared between the egress feed thread, which writes it, and the publish
/// loop, which reads it to choose the deadline field of each offer. 0
/// before the first report.
#[derive(Clone, Default)]
pub struct SealerVersion(Arc<AtomicU8>);

impl SealerVersion {
    fn set(&self, version: u8) {
        self.0.store(version, Ordering::Release);
    }

    fn get(&self) -> u8 {
        self.0.load(Ordering::Acquire)
    }

    /// Whether the sealer of the current session runs decision version 4 or
    /// newer: it then orders a reference by its stream position.
    #[must_use]
    pub fn orders_by_position(&self) -> bool {
        self.get() >= SEALER_V4_DECISION_VERSION
    }

    /// Whether the sealer of the current session runs decision version 5 or
    /// newer: it then takes each reference with the bytes of its
    /// transaction, and orders the bytes.
    #[must_use]
    pub fn carries_bytes(&self) -> bool {
        self.get() >= SEALER_V5_DECISION_VERSION
    }
}

/// The sealer's rules for the current session. One owner: the egress feed
/// thread. It exports the version as a metric, and shares it through
/// [`SealerVersion`].
pub struct SealerRules {
    partition: u32,
    shared: SealerVersion,
    /// The cluster session that the last session-start marker named.
    session: Option<i64>,
    /// A report for a session whose marker has not arrived yet.
    early: Option<(i64, u8)>,
}

impl SealerRules {
    #[must_use]
    pub fn new(partition: u32, shared: SealerVersion) -> Self {
        metrics::record_sealer_version(partition, 0);
        Self {
            partition,
            shared,
            session: None,
            early: None,
        }
    }

    /// The session `session_id` starts. A report that came first for it
    /// counts; otherwise the session has no version yet.
    pub fn on_session_start(&mut self, session_id: i64) {
        self.session = Some(session_id);
        let version = self
            .early
            .take()
            .filter(|(id, _)| *id == session_id)
            .map_or(0, |(_, version)| version);
        self.publish(version);
    }

    /// The sealer reports `version` for the session `session_id`.
    pub fn on_sealer_info(&mut self, session_id: i64, version: u8) {
        if self.session == Some(session_id) {
            self.publish(version);
            return;
        }
        self.early = Some((session_id, version));
    }

    fn publish(&mut self, version: u8) {
        self.shared.set(version);
        metrics::record_sealer_version(self.partition, version);
        tracing::info!(
            partition = self.partition,
            session = ?self.session,
            decision_version = version,
            "sealer rules"
        );
    }
}

#[cfg(test)]
#[path = "sealer_rules_tests.rs"]
mod tests;
