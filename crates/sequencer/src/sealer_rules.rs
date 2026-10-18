//! The decision version that the sealer reports for the current cluster
//! session (egress kind 17), as the egress feed thread reads it.
//!
//! The sealer sends kind 17 only under decision version 3: to each open
//! session when its rules switch, and then to each session when it opens.
//! A session with no report runs version 2, the rules with no nonce
//! release. The session thread puts a session-start marker on the egress
//! channel when a session opens, and the two can arrive in either order, so
//! each carries the cluster session id: a kind 17 that arrives before its
//! marker waits for it, and a kind 17 of an older session never counts.

use crate::metrics;
use crate::resync::SharedWatermark;

/// The sealer's rules for the current session. One owner: the egress feed
/// thread. It publishes the version through [`SharedWatermark`].
pub struct SealerRules {
    partition: u32,
    shared: SharedWatermark,
    /// The cluster session that the last session-start marker named.
    session: Option<i64>,
    /// A report for a session whose marker has not arrived yet.
    early: Option<(i64, u8)>,
}

impl SealerRules {
    #[must_use]
    pub fn new(partition: u32, shared: SharedWatermark) -> Self {
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
        self.shared.set_sealer_version(version);
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
