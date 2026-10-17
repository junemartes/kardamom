//! The decision version that the sealer reports for the current cluster
//! session (egress kind 17), as the egress feed thread reads it.
//!
//! The session thread puts a session-start marker on the egress channel
//! when a session opens, and the sealer sends kind 17 when it sees the
//! session open. The two can arrive in either order, so each carries the
//! cluster session id: a kind 17 that arrives before its marker waits for
//! it, and a kind 17 of an older session never counts.

use std::time::{Duration, Instant};

use crate::metrics;
use crate::resync::SharedWatermark;

/// How long a session may run with no decision version before the feed
/// warns: the sealer sends kind 17 when the session opens.
const MISSING_AFTER: Duration = Duration::from_secs(60);

/// The sealer's rules for the current session. One owner: the egress feed
/// thread. It publishes the version through [`SharedWatermark`].
pub struct SealerRules {
    partition: u32,
    shared: SharedWatermark,
    /// The cluster session that the last session-start marker named.
    session: Option<i64>,
    /// A report for a session whose marker has not arrived yet.
    early: Option<(i64, u8)>,
    /// Since when the current session has no version; `None` once it has
    /// one, or after the warning.
    missing_since: Option<Instant>,
}

impl SealerRules {
    #[must_use]
    pub fn new(partition: u32, shared: SharedWatermark, now: Instant) -> Self {
        metrics::record_sealer_version(partition, 0);
        Self {
            partition,
            shared,
            session: None,
            early: None,
            missing_since: Some(now),
        }
    }

    /// The session `session_id` starts. A report that came first for it
    /// counts; otherwise the session has no version yet.
    pub fn on_session_start(&mut self, session_id: i64, now: Instant) {
        self.session = Some(session_id);
        let version = self
            .early
            .take()
            .filter(|(id, _)| *id == session_id)
            .map_or(0, |(_, version)| version);
        self.publish(version, now);
    }

    /// The sealer reports `version` for the session `session_id`.
    pub fn on_sealer_info(&mut self, session_id: i64, version: u8, now: Instant) {
        if self.session == Some(session_id) {
            self.publish(version, now);
            return;
        }
        self.early = Some((session_id, version));
    }

    /// Warn once when the current session has had no version for
    /// [`MISSING_AFTER`]: the sequencer sends no nonce release meanwhile.
    pub fn check(&mut self, now: Instant) {
        let Some(since) = self.missing_since else {
            return;
        };
        if now.duration_since(since) < MISSING_AFTER {
            return;
        }
        self.missing_since = None;
        tracing::warn!(
            partition = self.partition,
            session = ?self.session,
            "the sealer reported no decision version for 60 s; no nonce release is sent"
        );
    }

    fn publish(&mut self, version: u8, now: Instant) {
        self.shared.set_sealer_version(version);
        metrics::record_sealer_version(self.partition, version);
        self.missing_since = (version == 0).then_some(now);
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
