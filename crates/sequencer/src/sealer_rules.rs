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

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kardamom_cluster_adapter::wire::FeatureActivation;

use crate::metrics;
use crate::resync::SharedWatermark;

/// How long after the activation time the sealer may take to report
/// version 3 before the epoch lane warns.
const RULES_AFTER: Duration = Duration::from_secs(60);

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

/// The epoch lane's watch over the feature activations that it offered.
/// The sealer sends its rules (egress kind 17) to each open session at the
/// switch, and again at each new leadership term. The sequencer holds its
/// nonce releases until it gets them. When no report of version 3 comes
/// [`RULES_AFTER`] past the activation time, the lane warns and counts it,
/// so a release queue that waits for ever is visible. One owner: the epoch
/// lane. It reads the version that the egress feed thread publishes.
pub struct ActivationWatch {
    partition: u32,
    shared: SharedWatermark,
    /// When the lane warns: [`RULES_AFTER`] past the activation time of the
    /// first activation it offered, `None` when no activation waits.
    warn_at: Option<Instant>,
}

impl ActivationWatch {
    #[must_use]
    pub fn new(partition: u32, shared: SharedWatermark) -> Self {
        Self {
            partition,
            shared,
            warn_at: None,
        }
    }

    /// The lane offered `activation` at `now`. The activation time is in
    /// unix ms, the chain's unit; a time in the past waits no more.
    pub fn on_offered(&mut self, activation: FeatureActivation, now: Instant) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, kardamom_types::time::duration_to_ms_saturating);
        // A time before now has no delay: saturating is the meaning.
        let delay = Duration::from_millis(activation.activation_ms.saturating_sub(now_ms));
        // A time too far ahead for an `Instant` never warns.
        self.warn_at = self
            .warn_at
            .or_else(|| now.checked_add(delay)?.checked_add(RULES_AFTER));
    }

    /// Warn once when the time has passed with no report of version 3.
    pub fn check(&mut self, now: Instant) {
        let Some(warn_at) = self.warn_at else {
            return;
        };
        if self.shared.sealer_takes_releases() {
            self.warn_at = None;
            return;
        }
        if now < warn_at {
            return;
        }
        self.warn_at = None;
        metrics::record_sealer_rules_missing(self.partition);
        tracing::warn!(
            partition = self.partition,
            "the sealer reported no decision version 3 (egress kind 17) 60 s after the activation time of a \
             feature activation that this replica sent; the nonce releases wait in the queue"
        );
    }
}

#[cfg(test)]
#[path = "sealer_rules_tests.rs"]
mod tests;
