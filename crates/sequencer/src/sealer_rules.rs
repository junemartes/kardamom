//! The decision version that the sealer reports for the current cluster
//! session (egress kind 17), as the egress feed thread reads it.
//!
//! The sealer sends kind 17 under decision version 3, and after a feature
//! activation also under version 2: to each open session at each switch,
//! to each session when it opens, and to each open session at each new
//! term. A session with no report runs version 2 with no activation, or
//! has not got its report yet. The session thread puts a session-start
//! marker on the egress channel when a session opens, and the two can
//! arrive in either order, so each carries the cluster session id: a kind
//! 17 that arrives before its marker waits for it, and a kind 17 of an
//! older session never counts.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kardamom_cluster_adapter::wire::FeatureActivation;

use crate::metrics;
use crate::resync::SharedWatermark;

/// How long after a switch of the rules the sealer may take to report it
/// before the epoch lane warns.
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

/// The epoch lane's watch over the newest feature activation that it
/// offered. An activation with a time ahead switches the sealer rules off
/// at the block after the first block of its epoch, a moment after the
/// offer, and on again at its time. An activation with a time at or before
/// now switches them on at that block. The sealer sends its rules
/// (egress kind 17) to each open session at each switch, and again at each
/// new leadership term. When the report does not match the rules of the
/// activation [`RULES_AFTER`] past the last switch, the lane warns once for
/// that switch and counts it, so a sealer that does not follow the
/// activation is visible. One owner: the epoch lane. It reads the version
/// that the egress feed thread publishes.
pub struct ActivationWatch {
    partition: u32,
    shared: SharedWatermark,
    /// The newest activation that the lane offered, `None` before the first.
    watched: Option<Watched>,
}

/// The newest activation that the lane offered, and its switches.
struct Watched {
    /// The L1 origin of the activation. The lane offers an epoch again
    /// after back pressure, a gap or a restart, and the same origin is the
    /// same activation.
    l1_origin: u64,
    /// When the lane offered it: the sealer switches a block or two later.
    offered: Instant,
    /// When its time comes, `None` when it is too far ahead for an `Instant`.
    due: Option<Instant>,
    /// The last switch that the watch settled: the report matched it, or
    /// the lane warned for it.
    settled: Option<Instant>,
}

impl Watched {
    /// The last switch at `now`, and whether it switched the rules on.
    fn last_switch(&self, now: Instant) -> (Instant, bool) {
        self.due
            .filter(|due| now >= *due)
            .map_or((self.offered, false), |due| (due, true))
    }
}

impl ActivationWatch {
    #[must_use]
    pub fn new(partition: u32, shared: SharedWatermark) -> Self {
        Self {
            partition,
            shared,
            watched: None,
        }
    }

    /// The lane offered `activation` at `now`. The activation time is in
    /// unix ms, the chain's unit; a time at or before now is due at once.
    pub fn on_offered(&mut self, activation: FeatureActivation, now: Instant) {
        if self
            .watched
            .as_ref()
            .is_some_and(|w| w.l1_origin == activation.l1_origin)
        {
            return;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, kardamom_types::time::duration_to_ms_saturating);
        // A time before now has no delay: saturating is the meaning.
        let delay = Duration::from_millis(activation.activation_ms.saturating_sub(now_ms));
        self.watched = Some(Watched {
            l1_origin: activation.l1_origin,
            offered: now,
            due: now.checked_add(delay),
            settled: None,
        });
    }

    /// Warn once for the last switch when the sealer has not reported its
    /// rules [`RULES_AFTER`] past it.
    pub fn check(&mut self, now: Instant) {
        let Some(watched) = self.watched.as_mut() else {
            return;
        };
        let (switch, on) = watched.last_switch(now);
        if watched.settled == Some(switch) {
            return;
        }
        if self.shared.sealer_runs_v3() == on {
            watched.settled = Some(switch);
            return;
        }
        if switch
            .checked_add(RULES_AFTER)
            .is_none_or(|warn_at| now < warn_at)
        {
            return;
        }
        watched.settled = Some(switch);
        metrics::record_sealer_rules_missing(self.partition);
        tracing::warn!(
            partition = self.partition,
            expected_version = if on { 3 } else { 2 },
            "the sealer reported other rules (egress kind 17) than the newest feature activation that this \
             replica sent gives, 60 s after the switch"
        );
    }
}

#[cfg(test)]
#[path = "sealer_rules_tests.rs"]
mod tests;
