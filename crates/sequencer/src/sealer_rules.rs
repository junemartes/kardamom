//! The decision version of the sealer, as the egress feed thread keeps it
//! from the reports (egress kind 17) and the reference refusals (egress
//! kind 18) of the sealer.
//!
//! The sealer sends kind 17 to each session when the session opens, and to
//! each open session at each new term. The session thread puts a
//! session-start marker on the egress channel when a session opens, and the
//! two can arrive in either order, so each carries the cluster session id:
//! a kind 17 that arrives before its marker waits for it.
//!
//! The kept version is sticky. It is the highest version that any session
//! reported, and a new session does not reset it. Only a report of the
//! current session lowers it. A new process starts from a floor that the
//! deploy sets. So a new session does not send references without bytes to
//! a sealer that refuses them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use kardamom_cluster_adapter::wire::{
    RefRefused, SEALER_V4_DECISION_VERSION, SEALER_V5_DECISION_VERSION,
};
use kardamom_types::{DropReason, TxDrop};

use crate::metrics;
use crate::outbound::DropPublisher;

/// The kept decision version of the sealer, shared between the egress feed
/// thread, which writes it, and the publish loop, which reads it to choose
/// the shape of each offer. The default starts at 0.
#[derive(Clone, Default)]
pub struct SealerVersion(Arc<AtomicU8>);

impl SealerVersion {
    /// A version that starts at `floor`, before the first report.
    #[must_use]
    pub fn starting_at(floor: u8) -> Self {
        Self(Arc::new(AtomicU8::new(floor)))
    }

    fn set(&self, version: u8) {
        self.0.store(version, Ordering::Release);
    }

    fn get(&self) -> u8 {
        self.0.load(Ordering::Acquire)
    }

    /// Whether the kept version is 4 or newer: the sealer then orders a
    /// reference by its stream position.
    #[must_use]
    pub fn orders_by_position(&self) -> bool {
        self.get() >= SEALER_V4_DECISION_VERSION
    }

    /// Whether the kept version is 5 or newer: the sealer then takes each
    /// reference with the bytes of its transaction, and orders the bytes.
    #[must_use]
    pub fn carries_bytes(&self) -> bool {
        self.get() >= SEALER_V5_DECISION_VERSION
    }
}

/// The sealer's rules as this process keeps them. One owner: the egress
/// feed thread. It exports the kept version as a metric, and shares it
/// through [`SealerVersion`]. It publishes a drop record for each
/// reference that the sealer refused.
pub struct SealerRules<D> {
    partition: u32,
    shared: SealerVersion,
    /// The kept version: the value of `shared`.
    kept: u8,
    /// The cluster session that the last session-start marker named.
    session: Option<i64>,
    /// A report for a session whose marker has not arrived yet.
    early: Option<(i64, u8)>,
    /// The `tx_drops` channel of the lane.
    drops: D,
}

impl<D: DropPublisher> SealerRules<D> {
    /// Rules that keep the version of `shared`, from its start value.
    #[must_use]
    pub fn new(partition: u32, shared: SealerVersion, drops: D) -> Self {
        let kept = shared.get();
        metrics::record_sealer_version(partition, kept);
        Self {
            partition,
            shared,
            kept,
            session: None,
            early: None,
            drops,
        }
    }

    /// The session `session_id` starts. A report that came first for it
    /// sets the version. Otherwise the version stays.
    pub fn on_session_start(&mut self, session_id: i64) {
        self.session = Some(session_id);
        let report = self
            .early
            .take()
            .filter(|(id, _)| *id == session_id)
            .map(|(_, version)| version);
        self.publish(report.unwrap_or(self.kept));
    }

    /// The sealer reports `version` for the session `session_id`. A report
    /// of the current session sets the version, also to a lower one. A
    /// report of another session only raises it, and waits for its marker.
    pub fn on_sealer_info(&mut self, session_id: i64, version: u8) {
        if self.session == Some(session_id) {
            self.publish(version);
            return;
        }
        self.early = Some((session_id, version));
        self.publish(self.kept.max(version));
    }

    /// The sealer refused a reference without bytes: it runs `refused`'s
    /// version. Raise the version to it, count the refusal, and publish a
    /// drop for the hash, so the client sends the transaction again. The
    /// sequencer offers nothing again by itself.
    pub fn on_ref_refused(&mut self, refused: &RefRefused) {
        metrics::record_ref_refused(self.partition);
        tracing::warn!(
            partition = self.partition,
            tx_hash = %refused.canonical_id,
            sender = ?refused.sender,
            nonce = refused.nonce,
            decision_version = refused.decision_version,
            "sealer refused a reference without bytes; dropping the transaction"
        );
        self.publish(self.kept.max(refused.decision_version));
        self.drops.publish_drop(TxDrop {
            tx_hash: refused.canonical_id,
            sender: refused.sender,
            nonce: refused.nonce,
            reason: DropReason::SealerRefusedReference,
        });
    }

    fn publish(&mut self, version: u8) {
        self.kept = version;
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
