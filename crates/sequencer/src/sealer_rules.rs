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
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use alloy_primitives::Address;
use kardamom_cluster_adapter::wire::{
    RefRefused, SEALER_V4_DECISION_VERSION, SEALER_V5_DECISION_VERSION,
};
use kardamom_types::{DropReason, TxDrop};

use crossbeam_channel::TrySendError;

use crate::metrics;
use crate::outbound::DropPublisher;
use crate::v2::AccountEvent;

/// The kept decision version of the sealer, shared between the egress feed
/// thread, which writes it, and the publish loop, which reads it to choose
/// the shape of each offer. The default starts at 0. Its session generation
/// survives a full account-event queue, so an idle loop with unconfirmed
/// offers can recover without new client traffic.
#[derive(Clone, Default)]
pub struct SealerVersion(Arc<SharedRules>);

#[derive(Default)]
struct SharedRules {
    version: AtomicU8,
    generation: AtomicU64,
}

impl SealerVersion {
    /// A version that starts at `floor`, before the first report.
    #[must_use]
    pub fn starting_at(floor: u8) -> Self {
        Self(Arc::new(SharedRules {
            version: AtomicU8::new(floor),
            generation: AtomicU64::new(0),
        }))
    }

    fn set(&self, version: u8) {
        self.0.version.store(version, Ordering::Release);
    }

    fn get(&self) -> u8 {
        self.0.version.load(Ordering::Acquire)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.0.generation.load(Ordering::Acquire)
    }

    fn next_session(&self) {
        // Generations identify changes; wrapping preserves that meaning.
        self.0.generation.fetch_add(1, Ordering::Release);
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
/// reference that the sealer refused, and tells the v2 loop.
pub struct SealerRules<D> {
    partition: u32,
    shared: SealerVersion,
    /// The cluster session that the last session-start marker named.
    session: Option<i64>,
    /// A report for a session whose marker has not arrived yet.
    early: Option<(i64, u8)>,
    /// The `tx_drops` channel of the lane.
    drops: D,
    /// The account events of the v2 loop, when it runs: each refusal goes
    /// there, so the filter forgets the refused run.
    refusals: Option<crossbeam_channel::Sender<AccountEvent>>,
}

impl<D: DropPublisher> SealerRules<D> {
    /// Rules that keep the version of `shared`, from its start value.
    #[must_use]
    pub fn new(partition: u32, shared: SealerVersion, drops: D) -> Self {
        metrics::record_sealer_version(partition, shared.get());
        Self {
            partition,
            shared,
            session: None,
            early: None,
            drops,
            refusals: None,
        }
    }

    /// Send each refusal to the v2 loop on `accounts`, its queue of account
    /// events.
    #[must_use]
    pub fn telling(mut self, accounts: crossbeam_channel::Sender<AccountEvent>) -> Self {
        self.refusals = Some(accounts);
        self
    }

    /// The session `session_id` starts. A report that came first for it
    /// sets the version. Otherwise the version stays.
    pub fn on_session_start(&mut self, session_id: i64) {
        let changed = self.session != Some(session_id);
        self.session = Some(session_id);
        let report = self
            .early
            .take()
            .filter(|(id, _)| *id == session_id)
            .map(|(_, version)| version);
        self.publish(report.unwrap_or(self.shared.get()));
        if changed {
            self.shared.next_session();
        }
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
        self.publish(self.shared.get().max(version));
    }

    /// The sealer refused a reference without bytes: it runs `refused`'s
    /// version. Raise the version to it, count the refusal, and publish a
    /// drop for the hash, so the client sends the transaction again. Tell
    /// the v2 loop, so it takes the resend. The sequencer offers nothing
    /// again by itself.
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
        self.publish(self.shared.get().max(refused.decision_version));
        self.tell_loop(AccountEvent::Refused {
            sender: refused.sender,
            nonce: refused.nonce,
        });
        self.drops.publish_drop(TxDrop {
            tx_hash: refused.canonical_id,
            sender: refused.sender,
            nonce: refused.nonce,
            reason: DropReason::SealerRefusedReference,
        });
    }

    /// The contiguity guard of the sealer rejected the reference of
    /// `sender` at `nonce`, because it expects `expected` (egress kind 5).
    /// Tell the v2 loop, so it offers the run again from the expected
    /// nonce.
    pub fn on_contiguity_reject(&self, sender: Address, nonce: u64, expected: u64) {
        self.tell_loop(AccountEvent::Rejected {
            sender,
            nonce,
            expected,
        });
    }

    /// Put the answer of the sealer on the queue of the v2 loop. A full
    /// queue loses it: the loss window of the filter then frees the nonce,
    /// later.
    fn tell_loop(&self, event: AccountEvent) {
        let full = self
            .refusals
            .as_ref()
            .is_some_and(|tx| matches!(tx.try_send(event), Err(TrySendError::Full(_))));
        if full {
            tracing::warn!(
                partition = self.partition,
                "account event queue full; the refusal waits for the loss window"
            );
        }
    }

    fn publish(&self, version: u8) {
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
