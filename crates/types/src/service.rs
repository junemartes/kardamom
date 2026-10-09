//! The lifecycle state of a service, as the services share it on the
//! `events` Aeron stream.
//!
//! A service is `Running`, `Halted` on its own fault, or `Paused` while it
//! waits on something outside itself: a root halt upstream, or an
//! operator. Every service publishes a [`ServiceEvent`] at once on every
//! change and again on a heartbeat, so a subscriber holds the latest state
//! of every live service. The stream is for visibility and for the
//! liveness of the services off the log: nothing that changes the
//! canonical order reads it.

use alloc::format;
use alloc::string::String;

use rkyv::{Archive, Deserialize, Serialize};

/// Why a service halted. Each cause names one runbook and says whether
/// the service resumes by itself when the cause goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum HaltCause {
    /// Two L1 sources answered differently for one block or one log
    /// query, and no light client settled it.
    L1SourceDisagreement,
    /// A finalized L1 block does not descend from the block the follower
    /// holds.
    L1ChainBreak,
    /// No L1 source answers.
    L1Unreachable,
    /// The sealer refused the replay from the service's cursor: the range
    /// is below its retention floor, and no copy of it is at hand.
    ReplayUnavailable,
    /// The sealed head is more than the DA-lag budget past the last block
    /// posted to L1.
    DaLag,
    /// The validator's re-execution disagreed with the executor's
    /// published result.
    ValidatorDivergence,
    /// The sealer emits no boundary: the cluster has no leader, or no
    /// quorum to commit one.
    SealerNoQuorum,
    /// The da-watcher's L1 cursor file exists, but it cannot be read or
    /// parsed.
    L1CursorUnreadable,
    /// The sealer refuses the next L1 epoch, because an earlier one is
    /// missing, and the sequencer does not hold the missing epoch. Or the
    /// sequencer holds too many relayed epochs that no boundary confirms.
    OriginGap,
    /// The last ordered canonical index is more than the record-lag
    /// budget past the highest index that an executor recorded. A new
    /// variant goes last: the `events` stream carries the archived
    /// discriminant, and a reader of an older build must decode the rest.
    RecordLag,
    /// The last header of the L1 follower's finality step is not the
    /// light client's finalized header for that number. A new variant
    /// goes last, for the reason [`RecoveryId`] gives.
    L1LightClientMismatch,
    /// A consumer of the `l1_blocks` stream received two records of one
    /// L1 block number with inconsistent payloads: one follower instance read
    /// a lie that its cross-check did not catch.
    L1FollowerDisagreement,
    /// Every executor archive holds a record at one canonical index, and
    /// no record passes the validator's check against the canonical
    /// `TxRef`. A new variant goes last, for the reason [`RecoveryId`]
    /// gives.
    ExecRecordMismatch,
}

impl HaltCause {
    /// Every cause, for the tests that check the rules and the runbooks.
    pub const ALL: [Self; 13] = [
        Self::L1SourceDisagreement,
        Self::L1ChainBreak,
        Self::L1Unreachable,
        Self::ReplayUnavailable,
        Self::DaLag,
        Self::ValidatorDivergence,
        Self::SealerNoQuorum,
        Self::L1CursorUnreadable,
        Self::OriginGap,
        Self::RecordLag,
        Self::L1LightClientMismatch,
        Self::L1FollowerDisagreement,
        Self::ExecRecordMismatch,
    ];

    /// The stable id: the `cause` label of the gauge and the alert.
    #[must_use]
    pub fn id(self) -> &'static str {
        self.recovery().id()
    }

    /// The runbook of this cause. Every cause has its own runbook, so the
    /// runbook id is also the cause id.
    #[must_use]
    pub fn recovery(self) -> RecoveryId {
        match self {
            Self::L1SourceDisagreement => RecoveryId::L1SourceDisagreement,
            Self::L1ChainBreak => RecoveryId::L1ChainBreak,
            Self::L1Unreachable => RecoveryId::L1Unreachable,
            Self::ReplayUnavailable => RecoveryId::ReplayUnavailable,
            Self::DaLag => RecoveryId::DaLag,
            Self::ValidatorDivergence => RecoveryId::ValidatorDivergence,
            Self::SealerNoQuorum => RecoveryId::SealerNoQuorum,
            Self::L1CursorUnreadable => RecoveryId::L1CursorUnreadable,
            Self::OriginGap => RecoveryId::OriginGap,
            Self::RecordLag => RecoveryId::RecordLag,
            Self::L1LightClientMismatch => RecoveryId::L1LightClientMismatch,
            Self::L1FollowerDisagreement => RecoveryId::L1FollowerDisagreement,
            Self::ExecRecordMismatch => RecoveryId::ExecRecordMismatch,
        }
    }

    /// Whether the service resumes by itself when the cause goes. A lie
    /// of an L1 source, an L1 outage, a DA lag, a record lag, and a lost
    /// quorum all clear on their own: the source agrees again, L1
    /// answers, the batcher posts, an executor records, a leader commits. An origin gap clears when a
    /// boundary confirms the missing epoch. A refused replay, a
    /// divergence, and an unreadable cursor need an operator: a range must
    /// be recovered or the chain reverted, a verdict must be examined, or a
    /// resume block must be chosen. A light client that disagrees with
    /// two agreeing sources, and two follower instances that disagree,
    /// need an operator to decide which side lies. A record that no
    /// executor archive holds with the canonical hash is an integrity
    /// fault: an operator finds the cause before the validator goes on.
    #[must_use]
    pub fn clears(self) -> Clears {
        match self {
            Self::L1SourceDisagreement
            | Self::L1ChainBreak
            | Self::L1Unreachable
            | Self::DaLag
            | Self::SealerNoQuorum
            | Self::OriginGap
            | Self::RecordLag => Clears::Auto,
            Self::ReplayUnavailable
            | Self::ValidatorDivergence
            | Self::L1CursorUnreadable
            | Self::L1LightClientMismatch
            | Self::L1FollowerDisagreement
            | Self::ExecRecordMismatch => Clears::Operator,
        }
    }
}

/// The runbook a halt names: `docs/runbooks/<id>.md` holds the steps, in
/// order. One variant per runbook file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum RecoveryId {
    L1SourceDisagreement,
    L1ChainBreak,
    L1Unreachable,
    ReplayUnavailable,
    DaLag,
    ValidatorDivergence,
    SealerNoQuorum,
    /// The last resort when no copy of the unposted range survives: the
    /// chain reverts to the posted head. No cause names it directly; the
    /// `replay_unavailable` runbook sends the operator here.
    RevertToPostedHead,
    /// A new variant goes last: the `events` stream carries the archived
    /// discriminant, and a reader of an older build must decode the rest.
    L1CursorUnreadable,
    /// The sequencer's origin gap. It goes after every older variant, for
    /// the same reason.
    OriginGap,
    /// The sealer's record lag. It goes after every older variant, for
    /// the same reason.
    RecordLag,
    /// The L1 follower's light client anchor failed.
    L1LightClientMismatch,
    /// Two follower instances published inconsistent records for one block.
    L1FollowerDisagreement,
    /// The validator's executor stream check: every executor archive holds
    /// a mismatched record.
    ExecRecordMismatch,
}

impl RecoveryId {
    /// Every runbook, for the test that checks each file exists.
    pub const ALL: [Self; 14] = [
        Self::L1SourceDisagreement,
        Self::L1ChainBreak,
        Self::L1Unreachable,
        Self::ReplayUnavailable,
        Self::DaLag,
        Self::ValidatorDivergence,
        Self::SealerNoQuorum,
        Self::L1CursorUnreadable,
        Self::OriginGap,
        Self::RecordLag,
        Self::L1LightClientMismatch,
        Self::L1FollowerDisagreement,
        Self::ExecRecordMismatch,
        Self::RevertToPostedHead,
    ];

    /// The stable id: the `recovery` label of the gauge, and the file
    /// name of the runbook.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::L1SourceDisagreement => "l1_source_disagreement",
            Self::L1ChainBreak => "l1_chain_break",
            Self::L1Unreachable => "l1_unreachable",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::DaLag => "da_lag",
            Self::ValidatorDivergence => "validator_divergence",
            Self::SealerNoQuorum => "sealer_no_quorum",
            Self::L1CursorUnreadable => "l1_cursor_unreadable",
            Self::OriginGap => "origin_gap",
            Self::RecordLag => "record_lag",
            Self::L1LightClientMismatch => "l1_light_client_mismatch",
            Self::L1FollowerDisagreement => "l1_follower_disagreement",
            Self::ExecRecordMismatch => "exec_record_mismatch",
            Self::RevertToPostedHead => "revert_to_posted_head",
        }
    }

    /// The runbook's path in the repository.
    #[must_use]
    pub fn runbook(self) -> String {
        format!("docs/runbooks/{}.md", self.id())
    }
}

/// Whether a halt ends by itself or on an operator's command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum Clears {
    /// The service retries its cause on a backoff and resumes when the
    /// cause goes.
    Auto,
    /// The service waits for the operator's clear, after the runbook's
    /// steps.
    Operator,
}

impl Clears {
    /// The value the JSON record carries.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Operator => "operator",
        }
    }
}

/// The record of one halt.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Halt {
    pub cause: HaltCause,
    /// The numbers: the block, the two hashes, the cursor and the floor.
    pub detail: String,
    pub recovery: RecoveryId,
    /// When the halt was raised, in milliseconds since the Unix epoch.
    pub since_unix_ms: u64,
    pub clears: Clears,
}

impl Halt {
    /// A halt for `cause`, raised now. The runbook and the clearing rule
    /// follow from the cause.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn new(cause: HaltCause, detail: impl Into<String>) -> Self {
        Self {
            cause,
            detail: detail.into(),
            recovery: cause.recovery(),
            since_unix_ms: unix_ms_now(),
            clears: cause.clears(),
        }
    }

    /// One line for a log or a readiness answer.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "halted: cause={} recovery={} clears={} detail={}",
            self.cause.id(),
            self.recovery.runbook(),
            self.clears.id(),
            self.detail
        )
    }
}

/// The name of one halt of one process: the root a dependent pauses on.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct HaltRef {
    pub service: String,
    pub instance: String,
    pub cause: HaltCause,
}

impl HaltRef {
    /// The service name of the sealer's state. The sealer has no Rust
    /// runtime on the stream; the ingress observes it and publishes it.
    pub const SEALER: &'static str = "sealer";
    /// The instance name of the sealer's state: the cluster as a whole.
    pub const SEALER_INSTANCE: &'static str = "cluster";

    /// The sealer halted on `cause`.
    #[must_use]
    pub fn sealer(cause: HaltCause) -> Self {
        Self {
            service: Self::SEALER.into(),
            instance: Self::SEALER_INSTANCE.into(),
            cause,
        }
    }

    /// Whether this root is the sealer.
    #[must_use]
    pub fn is_sealer(&self) -> bool {
        self.service == Self::SEALER
    }
}

/// Why a service waits.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum PauseReason {
    /// A root halt upstream. The pause ends by itself when the root
    /// clears.
    Upstream(HaltRef),
    /// An operator's pause, for example for maintenance. It ends on the
    /// operator's resume only.
    Operator { note: String },
}

impl PauseReason {
    /// The `reason` label of the pause gauge.
    #[must_use]
    pub fn id(&self) -> &'static str {
        match self {
            Self::Upstream(_) => "upstream",
            Self::Operator { .. } => "operator",
        }
    }

    /// The root this pause waits on, if it waits on a halt.
    #[must_use]
    pub fn root(&self) -> Option<&HaltRef> {
        match self {
            Self::Upstream(root) => Some(root),
            Self::Operator { .. } => None,
        }
    }
}

/// The record of one pause.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Pause {
    pub reason: PauseReason,
    /// When the pause began, in milliseconds since the Unix epoch.
    pub since_unix_ms: u64,
}

impl Pause {
    /// A pause for `reason`, begun now.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn new(reason: PauseReason) -> Self {
        Self {
            reason,
            since_unix_ms: unix_ms_now(),
        }
    }

    /// One line for a log or a readiness answer.
    #[must_use]
    pub fn summary(&self) -> String {
        match &self.reason {
            PauseReason::Upstream(root) => format!(
                "paused: upstream {} {} halted ({}); runbook {}",
                root.service,
                root.instance,
                root.cause.id(),
                root.cause.recovery().runbook()
            ),
            PauseReason::Operator { note } => format!("paused: operator ({note})"),
        }
    }
}

/// The lifecycle state of one service.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub enum ServiceState {
    Running,
    /// The service's own fault. It pages.
    Halted(Halt),
    /// The service waits on something outside itself. It does not page.
    Paused(Pause),
    /// The change back to running, published once.
    Resumed,
}

impl ServiceState {
    /// The state's name in the JSON records.
    #[must_use]
    pub fn id(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Halted(_) => "halted",
            Self::Paused(_) => "paused",
            Self::Resumed => "resumed",
        }
    }

    /// The halt, if the service is halted.
    #[must_use]
    pub fn halt(&self) -> Option<&Halt> {
        match self {
            Self::Halted(halt) => Some(halt),
            Self::Running | Self::Paused(_) | Self::Resumed => None,
        }
    }

    /// The pause, if the service is paused.
    #[must_use]
    pub fn pause(&self) -> Option<&Pause> {
        match self {
            Self::Paused(pause) => Some(pause),
            Self::Running | Self::Halted(_) | Self::Resumed => None,
        }
    }
}

/// One record of the `events` stream: the state of one process.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct ServiceEvent {
    /// The exporter's service name, for example `ingress`.
    pub service: String,
    /// The host id of the process.
    pub instance: String,
    /// Increases with each record of one process. A value lower than the
    /// last one means the process restarted.
    pub seq: u64,
    pub state: ServiceState,
}

impl ServiceEvent {
    /// The root this record names: its own halt.
    #[must_use]
    pub fn halt_ref(&self) -> Option<HaltRef> {
        self.state.halt().map(|halt| HaltRef {
            service: self.service.clone(),
            instance: self.instance.clone(),
            cause: halt.cause,
        })
    }
}

/// Milliseconds since the Unix epoch. A clock before the epoch reads 0,
/// and a clock past `u64::MAX` milliseconds saturates.
#[cfg(feature = "std")]
#[must_use]
pub fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_round_trips_through_rkyv() {
        let event = ServiceEvent {
            service: "ingress".into(),
            instance: "host-1".into(),
            seq: 7,
            state: ServiceState::Paused(Pause {
                reason: PauseReason::Upstream(HaltRef {
                    service: "sealer".into(),
                    instance: "cluster".into(),
                    cause: HaltCause::DaLag,
                }),
                since_unix_ms: 42,
            }),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).unwrap();
        let back: ServiceEvent =
            rkyv::from_bytes::<ServiceEvent, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn the_ids_name_their_runbooks() {
        assert_eq!(HaltCause::SealerNoQuorum.id(), "sealer_no_quorum");
        assert_eq!(HaltCause::SealerNoQuorum.clears(), Clears::Auto);
        assert_eq!(HaltCause::ValidatorDivergence.clears(), Clears::Operator);
        assert_eq!(HaltCause::RecordLag.id(), "record_lag");
        assert_eq!(HaltCause::RecordLag.clears(), Clears::Auto);
        assert_eq!(
            HaltCause::RecordLag.recovery().runbook(),
            "docs/runbooks/record_lag.md"
        );
        assert_eq!(
            RecoveryId::RevertToPostedHead.runbook(),
            "docs/runbooks/revert_to_posted_head.md"
        );
    }

    #[test]
    fn a_halted_event_names_itself_as_the_root() {
        let event = ServiceEvent {
            service: "validator".into(),
            instance: "aux".into(),
            seq: 1,
            state: ServiceState::Halted(Halt::new(HaltCause::ValidatorDivergence, "root")),
        };
        let root = event.halt_ref().unwrap();
        assert_eq!(root.service, "validator");
        assert_eq!(root.cause, HaltCause::ValidatorDivergence);
        let running = ServiceEvent {
            state: ServiceState::Running,
            ..event
        };
        assert_eq!(running.halt_ref(), None);
    }
}
