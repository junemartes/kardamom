//! The halt contract. A service that cannot continue safely halts: it
//! stays up, serves its metrics and its queries, makes no progress, fails
//! its readiness check, and says why and what to do. It does not exit: an
//! exit loses the cause and invites a restart against the same fault.
//!
//! One halt stands per process. The exporter serves it as JSON on
//! `/halt`, and the gauge [`HALT`] is 1 while it stands. A halt that
//! clears by itself ([`Clears::Auto`]) is held by the service, which
//! retries its cause on a backoff. A halt an operator clears
//! ([`Clears::Operator`]) ends on `POST /halt/clear`, after the steps of
//! the runbook its [`RecoveryId`] names.

use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;

use crate::ready::unix_seconds;

/// The gauge: 1 while the service is halted, labelled by the cause and
/// the runbook id, 0 once the halt cleared.
pub const HALT: &str = "kardamom_halt";

/// Why a service halted. Each cause names one runbook and says whether
/// the service resumes by itself when the cause goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

impl HaltCause {
    /// Every cause, for the tests that check the rules and the runbooks.
    pub const ALL: [Self; 6] = [
        Self::L1SourceDisagreement,
        Self::L1ChainBreak,
        Self::L1Unreachable,
        Self::ReplayUnavailable,
        Self::DaLag,
        Self::ValidatorDivergence,
    ];

    /// The stable id: the `cause` label of the gauge and the alert.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::L1SourceDisagreement => "l1_source_disagreement",
            Self::L1ChainBreak => "l1_chain_break",
            Self::L1Unreachable => "l1_unreachable",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::DaLag => "da_lag",
            Self::ValidatorDivergence => "validator_divergence",
        }
    }

    /// The runbook of this cause.
    #[must_use]
    pub fn recovery(self) -> RecoveryId {
        match self {
            Self::L1SourceDisagreement => RecoveryId::L1SourceDisagreement,
            Self::L1ChainBreak => RecoveryId::L1ChainBreak,
            Self::L1Unreachable => RecoveryId::L1Unreachable,
            Self::ReplayUnavailable => RecoveryId::ReplayUnavailable,
            Self::DaLag => RecoveryId::DaLag,
            Self::ValidatorDivergence => RecoveryId::ValidatorDivergence,
        }
    }

    /// Whether the service resumes by itself when the cause goes. A lie
    /// of an L1 source, an L1 outage, and a DA lag all clear on their
    /// own: the source agrees again, L1 answers, the batcher posts. A
    /// refused replay and a divergence need an operator: a range must be
    /// recovered or the chain reverted, or a verdict must be examined.
    #[must_use]
    pub fn clears(self) -> Clears {
        match self {
            Self::L1SourceDisagreement | Self::L1ChainBreak | Self::L1Unreachable | Self::DaLag => {
                Clears::Auto
            }
            Self::ReplayUnavailable | Self::ValidatorDivergence => Clears::Operator,
        }
    }
}

/// The runbook a halt names: `docs/runbooks/<id>.md` holds the steps, in
/// order. One variant per runbook file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryId {
    L1SourceDisagreement,
    L1ChainBreak,
    L1Unreachable,
    ReplayUnavailable,
    DaLag,
    ValidatorDivergence,
    /// The last resort when no copy of the unposted range survives: the
    /// chain reverts to the posted head. No cause names it directly; the
    /// `replay_unavailable` runbook sends the operator here.
    RevertToPostedHead,
}

impl RecoveryId {
    /// Every runbook, for the test that checks each file exists.
    pub const ALL: [Self; 7] = [
        Self::L1SourceDisagreement,
        Self::L1ChainBreak,
        Self::L1Unreachable,
        Self::ReplayUnavailable,
        Self::DaLag,
        Self::ValidatorDivergence,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clears {
    /// The service retries its cause on a backoff and resumes when the
    /// cause goes.
    Auto,
    /// The service waits for `POST /halt/clear`, after the runbook's
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Halt {
    pub cause: HaltCause,
    /// The numbers: the block, the two hashes, the cursor and the floor.
    pub detail: String,
    pub recovery: RecoveryId,
    pub since: SystemTime,
    pub clears: Clears,
}

impl Halt {
    /// A halt for `cause`, raised now. The runbook and the clearing rule
    /// follow from the cause.
    #[must_use]
    pub fn new(cause: HaltCause, detail: impl Into<String>) -> Self {
        Self {
            cause,
            detail: detail.into(),
            recovery: cause.recovery(),
            since: SystemTime::now(),
            clears: cause.clears(),
        }
    }

    /// The JSON record the `/halt` route serves.
    #[must_use]
    pub fn to_json(&self, service: &str) -> serde_json::Value {
        serde_json::json!({
            "service": service,
            "halted": true,
            "cause": self.cause.id(),
            "detail": self.detail,
            "recovery": self.recovery.id(),
            "runbook": self.recovery.runbook(),
            "since_unix_secs": unix_seconds(self.since),
            "clears": self.clears.id(),
        })
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

    fn set_gauge(&self, value: f64) {
        metrics::gauge!(HALT, "cause" => self.cause.id(), "recovery" => self.recovery.id())
            .set(value);
    }
}

/// The JSON record of `service`'s halt: the standing halt, or
/// `{"halted": false}`.
#[must_use]
pub fn to_json(service: &str, halt: Option<&Halt>) -> serde_json::Value {
    halt.map_or_else(
        || serde_json::json!({ "service": service, "halted": false }),
        |h| h.to_json(service),
    )
}

/// The one halt of this process. A watch channel: the service writes,
/// the exporter's routes and the holds read the latest value.
static STATE: LazyLock<watch::Sender<Option<Halt>>> = LazyLock::new(|| watch::channel(None).0);

/// Raise `halt`. A halt of the same cause that already stands keeps its
/// `since` and takes the new detail, so a follower that fails every tick
/// reports the first failure's time.
pub fn raise(halt: Halt) {
    STATE.send_modify(|standing| {
        let raised = match standing.take() {
            Some(old) if old.cause == halt.cause => Halt {
                since: old.since,
                ..halt
            },
            Some(old) => {
                old.set_gauge(0.0);
                tracing::error!(%old.detail, cause = old.cause.id(), "halt replaced");
                halt
            }
            None => {
                tracing::error!(
                    cause = halt.cause.id(),
                    runbook = %halt.recovery.runbook(),
                    clears = halt.clears.id(),
                    detail = %halt.detail,
                    "service halted"
                );
                halt
            }
        };
        raised.set_gauge(1.0);
        *standing = Some(raised);
    });
}

/// Clear the standing halt, and return it. `None` when none stood, and
/// then the watchers are not woken: a follower calls this on every good
/// tick.
pub fn clear() -> Option<Halt> {
    let mut cleared = None;
    STATE.send_if_modified(|standing| {
        cleared = standing.take();
        cleared.is_some()
    });
    if let Some(halt) = &cleared {
        halt.set_gauge(0.0);
        tracing::info!(cause = halt.cause.id(), "halt cleared; the service resumes");
    }
    cleared
}

/// The standing halt, if any.
#[must_use]
pub fn current() -> Option<Halt> {
    STATE.borrow().clone()
}

/// A receiver of every change of the standing halt.
#[must_use]
pub fn subscribe() -> watch::Receiver<Option<Halt>> {
    STATE.subscribe()
}

/// Resolve once no halt stands. An operator halt waits here for
/// `POST /halt/clear`.
pub async fn cleared() {
    let mut rx = STATE.subscribe();
    // The sender is a static, so it never closes.
    let _ = rx.wait_for(Option::is_none).await;
}

/// Hold an [`Clears::Auto`] halt: raise it, then run `attempt` every
/// `backoff` until it succeeds. The halt clears on the first success,
/// and the value is returned. Each failure refreshes the detail.
pub async fn hold_until<T, E, F, Fut>(halt: Halt, backoff: Duration, mut attempt: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let cause = halt.cause;
    raise(halt);
    loop {
        tokio::time::sleep(backoff).await;
        if let Some(value) = retry_once(cause, &mut attempt).await {
            return value;
        }
    }
}

/// One retry of a held halt: the value on success, with the halt
/// cleared; `None` on failure, with the detail refreshed.
async fn retry_once<T, E, F, Fut>(cause: HaltCause, attempt: &mut F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match attempt().await {
        Ok(value) => {
            clear();
            Some(value)
        }
        Err(e) => {
            raise(Halt::new(cause, e.to_string()));
            None
        }
    }
}

#[cfg(test)]
#[path = "halt_tests.rs"]
mod tests;
