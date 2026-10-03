//! The halt contract. A service that cannot continue safely halts: it
//! stays up, serves its metrics and its queries, makes no progress, fails
//! its readiness check, and says why and what to do. It does not exit: an
//! exit loses the cause and invites a restart against the same fault.
//!
//! One halt stands per process, in the process [`Lifecycle`]. The
//! exporter serves it as JSON on `/halt`, and the gauge [`HALT`] is 1
//! while it stands. A halt that clears by itself ([`Clears::Auto`]) is
//! held by the service, which retries its cause on a backoff. A halt an
//! operator clears ([`Clears::Operator`]) ends on `POST /halt/clear`,
//! after the steps of the runbook its [`RecoveryId`] names.
//!
//! The types live in `kardamom_types::service`, because the services
//! share them on the `events` stream.
//!
//! [`Lifecycle`]: crate::lifecycle::Lifecycle

use std::time::Duration;

pub use kardamom_types::service::{
    Clears, Halt, HaltCause, HaltRef, Pause, PauseReason, RecoveryId, ServiceEvent, ServiceState,
};

pub use crate::lifecycle::{HALT, PAUSED};
use crate::lifecycle::{Slots, process};

/// The JSON form of a lifecycle record.
pub trait Record {
    /// The record as a JSON object.
    fn to_json(&self) -> serde_json::Value;
}

impl Record for Halt {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "cause": self.cause.id(),
            "detail": self.detail,
            "recovery": self.recovery.id(),
            "runbook": self.recovery.runbook(),
            "since_unix_ms": self.since_unix_ms,
            "clears": self.clears.id(),
        })
    }
}

impl Record for HaltRef {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "service": self.service,
            "instance": self.instance,
            "cause": self.cause.id(),
            "runbook": self.cause.recovery().runbook(),
        })
    }
}

impl Record for Pause {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reason": self.reason.id(),
            "root": self.reason.root().map(Record::to_json),
            "note": match &self.reason {
                PauseReason::Operator { note } => Some(note.as_str()),
                PauseReason::Upstream(_) => None,
            },
            "since_unix_ms": self.since_unix_ms,
        })
    }
}

impl Record for Slots {
    /// The `/halt` record: the state, the halt's fields at the top level,
    /// and the pause.
    fn to_json(&self) -> serde_json::Value {
        let mut record = self
            .halt
            .as_ref()
            .map_or_else(|| serde_json::json!({}), Record::to_json);
        record["state"] = self.state().id().into();
        record["halted"] = self.halt.is_some().into();
        record["pause"] = self
            .pause
            .as_ref()
            .map_or(serde_json::Value::Null, Record::to_json);
        record
    }
}

/// The `/halt` record of `service`: its state, its halt, and its pause.
#[must_use]
pub fn to_json(service: &str, slots: &Slots) -> serde_json::Value {
    let mut record = slots.to_json();
    record["service"] = service.into();
    record
}

/// Raise `halt` on the process. See [`Lifecycle::raise`].
///
/// [`Lifecycle::raise`]: crate::lifecycle::Lifecycle::raise
pub fn raise(halt: Halt) {
    process().raise(halt);
}

/// Clear the process's standing halt, and return it. See
/// [`Lifecycle::clear`].
///
/// [`Lifecycle::clear`]: crate::lifecycle::Lifecycle::clear
#[allow(
    clippy::must_use_candidate,
    reason = "a follower clears for the effect on every good tick; the value is for a caller that reports it"
)]
pub fn clear() -> Option<Halt> {
    process().clear()
}

/// The process's standing halt, if any.
#[must_use]
pub fn current() -> Option<Halt> {
    process().slots().halt
}

/// Resolve once no halt stands on the process. An operator halt waits
/// here for `POST /halt/clear`.
pub async fn cleared() {
    process().cleared().await;
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
