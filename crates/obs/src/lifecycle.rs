//! The lifecycle of a service: running, halted on its own fault, or
//! paused while it waits on something outside itself.
//!
//! A [`Lifecycle`] holds two slots, the halt and the pause, in one watch
//! channel: the service writes, the exporter's routes, the readiness
//! check, and the events beacon read the latest value. A halt has
//! priority over a pause, so a paused service that finds its own fault
//! reads as halted.
//!
//! The process has one lifecycle, [`process`]. A component that the
//! other services must see on its own, for example the validator's
//! attester, or the sealer as the ingress observes it, owns another one
//! with its own `service` label.

use std::sync::LazyLock;

use kardamom_types::service::{Halt, HaltRef, Pause, PauseReason, ServiceState};
use metrics::Label;
use tokio::sync::watch;

/// The gauge: 1 while the service is halted, labelled by the cause and
/// the runbook id, 0 once the halt cleared.
pub const HALT: &str = "kardamom_halt";

/// The gauge: 1 while the service is paused, labelled by the reason, the
/// root's service, and the root's cause (`operator` for an operator's
/// pause), 0 once it resumed. The `cause` label is the key of the
/// Alertmanager inhibit rule that mutes a pause while its root pages.
pub const PAUSED: &str = "kardamom_paused";

/// The halt and the pause of one lifecycle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Slots {
    pub halt: Option<Halt>,
    pub pause: Option<Pause>,
}

impl Slots {
    /// The state these slots make: the halt first, then the pause.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        match (&self.halt, &self.pause) {
            (Some(halt), _) => ServiceState::Halted(halt.clone()),
            (None, Some(pause)) => ServiceState::Paused(pause.clone()),
            (None, None) => ServiceState::Running,
        }
    }

    /// One line for a readiness answer while the service makes no
    /// progress, `None` while it runs.
    #[must_use]
    pub fn summary(&self) -> Option<String> {
        self.halt
            .as_ref()
            .map(Halt::summary)
            .or_else(|| self.pause.as_ref().map(Pause::summary))
    }
}

/// One service's lifecycle.
pub struct Lifecycle {
    slots: watch::Sender<Slots>,
    /// The `service` label of the gauges. `None` for the process: the
    /// exporter's global label names it.
    service: Option<&'static str>,
}

/// The lifecycle of this process.
static PROCESS: LazyLock<Lifecycle> = LazyLock::new(|| Lifecycle::new(None));

/// The lifecycle of this process: the one the exporter serves on `/halt`
/// and `/ready`, and the beacon publishes.
#[must_use]
pub fn process() -> &'static Lifecycle {
    &PROCESS
}

impl Lifecycle {
    /// A lifecycle that starts running. `service` overrides the gauges'
    /// `service` label for a component the other services see on its
    /// own.
    #[must_use]
    pub fn new(service: Option<&'static str>) -> Self {
        Self {
            slots: watch::channel(Slots::default()).0,
            service,
        }
    }

    /// The current slots.
    #[must_use]
    pub fn slots(&self) -> Slots {
        self.slots.borrow().clone()
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        self.slots.borrow().state()
    }

    /// A receiver of every change of the slots.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Slots> {
        self.slots.subscribe()
    }

    /// Raise `halt`. A halt of the same cause that already stands keeps
    /// its `since` and takes the new detail, so a follower that fails
    /// every tick reports the first failure's time.
    pub fn raise(&self, halt: Halt) {
        self.slots.send_modify(|slots| {
            let raised = self.replace_halt(slots.halt.take(), halt);
            self.set_halt_gauge(&raised, 1.0);
            slots.halt = Some(raised);
        });
    }

    /// The halt that stands after `halt` arrives over `standing`.
    fn replace_halt(&self, standing: Option<Halt>, halt: Halt) -> Halt {
        match standing {
            Some(old) if old.cause == halt.cause => Halt {
                since_unix_ms: old.since_unix_ms,
                ..halt
            },
            Some(old) => {
                self.set_halt_gauge(&old, 0.0);
                tracing::error!(%old.detail, cause = old.cause.id(), "halt replaced");
                halt
            }
            None => {
                tracing::error!(
                    service = self.service,
                    cause = halt.cause.id(),
                    runbook = %halt.recovery.runbook(),
                    clears = halt.clears.id(),
                    detail = %halt.detail,
                    "service halted"
                );
                halt
            }
        }
    }

    /// Clear the standing halt, and return it. `None` when none stood,
    /// and then the watchers are not woken: a follower calls this on
    /// every good tick.
    pub fn clear(&self) -> Option<Halt> {
        let mut cleared = None;
        self.slots.send_if_modified(|slots| {
            cleared = slots.halt.take();
            cleared.is_some()
        });
        if let Some(halt) = &cleared {
            self.set_halt_gauge(halt, 0.0);
            tracing::info!(
                service = self.service,
                cause = halt.cause.id(),
                "halt cleared; the service resumes"
            );
        }
        cleared
    }

    /// Pause for `pause`. An operator's pause stands until the operator
    /// resumes: an upstream pause does not replace it. A pause for the
    /// same reason keeps its `since`, and the watchers are not woken.
    pub fn pause(&self, pause: Pause) {
        self.slots.send_if_modified(|slots| {
            let next = self.replace_pause(slots.pause.clone(), pause);
            let changed = slots.pause.as_ref() != Some(&next);
            slots.pause = Some(next);
            changed
        });
    }

    /// The pause that stands after `pause` arrives over `standing`.
    fn replace_pause(&self, standing: Option<Pause>, pause: Pause) -> Pause {
        match standing {
            Some(old) if old.reason == pause.reason => old,
            Some(old)
                if matches!(old.reason, PauseReason::Operator { .. })
                    && matches!(pause.reason, PauseReason::Upstream(_)) =>
            {
                old
            }
            Some(old) => {
                self.set_pause_gauge(&old.reason, 0.0);
                self.start_pause(pause)
            }
            None => self.start_pause(pause),
        }
    }

    /// Log and export a pause that begins.
    fn start_pause(&self, pause: Pause) -> Pause {
        tracing::warn!(service = self.service, "{}", pause.summary());
        self.set_pause_gauge(&pause.reason, 1.0);
        pause
    }

    /// End an upstream pause: its root cleared. An operator's pause
    /// stands. Returns the pause that ended.
    #[allow(
        clippy::must_use_candidate,
        reason = "a resume is called for its effect; the value is for a caller that reports it"
    )]
    pub fn resume_upstream(&self) -> Option<Pause> {
        self.end_pause(|reason| matches!(reason, PauseReason::Upstream(_)))
    }

    /// End any pause: the operator's resume. Returns the pause that
    /// ended. An upstream root that still stands pauses the service
    /// again on its next check.
    #[allow(
        clippy::must_use_candidate,
        reason = "a resume is called for its effect; the value is for a caller that reports it"
    )]
    pub fn resume(&self) -> Option<Pause> {
        self.end_pause(|_| true)
    }

    /// End the standing pause when `ends` accepts its reason.
    fn end_pause(&self, ends: impl Fn(&PauseReason) -> bool) -> Option<Pause> {
        let mut ended = None;
        self.slots.send_if_modified(|slots| {
            ended = slots.pause.take_if(|pause| ends(&pause.reason));
            ended.is_some()
        });
        if let Some(pause) = &ended {
            self.set_pause_gauge(&pause.reason, 0.0);
            tracing::info!(service = self.service, "pause ended; the service resumes");
        }
        ended
    }

    /// Follow one upstream root: pause on it while it stands, resume
    /// when it goes. A reaction calls this on every change of its view.
    pub fn follow(&self, root: Option<HaltRef>) {
        match root {
            Some(root) => self.pause(Pause::new(PauseReason::Upstream(root))),
            None => {
                self.resume_upstream();
            }
        }
    }

    /// Resolve once no halt stands.
    pub async fn cleared(&self) {
        let mut rx = self.slots.subscribe();
        // The sender lives as long as `self`, so the wait never ends on a
        // closed channel while `self` is borrowed.
        let _ = rx.wait_for(|slots| slots.halt.is_none()).await;
    }

    /// The gauge labels: the `service` override, then `pairs`.
    fn labels<const N: usize>(&self, pairs: [(&'static str, String); N]) -> Vec<Label> {
        self.service
            .map(|service| Label::new("service", service))
            .into_iter()
            .chain(pairs.into_iter().map(|(k, v)| Label::new(k, v)))
            .collect()
    }

    fn set_halt_gauge(&self, halt: &Halt, value: f64) {
        let labels = self.labels([
            ("cause", halt.cause.id().to_string()),
            ("recovery", halt.recovery.id().to_string()),
        ]);
        metrics::gauge!(HALT, labels).set(value);
    }

    fn set_pause_gauge(&self, reason: &PauseReason, value: f64) {
        let (root_service, cause) = reason.root().map_or_else(
            || ("operator".to_string(), "operator".to_string()),
            |root| (root.service.clone(), root.cause.id().to_string()),
        );
        let labels = self.labels([
            ("reason", reason.id().to_string()),
            ("root_service", root_service),
            ("cause", cause),
        ]);
        metrics::gauge!(PAUSED, labels).set(value);
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
