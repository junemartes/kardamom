//! The service events: each service publishes its lifecycle state, and a
//! subscriber keeps the latest state of every service.
//!
//! The [`Beacon`] publishes a [`ServiceEvent`] at once on every change of
//! a [`Lifecycle`] and again every [`HEARTBEAT`], `Running` included, so
//! a service that starts later learns every state within one heartbeat.
//! The change back to running is published once as `Resumed`.
//!
//! The [`Board`] keeps the latest record of every `(service, instance)`.
//! The records of one publisher arrive in order, so the latest record is
//! the state. A record with no heartbeat for [`EXPIRY`] is gone: the
//! process died or froze.
//!
//! The transport is the caller's: the beacon hands each record to a
//! publish function, and the [`BoardFeed`] reads records from a channel.
//!
//! [`Lifecycle`]: crate::lifecycle::Lifecycle

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::OnceLock;
use std::time::Duration;

use kardamom_types::service::{HaltCause, HaltRef, ServiceEvent, ServiceState};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::halt::Record;
use crate::lifecycle::{Slots, process};

/// How often a service publishes its state when nothing changes.
pub const HEARTBEAT: Duration = Duration::from_secs(5);

/// How long a record stays live without a heartbeat: three heartbeats.
pub const EXPIRY: Duration = Duration::from_secs(15);

/// How long the board keeps a gone record, so the chain status shows
/// what died.
const FORGET: Duration = Duration::from_mins(5);

/// How often the board feed refreshes its view when no record arrives,
/// so a record that stops is seen as gone.
const REFRESH: Duration = Duration::from_secs(1);

/// The name of one process on the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The exporter's service name.
    pub service: String,
    /// The host id.
    pub instance: String,
}

static IDENTITY: OnceLock<Identity> = OnceLock::new();

/// The identity of this process, once the exporter is installed.
#[must_use]
pub fn identity() -> Option<&'static Identity> {
    IDENTITY.get()
}

/// Record the identity of this process. The exporter's install calls
/// this once; a second call keeps the first identity.
pub(crate) fn set_identity(identity: Identity) {
    let _ = IDENTITY.set(identity);
}

/// The publisher side: one lifecycle's records, at once on a change and
/// on every heartbeat.
pub struct Beacon {
    identity: Identity,
    rx: watch::Receiver<Slots>,
    seq: u64,
    /// Whether the last record said the service runs. The first record
    /// of a process that starts running is `Running`, not `Resumed`.
    running: bool,
}

impl Beacon {
    /// A beacon for `identity`, over the lifecycle `rx` watches.
    #[must_use]
    pub fn new(identity: Identity, rx: watch::Receiver<Slots>) -> Self {
        Self {
            identity,
            rx,
            seq: 0,
            running: true,
        }
    }

    /// The beacon of this process, once the exporter is installed.
    #[must_use]
    pub fn of_process() -> Option<Self> {
        identity().map(|id| Self::new(id.clone(), process().subscribe()))
    }

    /// The next record: the current state, or `Resumed` on the first
    /// record after a halt or a pause.
    pub fn next_event(&mut self) -> ServiceEvent {
        let state = self.rx.borrow_and_update().state();
        let was_running = std::mem::replace(&mut self.running, state == ServiceState::Running);
        let state = if self.running && !was_running {
            ServiceState::Resumed
        } else {
            state
        };
        self.seq = self.seq.saturating_add(1);
        ServiceEvent {
            service: self.identity.service.clone(),
            instance: self.identity.instance.clone(),
            seq: self.seq,
            state,
        }
    }

    /// Publish through `publish` until the lifecycle drops.
    pub async fn run<F: FnMut(&ServiceEvent)>(mut self, mut publish: F) {
        while self.step(&mut publish).await.is_continue() {}
    }

    /// Publish one record, then wait for a change or the heartbeat.
    async fn step<F: FnMut(&ServiceEvent)>(&mut self, publish: &mut F) -> ControlFlow<()> {
        publish(&self.next_event());
        tokio::select! {
            changed = self.rx.changed() => match changed {
                Ok(()) => ControlFlow::Continue(()),
                Err(_) => ControlFlow::Break(()),
            },
            () = tokio::time::sleep(HEARTBEAT) => ControlFlow::Continue(()),
        }
    }
}

/// One record as the board last saw it.
#[derive(Debug, Clone)]
struct Seen {
    event: ServiceEvent,
    at: Instant,
}

/// The latest record of every `(service, instance)`.
#[derive(Debug, Default)]
pub struct Board {
    entries: BTreeMap<(String, String), Seen>,
}

impl Board {
    /// Take `event` as the latest state of its process.
    pub fn observe(&mut self, event: ServiceEvent, now: Instant) {
        let key = (event.service.clone(), event.instance.clone());
        let restarted = self
            .entries
            .get(&key)
            .is_some_and(|seen| event.seq < seen.event.seq);
        if restarted {
            tracing::info!(
                service = %event.service,
                instance = %event.instance,
                "service events: the process restarted"
            );
        }
        self.entries.insert(key, Seen { event, at: now });
    }

    /// Drop the records gone for longer than the board keeps them.
    pub fn forget(&mut self, now: Instant) {
        self.entries
            .retain(|_, seen| now.saturating_duration_since(seen.at) < FORGET);
    }

    /// The board at `now`.
    #[must_use]
    pub fn view(&self, now: Instant) -> BoardView {
        BoardView {
            services: self
                .entries
                .values()
                .map(|seen| {
                    let age = now.saturating_duration_since(seen.at);
                    ServiceView {
                        event: seen.event.clone(),
                        live: age < EXPIRY,
                        age,
                    }
                })
                .collect(),
        }
    }
}

/// One process on the board.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceView {
    pub event: ServiceEvent,
    /// Whether a record arrived within [`EXPIRY`].
    pub live: bool,
    /// How long ago the last record arrived.
    pub age: Duration,
}

impl ServiceView {
    /// The state's name in the chain status: `gone` for a record past
    /// its expiry, the state otherwise. `Resumed` reads as `running`.
    #[must_use]
    pub fn state_id(&self) -> &'static str {
        match (&self.event.state, self.live) {
            (_, false) => "gone",
            (ServiceState::Resumed, true) => "running",
            (state, true) => state.id(),
        }
    }

    /// The process as a JSON object.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "service": self.event.service,
            "instance": self.event.instance,
            "state": self.state_id(),
            "seq": self.event.seq,
            "age_ms": u64::try_from(self.age.as_millis()).unwrap_or(u64::MAX),
            "halt": self.event.state.halt().map(Record::to_json),
            "pause": self.event.state.pause().map(Record::to_json),
        })
    }
}

/// The board at one instant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoardView {
    pub services: Vec<ServiceView>,
}

impl BoardView {
    /// The live records of `service`.
    pub fn live_of<'a>(&'a self, service: &'a str) -> impl Iterator<Item = &'a ServiceView> {
        self.services
            .iter()
            .filter(move |view| view.live && view.event.service == service)
    }

    /// The live halted processes: the roots of every pause.
    #[must_use]
    pub fn roots(&self) -> Vec<HaltRef> {
        self.services
            .iter()
            .filter(|view| view.live)
            .filter_map(|view| view.event.halt_ref())
            .collect()
    }

    /// A live process of `service` halted on `cause`.
    #[must_use]
    pub fn halted_on(&self, service: &str, cause: HaltCause) -> Option<HaltRef> {
        self.live_of(service)
            .filter_map(|view| view.event.halt_ref())
            .find(|root| root.cause == cause)
    }

    /// A live halted process of `service`, any cause.
    #[must_use]
    pub fn any_halted(&self, service: &str) -> Option<HaltRef> {
        self.live_of(service)
            .find_map(|view| view.event.halt_ref())
    }

    /// The first root when every live process of `service` is halted,
    /// and at least one is live. `None` while one of them serves.
    #[must_use]
    pub fn all_halted(&self, service: &str) -> Option<HaltRef> {
        let mut live = self.live_of(service).peekable();
        live.peek()?;
        live.map(|view| view.event.halt_ref())
            .collect::<Option<Vec<_>>>()
            .and_then(|roots| roots.into_iter().next())
    }

    /// Every process as a JSON array.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        self.services.iter().map(ServiceView::to_json).collect()
    }
}

/// The subscriber side: a task that owns the board, reads records from a
/// channel, and publishes the view on every record and every second.
pub struct BoardFeed {
    board: Board,
    rx: mpsc::Receiver<ServiceEvent>,
    view: watch::Sender<BoardView>,
}

/// The depth of the record channel in front of the board.
const FEED_DEPTH: usize = 1024;

impl BoardFeed {
    /// A feed, the sender its transport writes the records to, and the
    /// receiver of the view.
    #[must_use]
    pub fn new() -> (Self, mpsc::Sender<ServiceEvent>, watch::Receiver<BoardView>) {
        let (tx, rx) = mpsc::channel(FEED_DEPTH);
        let (view, view_rx) = watch::channel(BoardView::default());
        (
            Self {
                board: Board::default(),
                rx,
                view,
            },
            tx,
            view_rx,
        )
    }

    /// Run until the record channel closes.
    pub async fn run(mut self) {
        while self.step().await.is_continue() {}
    }

    /// Take one record or one refresh, then publish the view.
    async fn step(&mut self) -> ControlFlow<()> {
        tokio::select! {
            event = self.rx.recv() => match event {
                Some(event) => self.board.observe(event, Instant::now()),
                None => return ControlFlow::Break(()),
            },
            () = tokio::time::sleep(REFRESH) => {}
        }
        let now = Instant::now();
        self.board.forget(now);
        self.view.send_replace(self.board.view(now));
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
