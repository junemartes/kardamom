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
//! More than one [`Publisher`] can write one `(service, instance)`: each
//! ingress publishes the sealer as it observes it, with its own `seq`. So
//! the board keeps the last `seq` per publisher. [`Board::observe`] states
//! the restart rule.
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

pub use kardamom_types::cluster_status::{SEALER_SILENCE, SNAPSHOT_SILENCE};

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

/// The transport's id of the publication that carries a record. A
/// restarted process opens a new publication, so it has a new id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Publisher(pub i32);

impl Publisher {
    const FIRST: Self = Self(i32::MIN);
    const LAST: Self = Self(i32::MAX);
}

/// One record and the publisher that sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub publisher: Publisher,
    pub event: ServiceEvent,
}

#[cfg(any(test, feature = "test-support"))]
impl Delivery {
    /// `event` from one fixed publisher, for a test with one publisher
    /// per `(service, instance)`.
    #[must_use]
    pub fn of_one_publisher(event: ServiceEvent) -> Self {
        Self {
            publisher: Publisher(1),
            event,
        }
    }
}

/// One value as the board last saw it.
#[derive(Debug, Clone)]
struct Seen<T> {
    value: T,
    at: Instant,
}

impl<T> Seen<T> {
    fn new(value: T, at: Instant) -> Self {
        Self { value, at }
    }

    /// Whether the board still keeps the value at `now`.
    fn kept(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.at) < FORGET
    }
}

/// The latest record of every `(service, instance)`, and the last `seq`
/// of every publisher of it.
#[derive(Debug, Default)]
pub struct Board {
    entries: BTreeMap<(String, String), Seen<ServiceEvent>>,
    seqs: BTreeMap<(String, String, Publisher), Seen<u64>>,
}

impl Board {
    /// Take the delivered record as the latest state of its process, and
    /// log a restart.
    ///
    /// A record shows a restart in two cases:
    ///
    /// - Its `seq` is lower than the last `seq` of the same publisher.
    /// - Its publisher is new, the key had exactly one publisher within
    ///   [`FORGET`], and its `seq` is lower than the last `seq` of that
    ///   publisher. A restarted process opens a new publication, so a
    ///   self-reporting service restarts as a new publisher of its key.
    ///
    /// A key with two live publishers, such as `sealer/cluster` from two
    /// ingresses, logs no restart for a new publisher. So a restarted
    /// ingress logs no sealer restart, and two `seq` counters that drift
    /// apart log nothing. The limit: a service that restarts twice within
    /// [`FORGET`] logs only the first restart, because the old publisher
    /// still counts. An ingress that joins after the other ingress ran
    /// alone for [`FORGET`] logs one sealer restart.
    pub fn observe(&mut self, delivery: Delivery, now: Instant) {
        let Delivery { publisher, event } = delivery;
        if self.restarted(publisher, &event, now) {
            tracing::info!(
                service = %event.service,
                instance = %event.instance,
                "service events: the process restarted"
            );
        }
        self.seqs.insert(
            (event.service.clone(), event.instance.clone(), publisher),
            Seen::new(event.seq, now),
        );
        self.entries.insert(
            (event.service.clone(), event.instance.clone()),
            Seen::new(event, now),
        );
    }

    /// Whether `event` from `publisher` shows a restart. See
    /// [`Self::observe`] for the rule.
    fn restarted(&self, publisher: Publisher, event: &ServiceEvent, now: Instant) -> bool {
        self.seqs
            .get(&(event.service.clone(), event.instance.clone(), publisher))
            .or_else(|| self.only_publisher(event, now))
            .is_some_and(|last| event.seq < last.value)
    }

    /// The last `seq` of the one publisher of `event`'s key within
    /// [`FORGET`]. `None` when the key has no publisher or more than one.
    fn only_publisher(&self, event: &ServiceEvent, now: Instant) -> Option<&Seen<u64>> {
        let first = (
            event.service.clone(),
            event.instance.clone(),
            Publisher::FIRST,
        );
        let last = (
            event.service.clone(),
            event.instance.clone(),
            Publisher::LAST,
        );
        let mut kept = self
            .seqs
            .range(first..=last)
            .map(|(_, seen)| seen)
            .filter(|seen| seen.kept(now));
        match (kept.next(), kept.next()) {
            (Some(only), None) => Some(only),
            _ => None,
        }
    }

    /// Drop the records gone for longer than the board keeps them.
    pub fn forget(&mut self, now: Instant) {
        self.entries.retain(|_, seen| seen.kept(now));
        self.seqs.retain(|_, seen| seen.kept(now));
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
                        event: seen.value.clone(),
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
        self.live_of(service).find_map(|view| view.event.halt_ref())
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
    rx: mpsc::Receiver<Delivery>,
    view: watch::Sender<BoardView>,
}

/// The depth of the record channel in front of the board.
const FEED_DEPTH: usize = 1024;

impl BoardFeed {
    /// A feed, the sender its transport writes the records to, and the
    /// receiver of the view.
    #[must_use]
    pub fn new() -> (Self, mpsc::Sender<Delivery>, watch::Receiver<BoardView>) {
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
            delivery = self.rx.recv() => match delivery {
                Some(delivery) => self.board.observe(delivery, Instant::now()),
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
