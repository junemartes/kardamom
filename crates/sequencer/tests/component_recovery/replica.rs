//! One production admission loop on its own thread, with the ports that a
//! test freezes, cuts or stops.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use alloy_primitives::Address;
use crossbeam_channel::{Receiver, Sender};
use kardamom_cluster_adapter::gateway::fakes::FakeIngress;
use kardamom_cluster_adapter::gateway::{ClusterIngress, OfferOutcome};
use kardamom_log::aeron_live::IdleCap;
use kardamom_sequencer::SequencerError;
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::outbound::cluster::ClusterRefPublisher;
use kardamom_sequencer::outbound::fakes::InMemorySidePublisher;
use kardamom_sequencer::sealer_rules::{SealerRules, SealerVersion};
use kardamom_sequencer::sequencer::{Ports, Shutdown};
use kardamom_sequencer::testkit::one_partition_cfg;
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, AdmissionSettings};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// The bound of every wait. A recovery past it fails the test.
pub(super) const BOUND: Duration = Duration::from_secs(10);
/// The production queue of account events of one replica.
const ACCOUNT_EVENTS: usize = 65_536;
/// Every metric label of a replica start is new, as a process start is.
static STARTS: AtomicU32 = AtomicU32::new(100);

/// The `tx_data` image of a replica. A frozen replica blocks in its poll,
/// so its whole thread stops, as SIGSTOP stops a process. A cut replica
/// reads nothing more.
struct Image {
    rx: Receiver<Inbound>,
    gates: Gates,
}

#[derive(Clone, Default)]
struct Gates {
    frozen: Arc<AtomicBool>,
    cut: Arc<AtomicBool>,
}

impl Gates {
    fn wait_while_frozen(&self) {
        std::iter::repeat_with(|| self.frozen.load(Ordering::Acquire))
            .take_while(|frozen| *frozen)
            .for_each(|_| std::thread::sleep(Duration::from_millis(1)));
    }
}

impl TxDataSubscriber for Image {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        self.gates.wait_while_frozen();
        if self.gates.cut.load(Ordering::Acquire) {
            return Ok(None);
        }
        Ok(self.rx.try_recv().ok())
    }

    fn queued(&self) -> usize {
        self.rx.len()
    }
}

/// The cluster session of a replica. A cut session takes no offer.
#[derive(Clone)]
struct Session {
    pub(super) peer: FakeIngress,
    gates: Gates,
}

impl ClusterIngress for Session {
    fn offer(&mut self, payload: &[u8]) -> OfferOutcome {
        if self.gates.cut.load(Ordering::Acquire) {
            return OfferOutcome::NotConnected;
        }
        self.peer.offer(payload)
    }
}

/// How a replica learns a sender that it holds no state of.
pub(super) enum Lookups {
    /// The controlled executor answers each request with its committed
    /// state.
    Answered,
    /// The test owns the request queue.
    Owned,
}

/// One production admission loop on its own thread.
pub(super) struct Replica {
    pub(super) input: Sender<Inbound>,
    gates: Gates,
    pub(super) peer: FakeIngress,
    /// Frames of `peer` that the sealer has read.
    pub(super) read: usize,
    pub(super) accounts: Sender<AccountEvent>,
    pub(super) rules: SealerRules<InMemorySidePublisher>,
    pub(super) lookups: Option<tokio::sync::mpsc::Receiver<Address>>,
    pub(super) mode: Lookups,
    /// Canonical frames that this session's rules took.
    pub(super) relayed: usize,
    pub(super) label: u32,
    shutdown: Shutdown,
    done: Receiver<Result<(), SequencerError>>,
    worker: Option<JoinHandle<()>>,
}

impl Replica {
    /// The process recorder: replicas tell their counters apart by their
    /// partition label.
    pub(super) fn recorder() -> &'static PrometheusHandle {
        static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
        HANDLE.get_or_init(|| {
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::set_global_recorder(recorder).expect("one recorder per test process");
            handle
        })
    }

    pub(super) fn start(lane: u8, mode: Lookups) -> Self {
        Self::recorder();
        let label = STARTS.fetch_add(1, Ordering::Relaxed);
        let (input, rx) = crossbeam_channel::bounded(4_096);
        let gates = Gates::default();
        let peer = FakeIngress::new();
        let mut image = Image {
            rx,
            gates: gates.clone(),
        };
        let mut refs = ClusterRefPublisher::new(Session {
            peer: peer.clone(),
            gates: gates.clone(),
        });
        let side = InMemorySidePublisher::default();
        let mut output = side.clone();
        let (accounts, accounts_rx) = crossbeam_channel::bounded(ACCOUNT_EVENTS);
        let (lookups_tx, lookups) = tokio::sync::mpsc::channel(4_096);
        let cfg = AdmissionSettings::default().loop_config(Duration::from_secs(30), label);
        let mut actor = AdmissionLoop::new(
            cfg,
            one_partition_cfg().vslot_set().unwrap(),
            accounts_rx,
            lookups_tx,
        );
        let version = SealerVersion::starting_at(5);
        let mut rules =
            SealerRules::new(label, version.clone(), side.clone()).telling(accounts.clone());
        rules.on_session_start(i64::from(lane) + 1);
        actor.enable_positioned_refs(version);
        let shutdown = Shutdown::new();
        let signal = shutdown.clone();
        let (finished, done) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            let result = actor.run(
                &mut Ports {
                    tx_data: &mut image,
                    refs: &mut refs,
                    errors: &mut output,
                },
                &signal,
                IdleCap::default(),
            );
            let _ = finished.send(result);
        });
        Self {
            input,
            gates,
            peer,
            read: 0,
            accounts,
            rules,
            lookups: Some(lookups),
            mode,
            relayed: 0,
            label,
            shutdown,
            done,
            worker: Some(worker),
        }
    }

    pub(super) fn cut(&self) -> bool {
        self.gates.cut.load(Ordering::Acquire)
    }

    pub(super) fn set_frozen(&self, frozen: bool) {
        self.gates.frozen.store(frozen, Ordering::Release);
    }

    pub(super) fn offers(&self) -> usize {
        self.peer.accepted().len()
    }

    /// The lookup requests, for a test that runs its own lookup task.
    pub(super) fn take_lookups(&mut self) -> tokio::sync::mpsc::Receiver<Address> {
        self.lookups
            .take()
            .expect("the lookup queue is still owned")
    }

    /// The queue of account events of the loop.
    pub(super) fn account_events(&self) -> Sender<AccountEvent> {
        self.accounts.clone()
    }

    pub(super) fn relayed(&self) -> usize {
        self.relayed
    }

    /// The account events that wait for the loop: a bounded queue.
    pub(super) fn backlog(&self) -> usize {
        self.accounts.len()
    }

    /// The published counter of this replica's process.
    pub(super) fn published(&self) -> Option<u64> {
        let key = format!(
            "kardamom_sequencer_tx_published_to_b_total{{partition=\"{}\"}}",
            self.label
        );
        Self::recorder()
            .render()
            .lines()
            .find_map(|line| line.strip_prefix(&key)?.trim().parse().ok())
    }

    /// The exported `kardamom_sequencer_` samples of this process.
    pub(super) fn exported(&self) -> usize {
        let label = format!("partition=\"{}\"", self.label);
        Self::recorder()
            .render()
            .lines()
            .filter(|line| line.starts_with("kardamom_sequencer_") && line.contains(&label))
            .count()
    }

    fn join(&mut self) -> Result<(), SequencerError> {
        let result = self.done.recv_timeout(BOUND).expect("replica did not stop");
        self.worker.take().unwrap().join().unwrap();
        result
    }

    /// The graceful stop: signal the loop and join it.
    pub(super) fn stop(&mut self) {
        self.shutdown.signal();
        self.join().unwrap();
    }

    /// The hard stop: every port closes at once and nothing is flushed.
    /// The thread then ends, and its memory goes with it.
    pub(super) fn kill(&mut self) {
        self.gates.cut.store(true, Ordering::Release);
        self.set_frozen(false);
        self.shutdown.signal();
        self.join().unwrap();
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        self.gates.cut.store(true, Ordering::Release);
        self.set_frozen(false);
        self.shutdown.signal();
        if let Some(worker) = self.worker.take()
            && self.done.recv_timeout(BOUND).is_ok()
        {
            let _ = worker.join();
        }
    }
}
