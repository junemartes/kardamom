//! The production actor runs on its own thread; controlled neighbors own its ports.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use kardamom_cluster_adapter::gateway::fakes::FakeIngress;
use kardamom_cluster_adapter::gateway::{ClusterIngress, OfferOutcome};
use kardamom_cluster_adapter::wire::{self, GuardHeader, RelayedRef, TxRefOffer};
use kardamom_log::aeron_live::IdleCap;
use kardamom_sequencer::SequencerError;
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::outbound::cluster::ClusterRefPublisher;
use kardamom_sequencer::outbound::fakes::InMemorySidePublisher;
use kardamom_sequencer::sealer_rules::{SealerRules, SealerVersion};
use kardamom_sequencer::sequencer::{Ports, Shutdown};
use kardamom_sequencer::testkit::{EnvelopeSpec, envelope_with, one_partition_cfg, pos, signer};
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, AdmissionSettings};
use kardamom_types::{TxDataLoc, TxEnvelope, TxRef};

const DEADLINE: Duration = Duration::from_secs(3);

struct Input {
    rx: Receiver<Inbound>,
    stalled: Arc<AtomicBool>,
    consumed: Arc<AtomicUsize>,
}

impl TxDataSubscriber for Input {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        if self.stalled.load(Ordering::Acquire) {
            return Ok(None);
        }
        match self.rx.try_recv() {
            Ok(item) => {
                self.consumed.fetch_add(1, Ordering::Release);
                Ok(Some(item))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(SequencerError::IngressDisconnected),
        }
    }

    fn queued(&self) -> usize {
        self.rx.len()
    }
}

#[derive(Clone)]
struct ObservedIngress {
    peer: FakeIngress,
    attempts: Arc<AtomicUsize>,
}

impl ClusterIngress for ObservedIngress {
    fn offer(&mut self, payload: &[u8]) -> OfferOutcome {
        let result = self.peer.offer(payload);
        self.attempts.fetch_add(1, Ordering::Release);
        result
    }
}

pub(super) struct Actor {
    input: Option<Sender<Inbound>>,
    stalled: Arc<AtomicBool>,
    consumed: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
    pub(super) peer: FakeIngress,
    pub(super) accounts: Sender<AccountEvent>,
    pub(super) lookups: tokio::sync::mpsc::Receiver<Address>,
    pub(super) rules: SealerRules<InMemorySidePublisher>,
    side: InMemorySidePublisher,
    shutdown: Shutdown,
    done: Receiver<Result<(), SequencerError>>,
    worker: Option<JoinHandle<()>>,
}

impl Actor {
    pub(super) fn start() -> Self {
        let (input, rx) = crossbeam_channel::bounded(8);
        let stalled = Arc::new(AtomicBool::new(false));
        let consumed = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let peer = FakeIngress::new();
        let mut inbound = Input {
            rx,
            stalled: stalled.clone(),
            consumed: consumed.clone(),
        };
        let mut refs = ClusterRefPublisher::new(ObservedIngress {
            peer: peer.clone(),
            attempts: attempts.clone(),
        });
        let side = InMemorySidePublisher::default();
        let mut output = side.clone();
        let (accounts, accounts_rx) = crossbeam_channel::bounded(16);
        let (lookups_tx, lookups) = tokio::sync::mpsc::channel(8);
        let cfg = AdmissionSettings {
            outbox_cap: NonZeroUsize::new(2).unwrap(),
            ..AdmissionSettings::default()
        }
        .loop_config(Duration::from_secs(30), 0);
        let mut actor = AdmissionLoop::new(
            cfg,
            one_partition_cfg().vslot_set().unwrap(),
            accounts_rx,
            lookups_tx,
        );
        let version = SealerVersion::starting_at(5);
        let mut rules =
            SealerRules::new(0, version.clone(), side.clone()).telling(accounts.clone());
        rules.on_session_start(1);
        actor.enable_positioned_refs(version);
        let shutdown = Shutdown::new();
        let signal = shutdown.clone();
        let (finished, done) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            let result = actor.run(
                &mut Ports {
                    tx_data: &mut inbound,
                    refs: &mut refs,
                    errors: &mut output,
                },
                &signal,
                IdleCap::default(),
            );
            let _ = finished.send(result);
        });
        Self {
            input: Some(input),
            stalled,
            consumed,
            attempts,
            peer,
            accounts,
            lookups,
            rules,
            side,
            shutdown,
            done,
            worker: Some(worker),
        }
    }

    pub(super) fn envelope(nonce: u64) -> TxEnvelope {
        envelope_with(
            &signer(11),
            nonce,
            nonce,
            EnvelopeSpec {
                real_hash: true,
                ..EnvelopeSpec::default()
            },
        )
    }

    pub(super) fn seed(&self) {
        self.accounts
            .send(AccountEvent::Answer {
                address: signer(11).address(),
                state: AccountState {
                    nonce: 0,
                    balance: U256::MAX,
                    delegated: false,
                    tx_idx: 1,
                },
            })
            .unwrap();
    }

    pub(super) fn send(&self, nonce: u64) {
        let loc = TxDataLoc::new(7, pos(i32::try_from((nonce + 1) * 64).unwrap()));
        self.input
            .as_ref()
            .unwrap()
            .send_timeout(
                Inbound {
                    lane: 0,
                    loc,
                    stream_position: loc.position.as_index(),
                    envelope: Self::envelope(nonce),
                },
                DEADLINE,
            )
            .unwrap();
    }

    pub(super) fn input_stalled(&self, stalled: bool) {
        self.stalled.store(stalled, Ordering::Release);
    }

    pub(super) fn consumed(&self) -> usize {
        self.consumed.load(Ordering::Acquire)
    }

    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    pub(super) fn wait_for(check: impl FnMut() -> bool) {
        let end = Instant::now() + DEADLINE;
        assert!(
            Self::poll_until(end, check),
            "component recovery deadline exceeded"
        );
    }

    fn poll_until(end: Instant, mut check: impl FnMut() -> bool) -> bool {
        std::iter::repeat_with(Instant::now)
            .take_while(|now| *now < end)
            .any(|_| {
                let ready = check();
                if !ready {
                    std::thread::sleep(Duration::from_millis(1));
                }
                ready
            })
    }

    pub(super) fn expect_offer(&self, index: usize, nonce: u64) -> Vec<u8> {
        Self::wait_for(|| self.peer.accepted().len() > index);
        let frame = self.peer.accepted()[index].clone();
        let envelope = Self::envelope(nonce);
        let position = pos(i32::try_from((nonce + 1) * 64).unwrap());
        let bytes = kardamom_log::codec::encode(&envelope).unwrap();
        let expected = TxRefOffer {
            guard: GuardHeader {
                sender: envelope.sender,
                nonce,
                max_inclusion_block: wire::POSITION_TAG | position.as_index(),
                tip: 0,
            },
            reference: TxRef::new(envelope.tx_hash, 0, position, 7),
            envelope: &bytes,
        }
        .encode();
        assert_eq!(frame, expected);
        frame
    }

    pub(super) fn canonical(&self, nonce: u64) {
        let frame = RelayedRef {
            index: nonce,
            canonical_id: Self::envelope(nonce).tx_hash,
            record_type: wire::RT_TX,
        }
        .encode();
        assert!(self.rules.on_canonical_frame(&frame));
    }

    pub(super) fn quiet(&self, offers: usize) {
        let end = Instant::now() + Duration::from_millis(30);
        assert!(!Self::poll_until(end, || self.peer.accepted().len() != offers));
        assert!(
            !self.worker.as_ref().unwrap().is_finished(),
            "actor exited during fault"
        );
    }

    pub(super) fn close_input(&mut self) {
        self.input = None;
    }

    pub(super) fn finish(&mut self) -> Result<(), SequencerError> {
        let result = self
            .done
            .recv_timeout(DEADLINE)
            .expect("actor did not stop");
        self.worker.take().unwrap().join().unwrap();
        result
    }

    pub(super) fn stop(&mut self) {
        self.shutdown.signal();
        self.finish().unwrap();
        assert!(self.side.errors.lock().unwrap().is_empty());
        assert!(self.side.drops.lock().unwrap().is_empty());
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.shutdown.signal();
        if let Some(worker) = self.worker.take() {
            // A failed assertion must not leave a polling actor behind or wait without a bound.
            if self.done.recv_timeout(DEADLINE).is_ok() {
                let _ = worker.join();
            }
        }
    }
}
