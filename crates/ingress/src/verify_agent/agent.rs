//! The verify agent: its input ring, its loop, and the handle that owns
//! its thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_primitives::{Address, Bytes as AlloyBytes};
use crossbeam_queue::ArrayQueue;
use kardamom_cache::LiveAccounts;
use kardamom_cache::metrics as cache_metrics;
use kardamom_log::parked::{OFFER_DEADLINE, Offered, Parked, ParkedQueue};
use kardamom_obs::pipeline::{LoopMeter, LoopState, Queue, Step};
use kardamom_types::TxEnvelope;
use metrics::Counter;
use tokio::sync::oneshot;

use super::doorbell::Doorbell;
use super::lane::{Admitted, AgentLanes, Answer, LaneFrame, LaneOffer};
use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::metrics::TX_REJECTED_TOTAL;
use crate::proxy::CheckProtocolLimits;
use crate::sig_verify::recover_single;

/// The most submits that one pass takes from the ring. The pass then
/// retries its parked frames, so a burst cannot hold a refused lane back
/// for longer than one batch.
const BATCH: usize = 64;

/// One submit, as the handler gives it to the agent: the raw bytes, the
/// decoded envelope, and the values that the handler fixes before the
/// push.
pub struct VerifyItem {
    pub raw_tx: AlloyBytes,
    pub envelope: ConsensusEnvelope,
    /// When the submit arrived at this ingress, for the end-to-end latency.
    pub arrived: Instant,
    pub correlation_id: u64,
    pub max_inclusion_block: u64,
    pub answer: oneshot::Sender<Answer>,
    /// When the handler pushed the item, for the step `ingress.verify_wait`.
    pub queued: Instant,
}

/// The ring of the agent had no room for a submit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RingFull;

/// The input of the agent: a bounded ring of submits, the doorbell of the
/// thread, and the stop flag of the owner.
pub(crate) struct AgentInput {
    ring: ArrayQueue<VerifyItem>,
    doorbell: Doorbell,
    stopping: AtomicBool,
}

impl AgentInput {
    fn new(capacity: usize) -> Self {
        Self {
            ring: ArrayQueue::new(capacity),
            doorbell: Doorbell::new(),
            stopping: AtomicBool::new(false),
        }
    }

    /// Give `item` to the agent. A full ring drops the item, and the
    /// handler answers at once.
    pub(crate) fn submit(&self, item: VerifyItem) -> Result<(), RingFull> {
        self.ring.push(item).map_err(|_| RingFull)?;
        self.doorbell.ring();
        Ok(())
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }
}

/// The pipeline metrics of the agent.
struct AgentMeters {
    /// The loop `ingress.verify_agent`: busy when a pass did work.
    main: LoopMeter,
    /// The step `ingress.recover`: the sender recovery of one submit.
    recover: Step,
    /// The step `ingress.verify_wait`: from the push to the take.
    wait: Step,
    /// The queue `ingress.verify_ring`: the submits that wait in the ring.
    ring: Queue,
    /// The refusals of the agent, as `kardamom_ingress_tx_rejected_total`
    /// with their reason. The agent takes the handles once, so each count
    /// costs no lookup, and reaches the recorder that was current at the
    /// start, also from the agent thread. The protocol limits count their
    /// own refusals, as on the tokio path.
    signature_invalid: Counter,
    nonce_too_low: Counter,
    txpool_full: Counter,
    internal: Counter,
}

impl AgentMeters {
    fn new() -> Self {
        let rejected =
            |reason: &'static str| metrics::counter!(TX_REJECTED_TOTAL, "reason" => reason);
        Self {
            main: LoopMeter::new("ingress.verify_agent"),
            recover: Step::new("ingress.recover"),
            wait: Step::new("ingress.verify_wait"),
            ring: Queue::new("ingress.verify_ring"),
            signature_invalid: rejected("signature-invalid"),
            nonce_too_low: rejected("nonce-too-low"),
            txpool_full: rejected("txpool-full"),
            internal: rejected("internal"),
        }
    }
}

/// The agent: it owns its lanes and its retry queue, and runs on one
/// thread. A test drives [`Self::poll`] with no thread.
pub(crate) struct VerifyAgent<L> {
    input: Arc<AgentInput>,
    lanes: AgentLanes<L>,
    parked: ParkedQueue<LaneFrame>,
    cfg: IngressConfig,
    live: Arc<LiveAccounts>,
    meters: AgentMeters,
}

impl<L: LaneOffer> VerifyAgent<L> {
    pub(crate) fn new(
        lanes: AgentLanes<L>,
        cfg: IngressConfig,
        live: Arc<LiveAccounts>,
        capacity: usize,
    ) -> Self {
        Self {
            input: Arc::new(AgentInput::new(capacity)),
            lanes,
            parked: ParkedQueue::new(),
            cfg,
            live,
            meters: AgentMeters::new(),
        }
    }

    pub(crate) fn input(&self) -> Arc<AgentInput> {
        self.input.clone()
    }

    /// One pass at `now`: retry the parked frames, then take at most
    /// [`BATCH`] submits from the ring. Returns the work done: the frames
    /// that left the retry queue and the submits taken.
    pub(crate) fn poll(&mut self, now: Instant) -> usize {
        let before = self.parked.len();
        let lanes = &self.lanes.lanes;
        self.parked.offer_each(now, |frame| frame.offer_on(lanes));
        let mut work = before - self.parked.len();
        while work < BATCH {
            let Some(item) = self.input.ring.pop() else {
                break;
            };
            self.admit(item, now);
            work += 1;
        }
        self.meters.ring.set(self.input.ring.len());
        work
    }

    /// Whether the agent holds a submit that has no answer yet.
    pub(crate) fn busy(&self) -> bool {
        !self.input.ring.is_empty() || !self.parked.is_empty()
    }

    /// Check one submit, then offer it, or answer its error.
    fn admit(&mut self, item: VerifyItem, now: Instant) {
        self.meters.wait.since(item.queued);
        match self.check(&item) {
            Ok(admitted) => self.publish(item, admitted, now),
            Err(error) => {
                let _ = item.answer.send(Err(error));
            }
        }
    }

    /// The protocol limits, the sender recovery and the nonce check, as the
    /// tokio path does them. Each refusal counts its own reason.
    fn check(&self, item: &VerifyItem) -> Result<Admitted, IngressError> {
        item.envelope.check_protocol_limits()?;
        let started = Instant::now();
        let recovered = recover_single(&item.envelope, &item.raw_tx);
        self.meters.recover.since(started);
        let (sender, tx_hash) =
            recovered.inspect_err(|_| self.meters.signature_invalid.increment(1))?;
        self.check_nonce(sender, item.envelope.nonce())?;
        Ok(Admitted { tx_hash, sender })
    }

    /// A nonce below the known nonce of `sender` is `nonce too low`. The
    /// agent reads the local account layer only: a sender with no local
    /// row admits, and the sequencer orders it or drops it.
    fn check_nonce(&self, sender: Address, nonce: u64) -> Result<(), IngressError> {
        if !self.cfg.admission_checks {
            return Ok(());
        }
        let Some(view) = self.live.get(sender) else {
            cache_metrics::record_lookup("live", "miss");
            return Ok(());
        };
        cache_metrics::record_lookup("live", "hit");
        if nonce >= view.nonce {
            return Ok(());
        }
        self.meters.nonce_too_low.increment(1);
        Err(IngressError::NonceTooLow {
            address: sender,
            tx_nonce: nonce,
            state_nonce: view.nonce,
        })
    }

    /// Encode the envelope, stamp its stages, and offer it on its lane.
    /// While a frame waits in the retry queue, a new frame waits behind
    /// it, so no lane reorders.
    fn publish(&mut self, item: VerifyItem, admitted: Admitted, now: Instant) {
        let envelope = TxEnvelope {
            correlation_id: item.correlation_id,
            raw_tx: item.raw_tx.0,
            sender: admitted.sender,
            tx_hash: admitted.tx_hash,
            max_inclusion_block: item.max_inclusion_block,
        };
        let bytes = match kardamom_log::codec::encode(&envelope) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.meters.internal.increment(1);
                let _ = item
                    .answer
                    .send(Err(IngressError::internal("encode tx_data", error)));
                return;
            }
        };
        if let Some(stamps) = &self.lanes.stamps {
            stamps.stamp(admitted.tx_hash, item.arrived);
        }
        let lane = self.cfg.lane_for(admitted.sender);
        let mut frame = LaneFrame::new(
            lane,
            bytes,
            now + OFFER_DEADLINE,
            admitted,
            item.answer,
            self.meters.txpool_full.clone(),
        );
        if !self.parked.is_empty() {
            self.parked.push(frame);
            return;
        }
        match frame.offer_on(&self.lanes.lanes) {
            Offered::Refused(_) => self.parked.push(frame),
            Offered::Delivered(position) => frame.delivered(position),
            Offered::UnknownPublication => frame.failed(format!("no tx_data lane {lane}")),
        }
    }

    /// The loop of the agent thread. It ends when the owner stops it and
    /// every submit has its answer.
    fn run(mut self) {
        self.input.doorbell.attach_current();
        loop {
            let worked = self.poll(Instant::now()) > 0;
            if self.input.stopping() && !self.busy() {
                return;
            }
            let state = if worked {
                LoopState::Busy
            } else {
                self.idle();
                LoopState::Idle
            };
            self.meters.main.mark(state);
        }
    }

    /// Park until a submit or a stop arrives, or for at most the idle cap,
    /// so a parked frame gets its next offer in time.
    fn idle(&self) {
        let input = &self.input;
        input.doorbell.park(self.lanes.idle_cap, || {
            !input.ring.is_empty() || input.stopping()
        });
    }
}

/// Ends the process when the agent thread unwinds from a panic. Each
/// submit waits for an answer that only the agent gives, so a process with
/// a dead agent is stuck, and the restart by the supervisor is the one
/// recovery. `abort` runs no destructor, as the fail-fast handler of the
/// Aeron clients runs none.
struct AbortOnPanic;

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!("verify agent: the thread panicked; aborting the process");
            std::process::abort();
        }
    }
}

/// The owner of the agent thread. The drop stops the agent: it answers
/// each submit that it holds, then ends, and the drop joins it. The lanes
/// end with the agent, so each lent publication goes back to the Aeron
/// thread.
pub(crate) struct VerifyAgentHandle {
    input: Arc<AgentInput>,
    thread: Option<JoinHandle<()>>,
}

impl VerifyAgentHandle {
    /// Start `agent` on a thread of its own, named `verify-agent`.
    ///
    /// # Errors
    ///
    /// Returns an error when the thread does not start.
    pub(crate) fn spawn<L: LaneOffer>(agent: VerifyAgent<L>) -> std::io::Result<Self> {
        let input = agent.input();
        let thread = std::thread::Builder::new()
            .name("verify-agent".to_string())
            .spawn(move || {
                let _abort = AbortOnPanic;
                agent.run();
            })?;
        Ok(Self {
            input,
            thread: Some(thread),
        })
    }

    /// Give `item` to the agent. See [`AgentInput::submit`].
    pub(crate) fn submit(&self, item: VerifyItem) -> Result<(), RingFull> {
        self.input.submit(item)
    }
}

impl Drop for VerifyAgentHandle {
    fn drop(&mut self) {
        self.input.stopping.store(true, Ordering::Release);
        self.input.doorbell.ring();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
