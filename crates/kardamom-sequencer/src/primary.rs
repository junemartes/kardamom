//! Primary sequencer event step + loop.
//!
//! [`PrimarySequencer::run_once`] polls the ingress source for at most one
//! message, drives the state machine, and performs the **dual-write**
//! defined by the D-Sh12 split architecture (spec §2.3):
//!
//!  1. Full `TxEnvelope` bytes → the sequencer's own per-A
//!     [`ChannelAPublisher`] (exclusive, ~memcpy speed). Returns the
//!     fragment's `BPosition` on the A-stream.
//!  2. Tiny [`kardamom_types::TxRef`]`{ sequencer_id, position_a }` →
//!     the canonical orderer [`ChannelBRefPublisher`] (concurrent
//!     multi-publisher).
//!
//! State (pending-nonce map, drain buffer) is advanced **only after both
//! publications succeed**. If *either* fails — e.g. an A or B back-pressure
//! signal — the state machine is rewound via
//! [`PartitionState::reinsert_for_retry`] and the error bubbles up so the
//! loop can apply its retry policy. On a B-backpressure retry, the
//! previously-A-published envelope becomes an orphan on the A-stream (no
//! ref on B points at it). Downstream consumers must tolerate
//! unreferenced A entries; this is the agreed cost of dual writes in the
//! split architecture (the canonical layer's exactly-once guarantee is on
//! channel B, by `TxRef` arrival).
//!
//! See also [`crate::outbound`] for the trait surface and in-memory fakes
//! used by tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_rlp::Decodable;
use kardamom_log::codec;
use kardamom_types::{TxEnvelope, TxRef};
use tracing::{trace, warn};

use crate::config::SequencerConfig;
use crate::duplicate::DuplicateNotification;
use crate::error::SequencerError;
use crate::inbound::IngressSource;
use crate::metrics;
use crate::outbound::{ChannelAPublisher, ChannelBRefPublisher, ReceiptCachePublisher};
use crate::partition::partition_for;
use crate::sender::sender_of;
use crate::state::{NonceOutcome, PartitionState, ProcessAction};

/// A canonical-ordered payload ready to dual-write (A + B) for a sender.
///
/// Stored inside `PartitionState<EncodedFrame>` so that
/// `reinsert_for_retry` puts a complete, ready-to-resend record back in the
/// pending buffer (instead of forcing a re-decode + re-encode on the retry
/// path). The B-side ref is constructed *after* the A publish succeeds
/// because the ref's `position_a` is the A publisher's return value.
#[derive(Debug, Clone)]
struct EncodedFrame {
    correlation_id: u64,
    bytes: Vec<u8>,
}

/// Encode a `TxEnvelope` for channel A using `kardamom_log::codec::encode`
/// (rkyv archival). The same encoding is consumed by downstream
/// `channel_a_subscriber()` calls via `codec::access` / `codec::materialize`.
fn encode_envelope_for_a(env: &TxEnvelope) -> Result<Vec<u8>, SequencerError> {
    codec::encode(env)
        .map(|av| av.into_vec())
        .map_err(SequencerError::from)
}

/// Cooperative shutdown signal shared with the loop driver. Cloneable so the
/// signal handler thread can keep one copy and the loop thread another.
#[derive(Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn from_atomic(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn signal(&self) {
        self.flag.store(true, Ordering::Release);
    }
    pub fn is_signaled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
    pub fn atomic(&self) -> Arc<AtomicBool> {
        self.flag.clone()
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PrimarySequencer {
    cfg: SequencerConfig,
    state: PartitionState<EncodedFrame>,
}

impl PrimarySequencer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self {
            cfg,
            state: PartitionState::new(cap),
        }
    }

    /// Construct a primary that inherits the next-nonce map from a hot-standby
    /// tailer's `into_state()`. Per spec §4.2 the pending future-nonce buffers
    /// are *not* carried over (those entries did not land on B and the new
    /// primary will see them again via fresh ingress messages).
    pub fn with_state(cfg: SequencerConfig, inherited: PartitionState<()>) -> Self {
        cfg.validate().expect("validated config");
        let mut state = PartitionState::new(cfg.max_pending_per_sender);
        for (addr, n) in inherited.iter_next_nonces() {
            state.seed_next_nonce(addr, n);
        }
        Self { cfg, state }
    }

    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    /// Dual-write helper: publish `payload.bytes` to channel A, then
    /// build a `TxRef` from the returned `BPosition` and publish to
    /// channel B. Returns `Ok(())` only if **both** writes succeeded.
    ///
    /// On A backpressure: nothing landed; the caller reinserts the payload
    /// at this nonce.
    ///
    /// On B backpressure: the envelope is already on the A archive but no
    /// `TxRef` ever points to it; this is an acceptable orphan per the
    /// split-architecture contract. The caller still reinserts so the
    /// retry produces a *fresh* A entry and a (this time matching) B ref.
    fn dual_publish<A, B>(
        &self,
        a: &mut A,
        b: &mut B,
        payload: &EncodedFrame,
    ) -> Result<(), SequencerError>
    where
        A: ChannelAPublisher,
        B: ChannelBRefPublisher,
    {
        // 1. Channel A — full envelope bytes. Bubble back on backpressure.
        let position_a = match a.try_publish(&payload.bytes) {
            Ok(pos) => pos,
            Err(SequencerError::Backpressure) => {
                metrics::record_backpressure(self.cfg.partition_index);
                return Err(SequencerError::Backpressure);
            }
            Err(e) => return Err(e),
        };

        // 2. Channel B — tiny TxRef. If this back-pressures the A entry is
        //    an orphan (documented).
        let txref = TxRef::new(self.cfg.sequencer_id, position_a);
        match b.try_publish_ref(&txref) {
            Ok(()) => {
                metrics::record_publish(self.cfg.partition_index);
                Ok(())
            }
            Err(SequencerError::Backpressure) => {
                metrics::record_backpressure(self.cfg.partition_index);
                warn!(
                    sequencer_id = self.cfg.sequencer_id,
                    correlation_id = payload.correlation_id,
                    ?position_a,
                    "B back-pressure after A publish; A entry will be orphaned on retry"
                );
                Err(SequencerError::Backpressure)
            }
            Err(e) => Err(e),
        }
    }

    /// Drive one ingress message through the state machine. Returns
    /// `Ok(true)` if work was done, `Ok(false)` if both the retry-drain and
    /// the ingress poll were empty.
    ///
    /// Order of operations:
    ///  1. First flush any payloads sitting at `pending[next_nonce]` (these
    ///     are the rebuffered-after-backpressure entries). If either
    ///     dual-write side blocks again, we re-rewind and return
    ///     `Backpressure` without touching ingress.
    ///  2. Then poll ingress for the next inbound envelope and process it.
    pub fn run_once<I, A, B, R>(
        &mut self,
        ingress: &mut I,
        a: &mut A,
        b: &mut B,
        rc: &mut R,
    ) -> Result<bool, SequencerError>
    where
        I: IngressSource,
        A: ChannelAPublisher,
        B: ChannelBRefPublisher,
        R: ReceiptCachePublisher,
    {
        let pending = self.state.drain_pending();
        if !pending.is_empty() {
            for (sender, n, payload) in pending {
                if let Err(SequencerError::Backpressure) = self.dual_publish(a, b, &payload) {
                    self.state.reinsert_for_retry(sender, n, payload);
                    return Err(SequencerError::Backpressure);
                }
                trace!(
                    nonce = n,
                    correlation_id = payload.correlation_id,
                    "dual-published (drain-pending)"
                );
            }
            return Ok(true);
        }

        let Some(envelope) = ingress.poll()? else {
            return Ok(false);
        };
        metrics::record_ingest(self.cfg.partition_index);

        let sender = sender_of(&envelope);

        // Defensive: drop messages routed to the wrong partition. Routing
        // disagreement between proxy and sequencer would otherwise silently
        // corrupt nonce state.
        let part = partition_for(sender, self.cfg.partition_count);
        if part != self.cfg.partition_index {
            warn!(
                expected = self.cfg.partition_index,
                got = part,
                "ingress message for wrong partition; skipping"
            );
            return Ok(true);
        }

        // Decode the alloy `TxEnvelope` out of `raw_tx` to extract `nonce`.
        // The decode is the only per-tx work the sequencer does beyond the
        // state-machine arithmetic; the result is discarded after the nonce
        // is read. We never call `recover_signer()` on it (D-Sh3).
        let nonce = decode_nonce(&envelope.raw_tx)?;

        let bytes = encode_envelope_for_a(&envelope)?;
        let frame = EncodedFrame {
            correlation_id: envelope.correlation_id,
            bytes,
        };

        let t0 = std::time::Instant::now();
        let result = self.state.process(sender, nonce, frame);
        let elapsed_us = t0.elapsed().as_micros() as f64;
        metrics::record_nonce_check_latency(self.cfg.partition_index, elapsed_us);

        match result.outcome {
            NonceOutcome::Matched => {}
            NonceOutcome::Buffered | NonceOutcome::BufferedReplaced => {
                metrics::record_buffered_future(self.cfg.partition_index);
            }
            NonceOutcome::BufferedEvicting { .. } => {
                metrics::record_buffered_future(self.cfg.partition_index);
                metrics::record_eviction(self.cfg.partition_index);
            }
            NonceOutcome::BufferedDisabled => {
                metrics::record_buffered_future(self.cfg.partition_index);
            }
            NonceOutcome::Past => metrics::record_past(self.cfg.partition_index),
        }

        for action in result.actions {
            match action {
                ProcessAction::Publish { nonce: n, payload } => {
                    match self.dual_publish(a, b, &payload) {
                        Ok(()) => {
                            trace!(
                                nonce = n,
                                correlation_id = payload.correlation_id,
                                "dual-published"
                            );
                        }
                        Err(SequencerError::Backpressure) => {
                            // Roll back: the state machine had advanced for
                            // this tx but the canonical ref never landed on
                            // B. The reinsert also re-buffers the payload
                            // so the retry replays it verbatim.
                            self.state.reinsert_for_retry(sender, n, payload);
                            return Err(SequencerError::Backpressure);
                        }
                        Err(e) => return Err(e),
                    }
                }
                ProcessAction::ReportDuplicate { nonce: n } => {
                    rc.publish_duplicate(DuplicateNotification {
                        correlation_id: envelope.correlation_id,
                        sender,
                        nonce: n,
                    });
                }
            }
        }
        Ok(true)
    }

    /// Pin this thread to the configured core (if any) and loop until
    /// shutdown.
    pub fn run<I, A, B, R>(
        &mut self,
        ingress: &mut I,
        a: &mut A,
        b: &mut B,
        rc: &mut R,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError>
    where
        I: IngressSource,
        A: ChannelAPublisher,
        B: ChannelBRefPublisher,
        R: ReceiptCachePublisher,
    {
        if let Some(core) = self.cfg.core_id {
            let id = core_affinity::CoreId { id: core };
            if !core_affinity::set_for_current(id) {
                tracing::warn!(core, "failed to pin sequencer thread to core");
            }
        }
        let mut backoff_us = 1u64;
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            match self.run_once(ingress, a, b, rc) {
                Ok(true) => backoff_us = 1,
                Ok(false) => {
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = backoff_us.saturating_mul(2).min(100);
                }
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}

/// Decode the nonce out of an RLP-encoded alloy `TxEnvelope`. The proxy
/// already verified the envelope is well-formed, but we re-decode here so
/// the sequencer doesn't need to be passed the nonce as a side channel.
fn decode_nonce(raw_tx: &bytes::Bytes) -> Result<u64, SequencerError> {
    let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref())
        .map_err(|e| SequencerError::MalformedFrame(format!("decode envelope: {e}")))?;
    Ok(env.nonce())
}
