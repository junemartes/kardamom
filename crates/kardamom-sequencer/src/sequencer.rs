//! Sequencer event step + loop.
//!
//! [`Sequencer::run_once`] polls the shard's tx_data subscription for at
//! most one fragment and republishes a canonical-order `TxRef` onto
//! tx_ordering.
//!
//! Per the MDS topology (D-Sh12 v2 / spec §2.3): the proxy has already
//! published the envelope onto tx_data, so the sequencer's input is
//! `(tx_data_position, envelope)` — the proxy's Aeron-offer position is the
//! lookup key downstream consumers (executor, batcher) use to resolve the
//! envelope.
//!
//! For each observed envelope:
//!
//!  1. Decode the nonce out of the envelope (the proxy already verified
//!     the signature and populated `sender` + `tx_hash`).
//!  2. Hydrate `next_nonce` from the state DB if this is the first time
//!     we've seen the sender (cache-miss path).
//!  3. Feed `(sender, nonce, RefMetadata)` to [`PartitionState::process`]:
//!     - match → emit a publish action for this nonce + drain any
//!       buffered higher nonces that newly become contiguous;
//!     - future → buffer (bounded per-sender);
//!     - past → emit a `DuplicateNotification`.
//!  4. For each `Publish` action, build
//!     `TxRef { tx_hash, shard_id, tx_data_position }` and publish to tx_ordering.
//!     If B back-pressures, [`PartitionState::reinsert_for_retry`] rewinds
//!     so the next loop iteration retries the same `(sender, nonce)`.
//!
//! Warm cache: because every observed envelope advances `next_nonce` on a
//! match, the in-memory map is naturally populated by the tx_data read
//! stream itself — no separate prefetch thread needed. Cold senders (no
//! activity since startup) hit the state-DB cache-miss path the first
//! time they're observed.
//!
//! See also [`crate::outbound`] for the trait surface and in-memory fakes
//! used by tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_rlp::Decodable;
use kardamom_types::{BPosition, StateDatabase, TxRef};
use tracing::{trace, warn};

use crate::config::SequencerConfig;
use crate::duplicate::DuplicateNotification;
use crate::error::SequencerError;
use crate::inbound::TxDataSubscriber;
use crate::metrics;
use crate::outbound::{ReceiptCachePublisher, TxOrderingRefPublisher};
use crate::partition::partition_for;
use crate::sender::sender_of;
use crate::state::{NonceOutcome, PartitionState, ProcessAction};

/// Metadata needed to (re-)publish a `TxRef` for a buffered or just-arrived
/// envelope. Stored inside `PartitionState<RefMetadata>` so that
/// [`PartitionState::reinsert_for_retry`] puts a complete, ready-to-resend
/// record back in the pending buffer on B-backpressure rewind.
///
/// We no longer carry the envelope bytes — the proxy already wrote them
/// onto tx_data; the sequencer only republishes the ref.
#[derive(Debug, Clone)]
struct RefMetadata {
    correlation_id: u64,
    /// Carries through to `TxRef.tx_hash`.
    tx_hash: alloy_primitives::B256,
    /// The Aeron-offer position the proxy got back when it published this
    /// envelope onto tx_data. Used by downstream consumers to look up
    /// the envelope on the A archive.
    tx_data_position: BPosition,
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

pub struct Sequencer<DB: StateDatabase> {
    cfg: SequencerConfig,
    state: PartitionState<RefMetadata>,
    /// Canonical state source for cache-miss hydration. When a tx arrives
    /// for a sender this sequencer has never seen, we consult `state_db`
    /// once to seed the in-memory `next_nonce` map with the committed
    /// canonical nonce. Subsequent txs from that sender hit the in-memory
    /// path.
    state_db: Arc<DB>,
}

impl<DB: StateDatabase> Sequencer<DB> {
    pub fn new(cfg: SequencerConfig, state_db: Arc<DB>) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self {
            cfg,
            state: PartitionState::new(cap),
            state_db,
        }
    }

    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    /// Cache-miss hydration: if the sender's next-nonce is unknown locally,
    /// fetch the canonical nonce from the state DB and seed the partition
    /// state. New senders (no on-chain account) hydrate at nonce 0; senders
    /// that already have on-chain activity hydrate at their committed nonce
    /// so the executor accepts the next tx without a future-nonce rejection.
    fn hydrate_if_unknown(&mut self, sender: alloy_primitives::Address) {
        if self.state.next_nonce_known(sender).is_some() {
            return;
        }
        let n = match self.state_db.basic(sender) {
            Ok(Some((nonce, _, _))) => nonce,
            Ok(None) => 0,
            Err(e) => {
                // State DB query failed (transient I/O, etc.). Fall back to
                // nonce 0; if the tx is from an established sender it'll
                // get rejected by the executor and the client will retry.
                // Better than dropping or stalling on a soft failure here.
                warn!(
                    sender = ?sender,
                    error = %e,
                    "state DB cache-miss lookup failed; hydrating with nonce 0"
                );
                0
            }
        };
        self.state.seed_next_nonce(sender, n);
    }

    /// Publish a `TxRef` for `meta` to tx_ordering. The `tx_data_position` was
    /// observed off the tx_data subscription — the proxy did the actual
    /// envelope write — so this is a single B write, not a dual write.
    /// On B-backpressure the caller reinserts the metadata so the retry
    /// republishes the same `(tx_hash, shard, tx_data_position)` triple.
    fn publish_ref<B>(&self, b: &mut B, meta: &RefMetadata) -> Result<(), SequencerError>
    where
        B: TxOrderingRefPublisher,
    {
        // shard_id under the MDS topology is the address-shard index. The
        // default deployment of K=M, one shard per sequencer pool, lets
        // `cfg.sequencer_id` double as the shard id; production
        // configurations with multiple sequencer pools per shard can set
        // these independently and `sequencer_id` here MUST be the shard.
        let txref = TxRef::new(meta.tx_hash, self.cfg.sequencer_id, meta.tx_data_position);
        match b.try_publish_ref(&txref) {
            Ok(()) => {
                metrics::record_publish(self.cfg.partition_index);
                Ok(())
            }
            Err(SequencerError::Backpressure) => {
                metrics::record_backpressure(self.cfg.partition_index);
                warn!(
                    sequencer_id = self.cfg.sequencer_id,
                    correlation_id = meta.correlation_id,
                    tx_data_position = ?meta.tx_data_position,
                    "B back-pressure; rewinding state machine for retry"
                );
                Err(SequencerError::Backpressure)
            }
            Err(e) => Err(e),
        }
    }

    /// Drive one ingress message through the state machine. Returns
    /// `Ok(true)` if work was done, `Ok(false)` if both the retry-drain and
    /// the tx_data poll were empty.
    ///
    /// Order of operations:
    ///  1. First flush any metadata sitting at `pending[next_nonce]` (these
    ///     are the rebuffered-after-backpressure entries). If the B publish
    ///     blocks again, re-rewind and return `Backpressure` without
    ///     touching tx_data.
    ///  2. Then poll tx_data for the next observed envelope and process it.
    pub fn run_once<I, B, R>(
        &mut self,
        channel_a: &mut I,
        b: &mut B,
        rc: &mut R,
    ) -> Result<bool, SequencerError>
    where
        I: TxDataSubscriber,
        B: TxOrderingRefPublisher,
        R: ReceiptCachePublisher,
    {
        let pending = self.state.drain_pending();
        if !pending.is_empty() {
            for (sender, n, meta) in pending {
                if let Err(SequencerError::Backpressure) = self.publish_ref(b, &meta) {
                    self.state.reinsert_for_retry(sender, n, meta);
                    return Err(SequencerError::Backpressure);
                }
                trace!(
                    nonce = n,
                    correlation_id = meta.correlation_id,
                    "published ref (drain-pending)"
                );
            }
            return Ok(true);
        }

        let Some((tx_data_position, envelope)) = channel_a.poll()? else {
            return Ok(false);
        };
        metrics::record_ingest(self.cfg.partition_index);

        let sender = sender_of(&envelope);

        // Defensive: drop messages routed to the wrong shard. Routing
        // disagreement between proxy and sequencer would otherwise silently
        // corrupt nonce state.
        let part = partition_for(sender, self.cfg.partition_count);
        if part != self.cfg.partition_index {
            warn!(
                expected = self.cfg.partition_index,
                got = part,
                "tx_data envelope for wrong shard; skipping"
            );
            return Ok(true);
        }

        // Decode the alloy `TxEnvelope` out of `raw_tx` to extract `nonce`.
        // The decode is the only per-tx work the sequencer does beyond the
        // state-machine arithmetic; the result is discarded after the nonce
        // is read. We never call `recover_signer()` on it (D-Sh3).
        let nonce = decode_nonce(&envelope.raw_tx)?;

        // Cache-miss hydration: first time we see this sender, fetch the
        // canonical nonce from the state DB and seed the in-memory map.
        // Cheap on cold senders; no-op (one HashMap::contains_key) on warm.
        // Steady-state: every observed envelope advances next_nonce on a
        // match, so the warm cache is intrinsic — no separate prefetch.
        self.hydrate_if_unknown(sender);

        let meta = RefMetadata {
            correlation_id: envelope.correlation_id,
            tx_hash: envelope.tx_hash,
            tx_data_position,
        };

        let t0 = std::time::Instant::now();
        let result = self.state.process(sender, nonce, meta);
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
                    match self.publish_ref(b, &payload) {
                        Ok(()) => {
                            trace!(
                                nonce = n,
                                correlation_id = payload.correlation_id,
                                "published ref"
                            );
                        }
                        Err(SequencerError::Backpressure) => {
                            // Roll back: the state machine had advanced for
                            // this tx but the canonical ref never landed on
                            // B. The reinsert re-buffers the metadata so the
                            // retry replays the same publish.
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
    pub fn run<I, B, R>(
        &mut self,
        channel_a: &mut I,
        b: &mut B,
        rc: &mut R,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError>
    where
        I: TxDataSubscriber,
        B: TxOrderingRefPublisher,
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
            match self.run_once(channel_a, b, rc) {
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
