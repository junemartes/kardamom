//! Sequencer event step + loop.
//!
//! [`Sequencer::run_once`] polls the shard's tx_data subscription for at
//! most one fragment and republishes a canonical-order `TxRef` onto
//! tx_ordering.
//!
//! Per the MDS topology: the proxy has already
//! published the envelope onto tx_data, so the sequencer's input is
//! `(tx_data_position, envelope)` — the proxy's Aeron-offer position is the
//! lookup key downstream consumers (executor, batcher) use to resolve the
//! envelope.
//!
//! For each observed envelope:
//!
//!  1. Decode the nonce out of the envelope (the proxy already verified
//!     the signature and populated `sender` + `tx_hash`).
//!  2. Seed `next_nonce` at 0 the first time we've seen the sender (the
//!     sequencer holds no state-DB reader; committed floors arrive via the
//!     receipt-floor resync, not a state read).
//!  3. Feed `(sender, nonce, RefMetadata)` to [`PartitionState::process`]:
//!     - match → emit a publish action for this nonce + drain any
//!       buffered higher nonces that newly become contiguous;
//!     - future → buffer (bounded per-sender);
//!     - past → emit a `TxError { reason: DuplicatedTx { expected_nonce } }`.
//!  4. For each `Publish` action, build
//!     `TxRef { tx_hash, shard_id, tx_data_position }` and publish to tx_ordering.
//!     If B back-pressures, [`PartitionState::reinsert_for_retry`] rewinds
//!     so the next loop iteration retries the same `(sender, nonce)`.
//!
//! Warm cache: because every observed envelope advances `next_nonce` on a
//! match, the in-memory map is naturally populated by the tx_data read
//! stream itself — no separate prefetch thread needed. Cold senders (no
//! activity since startup) seed at nonce 0.
//!
//! Cold-rejoin caveat (F02.1, RE-OPENED): seeding at 0 provides only a
//! *lower bound* on a sender's next nonce, so a restarted replica that
//! live-joins mid-stream buffers established senders' traffic against
//! nonces that will never reappear and does not regain coverage of them
//! (P=1 for those senders until its twin also restarts). The committed
//! floor is recovered out of band by the receipt-floor resync
//! (`crate::resync`), NOT by a state-DB read: an earlier "stream-adaptive
//! floor fast-forward" was REMOVED because it could not distinguish
//! twin-ordered gaps from client-abandoned ones, and adopting the latter
//! published canonical nonce gaps that fatally NonceTooHigh'd every
//! executor (see PartitionState's note + the CI round-4 analysis).
//!
//! See also [`crate::outbound`] for the trait surface and in-memory fakes
//! used by tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_rlp::Decodable;
use kardamom_types::{BPosition, TxError, TxErrorReason};
use tracing::{trace, warn};

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::TxDataSubscriber;
use crate::metrics;
use crate::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
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
    /// The Aeron publisher `session_id` of that tx_data fragment. Carries
    /// through to `TxRef.tx_data_session_id` so the executor join key
    /// `(shard, session, position)` stays unique under concurrent (active/active)
    /// ingress publishers.
    tx_data_session_id: i32,
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

pub struct Sequencer {
    cfg: SequencerConfig,
    state: PartitionState<RefMetadata>,
    /// Lag detection + receipt-floor resync
    /// (docs/agents/sequencer-lag-resync-spec.md). `None` when the binary
    /// did not wire the receipts / egress-watermark feeds (tests, IPC dev
    /// runs) — behaviour is then identical to pre-resync.
    resync: Option<crate::resync::ResyncController>,
    /// Published-but-UNCONFIRMED refs (#85): an `Accepted` offer only proves
    /// the bytes landed on the Aeron publication buffer, NOT that the Raft
    /// cluster committed them — a leader kill voids the dead-leader window
    /// and the uncommitted tail, and continuing from the optimistically
    /// advanced nonce state seals a canonical GAP. Every published ref is
    /// retained here until a receipt for its sender at/above its nonce
    /// proves canonical commitment (skip receipts count — ordering is the
    /// claim); entries older than `resync.confirm_timeout_ms` are rewound
    /// via `reinsert_for_retry` and re-published — the cluster dedup absorbs
    /// copies that DID commit, and voided ones get ordered.
    unconfirmed: UnconfirmedLedger,
}

/// #85 publish-confirmation ledger: (sender, nonce) → (ref metadata,
/// published-at). BTreeMap so per-sender ranges trim cheaply on confirmation
/// and the staleness sweep sees ascending nonce order.
type UnconfirmedLedger =
    std::collections::BTreeMap<(alloy_primitives::Address, u64), (RefMetadata, std::time::Instant)>;

impl Sequencer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self {
            cfg,
            state: PartitionState::new(cap),
            resync: None,
            unconfirmed: std::collections::BTreeMap::new(),
        }
    }

    /// Enable lag detection + receipt-floor resync. The controller starts in
    /// resync mode (startup trigger) and is driven from `run_once`.
    pub fn enable_resync(&mut self, controller: crate::resync::ResyncController) {
        self.resync = Some(controller);
    }

    /// Test-only: adjust the #85 confirm timeout mid-run (the republish
    /// sweep is wall-clock driven; tests flip it to 0 to force an immediate
    /// rewind without sleeping).
    #[cfg(any(test, feature = "testing"))]
    pub fn set_confirm_timeout_ms(&mut self, ms: u64) {
        self.cfg.resync.confirm_timeout_ms = ms;
    }

    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    /// Publish a `TxRef` for `meta` to tx_ordering. The `tx_data_position` was
    /// observed off the tx_data subscription — the proxy did the actual
    /// envelope write — so this is a single B write, not a dual write.
    /// On B-backpressure the caller reinserts the metadata so the retry
    /// republishes the same `(tx_hash, shard, tx_data_position)` triple.
    /// Build the wire `TxRef` for a drained meta (shared by the single and
    /// batch publish paths).
    fn make_txref(&self, meta: &RefMetadata) -> kardamom_types::TxRef {
        kardamom_types::TxRef::new(
            meta.tx_hash,
            self.cfg.sequencer_id,
            meta.tx_data_position,
            meta.tx_data_session_id,
        )
    }

    /// Publish a batch of drained `(sender, nonce, meta)` refs in order. On
    /// B-backpressure the failed item AND every item not yet published are
    /// rebuffered (in reverse, so each sender's floor ends rewound to its
    /// lowest unpublished nonce) — dropping the tail would permanently lose
    /// refs whose nonces the state machine already advanced past.
    fn flush_drained<B>(
        &mut self,
        b: &mut B,
        drained: Vec<(alloy_primitives::Address, u64, RefMetadata)>,
        ctx: &'static str,
    ) -> Result<(), SequencerError>
    where
        B: TxOrderingRefPublisher,
    {
        // Chunked batch publish: each chunk rides ONE cluster app message
        // (KIND_BATCH), amortizing the per-offer session round trip that
        // dominated the sequencer's per-tx cost. The chunk MUST stay under
        // one Aeron MTU (~1408B): the hand-rolled cluster ingress path does
        // not survive fragmented session messages (validated empirically —
        // 128-ref ~7KB chunks lost every fragmented batch: ramp died at
        // 750tps and 96k accepted refs never receipted). With the #85 guard
        // header (sender 20B + nonce 8B) each entry is 75B + 4B length
        // prefix; 16 × 79 + 3 ≈ 1.27KB stays under the MTU with margin
        // (20 × 79 + 3 ≈ 1.58KB would NOT). 16:1 still amortizes away the
        // dominant per-offer cost.
        const BATCH_MAX: usize = 16;
        let mut rest = std::collections::VecDeque::from(drained);
        while !rest.is_empty() {
            let chunk = BATCH_MAX.min(rest.len());
            let refs: Vec<(kardamom_types::TxRef, alloy_primitives::Address, u64)> = rest
                .iter()
                .take(chunk)
                .map(|(s, n, m)| (self.make_txref(m), *s, *n))
                .collect();
            let (published, err) = b.try_publish_ref_batch(&refs);
            for (sender, n, meta) in rest.drain(..published) {
                metrics::record_publish(self.cfg.partition_index);
                trace!(
                    nonce = n,
                    correlation_id = meta.correlation_id,
                    ctx,
                    "published ref"
                );
                // #85: retain until a receipt proves canonical commitment.
                // A batch acceptance is still only an OFFER — one KIND_BATCH
                // app message on the publication buffer — NOT a Raft commit;
                // the whole batch can vanish in a dead-leader window exactly
                // like a single offer, so every ref in the accepted prefix
                // enters the unconfirmed ledger individually.
                if self.resync.is_some() {
                    self.unconfirmed
                        .insert((sender, n), (meta, std::time::Instant::now()));
                }
            }
            match err {
                None => {}
                Some(SequencerError::Backpressure) => {
                    metrics::record_backpressure(self.cfg.partition_index);
                    // Rebuffer the failed chunk AND everything after it, in
                    // reverse, so each sender's floor ends rewound to its
                    // lowest unpublished nonce.
                    while let Some((s, n2, m)) = rest.pop_back() {
                        self.state.reinsert_for_retry(s, n2, m);
                    }
                    return Err(SequencerError::Backpressure);
                }
                Some(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Drive one ingress message through the state machine. Returns
    /// `Ok(true)` if work was done, `Ok(false)` if the retry-drain, the
    /// floor fast-forward sweep, and the tx_data poll were all empty.
    ///
    /// Order of operations:
    ///  1. First flush any metadata sitting at `pending[next_nonce]` (these
    ///     are the rebuffered-after-backpressure entries). If the B publish
    ///     blocks again, re-rewind and return `Backpressure` without
    ///     touching tx_data.
    ///  2. Then fast-forward any nonce floor that has been stalled behind a
    ///     buffered future-run for longer than the configured lag bound
    ///     (cold-rejoin coverage recovery — see the module docs).
    ///  3. Then poll tx_data for the next observed envelope and process it.
    pub fn run_once<I, B, R>(
        &mut self,
        channel_a: &mut I,
        b: &mut B,
        rc: &mut R,
    ) -> Result<bool, SequencerError>
    where
        I: TxDataSubscriber,
        B: TxOrderingRefPublisher,
        R: TxErrorPublisher,
    {
        // Resync bookkeeping runs FIRST, every iteration (including idle
        // ones, so boundary-silence detection keeps ticking): drain receipt
        // floors, advance the nonce state machine to any raised
        // executed-truth floor (drops proven-duplicate buffered entries;
        // newly-contiguous runs surface via `drain_pending` below), then
        // evaluate the watermark triggers.
        if let Some(r) = self.resync.as_mut() {
            let (raised, confirmations) = r.drain_floor_updates();
            // #85: a receipt at nonce N proves every one of OUR published
            // refs for that sender at nonce <= N survived into the committed
            // canonical stream (per-sender order is preserved end to end) —
            // drop them from the unconfirmed ledger.
            for (sender, confirmed) in confirmations {
                let keys: Vec<_> = self
                    .unconfirmed
                    .range((sender, 0)..=(sender, confirmed))
                    .map(|(k, _)| *k)
                    .collect();
                for k in keys {
                    self.unconfirmed.remove(&k);
                }
            }
            for (sender, floor) in raised {
                if let Some((from, dropped)) = self.state.advance_floor(sender, floor) {
                    metrics::record_floor_advance(self.cfg.partition_index);
                    // Every dropped buffered entry is a receipt-PROVEN
                    // duplicate skipped without any dedup-window reliance —
                    // the spec's `resync_skipped_executed_total`. (There is
                    // no separate flush-time filter: floors drain before any
                    // publish action is computed, so the state machine's
                    // floor is always current when `process` runs — a
                    // proven-stale incoming envelope takes the ordinary
                    // `Past`/DuplicatedTx path below, counted there.)
                    for _ in 0..dropped {
                        metrics::record_resync_skip(self.cfg.partition_index);
                    }
                    trace!(
                        sender = ?sender,
                        from,
                        floor,
                        dropped,
                        "resync: receipt floor advanced nonce state"
                    );
                }
            }
            r.observe(std::time::Instant::now());

            // #85 fix B: the sealer REJECTED a known sender's ref because its
            // nonce was not the expected next one — refs for
            // expected..nonce-1 vanished (voided offers). They are all in the
            // unconfirmed ledger; rewind them NOW instead of waiting out the
            // confirm timeout. Same descending-nonce discipline as the sweep
            // below. (A reject with nonce < expected means the ref already
            // committed once and its dedup entry aged out — the range below
            // is empty and this is a no-op; receipts confirm it normally.)
            for (sender, expected) in r.drain_contiguity_rejects() {
                let keys: Vec<_> = self
                    .unconfirmed
                    .range((sender, expected)..=(sender, u64::MAX))
                    .map(|(k, _)| *k)
                    .collect();
                if keys.is_empty() {
                    continue;
                }
                metrics::record_ref_republished(self.cfg.partition_index, keys.len());
                warn!(
                    sender = ?sender,
                    expected,
                    count = keys.len(),
                    "sealer contiguity reject; rewinding unconfirmed refs for republish (#85)"
                );
                for (s, n) in keys.into_iter().rev() {
                    if let Some((meta, _)) = self.unconfirmed.remove(&(s, n)) {
                        self.state.reinsert_for_retry(s, n, meta);
                    }
                }
            }

            // #85: republish refs whose confirmation has not arrived within
            // the timeout — the offer may have landed in a dead leader's
            // void. reinsert_for_retry rewinds the nonce state so the next
            // drain_pending re-publishes them; the cluster's first-seen
            // dedup absorbs every copy that DID commit, and voided ones get
            // ordered — no gap, no loss (this also un-wedges a sender whose
            // refs vanished entirely: the ledger keeps re-offering until a
            // receipt confirms). Bounded per iteration.
            let timeout = std::time::Duration::from_millis(self.cfg.resync.confirm_timeout_ms);
            let now = std::time::Instant::now();
            let stale: Vec<_> = self
                .unconfirmed
                .iter()
                .filter(|(_, (_, at))| now.duration_since(*at) >= timeout)
                .take(256)
                .map(|(k, _)| *k)
                .collect();
            if !stale.is_empty() {
                metrics::record_ref_republished(self.cfg.partition_index, stale.len());
                warn!(
                    count = stale.len(),
                    oldest_nonce = stale.first().map(|(_, n)| *n),
                    "unconfirmed refs past confirm timeout; rewinding for republish (#85)"
                );
            }
            // DESCENDING nonce order: reinsert_for_retry sets the sender's
            // rewind floor on every call, so the LAST call per sender must
            // carry the LOWEST nonce (same discipline as flush_drained's
            // backpressure rebuffer) — ascending order would strand the
            // lower nonces beneath the floor forever.
            for (sender, n) in stale.into_iter().rev() {
                if let Some((meta, _)) = self.unconfirmed.remove(&(sender, n)) {
                    self.state.reinsert_for_retry(sender, n, meta);
                }
            }
            metrics::record_unconfirmed_refs(self.cfg.partition_index, self.unconfirmed.len());
        }

        let pending = self.state.drain_pending();
        if !pending.is_empty() {
            self.flush_drained(b, pending, "drain-pending")?;
            return Ok(true);
        }

        // (The nonce-floor fast-forward sweep that ran here was REMOVED: it
        // adopted client-abandoned nonce holes into the canonical stream and
        // fatally NonceTooHigh'd every executor under ingress overload /
        // chaos outages. A sender with an unfillable gap now stalls here —
        // recoverable — exactly as on main. See PartitionState's note.)

        let Some((tx_data_loc, envelope)) = channel_a.poll()? else {
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
        // is read. We never call `recover_signer()` on it.
        let nonce = decode_nonce(&envelope.raw_tx)?;

        // Cold senders seed at nonce 0 — the sequencer holds no committed-state
        // reader (it is a pure reorderer). Committed-nonce truth arrives out of
        // band via the receipt-floor resync (`crate::resync`), which advances
        // per-sender floors from the tx_receipts stream. Steady-state: every
        // observed envelope advances next_nonce on a match, so the warm cache is
        // intrinsic — no separate prefetch.
        if self.state.next_nonce_known(sender).is_none() {
            self.state.seed_next_nonce(sender, 0);
        }

        let meta = RefMetadata {
            correlation_id: envelope.correlation_id,
            tx_hash: envelope.tx_hash,
            tx_data_position: tx_data_loc.position,
            tx_data_session_id: tx_data_loc.session_id,
        };

        let t0 = std::time::Instant::now();
        let result = self.state.process(sender, nonce, meta);
        metrics::record_nonce_check_latency(self.cfg.partition_index, t0.elapsed().as_secs_f64());

        match result.outcome {
            NonceOutcome::Matched => {}
            NonceOutcome::Buffered | NonceOutcome::BufferedReplaced => {
                metrics::record_buffered_future(self.cfg.partition_index);
            }
            NonceOutcome::BufferedEvicting { evicted_nonce } => {
                metrics::record_buffered_future(self.cfg.partition_index);
                metrics::record_eviction(self.cfg.partition_index);
                // Tell the evicted tx's parked submit (and any receipt
                // subscribers) it will never be sequenced — a silent evict
                // left the client waiting forever and its later nonces
                // permanently gapped.
                rc.publish_error(TxError {
                    sender,
                    nonce: evicted_nonce,
                    reason: TxErrorReason::Evicted {
                        expected_nonce: self.state.next_nonce(sender),
                    },
                });
            }
            NonceOutcome::RejectedTooFar { nonce: rejected } => {
                // Furthest-future nonce shed to protect the drainable run; the
                // client re-submits it once it is back within the window. Counts
                // as an eviction for observability (load-shed, not a wedge).
                metrics::record_eviction(self.cfg.partition_index);
                rc.publish_error(TxError {
                    sender,
                    nonce: rejected,
                    reason: TxErrorReason::Evicted {
                        expected_nonce: self.state.next_nonce(sender),
                    },
                });
            }
            NonceOutcome::BufferedDisabled => {
                metrics::record_buffered_future(self.cfg.partition_index);
            }
            NonceOutcome::Past => {
                // Two DIFFERENT things surface as `Past`, and conflating
                // them broke the load harness's seq_clean verdict (CI run
                // 30166583138: seq_clean=false from 200 tps — the ramp
                // ceiling collapsed to 100):
                //
                // - RECEIPT-PROVEN skip: the receipt floor advanced past
                //   this nonce because the tx already EXECUTED (the twin
                //   ordered it and the order→execute→receipt round trip
                //   outran this replica's inbound processing — routine for
                //   the marginally-behind twin under load). This is the
                //   resync mechanism absorbing a duplicate: NOT sequencer
                //   dirt (`dropped_past` stays flat, seq_clean holds) and
                //   NOT a client error (the tx succeeded; a DuplicatedTx
                //   notice would be spurious and could race the receipt at
                //   ingress).
                // - ordinary client double-submit / stale nonce: no floor
                //   proof — counted and reported exactly as before.
                if self
                    .resync
                    .as_ref()
                    .is_some_and(|r| r.floor(sender).is_some_and(|f| f > nonce))
                {
                    metrics::record_resync_skip(self.cfg.partition_index);
                } else {
                    metrics::record_past(self.cfg.partition_index);
                }
            }
        }

        let proven_executed = |n: u64| {
            self.resync
                .as_ref()
                .is_some_and(|r| r.floor(sender).is_some_and(|f| f > n))
        };
        let mut publishes = Vec::new();
        for action in result.actions {
            match action {
                ProcessAction::Publish { nonce: n, payload } => {
                    publishes.push((sender, n, payload));
                }
                ProcessAction::ReportDuplicate {
                    nonce: n,
                    expected_nonce,
                } => {
                    // Suppressed for receipt-proven skips (see above): the
                    // client's tx executed; there is nothing to report.
                    if !proven_executed(n) {
                        rc.publish_error(TxError {
                            sender,
                            nonce: n,
                            reason: TxErrorReason::DuplicatedTx { expected_nonce },
                        });
                    }
                }
            }
        }
        // On backpressure the state machine is rolled back: the reinsert
        // re-buffers every unpublished ref so the retry replays them.
        self.flush_drained(b, publishes, "ingress")?;
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
        R: TxErrorPublisher,
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
                Ok(true) => {
                    backoff_us = 1;
                    if let Some(r) = self.resync.as_mut() {
                        r.note_publish_ok();
                    }
                }
                Ok(false) => {
                    if let Some(r) = self.resync.as_mut() {
                        r.note_publish_ok();
                    }
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = backoff_us.saturating_mul(2).min(100);
                }
                Err(SequencerError::Backpressure) => {
                    // Sustained backpressure (incl. a not-yet-reconnected
                    // cluster session, which maps here) is the publish-stall
                    // resync trigger.
                    if let Some(r) = self.resync.as_mut() {
                        r.note_publish_stall(std::time::Instant::now());
                    }
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
