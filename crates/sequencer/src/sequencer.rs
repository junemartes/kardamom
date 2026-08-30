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

use std::time::Duration;

use kardamom_types::{BPosition, TxError, TxErrorReason};
use tracing::{trace, warn};

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::TxDataSubscriber;
use crate::metrics;
use crate::nonce_decode::decode_nonce;
use crate::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use crate::partition::partition_for;
use crate::sender::sender_of;
use crate::state::{NonceOutcome, PartitionState, ProcessAction, ProcessResult};
use crate::unconfirmed::{UnconfirmedKey, UnconfirmedLedger};

// Re-export: the shutdown signal lived here before it moved to
// `crate::shutdown`; the bin (and external callers) import
// `kardamom_sequencer::sequencer::Shutdown`.
pub use crate::shutdown::Shutdown;

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

pub struct Sequencer {
    cfg: SequencerConfig,
    state: PartitionState<RefMetadata>,
    /// Lag detection + receipt-floor resync
    /// (docs/agents/sequencer-lag-resync-spec.md). `None` when the binary
    /// did not wire the receipts / egress-watermark feeds (tests, IPC dev
    /// runs) — behaviour is then identical to pre-resync.
    resync: Option<crate::resync::ResyncController>,
    /// Pre-registered metric handles — recording is allocation-free (the
    /// per-call `counter!` boxing was 6 of 10 allocations per tx).
    hot: metrics::HotMetrics,
    /// Published-but-UNCONFIRMED refs (#85): every published ref is
    /// retained until a receipt proves canonical commitment. See
    /// [`crate::unconfirmed`] for the ledger semantics and the expiry-queue
    /// mechanics.
    unconfirmed: UnconfirmedLedger<RefMetadata>,
}

impl Sequencer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        let hot = metrics::HotMetrics::new(cfg.partition_index);
        Self {
            cfg,
            hot,
            state: PartitionState::new(cap),
            resync: None,
            unconfirmed: UnconfirmedLedger::new(),
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
                self.hot.publish.increment(1);
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
                    self.unconfirmed.record_published(sender, n, meta);
                }
            }
            match err {
                None => {}
                Some(SequencerError::Backpressure) => {
                    self.hot.backpressure.increment(1);
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

    /// Resync + #85 ledger bookkeeping. Runs FIRST, every `run_once`
    /// iteration (including idle ones, so boundary-silence detection keeps
    /// ticking): drain receipt floors, advance the nonce state machine to
    /// any raised executed-truth floor (drops proven-duplicate buffered
    /// entries; newly-contiguous runs surface via `drain_pending` in
    /// `run_once`), then evaluate the watermark triggers.
    fn resync_tick(&mut self) {
        if let Some(r) = self.resync.as_mut() {
            let (raised, confirmations) = r.drain_floor_updates();
            // #85: a receipt proves every one of OUR published refs for that
            // sender at/below its nonce committed — drop them from the
            // unconfirmed ledger.
            for (sender, confirmed) in confirmations {
                self.unconfirmed.confirm_through(sender, confirmed);
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

            // #85 fix B: the sealer REJECTED our ref because its nonce was
            // not the sender's expected next one. Two cases (split in the
            // drain):
            //
            // - committed-proof (nonce < expected): drop the ledger entry
            //   exactly like a receipt confirmation — see
            //   `UnconfirmedLedger::drop_committed` for the full story.
            let (drops, rewinds) = r.drain_contiguity_rejects();
            for (sender, n) in drops {
                if self.unconfirmed.drop_committed(sender, n) {
                    trace!(
                        sender = ?sender,
                        nonce = n,
                        "contiguity reject proves commitment; dropping unconfirmed entry (#85)"
                    );
                }
            }
            // - gap (nonce >= expected): refs for expected..nonce-1 vanished
            //   (voided offers). They are all in the unconfirmed ledger;
            //   rewind them NOW instead of waiting out the confirm timeout.
            //   The ledger hands them back in rewind-safe descending order.
            for (sender, expected) in rewinds {
                let taken = self.unconfirmed.take_gap_rewinds(sender, expected);
                if taken.is_empty() {
                    continue;
                }
                metrics::record_ref_republished(self.cfg.partition_index, taken.len());
                warn!(
                    sender = ?sender,
                    expected,
                    count = taken.len(),
                    "sealer contiguity reject; rewinding unconfirmed refs for republish (#85)"
                );
                self.rewind_for_republish(taken);
            }

            // #85: rewind refs past the confirm timeout for republish —
            // see `UnconfirmedLedger::sweep_expired` for why re-offering is
            // safe (no gap, no loss). Bounded per iteration.
            let timeout = std::time::Duration::from_millis(self.cfg.resync.confirm_timeout_ms);
            let now = std::time::Instant::now();
            let expired = self.unconfirmed.sweep_expired(timeout, now, 256);
            if !expired.is_empty() {
                metrics::record_ref_republished(self.cfg.partition_index, expired.len());
                warn!(
                    count = expired.len(),
                    // Rewind-safe descending order: the oldest (lowest)
                    // nonce is the LAST entry.
                    oldest_nonce = expired.last().map(|((_, n), _)| *n),
                    "unconfirmed refs past confirm timeout; rewinding for republish (#85)"
                );
                self.rewind_for_republish(expired);
                metrics::record_unconfirmed_refs(self.cfg.partition_index, self.unconfirmed.len());
            }
        }
    }

    /// Reinsert rewound ledger entries into the nonce state machine so the
    /// next `drain_pending` republishes them. `entries` arrive in the
    /// ledger's rewind-safe DESCENDING nonce order (see
    /// `UnconfirmedLedger::take_descending`) — do not reorder.
    fn rewind_for_republish(&mut self, entries: Vec<(UnconfirmedKey, RefMetadata)>) {
        for ((sender, nonce), meta) in entries {
            self.state.reinsert_for_retry(sender, nonce, meta);
        }
    }

    /// A receipt floor strictly above `nonce` proves the sender's tx at
    /// `nonce` already EXECUTED (the twin ordered it and the
    /// order→execute→receipt round trip outran this replica's inbound
    /// processing) — the resync mechanism absorbing a duplicate, not
    /// sequencer dirt and not a client error.
    fn proven_executed(&self, sender: alloy_primitives::Address, nonce: u64) -> bool {
        self.resync
            .as_ref()
            .is_some_and(|r| r.floor(sender).is_some_and(|f| f > nonce))
    }

    /// Tell an evicted tx's parked submit (and any receipt subscribers) it
    /// will never be sequenced — a silent evict left the client waiting
    /// forever and its later nonces permanently gapped.
    fn report_evicted<R>(&self, rc: &mut R, sender: alloy_primitives::Address, nonce: u64)
    where
        R: TxErrorPublisher,
    {
        rc.publish_error(TxError {
            sender,
            nonce,
            reason: TxErrorReason::Evicted {
                expected_nonce: self.state.next_nonce(sender),
            },
        });
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
        // Resync bookkeeping runs FIRST, every iteration — see `resync_tick`.
        self.resync_tick();

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
        self.hot.ingest.increment(1);

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
        self.hot
            .nonce_check_seconds
            .record(t0.elapsed().as_secs_f64());

        let publishes = self.handle_outcome(rc, sender, nonce, result);
        // On backpressure the state machine is rolled back: the reinsert
        // re-buffers every unpublished ref so the retry replays them.
        self.flush_drained(b, publishes, "ingress")?;
        Ok(true)
    }

    /// Process-outcome bookkeeping for one observed envelope: record
    /// metrics, notify clients of evictions/duplicates, and collect the
    /// publish actions for `flush_drained`. Split out of `run_once`; the
    /// sequencing of operations is unchanged.
    fn handle_outcome<R>(
        &mut self,
        rc: &mut R,
        sender: alloy_primitives::Address,
        nonce: u64,
        result: ProcessResult<RefMetadata>,
    ) -> Vec<(alloy_primitives::Address, u64, RefMetadata)>
    where
        R: TxErrorPublisher,
    {
        match result.outcome {
            NonceOutcome::Matched => {}
            NonceOutcome::Buffered | NonceOutcome::BufferedReplaced => {
                self.hot.buffered_future.increment(1);
            }
            NonceOutcome::BufferedEvicting { evicted_nonce } => {
                self.hot.buffered_future.increment(1);
                self.hot.evictions.increment(1);
                self.report_evicted(rc, sender, evicted_nonce);
            }
            NonceOutcome::RejectedTooFar { nonce: rejected } => {
                // Furthest-future nonce shed to protect the drainable run; the
                // client re-submits it once it is back within the window. Counts
                // as an eviction for observability (load-shed, not a wedge).
                self.hot.evictions.increment(1);
                self.report_evicted(rc, sender, rejected);
            }
            NonceOutcome::BufferedDisabled => {
                self.hot.buffered_future.increment(1);
            }
            NonceOutcome::Past => {
                // Two DIFFERENT things surface as `Past`, and conflating
                // them broke the load harness's seq_clean verdict (CI run
                // 30166583138: seq_clean=false from 200 tps — the ramp
                // ceiling collapsed to 100):
                //
                // - RECEIPT-PROVEN skip (`proven_executed`): the resync
                //   mechanism absorbing a duplicate — routine for the
                //   marginally-behind twin under load. NOT sequencer dirt
                //   (`dropped_past` stays flat, seq_clean holds) and NOT a
                //   client error (the tx succeeded; a DuplicatedTx notice
                //   would be spurious and could race the receipt at
                //   ingress).
                // - ordinary client double-submit / stale nonce: no floor
                //   proof — counted and reported exactly as before.
                if self.proven_executed(sender, nonce) {
                    metrics::record_resync_skip(self.cfg.partition_index);
                } else {
                    self.hot.dropped_past.increment(1);
                }
            }
        }

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
                    if !self.proven_executed(sender, n) {
                        rc.publish_error(TxError {
                            sender,
                            nonce: n,
                            reason: TxErrorReason::DuplicatedTx { expected_nonce },
                        });
                    }
                }
            }
        }
        publishes
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
        // Same escalation as the deposit pump: 1µs base, x2 per idle
        // iteration, 100µs cap, snap back on work. IdleBackoff with
        // grace 1 gives the exact old sleep sequence (1, 2, 4, ... 100).
        let mut backoff = kardamom_log::aeron_live::IdleBackoff::new(
            Duration::from_micros(1),
            Duration::from_micros(100),
            1,
        );
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            match self.run_once(channel_a, b, rc) {
                Ok(true) => {
                    backoff.reset();
                    if let Some(r) = self.resync.as_mut() {
                        r.note_publish_ok();
                    }
                }
                Ok(false) => {
                    if let Some(r) = self.resync.as_mut() {
                        r.note_publish_ok();
                    }
                    std::thread::sleep(backoff.idle_wait());
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
