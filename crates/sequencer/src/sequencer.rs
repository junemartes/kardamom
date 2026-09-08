//! Sequencer event step and loop.
//!
//! [`Sequencer::run_once`] polls the shard's tx_data subscription for at
//! most one fragment, and republishes a canonical-order `TxRef` onto
//! tx_ordering.
//!
//! Under the MDS topology, the proxy has already published the envelope
//! onto tx_data. So the sequencer's input is `(tx_data_position,
//! envelope)`. The proxy's Aeron-offer position is the lookup key that
//! downstream consumers (executor, batcher) use to resolve the envelope.
//!
//! For each observed envelope:
//!
//!  1. Decode the nonce from the envelope (the proxy already verified
//!     the signature and set `sender` and `tx_hash`).
//!  2. Seed `next_nonce` at 0 the first time this sequencer sees the
//!     sender (the sequencer holds no state-DB reader; committed floors
//!     arrive through the receipt-floor resync, not a state read).
//!  3. Feed `(sender, nonce, RefMetadata)` to [`PartitionState::process`]:
//!     - match: emit a publish action for this nonce, and drain any
//!       buffered higher nonces that just became contiguous;
//!     - future: buffer it (bounded per sender);
//!     - past: emit a `TxError { reason: DuplicatedTx { expected_nonce } }`.
//!  4. For each `Publish` action, build
//!     `TxRef { tx_hash, shard_id, tx_data_position }` and publish it to
//!     tx_ordering. If B applies backpressure,
//!     [`PartitionState::reinsert_for_retry`] rewinds the state, so the
//!     next loop iteration retries the same `(sender, nonce)`.
//!
//! Warm cache: because every observed envelope advances `next_nonce` on a
//! match, the in-memory map fills naturally from the tx_data read stream
//! itself. No separate prefetch thread is needed. A cold sender (no
//! activity since startup) seeds at nonce 0.
//!
//! Cold-rejoin caveat, deliberately re-opened: seeding at 0 gives only a
//! lower bound on a sender's next nonce. So a restarted replica that
//! joins mid-stream buffers an established sender's traffic against
//! nonces that will never reappear, and does not regain coverage of that
//! sender (P=1 coverage until its twin also restarts). The committed
//! floor is recovered out of band by the receipt-floor resync
//! (`crate::resync`), not by a state-DB read. An earlier stream-adaptive
//! floor fast-forward was removed, because it could not tell a
//! twin-ordered gap apart from a client-abandoned one. Adopting the
//! latter published canonical nonce gaps that fatally hit NonceTooHigh on
//! every executor (see PartitionState's note).
//!
//! See also [`crate::outbound`] for the trait surface and the in-memory
//! fakes used by tests.

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

/// Metadata needed to publish or republish a `TxRef` for a buffered or
/// just-arrived envelope. This is stored inside
/// `PartitionState<RefMetadata>`, so that
/// [`PartitionState::reinsert_for_retry`] puts a complete, ready-to-resend
/// record back in the pending buffer on a B-backpressure rewind.
///
/// This struct no longer carries the envelope bytes. The proxy already
/// wrote them onto tx_data; the sequencer only republishes the ref.
#[derive(Debug, Clone)]
struct RefMetadata {
    correlation_id: u64,
    /// Carries through to `TxRef.tx_hash`.
    tx_hash: alloy_primitives::B256,
    /// The tx_data lane that holds the envelope. Carries through to
    /// `TxRef.shard_id`. The executor joins the ref against the archive of
    /// this lane. The subscription that received the envelope sets it.
    /// Today the sequencer reads one lane, and the lane equals
    /// `cfg.sequencer_id`. A resize lets one sequencer read several lanes
    /// (see `docs/specs/dynamic-sequencer-sizing.md`).
    lane: u8,
    /// The Aeron-offer position the proxy got back when it published this
    /// envelope onto tx_data. Downstream consumers use this to look up
    /// the envelope on the A archive.
    tx_data_position: BPosition,
    /// The Aeron publisher `session_id` of that tx_data fragment. Carries
    /// through to `TxRef.tx_data_session_id`, so the executor join key
    /// `(shard, session, position)` stays unique under concurrent,
    /// active-active ingress publishers.
    tx_data_session_id: i32,
}

pub struct Sequencer {
    cfg: SequencerConfig,
    state: PartitionState<RefMetadata>,
    /// Lag detection and receipt-floor resync (see
    /// docs/agents/sequencer-lag-resync-spec.md). `None` when the binary
    /// did not wire the receipts and egress-watermark feeds (tests, IPC
    /// dev runs). Behavior is then identical to before resync existed.
    resync: Option<crate::resync::ResyncController>,
    /// Pre-registered metric handles. Recording through them does not
    /// allocate (the per-call `counter!` boxing used to be 6 of 10
    /// allocations per transaction).
    hot: metrics::HotMetrics,
    /// Published-but-unconfirmed refs. Every published ref is retained
    /// until a receipt proves canonical commitment. See
    /// [`crate::unconfirmed`] for the ledger semantics and the
    /// expiry-queue mechanics.
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

    /// Enable lag detection and receipt-floor resync. The controller
    /// starts in resync mode (the startup trigger), and `run_once` drives
    /// it.
    pub fn enable_resync(&mut self, controller: crate::resync::ResyncController) {
        self.resync = Some(controller);
    }

    /// Test-only: adjust the confirm timeout mid-run. The republish
    /// sweep is wall-clock driven, so tests set it to 0 to force an
    /// immediate rewind without sleeping.
    #[cfg(any(test, feature = "testing"))]
    pub fn set_confirm_timeout_ms(&mut self, ms: u64) {
        self.cfg.resync.confirm_timeout_ms = ms;
    }

    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    /// Build the wire `TxRef` for a drained ref (shared by the single and
    /// batch publish paths).
    fn make_txref(&self, meta: &RefMetadata) -> kardamom_types::TxRef {
        kardamom_types::TxRef::new(
            meta.tx_hash,
            meta.lane,
            meta.tx_data_position,
            meta.tx_data_session_id,
        )
    }

    /// Publish a batch of drained `(sender, nonce, meta)` refs in order.
    /// On B-backpressure, the failed item and every item not yet published
    /// are rebuffered, in reverse, so each sender's floor ends rewound to
    /// its lowest unpublished nonce. Dropping the tail would permanently
    /// lose refs whose nonces the state machine already advanced past.
    fn flush_drained<B>(
        &mut self,
        b: &mut B,
        drained: Vec<(alloy_primitives::Address, u64, RefMetadata)>,
        ctx: &'static str,
    ) -> Result<(), SequencerError>
    where
        B: TxOrderingRefPublisher,
    {
        // Chunked batch publish. Each chunk rides one cluster app message
        // (KIND_BATCH), which amortizes the per-offer session round trip
        // that dominated the sequencer's per-transaction cost. The chunk
        // must stay under one Aeron MTU (about 1408 bytes): the
        // hand-rolled cluster ingress path does not survive fragmented
        // session messages. With the guard header (sender 20 bytes, nonce
        // 8 bytes), each entry is 75 bytes plus a 4 byte length prefix.
        // 16 x 79 + 3 is about 1.27 KB, which stays under the MTU with
        // margin (20 x 79 + 3, about 1.58 KB, would not). A 16:1 ratio
        // still amortizes away the dominant per-offer cost.
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
                // Retain until a receipt proves canonical commitment. A
                // batch acceptance is still only an offer, one KIND_BATCH
                // app message on the publication buffer, not a Raft
                // commit. The whole batch can vanish in a dead-leader
                // window exactly like a single offer, so every ref in
                // the accepted prefix enters the unconfirmed ledger
                // individually.
                if self.resync.is_some() {
                    self.unconfirmed.record_published(sender, n, meta);
                }
            }
            match err {
                None => {}
                Some(SequencerError::Backpressure) => {
                    self.hot.backpressure.increment(1);
                    // Rebuffer the failed chunk and everything after it,
                    // in reverse, so each sender's floor ends rewound to
                    // its lowest unpublished nonce.
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

    /// Resync and unconfirmed-ledger bookkeeping. Runs first, on every
    /// `run_once` iteration, including idle ones, so boundary-silence
    /// detection keeps ticking. It drains receipt floors, advances the
    /// nonce state machine to any raised executed-truth floor (dropping
    /// proven-duplicate buffered entries; newly contiguous runs surface
    /// through `drain_pending` in `run_once`), then checks the watermark
    /// triggers.
    fn resync_tick(&mut self) {
        if let Some(r) = self.resync.as_mut() {
            let (raised, confirmations) = r.drain_floor_updates();
            // A receipt proves that every one of this sequencer's published
            // refs for that sender, at or below its nonce, committed. Drop
            // them from the unconfirmed ledger.
            for (sender, confirmed) in confirmations {
                self.unconfirmed.confirm_through(sender, confirmed);
            }
            for (sender, floor) in raised {
                if let Some((from, dropped)) = self.state.advance_floor(sender, floor) {
                    metrics::record_floor_advance(self.cfg.partition_index);
                    // Every dropped buffered entry is a receipt-proven
                    // duplicate, skipped without relying on any dedup
                    // window. This is the spec's
                    // `resync_skipped_executed_total`. There is no
                    // separate flush-time filter: floors drain before any
                    // publish action is computed, so the state machine's
                    // floor is always current when `process` runs. A
                    // proven-stale incoming envelope takes the ordinary
                    // `Past`/DuplicatedTx path below, and is counted
                    // there.
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

            // The sealer rejected this sequencer's ref because its nonce
            // was not the sender's expected next one. Two cases, split in
            // the drain:
            //
            // - Committed-proof (nonce < expected): drop the ledger entry
            //   exactly like a receipt confirmation. See
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
            // - Gap (nonce >= expected): refs for expected..nonce-1
            //   vanished (voided offers). They are all in the unconfirmed
            //   ledger. Rewind them now instead of waiting out the confirm
            //   timeout. The ledger hands them back in rewind-safe
            //   descending order.
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

            // Rewind refs past the confirm timeout for republish. See
            // `UnconfirmedLedger::sweep_expired` for why re-offering is
            // safe (no gap, no loss). Bounded per iteration.
            let timeout = std::time::Duration::from_millis(self.cfg.resync.confirm_timeout_ms);
            let now = std::time::Instant::now();
            let expired = self.unconfirmed.sweep_expired(timeout, now, 256);
            if !expired.is_empty() {
                metrics::record_ref_republished(self.cfg.partition_index, expired.len());
                warn!(
                    count = expired.len(),
                    // Rewind-safe descending order: the oldest (lowest)
                    // nonce is the last entry.
                    oldest_nonce = expired.last().map(|((_, n), _)| *n),
                    "unconfirmed refs past confirm timeout; rewinding for republish (#85)"
                );
                self.rewind_for_republish(expired);
                metrics::record_unconfirmed_refs(self.cfg.partition_index, self.unconfirmed.len());
            }
        }
    }

    /// Reinsert rewound ledger entries into the nonce state machine, so
    /// the next `drain_pending` republishes them. `entries` arrive in the
    /// ledger's rewind-safe descending nonce order (see
    /// `UnconfirmedLedger::take_descending`). Do not reorder them.
    fn rewind_for_republish(&mut self, entries: Vec<(UnconfirmedKey, RefMetadata)>) {
        for ((sender, nonce), meta) in entries {
            self.state.reinsert_for_retry(sender, nonce, meta);
        }
    }

    /// A receipt floor strictly above `nonce` proves that the sender's
    /// transaction at `nonce` already executed. The twin ordered it, and
    /// the order-execute-receipt round trip outran this replica's inbound
    /// processing. This is the resync mechanism absorbing a duplicate, not
    /// sequencer dirt and not a client error.
    fn proven_executed(&self, sender: alloy_primitives::Address, nonce: u64) -> bool {
        self.resync
            .as_ref()
            .is_some_and(|r| r.floor(sender).is_some_and(|f| f > nonce))
    }

    /// Tell an evicted transaction's parked submit call, and any receipt
    /// subscribers, that it will never be sequenced. A silent eviction
    /// would leave the client waiting forever, with its later nonces
    /// permanently gapped.
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
    /// `Ok(true)` if it did work, or `Ok(false)` if the retry-drain and
    /// the tx_data poll were both empty.
    ///
    /// Order of operations:
    ///  1. First flush any metadata sitting at `pending[next_nonce]` (these
    ///     are the entries rebuffered after backpressure). If the B
    ///     publish blocks again, rewind again and return `Backpressure`
    ///     without touching tx_data.
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
        R: TxErrorPublisher,
    {
        // Resync bookkeeping runs first, every iteration. See `resync_tick`.
        self.resync_tick();

        let pending = self.state.drain_pending();
        if !pending.is_empty() {
            self.flush_drained(b, pending, "drain-pending")?;
            return Ok(true);
        }

        // The nonce-floor fast-forward sweep that used to run here was
        // removed. It adopted client-abandoned nonce holes into the
        // canonical stream, and fatally hit NonceTooHigh on every executor
        // under ingress overload or chaos outages. A sender with an
        // unfillable gap now stalls here, recoverably. See
        // PartitionState's note.

        let Some((tx_data_loc, envelope)) = channel_a.poll()? else {
            return Ok(false);
        };
        self.hot.ingest.increment(1);

        let sender = sender_of(&envelope);

        // Defensive: drop messages routed to the wrong shard. Otherwise, a
        // routing disagreement between proxy and sequencer would silently
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

        // Decode the alloy `TxEnvelope` from `raw_tx` to extract `nonce`.
        // This decode is the only per-transaction work the sequencer does
        // beyond the state-machine arithmetic. The result is discarded
        // after the nonce is read. This never calls `recover_signer()`.
        let nonce = decode_nonce(&envelope.raw_tx)?;

        // A cold sender seeds at nonce 0. The sequencer holds no
        // committed-state reader; it is a pure reorderer. Committed-nonce
        // truth arrives out of band through the receipt-floor resync
        // (`crate::resync`), which advances per-sender floors from the
        // tx_receipts stream. In the steady state, every observed envelope
        // advances next_nonce on a match, so the warm cache builds itself.
        // No separate prefetch is needed.
        if self.state.next_nonce_known(sender).is_none() {
            self.state.seed_next_nonce(sender, 0);
        }

        let meta = RefMetadata {
            correlation_id: envelope.correlation_id,
            tx_hash: envelope.tx_hash,
            lane: channel_a.lane(),
            tx_data_position: tx_data_loc.position,
            tx_data_session_id: tx_data_loc.session_id,
        };

        let t0 = std::time::Instant::now();
        let result = self.state.process(sender, nonce, meta);
        self.hot
            .nonce_check_seconds
            .record(t0.elapsed().as_secs_f64());

        let publishes = self.handle_outcome(rc, sender, nonce, result);
        // On backpressure, the state machine rolls back. The reinsert
        // rebuffers every unpublished ref, so the retry replays them.
        self.flush_drained(b, publishes, "ingress")?;
        Ok(true)
    }

    /// Process-outcome bookkeeping for one observed envelope. Records
    /// metrics, notifies clients of evictions and duplicates, and collects
    /// the publish actions for `flush_drained`. This is split out of
    /// `run_once`; the sequence of operations is unchanged.
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
                // The furthest-future nonce is shed to protect the
                // drainable run. The client resubmits it once it is back
                // within the window. This counts as an eviction for
                // observability (a load shed, not a wedge).
                self.hot.evictions.increment(1);
                self.report_evicted(rc, sender, rejected);
            }
            NonceOutcome::BufferedDisabled => {
                self.hot.buffered_future.increment(1);
            }
            NonceOutcome::Past => {
                // Two different things surface as `Past`. Conflating them
                // broke the load harness's seq_clean verdict.
                //
                // - A receipt-proven skip (`proven_executed`): the resync
                //   mechanism absorbing a duplicate. This is routine for a
                //   twin that falls slightly behind under load. It is not
                //   sequencer dirt (`dropped_past` stays flat, and
                //   seq_clean holds), and not a client error (the
                //   transaction succeeded; a DuplicatedTx notice would be
                //   spurious and could race the receipt at ingress).
                // - An ordinary client double-submit or stale nonce: no
                //   floor proof. Count and report it exactly as before.
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
                    // Suppressed for receipt-proven skips (see above).
                    // The client's transaction executed, so there is
                    // nothing to report.
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
                    // Sustained backpressure (including a not-yet-reconnected
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
