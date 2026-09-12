//! Sequencer event step and loop.
//!
//! [`Sequencer::run_once`] polls the shard's `tx_data` subscription for at
//! most one fragment, and republishes a canonical-order `TxRef` onto
//! `tx_ordering`.
//!
//! Under the MDS topology, the proxy has already published the envelope
//! onto `tx_data`. So the sequencer's input is `(tx_data_position,
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
//!     `tx_ordering`. If B applies backpressure,
//!     [`PartitionState::reinsert_for_retry`] rewinds the state, so the
//!     next loop iteration retries the same `(sender, nonce)`.
//!
//! Warm cache: because every observed envelope advances `next_nonce` on a
//! match, the in-memory map fills naturally from the `tx_data` read stream
//! itself. No separate prefetch thread is needed. A cold sender (no
//! activity since startup) seeds at nonce 0.
//!
//! Cold-rejoin caveat, deliberately re-opened: seeding at 0 gives only a
//! lower bound on a sender's next nonce. So a restarted replica that
//! joins mid-stream buffers an established sender's traffic against
//! nonces that will never reappear, and does not regain coverage of that
//! sender (P=1 coverage until its twin also restarts). The committed
//! floor is recovered out of band by the receipt-floor resync
//! (`crate::resync`), not by a state-DB read. A sequencer cannot tell a
//! twin-ordered gap apart from a client-abandoned one, so it must never
//! locally infer and fast-forward past a nonce gap: adopting a
//! client-abandoned gap publishes a canonical nonce gap that fatally hits
//! `NonceTooHigh` on every executor (see `PartitionState`'s note).
//!
//! See also [`crate::outbound`] for the trait surface and the in-memory
//! fakes used by tests.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use kardamom_types::num::usize_to_u64;
use kardamom_types::shard_map::{VslotSet, vslot_for};
use kardamom_types::{BPosition, TxError, TxErrorReason};
use tracing::{trace, warn};

use crate::config::{ConfigError, SequencerConfig};
use crate::error::SequencerError;
use crate::inbound::{Inbound, TxDataSubscriber};
use crate::lookup::LookupRequester;
use crate::metrics;
use crate::nonce_decode::decode_nonce;
use crate::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use crate::sender::sender_of;
use crate::state::{NonceOutcome, PartitionState, ProcessAction, ProcessResult};
use crate::unconfirmed::{UnconfirmedKey, UnconfirmedLedger};

// Re-export: the bin (and external callers) import
// `kardamom_sequencer::sequencer::Shutdown`.
pub use crate::shutdown::Shutdown;

/// Metadata needed to publish or republish a `TxRef` for a buffered or
/// just-arrived envelope. This is stored inside
/// `PartitionState<RefMetadata>`, so that
/// [`PartitionState::reinsert_for_retry`] puts a complete, ready-to-resend
/// record back in the pending buffer on a B-backpressure rewind.
///
/// This struct holds a reference back to the envelope, not the envelope
/// bytes themselves. The proxy already wrote them onto `tx_data`; the
/// sequencer only republishes the ref.
#[derive(Debug, Clone)]
struct RefMetadata {
    correlation_id: u64,
    /// Carries through to `TxRef.tx_hash`.
    tx_hash: alloy_primitives::B256,
    /// The `tx_data` lane that holds the envelope. Carries through to
    /// `TxRef.shard_id`. The executor joins the ref against the archive of
    /// this lane. The subscription that received the envelope sets it. A
    /// resize lets one sequencer read several lanes (see
    /// `docs/specs/dynamic-sequencer-sizing.md`).
    lane: u8,
    /// The Aeron-offer position the proxy got back when it published this
    /// envelope onto `tx_data`. Downstream consumers use this to look up
    /// the envelope on the A archive.
    tx_data_position: BPosition,
    /// The Aeron publisher `session_id` of that `tx_data` fragment. Carries
    /// through to `TxRef.tx_data_session_id`, so the executor join key
    /// `(shard, session, position)` stays unique under concurrent,
    /// active-active ingress publishers.
    tx_data_session_id: i32,
}

/// One `Sequencer::run_once` iteration's port set: the `tx_data`
/// subscription, the `tx_ordering` publisher, and the `tx_errors` publisher.
/// Groups the three bounds behind one type parameter, instead of three
/// separate ones on `run_once` and `run`.
pub trait SequencerPorts: Send {
    type In: TxDataSubscriber;
    type Refs: TxOrderingRefPublisher;
    type Errors: TxErrorPublisher;

    /// Borrow the three ports for one iteration.
    fn split(&mut self) -> (&mut Self::In, &mut Self::Refs, &mut Self::Errors);
}

/// Borrows the three [`SequencerPorts`] for one `run_once`/`run` call: a
/// named struct instead of a positional `(&mut I, &mut B, &mut R)` tuple,
/// so each borrow states what it is.
pub struct Ports<'a, I, B, R> {
    pub tx_data: &'a mut I,
    pub refs: &'a mut B,
    pub errors: &'a mut R,
}

impl<I, B, R> SequencerPorts for Ports<'_, I, B, R>
where
    I: TxDataSubscriber,
    B: TxOrderingRefPublisher,
    R: TxErrorPublisher,
{
    type In = I;
    type Refs = B;
    type Errors = R;

    fn split(&mut self) -> (&mut I, &mut B, &mut R) {
        (self.tx_data, self.refs, self.errors)
    }
}

/// A resize warm-up in progress: the slots in shadow mode, and when the
/// mode ends. In shadow mode the state machine runs for these slots, but
/// the ref publisher and the `tx_errors` publisher stay silent. The
/// value exists only while at least one slot is in shadow.
#[derive(Debug, Clone, Copy)]
struct ShadowWindow {
    vslots: VslotSet,
    until: Instant,
}

/// The last pending-depth report: when it ran, and the depths it
/// reported. The report runs once per second and emits only the changed
/// vslots.
#[derive(Debug, Clone, Copy)]
struct DepthReport {
    at: Instant,
    depths: [u32; 256],
}

impl DepthReport {
    /// The report interval.
    const INTERVAL: Duration = Duration::from_secs(1);

    fn new(now: Instant) -> Self {
        Self {
            at: now,
            depths: [0; 256],
        }
    }

    /// Emit the changed vslots of `depths` for `partition`, if the interval
    /// has passed, and remember them.
    fn tick(&mut self, now: Instant, partition: u32, depths: &[u32; 256]) {
        if now.duration_since(self.at) < Self::INTERVAL {
            return;
        }
        self.at = now;
        (0u8..=u8::MAX)
            .zip(depths.iter().zip(self.depths.iter()))
            .filter(|(_, (new, old))| new != old)
            .for_each(|(vslot, (new, _))| metrics::record_pending_depth(partition, vslot, *new));
        self.depths = *depths;
    }
}

pub struct Sequencer {
    cfg: SequencerConfig,
    state: PartitionState<RefMetadata>,
    /// Lag detection and receipt-floor resync (see [`crate::resync`]).
    /// `None` when the binary did not wire the receipts and
    /// egress-watermark feeds (tests, IPC dev runs). Resync then does
    /// nothing.
    resync: Option<crate::resync::ResyncController>,
    /// Pre-registered metric handles. Recording through these handles
    /// does not allocate.
    hot: metrics::HotMetrics,
    /// Published-but-unconfirmed refs. Every published ref is retained
    /// until a receipt proves canonical commitment. See
    /// [`crate::unconfirmed`] for the ledger semantics and the
    /// expiry-queue mechanics.
    unconfirmed: UnconfirmedLedger<RefMetadata>,
    /// The nonce lookup seam. `None` when the binary has no executor
    /// endpoints (tests, IPC dev runs). See [`crate::lookup`].
    lookup: Option<LookupRequester>,
    /// The virtual slots this replica serves. The wrong-shard guard drops
    /// an envelope outside the set.
    vslots: VslotSet,
    /// The resize warm-up, while one is in progress.
    shadow: Option<ShadowWindow>,
    depth: DepthReport,
}

impl Sequencer {
    /// The constructor is the parse-once boundary: it validates `cfg`, so
    /// every other method can assume `partition_index < partition_count`.
    ///
    /// # Errors
    ///
    /// Returns an error if `cfg` fails [`SequencerConfig::validate`].
    pub fn new(cfg: SequencerConfig) -> Result<Self, ConfigError> {
        cfg.validate()?;
        let cap = cfg.max_pending_per_sender;
        let hot = metrics::HotMetrics::new(cfg.partition_index);
        let vslots = cfg.vslot_set()?;
        // The warm-up starts here. The binary constructs the sequencer
        // after it opened every lane subscription, so this is the moment
        // the old lanes are open. The margin in `shadow_warm` covers the
        // join.
        let now = Instant::now();
        let shadow = (!cfg.shadow_vslots.is_empty()).then(|| ShadowWindow {
            vslots: cfg.shadow_vslots,
            until: now + cfg.shadow_warm(),
        });
        metrics::record_shadow_vslots(cfg.partition_index, cfg.shadow_vslots.len());
        Ok(Self {
            state: PartitionState::new(cap, cfg.tx_ttl()),
            cfg,
            hot,
            resync: None,
            unconfirmed: UnconfirmedLedger::new(),
            lookup: None,
            vslots,
            shadow,
            depth: DepthReport::new(now),
        })
    }

    /// The virtual slots this replica serves.
    #[must_use]
    pub fn vslots(&self) -> &VslotSet {
        &self.vslots
    }

    /// The slots still in shadow mode. Empty outside a resize warm-up.
    #[must_use]
    pub fn shadow_vslots(&self) -> VslotSet {
        self.shadow.map_or(VslotSet::EMPTY, |s| s.vslots)
    }

    /// True when `sender` is in a shadow slot. Cheap outside a warm-up:
    /// no hash runs.
    #[inline]
    fn in_shadow(&self, sender: alloy_primitives::Address) -> bool {
        self.shadow
            .is_some_and(|s| s.vslots.contains(vslot_for(sender)))
    }

    /// Publish a `tx_errors` event, unless the sender is in shadow mode.
    /// The shard that publishes the refs also owns the errors.
    fn publish_error<R>(&self, rc: &mut R, err: TxError)
    where
        R: TxErrorPublisher,
    {
        if self.in_shadow(err.sender) {
            return;
        }
        rc.publish_error(err);
    }

    /// End shadow mode when the warm-up has passed. After the warm-up,
    /// this replica's buffers are a superset of the old shard's live
    /// state for the incoming slots, so publishing in parallel is safe:
    /// both shards publish identical refs, and the sealer dedups them.
    fn shadow_tick(&mut self, now: Instant) {
        let Some(window) = self.shadow.filter(|s| now >= s.until) else {
            return;
        };
        tracing::info!(
            vslots = %window.vslots,
            "shadow warm-up passed; publishing for the incoming vslots"
        );
        self.shadow = None;
        metrics::record_shadow_vslots(self.cfg.partition_index, 0);
    }

    /// Enable the nonce lookup. A park of a sender with no known floor
    /// then asks the lookup task for the sender's committed nonce.
    pub fn enable_nonce_lookup(&mut self, lookup: LookupRequester) {
        self.lookup = Some(lookup);
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

    #[must_use]
    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    /// Build the wire `TxRef` for a drained ref (shared by the single and
    /// batch publish paths).
    fn make_txref(meta: &RefMetadata) -> kardamom_types::TxRef {
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
        // Chunked batch publish; see `flush_chunk`'s doc for the chunk
        // size and the MTU budget behind it.
        let mut rest = std::collections::VecDeque::from(self.suppress_shadow(drained, ctx));
        while !rest.is_empty() {
            if let ControlFlow::Break(result) = self.flush_chunk(b, &mut rest, ctx) {
                return result;
            }
        }
        Ok(())
    }

    /// In shadow mode, the refs of the shadow slots advance the state
    /// machine and nothing else. The old shard publishes them. They
    /// enter no ledger: only the shard that offered a ref owns its
    /// republish. Per-sender order survives the split, and the order
    /// across senders does not matter to the canonical log. Returns the
    /// refs that stay to be offered.
    fn suppress_shadow(
        &mut self,
        drained: Vec<(alloy_primitives::Address, u64, RefMetadata)>,
        ctx: &'static str,
    ) -> Vec<(alloy_primitives::Address, u64, RefMetadata)> {
        if self.shadow.is_none() {
            return drained;
        }
        let (shadow, live): (Vec<_>, Vec<_>) = drained
            .into_iter()
            .partition(|(sender, _, _)| self.in_shadow(*sender));
        if !shadow.is_empty() {
            self.hot
                .shadow_suppressed
                .increment(usize_to_u64(shadow.len()));
            trace!(count = shadow.len(), ctx, "shadow mode: refs not offered");
        }
        live
    }

    /// One [`Self::flush_drained`] chunk: publish up to `BATCH_MAX` refs
    /// off the front of `rest`, and record what published. `Break` carries
    /// the flush's final error. `Continue` means the caller sends the next
    /// chunk.
    fn flush_chunk<B>(
        &mut self,
        b: &mut B,
        rest: &mut std::collections::VecDeque<(alloy_primitives::Address, u64, RefMetadata)>,
        ctx: &'static str,
    ) -> ControlFlow<Result<(), SequencerError>>
    where
        B: TxOrderingRefPublisher,
    {
        // Each chunk rides one cluster app message (KIND_BATCH), which
        // amortizes the per-offer session round trip that dominated the
        // sequencer's per-transaction cost. The chunk must stay under one
        // Aeron MTU (about 1408 bytes): the hand-rolled cluster ingress
        // path does not survive fragmented session messages. With the
        // guard header (sender 20 bytes, nonce 8 bytes), each entry is 75
        // bytes plus a 4 byte length prefix. 16 x 79 + 3 is about 1.27 KB,
        // which stays under the MTU with margin (20 x 79 + 3, about 1.58
        // KB, would not). A 16:1 ratio still amortizes away the dominant
        // per-offer cost.
        const BATCH_MAX: usize = 16;
        let chunk = BATCH_MAX.min(rest.len());
        let refs: Vec<(kardamom_types::TxRef, alloy_primitives::Address, u64)> = rest
            .iter()
            .take(chunk)
            .map(|(s, n, m)| (Self::make_txref(m), *s, *n))
            .collect();
        let (published, err) = b.try_publish_ref_batch(&refs);
        self.record_published_prefix(rest, published, ctx);
        match err {
            None => ControlFlow::Continue(()),
            Some(SequencerError::Backpressure) => {
                self.hot.backpressure.increment(1);
                self.rebuffer_rest(rest);
                ControlFlow::Break(Err(SequencerError::Backpressure))
            }
            Some(e) => ControlFlow::Break(Err(e)),
        }
    }

    /// Drain the `published` prefix off `rest`: bump the publish metric,
    /// trace each ref, and (if resync is active) record it in the
    /// unconfirmed ledger. Retain until a receipt proves canonical
    /// commitment. A batch acceptance is still only an offer, one
    /// `KIND_BATCH` app message on the publication buffer, not a Raft
    /// commit. The whole batch can vanish in a dead-leader window exactly
    /// like a single offer, so every ref in the accepted prefix enters
    /// the unconfirmed ledger individually.
    fn record_published_prefix(
        &mut self,
        rest: &mut std::collections::VecDeque<(alloy_primitives::Address, u64, RefMetadata)>,
        published: usize,
        ctx: &'static str,
    ) {
        for (sender, n, meta) in rest.drain(..published) {
            self.record_one_published(sender, n, meta, ctx);
        }
    }

    /// One published ref, for [`Self::record_published_prefix`]'s loop:
    /// bumps the publish metric, traces it, and (if resync is active)
    /// records it in the unconfirmed ledger.
    fn record_one_published(
        &mut self,
        sender: alloy_primitives::Address,
        n: u64,
        meta: RefMetadata,
        ctx: &'static str,
    ) {
        self.hot.publish.increment(1);
        trace!(
            nonce = n,
            correlation_id = meta.correlation_id,
            ctx,
            "published ref"
        );
        if self.resync.is_some() {
            self.unconfirmed.record_published(sender, n, meta);
        }
    }

    /// Rebuffer everything left in `rest`, in reverse, so each sender's
    /// floor ends rewound to its lowest unpublished nonce.
    fn rebuffer_rest(
        &mut self,
        rest: &mut std::collections::VecDeque<(alloy_primitives::Address, u64, RefMetadata)>,
    ) {
        while let Some((s, n2, m)) = rest.pop_back() {
            self.state.reinsert_for_retry(s, n2, m);
        }
    }

    /// Resync and unconfirmed-ledger bookkeeping. Runs first, on every
    /// `run_once` iteration, including idle ones, so boundary-silence
    /// detection keeps ticking.
    fn resync_tick(&mut self) {
        // Take `r` out of `self` for the duration of the two `&mut self`
        // calls below: `self.resync` and `self` (for `self.unconfirmed`,
        // `self.state`) cannot both be borrowed mutably at once through a
        // shared `&mut self` receiver.
        let Some(mut r) = self.resync.take() else {
            return;
        };
        self.apply_receipt_drain(&mut r);
        self.apply_contiguity_rejects(&mut r);
        self.resync = Some(r);
        self.sweep_confirm_timeouts();
    }

    /// Drain receipt floors, and advance the nonce state machine to any
    /// raised executed-truth floor. Dropping proven-duplicate buffered
    /// entries; newly contiguous runs surface through `drain_pending` in
    /// `run_once`.
    fn apply_receipt_drain(&mut self, r: &mut crate::resync::ResyncController) {
        let (raised, confirmations) = r.drain_floor_updates();
        // A receipt proves that every one of this sequencer's published
        // refs for that sender, at or below its nonce, committed. Drop
        // them from the unconfirmed ledger.
        for (sender, confirmed) in confirmations {
            self.unconfirmed.confirm_through(sender, confirmed);
        }
        for (sender, floor) in raised {
            self.apply_one_floor_update(sender, floor);
        }
        r.observe(Instant::now());
    }

    /// One receipt-proven floor update, for [`Self::apply_receipt_drain`]'s
    /// loop: advance the state machine's floor, and record the drops it
    /// proves, if the floor actually moved.
    fn apply_one_floor_update(&mut self, sender: alloy_primitives::Address, floor: u64) {
        let Some((from, dropped)) = self.state.advance_floor(sender, floor) else {
            return;
        };
        metrics::record_floor_advance(self.cfg.partition_index);
        // Every dropped buffered entry is a receipt-proven duplicate,
        // skipped without relying on any dedup window. This is the
        // spec's `resync_skipped_executed_total`. There is no separate
        // flush-time filter: floors drain before any publish action is
        // computed, so the state machine's floor is always current when
        // `process` runs. A proven-stale incoming envelope takes the
        // ordinary `Past`/DuplicatedTx path below, and is counted there.
        metrics::record_resync_skip(self.cfg.partition_index, usize_to_u64(dropped));
        trace!(
            sender = ?sender,
            from,
            floor,
            dropped,
            "resync: receipt floor advanced nonce state"
        );
    }

    /// The sealer rejected this sequencer's ref because its nonce was not
    /// the sender's expected next one. Two cases, split in the drain:
    ///
    /// - Committed-proof (nonce < expected): drop the ledger entry
    ///   exactly like a receipt confirmation. See
    ///   `UnconfirmedLedger::drop_committed` for the full story.
    /// - Gap (nonce >= expected): refs for expected..nonce-1 vanished
    ///   (voided offers). They are all in the unconfirmed ledger. Rewind
    ///   them now instead of waiting out the confirm timeout. The ledger
    ///   hands them back in rewind-safe descending order.
    fn apply_contiguity_rejects(&mut self, r: &mut crate::resync::ResyncController) {
        let (drops, rewinds) = r.drain_contiguity_rejects();
        for (sender, n) in drops {
            self.drop_committed_and_trace(sender, n);
        }
        for (sender, expected) in rewinds {
            self.rewind_one_gap(sender, expected);
        }
    }

    /// One committed-proof contiguity reject, for
    /// [`Self::apply_contiguity_rejects`]'s first loop.
    fn drop_committed_and_trace(&mut self, sender: alloy_primitives::Address, n: u64) {
        if self.unconfirmed.drop_committed(sender, n) {
            trace!(
                sender = ?sender,
                nonce = n,
                "contiguity reject proves commitment; dropping unconfirmed entry (#85)"
            );
        }
    }

    /// One gap contiguity reject, for
    /// [`Self::apply_contiguity_rejects`]'s second loop: rewind every
    /// unconfirmed ref the ledger holds for `sender` at or after
    /// `expected`, if any.
    fn rewind_one_gap(&mut self, sender: alloy_primitives::Address, expected: u64) {
        let taken = self.unconfirmed.take_gap_rewinds(sender, expected);
        if taken.is_empty() {
            return;
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

    /// Rewind refs past the confirm timeout for republish. See
    /// `UnconfirmedLedger::sweep_expired` for why re-offering is safe (no
    /// gap, no loss). Bounded per iteration.
    fn sweep_confirm_timeouts(&mut self) {
        let timeout = std::time::Duration::from_millis(self.cfg.resync.confirm_timeout_ms);
        let now = Instant::now();
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

    /// Ask for the committed nonce of `sender` after a park, when no
    /// receipt has proven a floor for it yet. A cold replica heals through
    /// this. A sender with a known floor parks for an ordinary reason (a
    /// gap the client has not filled), and a lookup would say nothing
    /// new. The lookup task dedups senders and bounds the rate, so a
    /// request per park is cheap. See [`crate::lookup`].
    fn request_lookup(&mut self, sender: alloy_primitives::Address) {
        let floor_known = self
            .resync
            .as_ref()
            .is_some_and(|r| r.floor(sender).is_some());
        let Some(lookup) = self.lookup.as_mut().filter(|_| !floor_known) else {
            return;
        };
        self.hot.lookup_requests.increment(1);
        lookup.request(sender);
    }

    /// Expire the parked entries whose lifetime (`tx_ttl`) has passed, and
    /// tell their parked submit calls and receipt subscribers. An entry
    /// waits on a nonce gap for at most `tx_ttl`. After that, the client
    /// gets an explicit error and can resubmit once the gap fills. This
    /// runs on every iteration, so an idle sequencer expires on time. The
    /// sweep is bounded per iteration. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.3.
    fn expiry_tick<R>(&mut self, rc: &mut R)
    where
        R: TxErrorPublisher,
    {
        let now = Instant::now();
        for (sender, nonce) in self.state.sweep_expired(now, 256) {
            self.report_expired(rc, sender, nonce);
        }
        self.shadow_tick(now);
        let depths = self.state.pending_depth_by(vslot_for);
        self.depth.tick(now, self.cfg.partition_index, &depths);
    }

    /// One expired entry, for [`Self::expiry_tick`]'s loop: count it,
    /// trace it, and report `Expired` to the client.
    fn report_expired<R>(&self, rc: &mut R, sender: alloy_primitives::Address, nonce: u64)
    where
        R: TxErrorPublisher,
    {
        self.hot.expired.increment(1);
        trace!(
            sender = ?sender,
            nonce,
            "pending entry expired after tx_ttl; reporting Expired"
        );
        let expected_nonce = self.state.next_nonce(sender);
        self.publish_error(
            rc,
            TxError {
                sender,
                nonce,
                reason: TxErrorReason::Expired { expected_nonce },
            },
        );
    }

    /// Tell an evicted transaction's parked submit call, and any receipt
    /// subscribers, that it will never be sequenced. A silent eviction
    /// would leave the client waiting forever, with its later nonces
    /// permanently gapped.
    fn report_evicted<R>(&self, rc: &mut R, sender: alloy_primitives::Address, nonce: u64)
    where
        R: TxErrorPublisher,
    {
        let expected_nonce = self.state.next_nonce(sender);
        self.publish_error(
            rc,
            TxError {
                sender,
                nonce,
                reason: TxErrorReason::Evicted { expected_nonce },
            },
        );
    }

    /// Drive one ingress message through the state machine. Returns
    /// `Ok(true)` if it did work, or `Ok(false)` if the retry-drain and
    /// the `tx_data` poll were both empty.
    ///
    /// Order of operations:
    ///  1. First flush any metadata sitting at `pending[next_nonce]` (these
    ///     are the entries rebuffered after backpressure). If the B
    ///     publish blocks again, rewind again and return `Backpressure`
    ///     without touching `tx_data`.
    ///  2. Then poll `tx_data` for the next observed envelope and process it.
    ///
    /// # Errors
    ///
    /// Returns `Err(SequencerError::Backpressure)` if the ref publisher
    /// blocks, or `Err(SequencerError::IngressDisconnected)` if the
    /// `tx_data` subscription closes.
    pub fn run_once<P: SequencerPorts>(&mut self, ports: &mut P) -> Result<bool, SequencerError> {
        // Resync bookkeeping runs first, every iteration. See `resync_tick`.
        self.resync_tick();
        let (channel_a, b, rc) = ports.split();
        self.expiry_tick(rc);

        let pending = self.state.drain_pending();
        if !pending.is_empty() {
            self.flush_drained(b, pending, "drain-pending")?;
            return Ok(true);
        }

        // A sender with an unfillable nonce gap stalls here, recoverably.
        // A local fast-forward past the gap would adopt it into the
        // canonical stream and fatally hit NonceTooHigh on every executor.
        // See PartitionState's note.

        let Some(Inbound {
            lane,
            loc: tx_data_loc,
            envelope,
        }) = channel_a.poll()?
        else {
            return Ok(false);
        };
        self.hot.ingest.increment(1);

        let sender = sender_of(&envelope);

        // The wrong-shard guard: drop an envelope whose vslot is not in
        // this replica's set. A routing disagreement between the ingress
        // and the sequencer would otherwise corrupt nonce state silently.
        // During a resize, a new shard reads the old lanes whole, so most
        // envelopes there belong to other shards. That is the normal
        // case, so this is a counter and a trace, not a warning.
        let vslot = vslot_for(sender);
        if !self.vslots.contains(vslot) {
            self.hot.wrong_shard.increment(1);
            trace!(
                lane,
                vslot, "tx_data envelope outside this replica's vslots; skipping"
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
            lane,
            tx_data_position: tx_data_loc.position,
            tx_data_session_id: tx_data_loc.session_id,
        };

        let t0 = Instant::now();
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
    /// the publish actions for `flush_drained`.
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
                self.request_lookup(sender);
            }
            NonceOutcome::BufferedDisabled => {
                self.hot.buffered_future.increment(1);
            }
            NonceOutcome::BufferedEvicting { evicted_nonce } => {
                self.hot.buffered_future.increment(1);
                self.hot.evictions.increment(1);
                self.request_lookup(sender);
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
            NonceOutcome::Past => self.record_past_outcome(sender, nonce),
        }

        self.collect_publishes(rc, sender, result.actions)
    }

    /// Record bookkeeping for a `Past` outcome. Two different things
    /// surface here, and must stay distinct:
    ///
    /// - A receipt-proven skip (`proven_executed`): the resync mechanism
    ///   absorbing a duplicate. This is routine for a twin that falls
    ///   slightly behind under load. It is not sequencer dirt
    ///   (`dropped_past` stays flat, and `seq_clean` holds), and not a
    ///   client error (the transaction succeeded; a `DuplicatedTx` notice
    ///   would be spurious and could race the receipt at ingress).
    /// - An ordinary client double-submit or stale nonce: no floor proof.
    ///   Count it, and report it.
    fn record_past_outcome(&mut self, sender: alloy_primitives::Address, nonce: u64) {
        if self.proven_executed(sender, nonce) {
            metrics::record_resync_skip(self.cfg.partition_index, 1);
        } else {
            self.hot.dropped_past.increment(1);
        }
    }

    /// Turn one process result's actions into the `(sender, nonce,
    /// payload)` publish list, reporting each non-proven duplicate on
    /// `rc` along the way. Split out of [`Self::handle_outcome`], whose
    /// `match` over [`NonceOutcome`] above is a separate concern from
    /// this loop over [`ProcessAction`]s.
    fn collect_publishes<R>(
        &self,
        rc: &mut R,
        sender: alloy_primitives::Address,
        actions: Vec<ProcessAction<RefMetadata>>,
    ) -> Vec<(alloy_primitives::Address, u64, RefMetadata)>
    where
        R: TxErrorPublisher,
    {
        let mut publishes = Vec::new();
        for action in actions {
            self.collect_one_action(rc, sender, action, &mut publishes);
        }
        publishes
    }

    /// One process action, for [`Self::collect_publishes`]'s loop: a
    /// publish appends to `publishes`; a non-proven duplicate reports on
    /// `rc`. A receipt-proven skip is suppressed: the client's
    /// transaction already executed, so there is nothing to report.
    fn collect_one_action<R>(
        &self,
        rc: &mut R,
        sender: alloy_primitives::Address,
        action: ProcessAction<RefMetadata>,
        publishes: &mut Vec<(alloy_primitives::Address, u64, RefMetadata)>,
    ) where
        R: TxErrorPublisher,
    {
        match action {
            ProcessAction::Publish { nonce: n, payload } => {
                publishes.push((sender, n, payload));
            }
            ProcessAction::ReportDuplicate {
                nonce: n,
                expected_nonce,
            } => self.report_duplicate_if_unproven(rc, sender, n, expected_nonce),
        }
    }

    /// Report a duplicate submission on `rc`, unless a receipt already
    /// proves this sender and nonce executed. A proven duplicate is
    /// routine and stays silent.
    fn report_duplicate_if_unproven<R>(
        &self,
        rc: &mut R,
        sender: alloy_primitives::Address,
        nonce: u64,
        expected_nonce: u64,
    ) where
        R: TxErrorPublisher,
    {
        if self.proven_executed(sender, nonce) {
            return;
        }
        self.publish_error(
            rc,
            TxError {
                sender,
                nonce,
                reason: TxErrorReason::DuplicatedTx { expected_nonce },
            },
        );
    }

    /// Pin this thread to the configured core (if any) and loop until
    /// shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if a `run_once` iteration fails with anything
    /// other than backpressure or a disconnected `tx_data` subscription
    /// (both handled internally).
    pub fn run<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        shutdown: &Shutdown,
    ) -> Result<(), SequencerError> {
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
        while !shutdown.is_signaled() && self.run_tick(ports, &mut backoff)? {}
        Ok(())
    }

    /// One [`Self::run`] iteration: dispatch on `run_once`'s outcome,
    /// updating the resync controller and idle backoff. Returns whether
    /// the loop should keep going; `false` only on a clean
    /// `IngressDisconnected` exit.
    fn run_tick<P: SequencerPorts>(
        &mut self,
        ports: &mut P,
        backoff: &mut kardamom_log::aeron_live::IdleBackoff,
    ) -> Result<bool, SequencerError> {
        match self.run_once(ports) {
            Ok(true) => {
                backoff.reset();
                self.resync_note_publish_ok();
                Ok(true)
            }
            Ok(false) => {
                self.resync_note_publish_ok();
                std::thread::sleep(backoff.idle_wait());
                Ok(true)
            }
            Err(SequencerError::Backpressure) => {
                // Sustained backpressure (including a not-yet-reconnected
                // cluster session, which maps here) is the publish-stall
                // resync trigger.
                self.resync_note_publish_stall();
                std::thread::sleep(Duration::from_micros(10));
                Ok(true)
            }
            Err(SequencerError::IngressDisconnected) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Tell the resync controller, if one is wired, that a publish
    /// succeeded this tick.
    fn resync_note_publish_ok(&mut self) {
        if let Some(r) = self.resync.as_mut() {
            r.note_publish_ok();
        }
    }

    /// Tell the resync controller, if one is wired, that a publish
    /// stalled on backpressure this tick.
    fn resync_note_publish_stall(&mut self) {
        if let Some(r) = self.resync.as_mut() {
            r.note_publish_stall(Instant::now());
        }
    }
}
