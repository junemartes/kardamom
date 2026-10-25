//! Outbound channel abstractions.
//!
//! Under the MDS topology, the sequencer does not publish to `tx_data`.
//! That is the proxy's job. The sequencer only reads A and publishes
//! B-refs: for each envelope it sees on its shard's `tx_data` (through
//! [`crate::inbound::TxDataSubscriber`]), if the nonce gate matches, it
//! publishes a tiny [`kardamom_types::TxRef`] onto the canonical orderer
//! `tx_ordering` ([`TxOrderingRefPublisher`]). This is an Aeron concurrent
//! multi-publisher stream, ordered together with refs from the other P-1
//! sequencers in this shard's group and sealer-emitted
//! `BlockBoundaryStart` markers.
//!
//! Pending-buffer state advances once the B publish succeeds. If B applies
//! backpressure, the state machine rewinds through
//! [`crate::state::PartitionState::reinsert_for_retry`], and the error
//! bubbles up.
//!
//! The sequencer also emits a [`types::TxError`] on the dedicated
//! `tx_errors` channel ([`SideChannelPublisher`]) when an inbound
//! transaction fails the nonce gate (today: past-nonce or duplicate; more
//! variants may come later). Ingress reads that channel and releases the
//! parked client right away with a JSON-RPC error. The same port carries
//! the `tx_status` stream: an `Offered` status for every ref the
//! sequencer offers, and a `Rejected` status next to every error. It also
//! carries the `tx_drops` stream: one [`types::TxDrop`] for every
//! transaction that the sequencer drops, for the status that the egress
//! serves.
//!
//! All surfaces are traits, so unit tests can use the in-memory fakes (no
//! Aeron media driver needed). Production wiring binds them to the real
//! `kardamom_log::publisher` types.

pub mod cluster;

use std::time::Instant;

use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::GuardHeader;
use kardamom_log::aeron_live::{
    TxDropsPublisherHandle, TxErrorsPublisherHandle, TxEventsSink, TxStatusPublisherHandle,
};
use kardamom_obs::pipeline::Step;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{EpochRecord, TxDrop, TxError, TxRef, TxStatus};

use crate::error::SequencerError;

/// One transaction ref offered to the canonical orderer, with the guard
/// header the sealer reads: `sender` and `nonce` for its per-sender
/// contiguity guard, and `max_inclusion_block` for its inclusion check.
/// None of the three is part of the relayed record.
#[derive(Clone, Debug)]
pub struct RefOffer {
    pub tx_ref: TxRef,
    /// The sealer's guard header: the sender and nonce for the contiguity
    /// guard, the inclusion deadline, and the tip the record bids.
    pub guard: GuardHeader,
    /// The archived `TxEnvelope` of the transaction, for a sealer that
    /// carries the bytes. With it, the offer is a full reference (record
    /// type 7); without it, a reference (record type 0).
    pub envelope: Option<bytes::Bytes>,
}

/// `TxOrdering` publisher contract, the canonical orderer. Publishes tiny
/// [`TxRef`]s for L2 transactions (about 41 bytes) into Aeron's concurrent
/// multi-publisher stream, plus whole [`EpochRecord`]s for L1 epochs and
/// [`RemoteEpochRecord`]s for peer chains. Every lane shares the
/// `tx_ordering` channel, so all three interleave in one canonical order.
///
/// A blocked transport must surface as `Err(SequencerError::Backpressure)`,
/// so the state machine can rewind.
pub trait TxOrderingRefPublisher: Send {
    /// The offer's guard header rides the ingress frame, for the sealer's
    /// per-sender contiguity guard and its inclusion check. It is not part
    /// of the relayed record.
    ///
    /// # Errors
    ///
    /// Returns `Err(SequencerError::Backpressure)` if the transport is
    /// blocked.
    fn try_publish_ref(&mut self, offer: &RefOffer) -> Result<(), SequencerError>;

    /// Publish a run of refs. This amortizes per-offer overhead where the
    /// transport supports it. Returns `(published, error)`: the first
    /// `published` refs are durably offered, and an error applies to the
    /// rest. The default loops over single refs (fakes and non-batching
    /// transports keep exact semantics). The cluster transport packs the
    /// whole slice into one `KIND_BATCH` app message (all-or-nothing per
    /// call).
    fn try_publish_ref_batch(&mut self, refs: &[RefOffer]) -> (usize, Option<SequencerError>) {
        for (i, offer) in refs.iter().enumerate() {
            if let Err(e) = self.try_publish_ref(offer) {
                return (i, Some(e));
            }
        }
        (refs.len(), None)
    }

    /// Publish an [`EpochRecord`] observed on `tx_deposits` as an
    /// origin-advancing record. The sealer closes the open block, adopts
    /// the epoch's L1 number, then relays it, so the epoch's deposits lead
    /// a block. This has the same backpressure semantics as
    /// `try_publish_ref`. Epochs are not nonce-gated and have no pending
    /// state to rewind, so on `Backpressure` the caller retries the same
    /// epoch on the next tick.
    ///
    /// # Errors
    ///
    /// Returns `Err(SequencerError::Backpressure)` if the transport is
    /// blocked.
    fn try_publish_epoch(&mut self, e: &EpochRecord) -> Result<(), SequencerError>;

    /// Publish a [`RemoteEpochRecord`] observed on `tx_remote_epochs` as a
    /// REMOTE-ORIGIN-ADVANCING record: [`Self::try_publish_epoch`] for a peer
    /// Kardamom chain rather than L1, so the sealer tracks the adopted
    /// position per origin chain instead of stamping it into boundaries.
    /// Same backpressure semantics — nothing is nonce-gated and there is no
    /// pending state to rewind, so on `Backpressure` the caller retries the
    /// same record next tick.
    ///
    /// # Errors
    ///
    /// Returns `Err(SequencerError::Backpressure)` if the transport is
    /// blocked.
    fn try_publish_remote_epoch(&mut self, r: &RemoteEpochRecord) -> Result<(), SequencerError>;
}

/// The `tx_drops` channel: one record for each transaction that the
/// sequencer drops. Best effort: a failure is logged, and the egress then
/// has no status for the hash.
pub trait DropPublisher: Send {
    /// Send one drop record.
    fn send_drop(&mut self, d: TxDrop);

    /// Count the drop in `kardamom_sequencer_tx_drops_total{reason}`, and
    /// send it.
    fn publish_drop(&mut self, d: TxDrop) {
        crate::metrics::record_tx_drop(d.reason);
        self.send_drop(d);
    }
}

/// The side channels: `tx_errors`, `tx_status` and `tx_drops`. All are
/// best effort. The caller logs a failure and does not propagate it. The
/// canonical state has already advanced, or the inbound transaction was
/// rejected, so there is nothing to roll back.
pub trait SideChannelPublisher: DropPublisher {
    fn publish_error(&mut self, e: TxError);
    fn publish_status(&mut self, s: TxStatus);

    /// Append a `sent` record of `tx_events` for an offered reference. A
    /// port with no `tx_events` stream does nothing.
    fn note_sent(&mut self, _tx_hash: &B256) {}

    /// Send the `tx_events` frames that are due. The loop calls it once for
    /// each iteration. A port with no `tx_events` stream does nothing.
    fn flush_events(&mut self) {}
}

/// The live `tx_drops` channel. A clone shares the publication, so the
/// publish loop and the egress feed thread publish on one stream.
impl DropPublisher for TxDropsPublisherHandle {
    fn send_drop(&mut self, d: TxDrop) {
        if let Err(err) = self.publish_best_effort(&d) {
            tracing::warn!(error = %err, "tx_drops publish failed (dropped)");
        }
    }
}

/// The live side channels. The sequencer publishes rejections on the
/// `tx_errors` Aeron channel, which the ingress reads to release parked
/// clients early, statuses on the `tx_status` channel, which the
/// notifier reads, and drops on the `tx_drops` channel, which the egress
/// reads. A publish failure is logged and dropped: the canonical state has
/// already advanced, or the transaction was rejected, so there is nothing
/// to roll back.
pub struct SideChannels {
    errors: TxErrorsPublisherHandle,
    status: TxStatusPublisherHandle,
    drops: TxDropsPublisherHandle,
    /// One `tx_errors` publish. The publish waits for the offer, so the
    /// publish loop pays it for each refused transaction.
    errors_publish: Step,
    /// The `tx_events` frames of this replica, with
    /// `KARDAMOM_TX_EVENTS=on`: `sent`, best effort.
    events: Option<TxEventsSink>,
    /// The lane of the `sent` records.
    lane: u8,
}

impl SideChannels {
    #[must_use]
    pub fn new(
        errors: TxErrorsPublisherHandle,
        status: TxStatusPublisherHandle,
        drops: TxDropsPublisherHandle,
    ) -> Self {
        Self {
            errors,
            status,
            drops,
            errors_publish: Step::new("sequencer.tx_errors_publish"),
            events: None,
            lane: 0,
        }
    }

    /// Publish the `sent` records of `lane` on `tx_events` through `sink`
    /// (`KARDAMOM_TX_EVENTS=on`).
    #[must_use]
    pub fn with_tx_events(mut self, sink: TxEventsSink, lane: u8) -> Self {
        self.events = Some(sink);
        self.lane = lane;
        self
    }
}

impl SideChannelPublisher for SideChannels {
    fn publish_error(&mut self, e: TxError) {
        let started = Instant::now();
        let published = self.errors.publish(&e);
        self.errors_publish.since(started);
        if let Err(err) = published {
            tracing::warn!(error = %err, "tx_errors publish failed (dropped)");
        }
    }

    fn publish_status(&mut self, s: TxStatus) {
        if let Err(err) = self.status.publish_best_effort(&s) {
            tracing::warn!(error = %err, "tx_status publish failed (dropped)");
        }
    }

    fn note_sent(&mut self, tx_hash: &B256) {
        if let Some(events) = self.events.as_mut() {
            events.sent(tx_hash, self.lane);
        }
    }

    fn flush_events(&mut self) {
        if let Some(events) = self.events.as_mut() {
            events.flush_due();
        }
    }
}

impl DropPublisher for SideChannels {
    fn send_drop(&mut self, d: TxDrop) {
        self.drops.send_drop(d);
    }
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use alloy_primitives::B256;
    use kardamom_log::aeron_live::{FrameOffer, TxEventsSink};
    use kardamom_types::tx_events::TxEventsView;
    use kardamom_types::xchain::RemoteEpochRecord;
    use kardamom_types::{EpochRecord, TxRef, TxStatus};

    use super::{
        DropPublisher, RefOffer, SequencerError, SideChannelPublisher, TxDrop, TxError,
        TxOrderingRefPublisher,
    };

    /// In-memory `tx_ordering` publisher. Records every published offer,
    /// `EpochRecord`, and `RemoteEpochRecord` in arrival order, so tests
    /// can check the canonical sequence.
    #[derive(Default, Clone)]
    pub struct InMemoryTxOrderingRefPublisher {
        /// Every published offer whole, guard header included. One vector
        /// holds the whole record: the allocation harness reserves it,
        /// so the fake costs the measured loop nothing.
        pub offers: Arc<Mutex<Vec<RefOffer>>>,
        pub epochs: Arc<Mutex<Vec<EpochRecord>>>,
        pub remote_epochs: Arc<Mutex<Vec<RemoteEpochRecord>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
        /// The next ref offer reports a new session, as a fenced cluster
        /// publisher does, and takes nothing. The flag then clears.
        pub new_session: Arc<Mutex<bool>>,
    }

    impl TxOrderingRefPublisher for InMemoryTxOrderingRefPublisher {
        fn try_publish_ref(&mut self, offer: &RefOffer) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            if std::mem::take(&mut *self.new_session.lock().unwrap()) {
                return Err(SequencerError::SessionChanged);
            }
            self.offers.lock().unwrap().push(offer.clone());
            Ok(())
        }

        fn try_publish_epoch(&mut self, e: &EpochRecord) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.epochs.lock().unwrap().push(e.clone());
            Ok(())
        }

        fn try_publish_remote_epoch(
            &mut self,
            r: &RemoteEpochRecord,
        ) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.remote_epochs.lock().unwrap().push(r.clone());
            Ok(())
        }
    }

    impl InMemoryTxOrderingRefPublisher {
        /// The `TxRef` of every published offer, in arrival order.
        ///
        /// # Panics
        ///
        /// Panics if the offers mutex is poisoned.
        #[must_use]
        pub fn refs(&self) -> Vec<TxRef> {
            self.offers
                .lock()
                .unwrap()
                .iter()
                .map(|o| o.tx_ref)
                .collect()
        }
    }

    /// In-memory side channels. Records every error, every status and
    /// every drop in arrival order.
    #[derive(Default, Clone)]
    pub struct InMemorySidePublisher {
        pub errors: Arc<Mutex<Vec<TxError>>>,
        pub statuses: Arc<Mutex<Vec<TxStatus>>>,
        pub drops: Arc<Mutex<Vec<TxDrop>>>,
        /// The `tx_events` sink, when a test turns the stream on. It writes
        /// the real frames and counts the offers.
        pub events: Option<Arc<Mutex<TxEventsSink<CountedOffers>>>>,
    }

    impl InMemorySidePublisher {
        /// Turn the `tx_events` stream on, with `offers` counting the
        /// frames that the sink sends.
        #[must_use]
        pub fn with_tx_events(mut self, offers: CountedOffers) -> Self {
            let sink = TxEventsSink::new(offers, 0);
            self.events = Some(Arc::new(Mutex::new(sink)));
            self
        }
    }

    /// Counts the frames and the records that a sink offers. Allocates
    /// nothing on an offer.
    #[derive(Default, Clone)]
    pub struct CountedOffers {
        pub frames: Arc<AtomicUsize>,
        pub sent: Arc<AtomicUsize>,
    }

    impl FrameOffer for CountedOffers {
        fn offer_frame(&self, frame: &[u8]) {
            let view = TxEventsView::parse(frame).expect("the sink writes valid frames");
            self.frames.fetch_add(1, Ordering::Relaxed);
            self.sent.fetch_add(view.len(), Ordering::Relaxed);
        }
    }

    impl SideChannelPublisher for InMemorySidePublisher {
        fn publish_error(&mut self, e: TxError) {
            self.errors.lock().unwrap().push(e);
        }

        fn publish_status(&mut self, s: TxStatus) {
            self.statuses.lock().unwrap().push(s);
        }

        fn note_sent(&mut self, tx_hash: &B256) {
            if let Some(events) = &self.events {
                events.lock().unwrap().sent(tx_hash, 0);
            }
        }

        fn flush_events(&mut self) {
            if let Some(events) = &self.events {
                events.lock().unwrap().flush_due();
            }
        }
    }

    impl DropPublisher for InMemorySidePublisher {
        fn send_drop(&mut self, d: TxDrop) {
            self.drops.lock().unwrap().push(d);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use alloy_primitives::{Address, B256};
    use kardamom_types::{BPosition, TxError, TxErrorReason, TxRef};

    /// A ref offer at `nonce`, with no deadline.
    fn offer(nonce: u64) -> RefOffer {
        RefOffer {
            envelope: None,
            tx_ref: TxRef::new(
                B256::ZERO,
                u8::try_from(nonce).unwrap(),
                BPosition::default(),
                0,
            ),
            guard: GuardHeader {
                nonce,
                ..GuardHeader::EXEMPT
            },
        }
    }

    #[test]
    fn fake_b_records_refs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_ref(&offer(0)).unwrap();
        p.try_publish_ref(&offer(1)).unwrap();
        assert_eq!(p.refs().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_ref(&offer(0)),
            Err(SequencerError::Backpressure)
        ));
    }

    #[test]
    fn fake_side_channels_record_errors_and_statuses() {
        let mut p = InMemorySidePublisher::default();
        let error = TxError {
            sender: Address::repeat_byte(0xAB),
            nonce: 7,
            reason: TxErrorReason::DuplicatedTx { expected_nonce: 10 },
        };
        p.publish_status(TxStatus::rejected(B256::repeat_byte(0x01), &error));
        p.publish_error(error);
        assert_eq!(p.errors.lock().unwrap().len(), 1);
        assert_eq!(p.statuses.lock().unwrap().len(), 1);
    }

    /// Each drop counts once in `kardamom_sequencer_tx_drops_total`, with
    /// the label of its reason.
    #[test]
    fn publish_drop_counts_the_drop_by_reason() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let mut p = InMemorySidePublisher::default();
        metrics::with_local_recorder(&recorder, || {
            p.publish_drop(TxDrop {
                tx_hash: B256::repeat_byte(0x01),
                sender: Address::repeat_byte(0xAB),
                nonce: 7,
                reason: kardamom_types::DropReason::SealerRefusedReference,
            });
        });
        let text = recorder.handle().render();
        assert!(
            text.lines().any(|line| line
                == r#"kardamom_sequencer_tx_drops_total{reason="sealer_refused_reference"} 1"#),
            "{text}"
        );
        assert_eq!(p.drops.lock().unwrap().len(), 1);
    }

    fn test_epoch(n: u64) -> EpochRecord {
        EpochRecord {
            l1_number: n,
            l1_hash: B256::repeat_byte(u8::try_from(n).unwrap()),
            deposits: Vec::new(),
        }
    }

    #[test]
    fn fake_b_records_epochs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_epoch(&test_epoch(7)).unwrap();
        assert_eq!(p.epochs.lock().unwrap().len(), 1);
    }

    #[test]
    fn fake_b_epochs_back_off_under_pressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_epoch(&test_epoch(1)),
            Err(SequencerError::Backpressure)
        ));
    }
}
