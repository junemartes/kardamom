//! The pipeline metrics of the publish loop. The v1 and the v2 loop share
//! them, so the dashboard reads one set of names for either loop. See
//! `kardamom_obs::pipeline`.
//!
//! - The loop `sequencer.main` splits each pass by the outcome of the
//!   pass: busy when it did work, idle when it found no input, and
//!   blocked when the sealer refused the offer.
//! - The step `sequencer.offer_round_trip` is one offer to the sealer: the
//!   request to the session thread, the Aeron publish there, and the
//!   reply.
//! - The batch `sequencer.offer_refs` is the refs that one offer took.
//! - The queue `sequencer.tx_data` is the envelopes that wait in the
//!   `tx_data` lanes.

use std::time::Instant;

use kardamom_obs::pipeline::{Batch, LoopMeter, LoopState, Queue, Step};

use crate::error::SequencerError;
use crate::inbound::TxDataSubscriber;
use crate::outbound::{RefOffer, TxOrderingRefPublisher};

/// The loop sets its queue gauges once in this many passes. A pass takes
/// some microseconds under load and at most the idle cap when quiet, so
/// the gauges move at least once in some milliseconds.
const DEPTH_EVERY: u32 = 64;

/// The pipeline metrics of one publish loop. The loop owns them.
pub(crate) struct PublishMeters {
    main: LoopMeter,
    round_trip: Step,
    offer_refs: Batch,
    tx_data: Queue,
    /// The passes since the last queue gauge update.
    passes: u32,
}

impl PublishMeters {
    pub(crate) fn new() -> Self {
        Self {
            main: LoopMeter::new("sequencer.main"),
            round_trip: Step::new("sequencer.offer_round_trip"),
            offer_refs: Batch::new("sequencer.offer_refs"),
            tx_data: Queue::new("sequencer.tx_data"),
            passes: 0,
        }
    }

    /// The state of a pass with `outcome`. A back-pressured pass has work
    /// that the sealer does not take.
    pub(crate) fn state_of(outcome: &Result<bool, SequencerError>) -> LoopState {
        match outcome {
            Ok(false) => LoopState::Idle,
            Err(SequencerError::Backpressure) => LoopState::Blocked,
            Ok(true) | Err(_) => LoopState::Busy,
        }
    }

    /// Charge the pass that ends now, its wait included, to `state`.
    /// Returns whether the queue gauges are due in this pass. When they
    /// are, it sets the depth of `input`.
    pub(crate) fn pass<I: TxDataSubscriber>(&mut self, state: LoopState, input: &I) -> bool {
        self.main.mark(state);
        self.passes = self.passes.wrapping_add(1);
        if self.passes < DEPTH_EVERY {
            return false;
        }
        self.passes = 0;
        self.tx_data.set(input.queued());
        true
    }

    /// Offer `offers` through `refs`, and record the round trip and the
    /// refs that the offer took. An offer that took nothing records no
    /// batch.
    pub(crate) fn offer<B: TxOrderingRefPublisher>(
        &self,
        refs: &mut B,
        offers: &[RefOffer],
    ) -> (usize, Option<SequencerError>) {
        let started = Instant::now();
        let (published, err) = refs.try_publish_ref_batch(offers);
        self.round_trip.since(started);
        if published > 0 {
            self.offer_refs.record(published);
        }
        (published, err)
    }
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
