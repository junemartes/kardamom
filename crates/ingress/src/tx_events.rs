//! The `accepted` records of `tx_events`
//! (`docs/specs/2026-10-25-client-events.md`, 4.8).
//!
//! The ingress publishes each transaction from its own request task, so it
//! has no poll cycle to put a frame on. After the `tx_data` publish, the
//! task pushes the hash into a bounded queue with no lock. One task takes
//! the queue each [`FRAME_MAX_AGE`] into the frame, and offers the frame
//! once. The request task pays one copy of 32 bytes and one atomic
//! operation. A full queue drops the hash and counts it. Nothing here ever
//! waits, so the stream never slows a submit.

use std::sync::Arc;

use alloy_primitives::B256;
use crossbeam_queue::ArrayQueue;
use kardamom_log::aeron_live::{FRAME_MAX_AGE, FrameOffer, TxEventsPublisherHandle, TxEventsSink};
use metrics::Counter;

/// The hashes waiting for the next frame: about 2 MiB, more than 60 ms of
/// the target rate of 1,000,000 tx/s.
const QUEUE_HASHES: usize = 65_536;

/// The counter of the hashes that a full queue dropped.
const QUEUE_FULL_TOTAL: &str = "kardamom_tx_events_queue_full_total";

/// The queue of the `accepted` records. A clone shares the queue.
#[derive(Clone)]
pub struct AcceptedQueue {
    hashes: Arc<ArrayQueue<B256>>,
    /// `kardamom_tx_events_queue_full_total{origin}`, registered once.
    full: Counter,
}

impl AcceptedQueue {
    /// An empty queue of the ingress `origin`.
    #[must_use]
    pub fn new(origin: u16) -> Self {
        Self {
            hashes: Arc::new(ArrayQueue::new(QUEUE_HASHES)),
            full: metrics::counter!(QUEUE_FULL_TOTAL, "origin" => origin.to_string()),
        }
    }

    /// Start the task that sends the frames of `publisher` for `origin`,
    /// and return the queue that feeds it.
    #[must_use]
    pub fn spawn(publisher: TxEventsPublisherHandle, origin: u16) -> Self {
        let queue = Self::new(origin);
        let drain = queue.clone();
        tokio::spawn(async move {
            let mut sink = TxEventsSink::new(publisher, origin);
            let mut tick = tokio::time::interval(FRAME_MAX_AGE);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                drain.drain_into(&mut sink);
            }
        });
        queue
    }

    /// Queue the `accepted` record of `tx_hash`. Never waits.
    pub fn note(&self, tx_hash: B256) {
        if self.hashes.push(tx_hash).is_err() {
            self.full.increment(1);
        }
    }

    /// Move every queued hash into `sink`, then send its frame.
    pub fn drain_into<P: FrameOffer>(&self, sink: &mut TxEventsSink<P>) {
        while let Some(hash) = self.hashes.pop() {
            sink.accepted(&hash);
        }
        sink.flush();
    }
}
