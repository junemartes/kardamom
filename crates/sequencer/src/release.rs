//! The nonce releases that the sequencer sends to the sealer.
//!
//! The sealer's contiguity guard counts every ordered entry, and a skip
//! uses no nonce on the chain. So for each skip receipt of its senders,
//! the sequencer tells the sealer the nonce that the chain expects next.
//! The sealer applies a release once for each skipped entry, and both
//! twins of a lane send it.
//!
//! The sender holds two bounds:
//!
//! - One release for each skipped entry: the hashes of the recent
//!   releases drop a second skip receipt of the same entry, for example
//!   after a receipt resubscribe.
//! - A queue for the releases that the transport did not take yet. A full
//!   queue drops its oldest release. The twin sends the same release, and
//!   the sealer applies the first copy that reaches it.

use std::collections::{HashSet, VecDeque};

use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::NonceRelease;
use tracing::warn;

use crate::error::SequencerError;
use crate::metrics;
use crate::outbound::TxOrderingRefPublisher;

/// How many hashes of sent releases the sender remembers.
const RECENT: usize = 4096;
/// How many releases wait for the transport at most.
const QUEUE: usize = 1024;

/// The releases of one replica: a queue and the recent skipped entries.
#[derive(Debug)]
pub(crate) struct ReleaseSender {
    partition: u32,
    /// The releases that wait for the transport, oldest first.
    queue: VecDeque<NonceRelease>,
    /// The skipped entries of the recent releases, oldest first.
    recent: VecDeque<B256>,
    /// The set of [`Self::recent`], for the lookup.
    seen: HashSet<B256>,
}

impl ReleaseSender {
    pub(crate) fn new(partition: u32) -> Self {
        Self {
            partition,
            queue: VecDeque::new(),
            recent: VecDeque::new(),
            seen: HashSet::new(),
        }
    }

    /// Queue the release of one skip receipt, unless the skipped entry
    /// already has one.
    pub(crate) fn push(&mut self, release: NonceRelease) {
        if !self.seen.insert(release.tx_hash) {
            metrics::record_nonce_release(self.partition, "duplicate");
            return;
        }
        self.recent.push_back(release.tx_hash);
        if self.recent.len() > RECENT {
            self.forget_oldest();
        }
        if self.queue.len() == QUEUE {
            self.queue.pop_front();
            metrics::record_nonce_release(self.partition, "dropped");
        }
        self.queue.push_back(release);
    }

    /// Offer the queued releases in order, until the transport pushes back.
    pub(crate) fn flush<P: TxOrderingRefPublisher>(&mut self, publisher: &mut P) {
        while self.send_front(publisher) {}
    }

    /// Offer the oldest queued release. Returns whether the caller can
    /// offer the next one.
    fn send_front<P: TxOrderingRefPublisher>(&mut self, publisher: &mut P) -> bool {
        let Some(release) = self.queue.front() else {
            return false;
        };
        match publisher.try_publish_release(release) {
            Err(SequencerError::Backpressure) => return false,
            Ok(()) => metrics::record_nonce_release(self.partition, "sent"),
            Err(error) => {
                warn!(%error, sender = ?release.sender, "nonce release refused by the transport; dropped");
                metrics::record_nonce_release(self.partition, "dropped");
            }
        }
        self.queue.pop_front();
        true
    }

    /// Forget the oldest remembered skipped entry.
    fn forget_oldest(&mut self) {
        if let Some(oldest) = self.recent.pop_front() {
            self.seen.remove(&oldest);
        }
    }

    /// The number of releases that wait for the transport.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
