//! Outbound sink for derived remote epochs — the interop mirror of
//! [`crate::publisher::EpochPublisher`].
//!
//! The unit is a RECORD, not a message: one record per origin block that
//! carried messages, messages by value. Unlike L1 epochs there is no empty
//! record — remote origins advance only when there is something to say, so the
//! no-skip rule is enforced on the dense per-pair `seq` instead of on origin
//! blocks (`kardamom_types::xchain`, spec §6).
//!
//! ## A failed publish must never be reported as complete
//!
//! Production binds this to `kardamom_log`'s `tx_remote_epochs` publication
//! (`LiveRemoteEpochsPublisher` in the `kardamom-da-watcher` binary); tests
//! use [`fakes::InMemoryRemoteEpochPublisher`].
//!
//! [`PublishError::Backpressure`] and [`PublishError::Transport`] are both
//! NON-FATAL: the watcher holds its cursor and re-derives the same batch. That
//! is safe only because re-derivation is byte-identical, so cluster dedup on
//! `canonical_id` collapses a record that did land. The asymmetry matters —
//! reporting a landed record as failed costs one deduped duplicate, while
//! reporting a FAILED publish as complete advances the cursor past a record
//! that never existed and leaves a permanent hole in the pair's dense seq,
//! which no retry can fill and which the destination halts on.

use kardamom_types::BPosition;
use kardamom_types::xchain::RemoteEpochRecord;

pub use crate::publisher::PublishError;

/// Sink for the remote epochs the interop watcher derives. `Send + Sync`
/// because the watcher's tokio task moves it between executions.
pub trait RemoteEpochPublisher: Send + Sync + 'static {
    /// Publish one record. Returns the assigned wire position so callers can
    /// correlate.
    fn publish(&self, record: &RemoteEpochRecord) -> Result<BPosition, PublishError>;
}

#[cfg(any(test, feature = "test-support"))]
pub mod fakes {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use alloy_primitives::B256;

    use super::*;

    /// In-memory [`RemoteEpochPublisher`] recording every published record in
    /// order, mirroring [`crate::publisher::fakes::InMemoryEpochPublisher`] —
    /// plus the CLUSTER'S first-seen dedup on `canonical_id`, modelled here
    /// because it is half of the safety argument: a stale-cursor restart
    /// re-publishes a record that already landed, and the claim "that is
    /// harmless" is only true because this dedup absorbs it. A fake without
    /// it could not test the property end to end.
    #[derive(Default, Clone)]
    pub struct InMemoryRemoteEpochPublisher {
        pub published: Arc<Mutex<Vec<RemoteEpochRecord>>>,
        seen: Arc<Mutex<BTreeSet<B256>>>,
        /// Publishes absorbed as duplicates (offer succeeded, record already
        /// present — the cluster's first-seen outcome).
        pub deduped: Arc<Mutex<u64>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl InMemoryRemoteEpochPublisher {
        /// Records ACCEPTED so far (first-seen, in order), cloned.
        pub fn records(&self) -> Vec<RemoteEpochRecord> {
            self.published.lock().unwrap().clone()
        }

        /// How many publishes were absorbed as `canonical_id` duplicates.
        pub fn deduped_count(&self) -> u64 {
            *self.deduped.lock().unwrap()
        }
    }

    impl RemoteEpochPublisher for InMemoryRemoteEpochPublisher {
        fn publish(&self, record: &RemoteEpochRecord) -> Result<BPosition, PublishError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(PublishError::Backpressure);
            }
            // A duplicate is a SUCCESSFUL publish whose record the cluster
            // already holds — the transport accepted the offer either way,
            // which is exactly why the producer cannot tell (and must not
            // need to tell) whether its retry was the first copy.
            if !self.seen.lock().unwrap().insert(record.canonical_id()) {
                *self.deduped.lock().unwrap() += 1;
                let len = self.published.lock().unwrap().len();
                return Ok(BPosition {
                    term_id: 0,
                    term_offset: (len as i32) * 64,
                });
            }
            let mut v = self.published.lock().unwrap();
            v.push(record.clone());
            Ok(BPosition {
                term_id: 0,
                term_offset: (v.len() as i32) * 64,
            })
        }
    }
}
