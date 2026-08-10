//! Outbound sink for derived remote epochs — the interop mirror of
//! [`crate::publisher::EpochPublisher`].
//!
//! The unit is a RECORD, not a message: one record per origin block that
//! carried messages, messages by value. Unlike L1 epochs there is no empty
//! record — remote origins advance only when there is something to say, so the
//! no-skip rule is enforced on the dense per-pair `seq` instead of on origin
//! blocks (`kardamom_types::xchain`, spec §6).
//!
//! ## No live implementation yet, on purpose
//!
//! There is no `kardamom_log` handle to bind this to: `ChannelsConfig` has
//! `tx_deposits` but no remote-epoch channel, so a live publisher would mean
//! inventing a channel, a stream id, and a subscriber with nothing on the
//! other end. That channel — and the sequencer relay that reads it onto
//! `tx_ordering` as a remote-origin record — is the next slice's work. Until
//! then the trait is the seam and [`fakes::InMemoryRemoteEpochPublisher`] is
//! the only implementation, which is enough to close the loop end to end
//! against a simulated origin.

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
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory [`RemoteEpochPublisher`] recording every published record in
    /// order, mirroring [`crate::publisher::fakes::InMemoryEpochPublisher`].
    #[derive(Default, Clone)]
    pub struct InMemoryRemoteEpochPublisher {
        pub published: Arc<Mutex<Vec<RemoteEpochRecord>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl InMemoryRemoteEpochPublisher {
        /// Records published so far, cloned.
        pub fn records(&self) -> Vec<RemoteEpochRecord> {
            self.published.lock().unwrap().clone()
        }
    }

    impl RemoteEpochPublisher for InMemoryRemoteEpochPublisher {
        fn publish(&self, record: &RemoteEpochRecord) -> Result<BPosition, PublishError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(PublishError::Backpressure);
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
