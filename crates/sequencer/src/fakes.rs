//! Generic in-memory scripted-queue fake, shared by every single-record-type
//! poll surface (today: [`crate::epoch::EpochSubscriber`] and
//! [`crate::remote_epoch::RemoteEpochSubscriber`]).
//!
//! Push test inputs with [`ScriptedQueue::push`], close with
//! [`ScriptedQueue::close`]. A trait impl for a concrete record type calls
//! [`ScriptedQueue::poll_next`] and maps the result straight through.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use kardamom_types::BPosition;

use crate::error::SequencerError;

/// In-memory scripted queue, drained FIFO. `T` is the record type (for
/// example `EpochRecord` or `RemoteEpochRecord`).
#[derive(Clone)]
pub struct ScriptedQueue<T> {
    queue: Arc<Mutex<VecDeque<(BPosition, T)>>>,
    closed: Arc<Mutex<bool>>,
}

impl<T> Default for ScriptedQueue<T> {
    fn default() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }
}

impl<T> ScriptedQueue<T> {
    /// # Panics
    ///
    /// Panics if the queue mutex is poisoned.
    pub fn push(&self, pos: BPosition, item: T) {
        self.queue.lock().unwrap().push_back((pos, item));
    }

    /// # Panics
    ///
    /// Panics if the closed-flag mutex is poisoned.
    pub fn close(&self) {
        *self.closed.lock().unwrap() = true;
    }

    /// Pop the next item, or report disconnect once closed and drained.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::IngressDisconnected`] once [`Self::close`]
    /// was called and the queue is empty.
    ///
    /// # Panics
    ///
    /// Panics if the queue mutex is poisoned.
    pub fn poll_next(&mut self) -> Result<Option<(BPosition, T)>, SequencerError> {
        if let Some(item) = self.queue.lock().unwrap().pop_front() {
            return Ok(Some(item));
        }
        if *self.closed.lock().unwrap() {
            return Err(SequencerError::IngressDisconnected);
        }
        Ok(None)
    }
}

#[cfg(test)]
pub(crate) mod pump_contract {
    //! The three plumbing assertions every `ScriptedQueue<T>`-backed pump
    //! shares: idle reports no work, a closed queue surfaces disconnect,
    //! and backpressure propagates without consuming the item. Each origin
    //! module ([`crate::epoch`], [`crate::remote_epoch`]) keeps its own
    //! record-specific "forwards verbatim" test; this owns the rest.

    use super::ScriptedQueue;
    use crate::error::SequencerError;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

    /// Run the three shared assertions for one origin pump. `dummy`
    /// stands in for a well-formed record; `process` is [`crate::epoch::
    /// process_epoch`] or [`crate::remote_epoch::process_remote_epoch`],
    /// closed over its own subscriber type via `T`.
    ///
    /// # Panics
    ///
    /// Panics (via the assertions) when a pump under test does not honor
    /// the shared idle/closed/backpressure contract.
    pub fn run<T: Clone>(
        dummy: &T,
        mut process: impl FnMut(
            &mut ScriptedQueue<T>,
            &mut InMemoryTxOrderingRefPublisher,
        ) -> Result<bool, SequencerError>,
    ) {
        // Idle subscription reports no work.
        let mut sub = ScriptedQueue::<T>::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(!process(&mut sub, &mut pubr).unwrap());

        // Closed subscription surfaces disconnect.
        let mut sub = ScriptedQueue::<T>::default();
        sub.close();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(matches!(
            process(&mut sub, &mut pubr),
            Err(SequencerError::IngressDisconnected)
        ));

        // Backpressure propagates so the caller retries.
        let mut sub = ScriptedQueue::<T>::default();
        sub.push(kardamom_types::BPosition::default(), dummy.clone());
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        *pubr.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            process(&mut sub, &mut pubr),
            Err(SequencerError::Backpressure)
        ));
    }
}
