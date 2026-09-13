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

    /// Number of items still queued. Lets a pump-contract test prove that a
    /// retry, while a held record is backpressured, does not also poll the
    /// next item off the queue.
    ///
    /// # Panics
    ///
    /// Panics if the queue mutex is poisoned.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

#[cfg(test)]
pub(crate) mod pump_contract {
    //! The plumbing assertions every `ScriptedQueue<T>`-backed pump shares:
    //! idle reports no work, a closed queue surfaces disconnect,
    //! backpressure holds the popped record without dropping it, a retry
    //! under continued backpressure does not also poll the next record,
    //! and the held record is relayed exactly once once backpressure
    //! clears. Each origin module ([`crate::epoch`],
    //! [`crate::remote_epoch`]) keeps its own record-specific "forwards
    //! verbatim" test; this owns the rest.

    use super::ScriptedQueue;
    use crate::error::SequencerError;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;
    use crate::pump::Pump;

    /// Run the shared assertions for one origin pump. `first` and `second`
    /// stand in for two well-formed, distinct records; `process` is
    /// [`crate::epoch::process_epoch`] or
    /// [`crate::remote_epoch::process_remote_epoch`], closed over its own
    /// subscriber type via `T`.
    ///
    /// # Panics
    ///
    /// Panics (via the assertions) when a pump under test does not honor
    /// the shared idle/closed/backpressure/retry contract.
    pub(crate) fn run<T: Clone>(
        first: &T,
        second: &T,
        mut process: impl FnMut(
            &mut ScriptedQueue<T>,
            &mut InMemoryTxOrderingRefPublisher,
            &mut Pump<T>,
        ) -> Result<bool, SequencerError>,
    ) {
        // Idle subscription reports no work.
        let mut sub = ScriptedQueue::<T>::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let mut pump = Pump::default();
        assert!(!process(&mut sub, &mut pubr, &mut pump).unwrap());

        // Closed subscription surfaces disconnect.
        let mut sub = ScriptedQueue::<T>::default();
        sub.close();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let mut pump = Pump::default();
        assert!(matches!(
            process(&mut sub, &mut pubr, &mut pump),
            Err(SequencerError::IngressDisconnected)
        ));

        // Backpressure holds the popped record, and a retry while
        // backpressure continues does not poll a second record off the
        // queue.
        let mut sub = ScriptedQueue::<T>::default();
        sub.push(kardamom_types::BPosition::default(), first.clone());
        sub.push(kardamom_types::BPosition::default(), second.clone());
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let mut pump = Pump::default();
        *pubr.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            process(&mut sub, &mut pubr, &mut pump),
            Err(SequencerError::Backpressure)
        ));
        assert!(pump.is_held(), "the popped record is held, not dropped");
        assert_eq!(
            sub.len(),
            1,
            "the second record stays queued while the first is held"
        );
        assert!(matches!(
            process(&mut sub, &mut pubr, &mut pump),
            Err(SequencerError::Backpressure)
        ));
        assert_eq!(sub.len(), 1, "the retry does not poll past the held record");

        // Once backpressure clears, the held record is relayed exactly
        // once, then the queue resumes with the second record.
        *pubr.fail_with_backpressure.lock().unwrap() = false;
        assert!(process(&mut sub, &mut pubr, &mut pump).unwrap());
        assert!(!pump.is_held(), "the slot empties on success");
        assert!(process(&mut sub, &mut pubr, &mut pump).unwrap());
        assert!(!process(&mut sub, &mut pubr, &mut pump).unwrap());
    }
}
