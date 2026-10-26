//! The observer reads existing transaction counters and controls only the paced window.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

use super::tracker::{PendingSample, Tracker};

/// Failure evidence reads existing counters. Concurrent tasks can advance between reads.
#[derive(Debug, serde::Serialize)]
pub struct ProgressSnapshot {
    offered: u64,
    accepted: u64,
    receipted: u64,
    submitting: bool,
    accepted_pending: u64,
    unaccepted_pending: u64,
    pending_sample: Vec<PendingSample>,
}

#[derive(Clone)]
pub struct Progress {
    pub(super) tracker: Arc<Tracker>,
    submitting: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
}

impl Progress {
    pub(super) fn new(tracker: Arc<Tracker>) -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            tracker,
            submitting: Arc::new(AtomicBool::new(false)),
            stop,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ProgressSnapshot {
        let counts = self.tracker.counts();
        let pending = self.tracker.remaining_pending();
        ProgressSnapshot {
            offered: counts.offered,
            accepted: counts.accepted,
            receipted: counts.receipted,
            submitting: self.is_submitting(),
            accepted_pending: pending.missing,
            unaccepted_pending: pending.unlanded,
            pending_sample: self.tracker.sample_pending(16),
        }
    }

    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.tracker.counts().accepted
    }

    #[must_use]
    pub fn is_submitting(&self) -> bool {
        self.submitting.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_flowing(&self) -> bool {
        self.is_submitting() && self.accepted() > 0
    }

    /// Stop new submissions and drain every transaction already offered.
    pub fn finish_submissions(&self) {
        self.stop.send_replace(true);
    }

    pub(super) fn begin(&self) -> SubmissionWindow<'_> {
        self.submitting.store(true, Ordering::Release);
        SubmissionWindow {
            submitting: &self.submitting,
        }
    }

    pub(super) fn stop_requested(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }
}

pub(super) struct SubmissionWindow<'a> {
    submitting: &'a AtomicBool,
}

impl Drop for SubmissionWindow<'_> {
    fn drop(&mut self) {
        self.submitting.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;
