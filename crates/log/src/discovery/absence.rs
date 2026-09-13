//! A removal requires continuous, confirmed absence for the configured grace.

use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct Absence {
    since: Option<Instant>,
}

impl Absence {
    pub(super) fn clear(&mut self) {
        self.since = None;
    }

    pub(super) fn expired(&mut self, present: bool, now: Instant, grace: Duration) -> bool {
        if present {
            self.clear();
            return false;
        }
        let since = *self.since.get_or_insert(now);
        now.duration_since(since) >= grace
    }
}
