//! Request retries share session lifetime but use separate acknowledgement rules.

use super::{ReplayAsk, Resend, SUBSCRIBE_RESEND_MS};

pub(super) struct SessionRequests {
    replay: ReplayAsk,
    subscription: Option<Resend>,
}

impl SessionRequests {
    pub(super) fn new(subscribe: bool) -> Self {
        Self {
            replay: ReplayAsk::new(),
            subscription: subscribe.then(|| Resend::new(SUBSCRIBE_RESEND_MS)),
        }
    }

    pub(super) fn rearm(&mut self) {
        self.replay.rearm();
        if let Some(subscription) = &mut self.subscription {
            subscription.rearm();
        }
    }

    pub(super) fn on_payload(&mut self, kind: u8) {
        self.replay.on_payload(kind);
    }

    /// No egress kind acknowledges registration. Boundaries and status
    /// broadcast, and records can use the empty-consumer fallback. Refresh
    /// on the bounded cadence for the lifetime of a subscribing session.
    pub(super) fn subscribe_due(&mut self, now: u64) -> bool {
        self.subscription
            .as_mut()
            .is_some_and(|refresh| refresh.due(now))
    }

    pub(super) fn replay_due(&mut self, now: u64) -> bool {
        self.replay.due(now)
    }
}
