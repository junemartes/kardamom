//! The destinations attached to the subscriptions of the Aeron thread.
//!
//! A destination is its own Aeron subscription on the destination URI. It
//! sends its messages to the sink of the subscription that it is attached
//! to. The runtime never adds a destination to an Aeron multi-destination
//! subscription. The Java media driver sizes the connection table of an
//! image from the destination index of the image, and grows it only for a
//! destination added after the image forms. The removal of a destination
//! with a higher index than that table then throws in the driver, and the
//! other images of the subscription keep a stale connection.
//!
//! A detach does not close the subscription of the destination at once.
//! The destination lingers: its subscription stays open and polled until
//! it has no image, or until the linger ends. An attach of the same URI
//! during the linger keeps the open subscription and its image. A short
//! catalog flap thus leaves the image and its position unchanged, and the
//! driver repairs any loss from the term buffer of the publisher.

use std::time::{Duration, Instant};

use tracing::info;

use super::thread::SubEntry;

/// A destination attached to the subscription `sub_id`, keyed by
/// `(sub_id, uri)`. Dropping the row closes its Aeron subscription.
pub(super) struct Destination {
    pub(super) sub_id: u32,
    pub(super) uri: String,
    pub(super) entry: SubEntry,
    /// The time of the detach and the linger. `None` while attached.
    leaving: Option<(Instant, Duration)>,
}

impl Destination {
    pub(super) fn new(sub_id: u32, uri: &str, entry: SubEntry) -> Self {
        Self {
            sub_id,
            uri: uri.to_string(),
            entry,
            leaving: None,
        }
    }

    /// Whether this row is the destination `uri` of `sub_id`.
    pub(super) fn is(&self, sub_id: u32, uri: &str) -> bool {
        self.sub_id == sub_id && self.uri == uri
    }

    /// Start the linger of a detach, unless one runs already.
    pub(super) fn leave(&mut self, now: Instant, linger: Duration) {
        if self.leaving.is_none() {
            info!(uri = %self.uri, ?linger, "aeron: destination detached; it closes once its image goes");
            self.leaving = Some((now, linger));
        }
    }

    /// Cancel the linger of a detach: the destination is attached again,
    /// with the same subscription and image.
    pub(super) fn stay(&mut self) {
        if self.leaving.take().is_some() {
            info!(uri = %self.uri, "aeron: destination attached again; it keeps its image");
        }
    }

    /// Whether a detached destination closes now: its subscription has
    /// no image, or its linger ended. An attached destination never
    /// closes here. A failed image count keeps the row until the linger
    /// ends.
    pub(super) fn closes(&self, now: Instant) -> bool {
        let Some((left_at, linger)) = self.leaving else {
            return false;
        };
        let no_image = self.entry.image_count().is_ok_and(|n| n == 0);
        let closes = no_image || now.saturating_duration_since(left_at) >= linger;
        if closes {
            info!(uri = %self.uri, no_image, "aeron: detached destination closed");
        }
        closes
    }
}
