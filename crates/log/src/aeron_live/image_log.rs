//! The image log of a subscription: one line when the driver adds an
//! image of a publisher session, one when it removes the image. A
//! subscriber that stays without an image of a publisher it attached
//! shows the fault on its own side, with the session and the source.

use tracing::{info, warn};

use super::Sub;

/// The image log of one subscription.
pub(super) struct ImageLog {
    pub(super) stream_id: i32,
    pub(super) uri: String,
}

impl ImageLog {
    fn line(&self, image: &rusteron_client::AeronImage, event: &str) {
        match image.get_constants() {
            Ok(c) => info!(
                stream_id = self.stream_id,
                uri = %self.uri,
                session_id = c.session_id(),
                source = c.source_identity(),
                correlation_id = c.correlation_id(),
                "aeron: image {event}"
            ),
            Err(e) => warn!(
                stream_id = self.stream_id,
                uri = %self.uri,
                error = ?e,
                "aeron: image {event}; the image constants are unreadable"
            ),
        }
    }
}

impl rusteron_client::AeronAvailableImageCallback for ImageLog {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: rusteron_client::AeronSubscription,
        image: rusteron_client::AeronImage,
    ) {
        self.line(&image, "available");
    }
}

impl rusteron_client::AeronUnavailableImageCallback for ImageLog {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: rusteron_client::AeronSubscription,
        image: rusteron_client::AeronImage,
    ) {
        self.line(&image, "unavailable");
    }
}

/// An open subscription with the two image handlers the driver calls
/// for it. The handlers are leaked for the C side and released with the
/// subscription.
pub(super) struct OpenedSub {
    pub(super) sub: Sub,
    pub(super) available: rusteron_client::Handler<ImageLog>,
    pub(super) unavailable: rusteron_client::Handler<ImageLog>,
}
