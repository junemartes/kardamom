//! The image log of every subscription of one Aeron thread: one line
//! when the driver adds an image of a publisher session, one when it
//! removes the image. A subscriber that stays without an image of a
//! publisher it attached shows the fault on its own side, with the
//! session and the source.

use tracing::{info, warn};

/// The two image handlers the Aeron thread passes to every
/// `add_subscription`. They hold no state of their own: each line reads
/// the stream and the channel from the subscription and the session and
/// the source from the image. The handlers live as long as the process.
/// The client conductor calls the unavailable-image handler of a
/// subscription while it closes the subscription, after the row of the
/// table is gone, so a handler released with its row is a dangling
/// pointer on that call.
pub(super) struct ImageHandlers {
    pub(super) available: &'static rusteron_client::Handler<ImageLog>,
    pub(super) unavailable: &'static rusteron_client::Handler<ImageLog>,
}

impl ImageHandlers {
    /// Leak the two handlers for the life of the process.
    pub(super) fn leak() -> Self {
        Self {
            available: Box::leak(Box::new(rusteron_client::Handler::leak(ImageLog))),
            unavailable: Box::leak(Box::new(rusteron_client::Handler::leak(ImageLog))),
        }
    }
}

/// The image log of one event. See [`ImageHandlers`].
pub(super) struct ImageLog;

impl ImageLog {
    fn line(
        subscription: &rusteron_client::AeronSubscription,
        image: &rusteron_client::AeronImage,
        event: &str,
    ) {
        let (stream_id, channel) = match subscription.get_constants() {
            Ok(c) => (c.stream_id(), c.channel().to_string()),
            Err(_) => (0, String::new()),
        };
        match image.get_constants() {
            Ok(c) => info!(
                stream_id,
                channel,
                session_id = c.session_id(),
                source = c.source_identity(),
                correlation_id = c.correlation_id(),
                "aeron: image {event}"
            ),
            Err(e) => warn!(
                stream_id,
                channel,
                error = ?e,
                "aeron: image {event}; the image constants are unreadable"
            ),
        }
    }
}

impl rusteron_client::AeronAvailableImageCallback for ImageLog {
    fn handle_aeron_on_available_image(
        &mut self,
        subscription: rusteron_client::AeronSubscription,
        image: rusteron_client::AeronImage,
    ) {
        Self::line(&subscription, &image, "available");
    }
}

impl rusteron_client::AeronUnavailableImageCallback for ImageLog {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        subscription: rusteron_client::AeronSubscription,
        image: rusteron_client::AeronImage,
    ) {
        Self::line(&subscription, &image, "unavailable");
    }
}
