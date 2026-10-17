//! [`AeronFeed`]: the member feed over `aeron:ipc`, on the media driver
//! that the member and its executor share.

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use kardamom_cluster_adapter::wire::{FEED_CONTROL_STREAM_ID, FEED_STREAM_ID, encode_feed_request};
use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame};
use kardamom_log::error::LogError;
use tokio_util::sync::CancellationToken;

use super::transport::{Poll, Request, Transport};

/// The channel of the feed subscription.
const FEED_CHANNEL: &str = "aeron:ipc";
/// The channel of the control publication. A request is 25 bytes, so the
/// smallest term is enough.
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=65536";

/// The member feed: the subscription on stream 1006, behind the fragment
/// assembler of the runtime, and the control publication on stream 1007.
pub struct AeronFeed {
    rt: AeronRuntime,
    sub_id: u32,
    frames: Receiver<RawFrame>,
    control: PubHandle,
    stop: CancellationToken,
}

impl AeronFeed {
    /// Open the feed on `rt`, which runs on the media driver of the pair.
    /// The feed reads as closed once `stop` is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription or the publication does not
    /// open.
    pub fn open(rt: AeronRuntime, stop: CancellationToken) -> Result<Self, LogError> {
        let (sub_id, frames) = rt.open_subscription_raw_crossbeam(FEED_CHANNEL, FEED_STREAM_ID)?;
        let control = rt.open_exclusive_publication(CONTROL_CHANNEL, FEED_CONTROL_STREAM_ID)?;
        Ok(Self {
            rt,
            sub_id,
            frames,
            control,
            stop,
        })
    }
}

impl Transport for AeronFeed {
    /// The correlation id of the image of the member's publication. The
    /// publication is exclusive, so the feed has at most one image. A
    /// failed read reads as no image.
    fn connection(&mut self) -> Option<u64> {
        self.rt
            .image_id(self.sub_id)
            .ok()
            .flatten()
            .map(i64::cast_unsigned)
    }

    /// One offer, with no retry: the reader sends a new request when no
    /// answer comes.
    fn request(&mut self, request: Request) {
        let frame = encode_feed_request(request.from_index, request.from_block, request.request_id);
        let mut bytes = rkyv::util::AlignedVec::with_capacity(frame.len());
        bytes.extend_from_slice(&frame);
        self.control.publish_lossy(bytes);
    }

    fn poll(&mut self, wait: Duration) -> Poll {
        if self.stop.is_cancelled() {
            return Poll::Closed;
        }
        match self.frames.recv_timeout(wait) {
            Ok(frame) => Poll::Frame(frame.bytes),
            Err(RecvTimeoutError::Timeout) => Poll::Idle,
            Err(RecvTimeoutError::Disconnected) => Poll::Closed,
        }
    }

    fn close(&mut self) {
        let discarded = self.frames.try_iter().count();
        tracing::trace!(discarded, "discarded the frames of the member feed");
    }
}
