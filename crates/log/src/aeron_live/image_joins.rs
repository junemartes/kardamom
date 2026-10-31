//! The joins of new images of one subscription, as markers on its frame
//! channel.
//!
//! A subscriber that joins a publication after its start misses the
//! frames before its join. When no frame follows the join, the subscriber
//! sees nothing at all of the new publication. So a reader that repairs a
//! late join must learn of the join itself, not only of the first frame
//! after it.
//!
//! The Aeron thread scans the images of a watched subscription at most
//! every [`SCAN_EVERY`], before it polls the subscription. For each image
//! whose correlation id it has not seen, it sends one marker [`RawFrame`]
//! with `joined` set: the session, the join position and the initial term
//! id of the image, and no bytes. A scan compares the ids, so an image
//! that leaves and one that joins between two scans are both seen. The
//! marker comes before each frame that the same pass polls. A frame of an
//! image that joins after the scan can come before the marker of the
//! image; a reader handles both orders.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tracing::warn;

use super::{FrameSink, RawFrame, Sub};
use crate::term_layout::TermLayout;

/// The least time between two scans of one subscription. A scan reads
/// each image of the subscription, so it does not run on each pass of the
/// hot loop.
const SCAN_EVERY: Duration = Duration::from_millis(10);

/// The images of one subscription that the Aeron thread has reported.
#[derive(Default)]
pub(super) struct ImageJoins {
    /// The correlation ids of the images of the last scan.
    seen: HashSet<i64>,
    last_scan: Option<Instant>,
}

/// The constants of one image that a marker needs, read before the image
/// is released.
struct Image {
    correlation_id: i64,
    session_id: i32,
    join: i64,
    initial_term_id: i32,
    term_length: usize,
}

impl ImageJoins {
    /// Send a marker to `sink` for each new image of `sub`.
    pub(super) fn scan(&mut self, sub: &Sub, sink: &FrameSink) {
        if self.last_scan.is_some_and(|at| at.elapsed() < SCAN_EVERY) {
            return;
        }
        self.last_scan = Some(Instant::now());
        let Some(count) = sub.image_count().ok().and_then(|c| usize::try_from(c).ok()) else {
            return;
        };
        let images: Vec<Image> = (0..count).filter_map(|i| Image::at(sub, i)).collect();
        images
            .iter()
            .filter(|image| !self.seen.contains(&image.correlation_id))
            .filter_map(Image::marker)
            .for_each(|marker| sink.send(marker));
        self.seen = images.iter().map(|image| image.correlation_id).collect();
    }
}

impl Image {
    /// The image at `index` of `sub`, or `None` when it is gone or its
    /// constants are unreadable. The client retains the image that
    /// `image_at_index` returns, so this releases it before it returns.
    fn at(sub: &Sub, index: usize) -> Option<Self> {
        let image = sub.image_at_index(index);
        if image.get_inner().is_null() {
            return None;
        }
        let read = image.get_constants().ok().map(|c| Self {
            correlation_id: c.correlation_id(),
            session_id: c.session_id(),
            join: c.join_position(),
            initial_term_id: c.initial_term_id(),
            term_length: c.term_buffer_length(),
        });
        if let Err(e) = sub.image_release(&image) {
            warn!(error = ?e, "image release failed");
        }
        read
    }

    /// The marker of the join of this image, or `None` when its term
    /// layout cannot place the join.
    fn marker(&self) -> Option<RawFrame> {
        let pos = i32::try_from(self.term_length)
            .map_err(|e| e.to_string())
            .and_then(|length| TermLayout::new(length, self.initial_term_id))
            .and_then(|layout| layout.decode(self.join))
            .inspect_err(
                |error| warn!(%error, session = self.session_id, "image join not reported"),
            )
            .ok()?;
        Some(RawFrame {
            bytes: Vec::new(),
            pos,
            session: self.session_id,
            stream_pos: self.join,
            initial_term_id: self.initial_term_id,
            joined: true,
        })
    }
}
