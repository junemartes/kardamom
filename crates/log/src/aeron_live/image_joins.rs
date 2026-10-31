//! The joins of new images of one subscription, as markers on its frame
//! channel.
//!
//! A subscriber that joins a publication after its start misses the
//! frames before its join. When no frame follows the join, the subscriber
//! sees nothing at all of the new publication. So a reader that repairs a
//! late join must learn of the join itself, not only of the first frame
//! after it.
//!
//! The Aeron thread scans the images of a watched subscription when their
//! count changes, before it polls the subscription. For each image that
//! it has not seen, it sends one marker [`RawFrame`] with `joined` set: the
//! session, the join position and the initial term id of the image, and
//! no bytes. The marker comes before each frame that the same pass polls.
//! A frame of an image that joins after the scan can come before the
//! marker of the image; a reader handles both orders.

use std::collections::HashSet;

use tracing::warn;

use super::{FrameSink, RawFrame, Sub};
use crate::term_layout::TermLayout;

/// The images of one subscription that the Aeron thread has reported.
#[derive(Default)]
pub(super) struct ImageJoins {
    /// The image count of the last scan.
    count: i32,
    /// The correlation ids of the images of the last scan.
    seen: HashSet<i64>,
}

impl ImageJoins {
    /// Send a marker to `sink` for each new image of `sub`. Scan only
    /// when the image count changed.
    pub(super) fn scan(&mut self, sub: &Sub, sink: &FrameSink) {
        let Ok(count) = sub.image_count() else {
            return;
        };
        if count == self.count {
            return;
        }
        self.count = count;
        let mut now = HashSet::new();
        // Aeron never gives a negative count.
        for index in 0..count.unsigned_abs() as usize {
            let image = sub.image_at_index(index);
            if image.get_inner().is_null() {
                continue;
            }
            if let Ok(c) = image.get_constants() {
                now.insert(c.correlation_id());
                if !self.seen.contains(&c.correlation_id())
                    && let Some(marker) = Self::marker(&c)
                {
                    sink.send(marker);
                }
            }
            if let Err(e) = sub.image_release(&image) {
                warn!(error = ?e, "image release failed");
            }
        }
        self.seen = now;
    }

    /// The marker of the join of the image with constants `c`, or `None`
    /// when its term layout cannot place the join.
    fn marker(c: &rusteron_client::AeronImageConstants) -> Option<RawFrame> {
        let join = c.join_position();
        let pos = i32::try_from(c.term_buffer_length())
            .map_err(|e| e.to_string())
            .and_then(|length| TermLayout::new(length, c.initial_term_id()))
            .and_then(|layout| layout.decode(join))
            .inspect_err(|error| warn!(%error, session = c.session_id(), "image join not reported"))
            .ok()?;
        Some(RawFrame {
            bytes: Vec::new(),
            pos,
            session: c.session_id(),
            stream_pos: join,
            initial_term_id: c.initial_term_id(),
            joined: true,
        })
    }
}
