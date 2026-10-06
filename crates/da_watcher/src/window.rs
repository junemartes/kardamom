//! The L1 watcher's anchored position: the confirmed block, and the
//! epochs it published after that block that no boundary confirmed yet.
//!
//! The sealer's boundaries carry its L1 origin. An origin confirms every
//! epoch up to it. An epoch between a publish and its commit can be lost:
//! cluster ingress is at-most-once across a leader kill, and a sequencer
//! that restarts loses its queue. So the window keeps every published
//! epoch until a boundary confirms it, and publishes them again when no
//! boundary confirms one for [`REPUBLISH_AFTER`].

use std::collections::VecDeque;

use kardamom_types::EpochRecord;
use kardamom_types::epoch_delivery::{PUBLISH_WINDOW, REPUBLISH_AFTER};
use tokio::time::Instant;

use crate::l1_cursor::L1Cursor;

/// The confirmed block and the unconfirmed epochs after it.
///
/// `epochs` holds the blocks `base.number + 1 ..= head`, ascending and
/// without a hole. `base` is the sealer's confirmed origin, or the block
/// the watcher resumed after while no boundary confirmed one. The next
/// epoch descends from the head.
#[derive(Debug, Clone)]
pub(crate) struct Window {
    base: L1Cursor,
    epochs: VecDeque<EpochRecord>,
    /// When the base last moved, or when the last re-publish ran. The
    /// next re-publish is due [`REPUBLISH_AFTER`] later.
    since: Instant,
}

/// Where an origin falls against a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Confirm {
    /// The origin is in `base..=head`. The window dropped every epoch at
    /// or below it.
    Inside,
    /// The origin is below the base or past the head. The window cannot
    /// follow it: the watcher anchors again at the origin.
    Outside,
}

impl Window {
    /// An empty window after `base`.
    pub(crate) fn new(base: L1Cursor) -> Self {
        Self {
            base,
            epochs: VecDeque::new(),
            since: Instant::now(),
        }
    }

    /// The confirmed block, or the block the watcher resumed after.
    pub(crate) fn base(&self) -> L1Cursor {
        self.base
    }

    /// The last published block: the next epoch descends from it.
    pub(crate) fn head(&self) -> L1Cursor {
        self.epochs.back().map_or(self.base, Self::cursor_of)
    }

    /// The published epochs that no boundary confirmed yet.
    pub(crate) fn len(&self) -> usize {
        self.epochs.len()
    }

    /// The window holds [`PUBLISH_WINDOW`] epochs: the watcher publishes
    /// no new epoch until a boundary confirms one.
    pub(crate) fn is_full(&self) -> bool {
        self.epochs.len() >= PUBLISH_WINDOW.get()
    }

    /// Keep a published epoch, the block after the head. The first epoch
    /// of an empty window starts the re-publish timer.
    pub(crate) fn push(&mut self, epoch: EpochRecord) {
        if self.epochs.is_empty() {
            self.since = Instant::now();
        }
        self.epochs.push_back(epoch);
    }

    /// Apply a confirmed origin. An origin in `base..=head` drops every
    /// epoch at or below it, and a base that moves restarts the timer.
    pub(crate) fn confirm(&mut self, origin: u64) -> Confirm {
        if origin < self.base.number || origin > self.head().number {
            return Confirm::Outside;
        }
        let done = self.epochs.partition_point(|e| e.l1_number <= origin);
        if let Some(confirmed) = self.epochs.drain(..done).next_back() {
            self.base = Self::cursor_of(&confirmed);
            self.since = Instant::now();
        }
        Confirm::Inside
    }

    /// When the next re-publish is due: `None` while no epoch waits.
    pub(crate) fn republish_at(&self) -> Option<Instant> {
        if self.epochs.is_empty() {
            return None;
        }
        self.since.checked_add(REPUBLISH_AFTER)
    }

    /// The unconfirmed epochs in L1 order, for a re-publish. The call
    /// restarts the timer, so a re-publish runs at most once for each
    /// [`REPUBLISH_AFTER`].
    pub(crate) fn republish(&mut self) -> impl Iterator<Item = &EpochRecord> {
        self.since = Instant::now();
        self.epochs.iter()
    }

    fn cursor_of(epoch: &EpochRecord) -> L1Cursor {
        L1Cursor {
            number: epoch.l1_number,
            hash: epoch.l1_hash,
        }
    }
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
