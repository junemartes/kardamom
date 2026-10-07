//! The recorded cursor: the highest canonical index whose every record
//! the local archive has written.
//!
//! The reader sends one progress mark after each message that takes a
//! slot. A mark is recorded when the archive's recording position reaches
//! the end of the last record offered before the mark. The cursor is the
//! highest recorded mark. It never passes the recording position: a mark
//! whose records the archive has not written waits in a queue until a
//! recording position at or past their end arrives.

use std::collections::VecDeque;

/// The recorded-cursor computation of one recorded publication. The
/// publisher thread owns it.
#[derive(Debug)]
pub(crate) struct RecordedCursor {
    /// The highest recording position the archive reported.
    recorded: i64,
    /// The end position of the last record offered.
    offered_end: i64,
    /// Marks whose records the archive has not written yet:
    /// `(end position of the last record before the mark, mark)`, oldest
    /// first. Both fields rise from front to back.
    waiting: VecDeque<(i64, u64)>,
    /// The highest recorded mark.
    through: Option<u64>,
}

impl RecordedCursor {
    /// A cursor for a session whose recording starts at `start`. Nothing
    /// is offered yet, so the first offered record starts at `start` or
    /// after it.
    pub(crate) fn new(start: i64) -> Self {
        Self {
            recorded: start,
            offered_end: start,
            waiting: VecDeque::new(),
            through: None,
        }
    }

    /// The end position of the last record offered: a position at or
    /// before the start of the next record.
    pub(crate) fn offered_end(&self) -> i64 {
        self.offered_end
    }

    /// The highest recorded mark, `None` before the first one.
    pub(crate) fn through(&self) -> Option<u64> {
        self.through
    }

    /// One record went onto the recorded publication and ends at `end`.
    pub(crate) fn offered(&mut self, end: i64) {
        self.offered_end = end;
    }

    /// The reader passed every slot at or below `mark`. Returns the new
    /// cursor when it moved.
    pub(crate) fn passed(&mut self, mark: u64) -> Option<u64> {
        if self.offered_end <= self.recorded {
            return self.advance(mark);
        }
        self.waiting.push_back((self.offered_end, mark));
        None
    }

    /// The archive reported the recording position `position`. Returns the
    /// new cursor when it moved.
    pub(crate) fn recorded(&mut self, position: i64) -> Option<u64> {
        self.recorded = self.recorded.max(position);
        let recorded = self.recorded;
        let last = std::iter::from_fn(|| {
            self.waiting
                .front()
                .is_some_and(|&(end, _)| end <= recorded)
                .then(|| self.waiting.pop_front())
                .flatten()
        })
        .last()?;
        self.advance(last.1)
    }

    fn advance(&mut self, mark: u64) -> Option<u64> {
        self.through = Some(mark);
        self.through
    }
}
