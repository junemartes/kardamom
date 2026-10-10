//! The record buffer of the executor stream: the distinct copies of each
//! canonical index, keyed by the index.
//!
//! Every executor publishes each record, so a consumer gets one copy per
//! executor. An honest fleet publishes identical copies: the buffer keeps
//! the first one and drops the repeats. A copy that differs is kept beside
//! the first, so the check at the take can pick the copy that matches the
//! canonical hash.

use std::num::NonZeroUsize;
use std::time::Duration;

use kardamom_types::ExecTxRecord;

use crate::keyed_buffer::{Bounds, KeyedBuffer, Skip, Slot};
use crate::metrics::EXEC_STREAM_DROPPED_TOTAL;

/// The bound of distinct copies of one index. Honest executors publish one
/// value, so a second one already shows a fault.
const MAX_COPIES: usize = 8;

/// The distinct copies of one canonical index, in arrival order.
struct Copies(Vec<ExecTxRecord>);

impl Slot for Copies {
    type Item = ExecTxRecord;

    fn first(record: ExecTxRecord) -> Self {
        Self(vec![record])
    }

    fn join(&mut self, record: ExecTxRecord) -> Result<(), Skip> {
        if self.0.contains(&record) {
            return Err(Skip::Repeat);
        }
        if self.0.len() >= MAX_COPIES {
            return Err(Skip::Bound);
        }
        self.0.push(record);
        Ok(())
    }

    fn results(&self) -> usize {
        self.0.len()
    }
}

/// The bounds of the record buffer.
///
/// - The cap holds 65 536 indices: one void window. A replay from a
///   locator delivers a run of records, and the live head keeps coming at
///   the same time.
/// - The lookbehind is 4096 indices: a live head further ahead than that
///   means the wanted record aged out of the live stream, so the take
///   ends at once and the wait refetches.
/// - Nothing below the cursor is kept: an index that the reader passed
///   never comes back.
/// - The reach is 2^32 indices above the cursor.
const BOUNDS: Bounds = Bounds {
    cap: NonZeroUsize::new(1 << 16).expect("compile-time constant"),
    lookbehind: 4096,
    late_window: 0,
    reach: 1 << 32,
};

/// The buffer of executor stream records, keyed by canonical index. The
/// feed thread and the replays of the wait insert. The `tx_ordering`
/// reader takes.
pub(super) struct RecordBuffer(KeyedBuffer<u64, Copies>);

impl Default for RecordBuffer {
    fn default() -> Self {
        Self(KeyedBuffer::new(BOUNDS))
    }
}

impl RecordBuffer {
    /// Insert one record under its index. A refused copy counts by its
    /// reason: `repeat` is the dedup of the copies of the other executors.
    pub(super) fn insert(&self, record: ExecTxRecord) {
        let inserted = self.0.insert(record.index, record);
        if let Some(skip) = inserted.refused {
            ::metrics::counter!(EXEC_STREAM_DROPPED_TOTAL, "reason" => skip.id()).increment(1);
        }
        if inserted.evicted > 0 {
            ::metrics::counter!(EXEC_STREAM_DROPPED_TOTAL, "reason" => Skip::Evicted.id())
                .increment(u64::try_from(inserted.evicted).unwrap_or(u64::MAX));
        }
    }

    /// The copies of `index`, after a wait of up to `timeout`. `None`
    /// when no copy arrived, or when the live head is far ahead.
    #[must_use]
    pub(super) fn take(&self, index: u64, timeout: Duration) -> Option<Vec<ExecTxRecord>> {
        self.0.take(index, timeout).current.map(|copies| copies.0)
    }
}
