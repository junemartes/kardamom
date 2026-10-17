//! [`Answer`]: the reader's view of its newest request on one source. It
//! applies the frame rules of the member feed to each frame: the request
//! ids, the cursor, the slots of each record, and the timers.

use std::time::{Duration, Instant};

use alloy_primitives::keccak256;
use kardamom_cluster_adapter::wire::{EgressItem, WireError};
use kardamom_types::BlockBoundaryStart;

use super::transport::Request;
use crate::reader::Ordered;

/// The waits of the reader. A test passes short waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Timing {
    /// Send a new request when no frame of the answer comes within this
    /// wait.
    pub(crate) resend: Duration,
    /// Leave a source that sends no frame within this wait: three boundary
    /// ticks.
    pub(crate) silence: Duration,
    /// Go back to the feed only after its member stays ready this long.
    pub(crate) ready: Duration,
    /// The wait of one poll of the source, and the period of the timers.
    pub(crate) poll: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            resend: Duration::from_secs(2),
            silence: Duration::from_secs(6),
            ready: Duration::from_secs(30),
            poll: Duration::from_millis(50),
        }
    }
}

/// Why the reader leaves a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    /// The cursor is below the retained floor of the source.
    Unavailable {
        oldest_index: u64,
        oldest_block: u64,
    },
    /// The cursor is above the head of the source.
    Ahead { head_index: u64, head_block: u64 },
    /// The raw transaction of the entry at `index` does not hash to its
    /// canonical id.
    Mismatch { index: u64 },
    /// A frame of a known kind does not decode.
    Undecodable,
    /// No frame came within the silence window.
    Silent,
    /// The source lost its connection.
    Lost,
    /// The source never had a connection in this run of the reader.
    Absent,
}

/// What the reader does with one frame.
#[derive(Debug)]
pub(crate) enum Step {
    /// Buffer the record at `index`.
    Record(u64, Ordered),
    /// Buffer the boundary.
    Boundary(BlockBoundaryStart),
    /// The replay of the newest request reached the head of the source.
    Live { up_to_index: u64, up_to_block: u64 },
    /// Drop the frame.
    Drop,
    /// Ask again from the delivery cursor: the stream has a hole, or the
    /// source dropped frames.
    Ask,
    /// Leave the source.
    Fault(Fault),
}

/// What the reader does when no frame comes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wake {
    Wait,
    /// Send a request with a new id.
    Ask,
    Fault(Fault),
}

/// The newest request on one source and the state of its answer.
pub(crate) struct Answer {
    timing: Timing,
    /// The id of the newest request. `None` until the first request of the
    /// connection.
    request_id: Option<u64>,
    /// The connection that the newest request went out on.
    asked_on: Option<u64>,
    /// The next index and the next block that the source must send: the
    /// cursor of the frames that the reader took from this answer.
    expect_index: u64,
    expect_block: u64,
    /// The replay of the newest request reached the head.
    live: bool,
    asked_at: Instant,
    heard_at: Instant,
}

impl Answer {
    /// The state of a source that the reader starts to read, with no
    /// request yet.
    pub(crate) fn new(timing: Timing, now: Instant) -> Self {
        Self {
            timing,
            request_id: None,
            asked_on: None,
            expect_index: 0,
            expect_block: 0,
            live: false,
            asked_at: now,
            heard_at: now,
        }
    }

    /// The source is new to the reader: no request yet, and the silence
    /// window starts now.
    pub(crate) fn reset(&mut self, now: Instant) {
        self.request_id = None;
        self.asked_on = None;
        self.live = false;
        self.asked_at = now;
        self.heard_at = now;
    }

    /// A new request from the cursor `(index, block)` with the new id
    /// `request_id`, on the connection `connection`. The answer starts
    /// again from that cursor.
    pub(crate) fn ask(
        &mut self,
        (index, block): (u64, u64),
        request_id: u64,
        connection: Option<u64>,
        now: Instant,
    ) -> Request {
        self.request_id = Some(request_id);
        self.asked_on = connection;
        self.expect_index = index;
        self.expect_block = block;
        self.live = false;
        self.asked_at = now;
        Request {
            from_index: index,
            from_block: block,
            request_id,
        }
    }

    /// The step of the timers when no frame came. `connection` is the
    /// connection of the source now. A connection other than the one of
    /// the newest request is a new connection: it starts with a new
    /// request, because the member of a new image answers nothing yet.
    pub(crate) fn wake(&self, connection: Option<u64>, now: Instant) -> Wake {
        let asked = self.request_id.is_some();
        if connection.is_none() && asked {
            return Wake::Fault(Fault::Lost);
        }
        if now.duration_since(self.heard_at) >= self.timing.silence {
            return Wake::Fault(Fault::Silent);
        }
        let quiet = now.duration_since(self.asked_at.max(self.heard_at));
        let resend = !self.live && quiet >= self.timing.resend;
        if connection.is_some() && (!asked || connection != self.asked_on || resend) {
            return Wake::Ask;
        }
        Wake::Wait
    }

    /// The step for one frame of the source, at `now`.
    pub(crate) fn frame(&mut self, bytes: &[u8], now: Instant) -> Step {
        if self.request_id.is_none() {
            return Step::Drop;
        }
        let step = match EgressItem::decode(bytes) {
            Ok(item) => self.item(item),
            Err(WireError::BadEgressKind(_)) => Step::Drop,
            Err(e) => {
                tracing::warn!(error = %e, "a frame of the source does not decode");
                Step::Fault(Fault::Undecodable)
            }
        };
        if !matches!(step, Step::Drop) {
            self.heard_at = now;
        }
        step
    }

    fn item(&mut self, item: EgressItem) -> Step {
        match item {
            EgressItem::Record { index, msg } => self.record(index, msg.into()),
            EgressItem::Tx { index, tx } => {
                if index == self.expect_index && keccak256(&tx.envelope.raw_tx) != tx.canonical_id {
                    return Step::Fault(Fault::Mismatch { index });
                }
                self.record(index, Ordered::Tx(tx))
            }
            EgressItem::Boundary(b) => self.boundary(b),
            EgressItem::ReplayDone {
                up_to_index,
                up_to_block,
                request_id,
            } => self.answer(request_id, || Step::Live {
                up_to_index,
                up_to_block,
            }),
            EgressItem::ReplayUnavailable {
                oldest_index,
                oldest_block,
                request_id,
            } => self.answer(request_id, || {
                Step::Fault(Fault::Unavailable {
                    oldest_index,
                    oldest_block,
                })
            }),
            EgressItem::ReplayAhead {
                head_index,
                head_block,
                request_id,
            } => self.answer(request_id, || {
                Step::Fault(Fault::Ahead {
                    head_index,
                    head_block,
                })
            }),
            EgressItem::FeedGap { request_id, .. } => self.answer(Some(request_id), || Step::Ask),
            EgressItem::RelayedRef(_)
            | EgressItem::SessionStart
            | EgressItem::Status(_)
            | EgressItem::ContiguityReject { .. }
            | EgressItem::PastDeadline { .. }
            | EgressItem::WindowFull { .. }
            | EgressItem::RemoteOriginReject { .. }
            | EgressItem::OriginGap { .. }
            | EgressItem::DaLagReject { .. }
            | EgressItem::RecordLagReject { .. } => Step::Drop,
        }
    }

    /// An answer frame. It counts only when it names the newest request.
    /// A frame with no id comes from no request of this reader.
    fn answer(&mut self, request_id: Option<u64>, step: impl FnOnce() -> Step) -> Step {
        if request_id != self.request_id {
            return Step::Drop;
        }
        let step = step();
        self.live = matches!(step, Step::Live { .. });
        step
    }

    /// A relayed record. A record below the cursor comes from an older
    /// answer: drop it. A record above the cursor is a hole. The next index
    /// is the index plus the slots of the record.
    fn record(&mut self, index: u64, item: Ordered) -> Step {
        if index < self.expect_index {
            return Step::Drop;
        }
        let next = index.checked_add(item.slots());
        match (index == self.expect_index, next) {
            (true, Some(next)) => {
                self.expect_index = next;
                Step::Record(index, item)
            }
            _ => Step::Ask,
        }
    }

    /// A boundary. It closes the block at the cursor: its end is the next
    /// index, and its block is the block of the cursor.
    fn boundary(&mut self, b: BlockBoundaryStart) -> Step {
        if b.block_number < self.expect_block {
            return Step::Drop;
        }
        let next = self.expect_block.checked_add(1);
        match (
            b.block_number == self.expect_block && b.end_tx_idx.as_index() == self.expect_index,
            next,
        ) {
            (true, Some(next)) => {
                self.expect_block = next;
                Step::Boundary(b)
            }
            _ => Step::Ask,
        }
    }
}
