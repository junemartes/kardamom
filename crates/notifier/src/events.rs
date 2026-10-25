//! The events path (`KARDAMOM_NOTIFIER_EVENTS`,
//! `docs/specs/2026-10-25-client-events.md`): the stages from the
//! one-way publications, through the stage table.
//!
//! - `tx_events`: `accepted` (ingress) and `sent` (sequencer).
//! - `egress_receipts`: `receipt`, and the rollback frames.
//! - `tx_receipts` of every executor: `executed`, only with
//!   `KARDAMOM_NOTIFIER_EXECUTED`, for the evaluation of the stages.
//!
//! The path decides; the hub stores and fans out what it sends. A stage
//! that the table does not send costs a lookup and a few bit operations,
//! with no allocation.

use std::time::Instant;

use alloy_primitives::B256;
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::tx_events::{TxEvent, TxEventKind, TxEventsView};
use kardamom_types::{Receipt, TxStatus};
use metrics::{Counter, Histogram};

use crate::dto::{Stage as Word, TxStatusEvent};
use crate::metrics as m;
use crate::stages::{EXECUTORS, Event, STAGES, Seen, Stage, StageTable};

/// One event that the table sent: the client shape, and the identity it
/// carries for the ring.
pub struct Send {
    pub event: TxStatusEvent,
    pub identity: Option<(alloy_primitives::Address, u64)>,
}

/// The metric handles, made once, so an event formats no label.
struct Handles {
    latency: [Histogram; STAGES.len()],
    not_sent: [Counter; STAGES.len()],
    executed: [Histogram; EXECUTORS],
    merge: Histogram,
    evicted: Counter,
    bad_frames: Counter,
    past_posted: Counter,
}

impl Handles {
    fn new() -> Self {
        Self {
            latency: STAGES
                .map(|s| metrics::histogram!(m::STAGE_LATENCY_SECONDS, "stage" => s.label())),
            not_sent: STAGES
                .map(|s| metrics::counter!(m::STAGE_NOT_SENT_TOTAL, "stage" => s.label())),
            executed: std::array::from_fn(
                |slot| metrics::histogram!(m::EXECUTED_LATENCY_SECONDS, "executor" => slot.to_string()),
            ),
            merge: metrics::histogram!(m::MERGE_LATENCY_SECONDS),
            evicted: metrics::counter!(m::STAGE_TABLE_EVICTED_TOTAL),
            bad_frames: metrics::counter!(m::BAD_FRAMES_TOTAL),
            past_posted: metrics::counter!(m::ROLLBACK_PAST_POSTED_TOTAL),
        }
    }
}

/// The number of `seq` counters: one for each origin and kind.
const SEQ_SLOTS: usize = (u16::MAX as usize + 1) * TxEventKind::ALL.len();

/// The table, the metric handles, and the state of the gap counts.
pub struct EventsPath {
    table: StageTable,
    handles: Handles,
    /// The last `seq` of each origin and kind, `u64::MAX` before the first.
    last_seq: Box<[u64]>,
    /// The `tx_receipts` session of each executor slot.
    sessions: [Option<i32>; EXECUTORS],
    next_slot: usize,
}

impl EventsPath {
    /// A path with a table of `capacity` entries. The allocations of the
    /// path are all here.
    #[must_use]
    pub fn new(capacity: usize, start: Instant) -> Self {
        Self {
            table: StageTable::new(capacity, start),
            handles: Handles::new(),
            last_seq: vec![u64::MAX; SEQ_SLOTS].into_boxed_slice(),
            sessions: [None; EXECUTORS],
            next_slot: 0,
        }
    }

    /// Take one `tx_events` frame at `now`. `send` gets each event to send.
    pub fn on_frame(&mut self, frame: &[u8], now: Instant, mut send: impl FnMut(Send)) {
        let Ok(view) = TxEventsView::parse(frame) else {
            self.handles.bad_frames.increment(1);
            return;
        };
        let _gap = self.note_seq(&view);
        for record in view.events() {
            let (hash, stage) = match record {
                TxEvent::Accepted { hash } => (hash, Stage::Accepted),
                TxEvent::Sent { hash, .. } => (hash, Stage::Sent),
            };
            let event = Event {
                hash,
                stage,
                block: None,
                executor: None,
            };
            if self.take(event, now) {
                send(Send {
                    event: bare(hash, word(stage)),
                    identity: None,
                });
            }
        }
    }

    /// Take one frame of `egress_receipts` at `now`: the merged receipts,
    /// or a rollback.
    pub fn on_merged(&mut self, frame: &ReceiptFrame, now: Instant, mut send: impl FnMut(Send)) {
        match frame {
            ReceiptFrame::Batch(batch) => {
                for receipt in &batch.receipts {
                    if self.take(receipt_event(receipt, Stage::Receipt, None), now) {
                        send(with_receipt(receipt, Word::Receipt));
                    }
                }
            }
            ReceiptFrame::Rollback(rollback) => {
                let rolled = self.table.roll_back(rollback.to_block, |hash| {
                    send(Send {
                        event: bare(hash, Word::RolledBack),
                        identity: None,
                    });
                });
                self.handles
                    .past_posted
                    .increment(u64::try_from(rolled.past_posted).unwrap_or(u64::MAX));
            }
        }
    }

    /// Take one frame of an executor's `tx_receipts` at `now`. The
    /// session tells the executors apart.
    pub fn on_copy(
        &mut self,
        session: i32,
        frame: &ReceiptFrame,
        now: Instant,
        mut send: impl FnMut(Send),
    ) {
        let ReceiptFrame::Batch(batch) = frame else {
            // The rollback of one executor reaches the clients through the
            // merged stream.
            return;
        };
        let slot = self.slot_of(session);
        for receipt in &batch.receipts {
            if self.take(receipt_event(receipt, Stage::Executed, Some(slot)), now) {
                send(with_receipt(receipt, Word::Executed));
            }
        }
    }

    /// Take a `tx_status` rejection at `now`. The rejection is terminal in
    /// the table, so a late `sent` of the twin is not sent after it. The
    /// record of `tx_errors` and `tx_status` stays the input of `rejected`
    /// until the decisions on #819.
    pub fn on_rejected(&mut self, status: &TxStatus, now: Instant, mut send: impl FnMut(Send)) {
        let event = Event {
            hash: status.tx_hash,
            stage: Stage::Rejected,
            block: None,
            executor: None,
        };
        if self.take(event, now) {
            let identity = status.identity();
            send(Send {
                event: TxStatusEvent::from_status(status, identity),
                identity,
            });
        }
    }

    /// One event through the table, with its metrics. True to send it.
    fn take(&mut self, event: Event, now: Instant) -> bool {
        let seen: Seen = self.table.observe(event, now);
        let index = event.stage.index();
        if let Some(latency) = seen.latency {
            self.handles.latency[index].record(latency.as_secs_f64());
        }
        if let (Some(latency), Some(slot)) = (seen.executor_latency, event.executor) {
            self.handles.executed[slot].record(latency.as_secs_f64());
        }
        if let Some(merge) = seen.merge_latency {
            self.handles.merge.record(merge.as_secs_f64());
        }
        if seen.evicted {
            self.handles.evicted.increment(1);
        }
        if !seen.send {
            self.handles.not_sent[index].increment(1);
        }
        seen.send
    }

    /// Count a jump in the `seq` of the frame's origin and kind. A frame
    /// of a restarted publisher starts again at 0, which counts as one gap.
    /// True when the frame starts after a gap.
    fn note_seq(&mut self, view: &TxEventsView<'_>) -> bool {
        let slot = usize::from(view.origin) * TxEventKind::ALL.len() + view.kind.index();
        let last = self.last_seq[slot];
        self.last_seq[slot] = view.seq;
        let gap = last != u64::MAX && view.seq != last.wrapping_add(1);
        if gap {
            metrics::counter!(m::EVENT_GAPS_TOTAL, "origin" => view.origin.to_string())
                .increment(1);
        }
        gap
    }

    /// The slot of an executor session. A new session takes the next
    /// slot, so a restarted executor reuses the oldest one.
    fn slot_of(&mut self, session: i32) -> usize {
        if let Some(slot) = self.sessions.iter().position(|s| *s == Some(session)) {
            return slot;
        }
        let slot = self.next_slot;
        self.sessions[slot] = Some(session);
        self.next_slot = (slot + 1) % EXECUTORS;
        slot
    }
}

fn receipt_event(receipt: &Receipt, stage: Stage, executor: Option<usize>) -> Event {
    Event {
        hash: receipt.tx_hash,
        stage,
        block: Some(receipt.block_number),
        executor,
    }
}

fn word(stage: Stage) -> Word {
    match stage {
        Stage::Accepted => Word::Accepted,
        Stage::Sent => Word::Sent,
        Stage::Executed => Word::Executed,
        Stage::Receipt => Word::Receipt,
        Stage::Posted => Word::Posted,
        Stage::Included => Word::Included,
        Stage::Rejected => Word::Rejected,
    }
}

/// An event that carries only the hash and the stage.
fn bare(tx_hash: B256, stage: Word) -> TxStatusEvent {
    TxStatusEvent {
        tx_hash,
        sender: None,
        nonce: None,
        stage,
        status: None,
        reason: None,
        expected_nonce: None,
    }
}

/// An event of a receipt: its sender, nonce and status.
fn with_receipt(receipt: &Receipt, stage: Word) -> Send {
    Send {
        event: TxStatusEvent {
            sender: Some(receipt.from),
            nonce: Some(receipt.nonce),
            status: Some(u8::from(receipt.status)),
            ..bare(receipt.tx_hash, stage)
        },
        identity: Some((receipt.from, receipt.nonce)),
    }
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
