//! The stage table (`docs/specs/2026-10-25-client-events.md`, 4.3 and
//! 4.4): the state of each recent transaction, in one fixed table.
//!
//! The streams of the stages have no order across publishers, and most
//! stages come more than once: two sequencers send `sent`, two egress
//! instances send each receipt, three executors send `executed`. The
//! notifier sends only the latest state of a hash. An event above the
//! state is sent, and it becomes the state. An event at or below the
//! state came too late or is a copy, and it is not sent. So a client
//! sees the stages in order, and it can see a stage skipped.
//!
//! The table is allocated once, at the start. An entry holds no `Vec`, no
//! `String` and no box, so an event that is not sent allocates nothing.
//! One thread owns the table, the hub task, so it needs no lock.

use std::time::{Duration, Instant};

use alloy_primitives::B256;

/// A stage of the order, with its value in the `state` of an entry. The
/// values follow the order, so one comparison decides a late event.
/// `Posted` and `Included` wait for the batcher frames, and `Rejected`
/// for the sequencer refusals. Their values are fixed now, so the table
/// does not change when they come.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum Stage {
    Accepted = 1,
    Sent = 2,
    Executed = 3,
    Receipt = 4,
    Posted = 5,
    Included = 6,
    /// A validation refusal. An execution result takes precedence.
    Rejected = 7,
}

/// The stages that the latency metrics name, in the order of their
/// index.
pub const STAGES: [Stage; 7] = [
    Stage::Accepted,
    Stage::Sent,
    Stage::Executed,
    Stage::Receipt,
    Stage::Posted,
    Stage::Included,
    Stage::Rejected,
];

impl Stage {
    /// The bit of the stage in `observed`. It keeps the bits of
    /// `TxStageKind::bit`: `sent` takes the bit of `Offered`, and
    /// `receipt` the bit of `Sealed`, which goes. Bit 128 is `dropped`,
    /// which is not a stage of the order.
    #[must_use]
    pub const fn bit(self) -> u8 {
        match self {
            Self::Sent => 1,
            Self::Receipt => 2,
            Self::Executed => 4,
            Self::Rejected => 8,
            Self::Accepted => 16,
            Self::Posted => 32,
            Self::Included => 64,
        }
    }

    /// The index of the stage in [`STAGES`].
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize - 1
    }

    /// The word of the stage in the metric labels.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Sent => "sent",
            Self::Executed => "executed",
            Self::Receipt => "receipt",
            Self::Posted => "posted",
            Self::Included => "included",
            Self::Rejected => "rejected",
        }
    }
}

/// The bit of `dropped` in `observed`.
pub const DROPPED_BIT: u8 = 128;

/// How many slots a lookup probes. A full probe overwrites its oldest
/// entry.
pub const PROBE: usize = 16;

/// The most executors that the `executors` bitmap tells apart.
pub const EXECUTORS: usize = 8;

/// One transaction. A free entry has no bit in `observed`.
#[derive(Clone, Copy, Debug, Default)]
struct Entry {
    hash: [u8; 32],
    /// The block of the first `executed` or `receipt`, 0 before it.
    block: u64,
    /// The time of the first observed event, in milliseconds from the
    /// start of the table.
    accepted_ms: u32,
    /// The time of the first observed `executed`, for the merge latency.
    executed_ms: u32,
    /// The highest stage sent, 0 when none was sent.
    state: u8,
    /// One bit for each stage that came (see [`Stage::bit`]).
    observed: u8,
    /// One bit for each executor that reported.
    executors: u8,
}

impl Entry {
    const fn is_free(&self) -> bool {
        self.observed == 0
    }

    /// Execution proves that a validation refusal from a twin is stale.
    /// A refusal suppresses admission events until execution proves it wrong.
    fn advances_to(&self, stage: Stage) -> bool {
        if stage == Stage::Rejected {
            return self.state < Stage::Executed as u8;
        }
        if self.state == Stage::Rejected as u8 {
            return stage >= Stage::Executed;
        }
        stage as u8 > self.state
    }
}

/// One observed event.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub hash: B256,
    pub stage: Stage,
    /// The block, for `executed` and `receipt`.
    pub block: Option<u64>,
    /// The executor slot, for `executed`.
    pub executor: Option<usize>,
}

/// What the table decided for one event.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Seen {
    /// The event is above the state: send it.
    pub send: bool,
    /// The first observation of the stage for the hash: the time from the
    /// first observed `accepted`. `None` for a copy, or with no
    /// `accepted` before it.
    pub latency: Option<Duration>,
    /// The first observation of this executor for the hash: the time from
    /// the first observed `accepted`.
    pub executor_latency: Option<Duration>,
    /// The first `receipt` after an `executed`: the time between them.
    pub merge_latency: Option<Duration>,
    /// The event took the slot of another transaction.
    pub evicted: bool,
}

/// What a rollback did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Rolled {
    /// Entries reset to `sent` or `accepted`.
    pub reset: usize,
    /// Entries at `posted` or above, which a rollback must not reach.
    pub past_posted: usize,
}

/// The fixed table.
pub struct StageTable {
    entries: Box<[Entry]>,
    mask: usize,
    start: Instant,
}

impl StageTable {
    /// A table of at least `capacity` entries, rounded up to a power of
    /// two. The one allocation of the table.
    #[must_use]
    pub fn new(capacity: usize, start: Instant) -> Self {
        let size = capacity.max(PROBE).next_power_of_two();
        Self {
            entries: vec![Entry::default(); size].into_boxed_slice(),
            mask: size - 1,
            start,
        }
    }

    /// The number of slots.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Take one event at `now`, and say whether to send it.
    pub fn observe(&mut self, event: Event, now: Instant) -> Seen {
        let now_ms = self.millis(now);
        let (slot, evicted) = self.slot(event.hash.as_ref(), now_ms);
        let entry = &mut self.entries[slot];
        if entry.is_free() {
            entry.hash.copy_from_slice(event.hash.as_ref());
            entry.accepted_ms = now_ms;
        }
        let bit = event.stage.bit();
        let first = entry.observed & bit == 0;
        let since = Duration::from_millis(u64::from(now_ms.wrapping_sub(entry.accepted_ms)));
        let counts = entry.observed & Stage::Accepted.bit() != 0;
        let mut seen = Seen {
            evicted,
            ..Seen::default()
        };
        if first && counts && event.stage != Stage::Accepted {
            seen.latency = Some(since);
        }
        if let Some(executor) = event.executor.filter(|e| *e < EXECUTORS) {
            let executor_bit = 1_u8 << executor;
            if entry.executors & executor_bit == 0 && counts {
                seen.executor_latency = Some(since);
            }
            entry.executors |= executor_bit;
        }
        if event.stage == Stage::Executed && first {
            entry.executed_ms = now_ms;
        }
        if event.stage == Stage::Receipt && first && entry.observed & Stage::Executed.bit() != 0 {
            seen.merge_latency = Some(Duration::from_millis(u64::from(
                now_ms.wrapping_sub(entry.executed_ms),
            )));
        }
        entry.observed |= bit;
        if entry.block == 0
            && let Some(block) = event.block
        {
            entry.block = block;
        }
        if entry.advances_to(event.stage) {
            entry.state = event.stage as u8;
            seen.send = true;
        }
        seen
    }

    /// Revoke the `executed` and `receipt` of every block above
    /// `to_block`: a rollback frame of `egress_receipts`. `revoked` gets
    /// the hash of each entry that it reset.
    pub fn roll_back(&mut self, to_block: u64, mut revoked: impl FnMut(B256)) -> Rolled {
        let mut rolled = Rolled::default();
        let mut revocable = self
            .entries
            .iter_mut()
            .filter(|e| !e.is_free() && e.block > to_block);
        for entry in &mut revocable {
            if entry.state >= Stage::Posted as u8 && entry.state != Stage::Rejected as u8 {
                rolled.past_posted += 1;
                continue;
            }
            if entry.state != Stage::Executed as u8 && entry.state != Stage::Receipt as u8 {
                continue;
            }
            entry.state = if entry.observed & Stage::Sent.bit() == 0 {
                Stage::Accepted as u8
            } else {
                Stage::Sent as u8
            };
            entry.observed &= !(Stage::Executed.bit() | Stage::Receipt.bit());
            entry.executors = 0;
            entry.block = 0;
            rolled.reset += 1;
            revoked(B256::from(entry.hash));
        }
        rolled
    }

    /// The slot of `hash`: its entry, else a free slot of its probe,
    /// else the oldest entry of the probe, which the caller overwrites.
    /// The second value is true when an entry goes.
    fn slot(&mut self, hash: &[u8], now_ms: u32) -> (usize, bool) {
        let start = Self::home(hash);
        let mut free = None;
        let mut oldest = (start & self.mask, 0_u32);
        for step in 0..PROBE {
            let slot = (start + step) & self.mask;
            let entry = &self.entries[slot];
            if entry.is_free() {
                free = free.or(Some(slot));
                continue;
            }
            if entry.hash == *hash {
                return (slot, false);
            }
            let age = now_ms.wrapping_sub(entry.accepted_ms);
            if age >= oldest.1 {
                oldest = (slot, age);
            }
        }
        if let Some(slot) = free {
            return (slot, false);
        }
        self.entries[oldest.0] = Entry::default();
        (oldest.0, true)
    }

    /// The home slot of a hash: its first 8 bytes. A keccak hash is
    /// uniform, so no further mixing is needed.
    fn home(hash: &[u8]) -> usize {
        let mut word = [0_u8; 8];
        word.copy_from_slice(&hash[..8]);
        usize::from_le_bytes(word)
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "the table keeps milliseconds in 32 bits and compares them with a wrapping subtraction"
    )]
    fn millis(&self, now: Instant) -> u32 {
        now.saturating_duration_since(self.start).as_millis() as u32
    }
}

#[cfg(test)]
#[path = "stages_tests.rs"]
mod tests;
