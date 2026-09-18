//! Remote-epoch verification — the interop mirror of [`crate::epoch_verify`].
//!
//! Deriving remote epochs is only half the guarantee, exactly as for L1
//! epochs: without a checker, a buggy or malicious watcher/sealer produces a
//! canonical stream whose interop lane nobody can re-derive and nothing
//! notices. This module checks two things:
//!
//! - **Pair-sequence rules, synchronous.** Per-origin `seq` monotonicity —
//!   dense, no regress, no skip — plus record well-formedness (internally
//!   dense, position-derived `source_hash`; the record's `messages` is
//!   never empty, so nothing here checks that). These read only local
//!   state, so they run inline on the exec thread and reject BEFORE the
//!   record's messages execute. A violation is a chain fault: divergence
//!   halt, the same posture as [`EpochFault`](crate::epoch_verify::EpochFault).
//! - **Content-vs-origin, not checked here.** Whether the batch matches what
//!   the origin chain actually sent needs a transport to the origin and
//!   per-pair trust config. A fabricated-but-well-sequenced batch is not
//!   caught by this validator alone; it is caught by any peer running its
//!   own validator of the origin.

use std::collections::BTreeMap;
use std::sync::Arc;

use kardamom_engine::delta::ParentState;
use kardamom_engine::{ExecutorError, RemoteEpochObserver};
use kardamom_types::StateDatabase;
use kardamom_types::xchain::{BoundsFault, INBOX, Inbox, RemoteEpochRecord, remote_source_hash};

use crate::Divergence;
use crate::metrics;

/// Why a remote-epoch record failed the inline checks. Every variant is a
/// chain-level fault — the record is already ON the canonical stream, so
/// disagreeing about it is a consensus fault, not a pair problem.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RemoteEpochFault {
    /// The pair's seq went backwards (or repeated): `got < expected`.
    #[error(
        "pair (origin {origin}) seq regressed: expected {expected}, got {got} — \
         a repeated or rewound batch"
    )]
    SeqRegressed {
        origin: u64,
        expected: u64,
        got: u64,
    },
    /// The pair's seq skipped ahead: the messages in between are
    /// unaccounted for.
    #[error(
        "pair (origin {origin}) skipped {} message(s): expected seq {expected}, got {got} — \
         the messages in between are unaccounted for",
        got.saturating_sub(*expected)
    )]
    SeqSkipped {
        origin: u64,
        expected: u64,
        got: u64,
    },
    /// `messages[index].seq` breaks the dense-from-`first_seq` rule.
    #[error(
        "remote-epoch record for origin {origin} is not dense: messages[{index}] carries \
         seq {got}, expected {expected}"
    )]
    NonDense {
        origin: u64,
        index: usize,
        expected: u64,
        got: u64,
    },
    /// A message's `source_hash` is not `remote_source_hash(origin, seq)` —
    /// the canonical id is position-derived, so a mismatch means the record
    /// was not produced by the shared derivation rule.
    #[error(
        "message (origin {origin}, seq {seq}) carries a source_hash that is not \
         remote_source_hash(origin, seq)"
    )]
    SourceHashMismatch { origin: u64, seq: u64 },
    /// The record's seq range does not fit in u64. The lane cursor
    /// (`last_seq + 1`) would overflow.
    #[error(
        "remote-epoch record for origin {origin} starts at seq {first_seq}; its seq range \
         overflows u64"
    )]
    SeqOverflow { origin: u64, first_seq: u64 },
    /// A message violates the Outbox's own send-time bounds. The same
    /// bounds `derive_remote_epoch` applies on the producer; the origin
    /// `Outbox` rejects each of these at send time, so a record that
    /// carries one did not come from an honest origin.
    #[error("message (origin {origin}) violates outbox bounds: {fault}")]
    Bounds { origin: u64, fault: BoundsFault },
}

/// A record's seq range, proven to fit `u64` at construction:
/// `first..=last`, with `next_cursor` (`last + 1`) proven at the same
/// time. Built once by [`LaneCheck::lane_position`]; [`LaneCheck::messages`]
/// and `observe` both derive from it, instead of each re-deriving an
/// index-based seq under a comment claiming the arithmetic "cannot fail" —
/// this type is what proves that, once, at the one place it is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeqRange {
    first: u64,
    last: u64,
    next: u64,
}

impl SeqRange {
    /// `None` if `first + len` (the room needed for `next_cursor`, one more
    /// than `last` itself) does not fit `u64`.
    fn new(first: u64, len: core::num::NonZeroUsize) -> Option<Self> {
        let len = u64::try_from(len.get()).ok()?;
        let next = first.checked_add(len)?;
        let last = next.checked_sub(1)?;
        Some(Self { first, last, next })
    }

    /// Every seq in the range, in order.
    fn iter(&self) -> core::ops::RangeInclusive<u64> {
        self.first..=self.last
    }

    /// The lane cursor after this record: one past `last`.
    fn next_cursor(&self) -> u64 {
        self.next
    }
}

/// The inline record checks, split out so they are testable without an
/// engine: pair-seq monotonicity against `expected`
/// ([`LaneCheck::lane_position`]) plus per-message well-formedness
/// ([`LaneCheck::messages`]).
///
/// `expected` is never optional. Before the first record for an origin the
/// caller seeds it from `Inbox.nextSeq[origin]` in the parent state, so a
/// resumed or late-joining validator checks its first record against the
/// chain's own lane cursor. The L1 path's first-epoch exemption does not
/// apply here: a skipped lane seq is a permanent hole, and the state holds
/// the cursor that proves it.
pub(crate) fn check_remote_epoch(
    expected: u64,
    rec: &RemoteEpochRecord,
) -> Result<SeqRange, RemoteEpochFault> {
    LaneCheck { expected, rec }.run()
}

/// One record checked against one pair's expected lane position. Groups
/// `expected` and `rec` as state so [`Self::lane_position`] and
/// [`Self::messages`] read them as fields instead of each retaking both as
/// parameters.
struct LaneCheck<'a> {
    expected: u64,
    rec: &'a RemoteEpochRecord,
}

impl LaneCheck<'_> {
    /// The position check, then the per-message check against the range
    /// it proves.
    fn run(&self) -> Result<SeqRange, RemoteEpochFault> {
        let range = self.lane_position()?;
        self.messages(&range)?;
        Ok(range)
    }

    /// The record's position in the pair's lane: starts at `expected`,
    /// and its seq range fits in `u64` with room for the next cursor.
    /// `messages` is never empty — the type guarantees it — so there is
    /// nothing to check for that here.
    fn lane_position(&self) -> Result<SeqRange, RemoteEpochFault> {
        let origin = self.rec.origin_chain_id;
        if self.rec.first_seq < self.expected {
            return Err(RemoteEpochFault::SeqRegressed {
                origin,
                expected: self.expected,
                got: self.rec.first_seq,
            });
        }
        if self.rec.first_seq > self.expected {
            return Err(RemoteEpochFault::SeqSkipped {
                origin,
                expected: self.expected,
                got: self.rec.first_seq,
            });
        }
        SeqRange::new(self.rec.first_seq, self.rec.messages.len()).ok_or(
            RemoteEpochFault::SeqOverflow {
                origin,
                first_seq: self.rec.first_seq,
            },
        )
    }

    /// Every message's shape: dense from `first_seq`, a position-derived
    /// `source_hash`, and within the Outbox's send-time bounds.
    fn messages(&self, range: &SeqRange) -> Result<(), RemoteEpochFault> {
        let origin = self.rec.origin_chain_id;
        range
            .iter()
            .zip(self.rec.messages.iter())
            .enumerate()
            .try_for_each(|(index, (want_seq, msg))| {
                if msg.seq != want_seq {
                    return Err(RemoteEpochFault::NonDense {
                        origin,
                        index,
                        expected: want_seq,
                        got: msg.seq,
                    });
                }
                if msg.source_hash != remote_source_hash(origin, msg.seq) {
                    return Err(RemoteEpochFault::SourceHashMismatch {
                        origin,
                        seq: msg.seq,
                    });
                }
                msg.check_bounds()
                    .map_err(|fault| RemoteEpochFault::Bounds { origin, fault })
            })
    }
}

/// Engine-side seam: the [`RemoteEpochObserver`] the destination validator
/// wires in place of the executor's `None`. Runs only the inline
/// pair-sequence checks in this module; content-vs-origin is not checked
/// here (see the module docs).
pub struct RemoteEpochVerifier {
    /// Per-origin next expected seq (one past the last verified record's
    /// `last_seq`). `BTreeMap`: one interop node verifies every origin
    /// pair for this destination, not one process per peer, so the cursor
    /// set is keyed by origin. An origin not in the map is seeded from
    /// `Inbox.nextSeq[origin]` in the parent state on its first record.
    next_seq: BTreeMap<u64, u64>,
    divergence: Arc<Divergence>,
}

impl RemoteEpochVerifier {
    pub fn new(divergence: Arc<Divergence>) -> Self {
        Self {
            next_seq: BTreeMap::new(),
            divergence,
        }
    }
}

impl RemoteEpochVerifier {
    /// The expected first seq for `origin`: the in-memory cursor, or on
    /// first sight `Inbox.nextSeq[origin]` from the parent state. A read
    /// failure is a fault: guessing a cursor is how a hole goes unseen.
    fn expected_for<S: StateDatabase>(
        &mut self,
        origin: u64,
        parent: &ParentState<'_, S>,
    ) -> Result<u64, ExecutorError> {
        if let Some(v) = self.next_seq.get(&origin) {
            return Ok(*v);
        }
        let raw = parent
            .storage(INBOX, Inbox::next_seq_slot(origin))
            .map_err(|e| {
                ExecutorError::State(format!(
                    "seed Inbox.nextSeq[{origin}] from the parent state: {e}"
                ))
            })?;
        let seeded = u64::try_from(raw).map_err(|_| {
            ExecutorError::State(format!(
                "Inbox.nextSeq[{origin}] = {raw} does not fit a u64"
            ))
        })?;
        tracing::info!(
            origin,
            next_seq = seeded,
            "remote-epoch verifier seeded the lane cursor from Inbox.nextSeq"
        );
        Ok(seeded)
    }
}

impl<S: StateDatabase> RemoteEpochObserver<S> for RemoteEpochVerifier {
    fn observe(
        &mut self,
        rec: &RemoteEpochRecord,
        parent: &ParentState<'_, S>,
    ) -> Result<(), ExecutorError> {
        // A verdict recorded elsewhere (write-set or receipt divergence)
        // lands here on the next record, exactly like EpochVerifier.
        if let Some(reason) = self.divergence.halt_reason("validator halted") {
            return Err(ExecutorError::State(reason));
        }
        let expected = match self.expected_for(rec.origin_chain_id, parent) {
            Ok(v) => v,
            Err(e) => {
                metrics::counter_remote_epoch_fault();
                self.divergence
                    .record(format!("remote-epoch verification failed: {e}"));
                return Err(e);
            }
        };
        let range = match check_remote_epoch(expected, rec) {
            Ok(range) => range,
            Err(fault) => {
                metrics::counter_remote_epoch_fault();
                self.divergence
                    .record(format!("remote-epoch verification failed: {fault}"));
                return Err(ExecutorError::State(fault.to_string()));
            }
        };
        self.next_seq
            .insert(rec.origin_chain_id, range.next_cursor());
        metrics::counter_remote_epoch_verified();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
