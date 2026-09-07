//! Remote-epoch verification — the interop mirror of [`crate::epoch_verify`].
//!
//! Deriving remote epochs is only half the guarantee, exactly as for L1
//! epochs: without a checker, a buggy or malicious watcher/sealer produces a
//! canonical stream whose interop lane nobody can re-derive and nothing
//! notices. This module checks two things:
//!
//! - **Pair-sequence rules, synchronous.** Per-origin `seq` monotonicity —
//!   dense, no regress, no skip — plus record well-formedness (non-empty,
//!   internally dense, position-derived `source_hash`). These read only local
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

use kardamom_engine::{ExecutorError, RemoteEpochObserver};
use kardamom_types::xchain::{RemoteEpochRecord, remote_source_hash};

use crate::Divergence;
use crate::metrics;

/// Why a remote-epoch record failed the inline checks. Every variant is a
/// chain-level fault — the record is already ON the canonical stream, so
/// disagreeing about it is a consensus fault, not a pair problem (§10's
/// failure-semantics asymmetry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteEpochFault {
    /// The pair's seq went backwards (or repeated): `got < expected`.
    SeqRegressed {
        origin: u64,
        expected: u64,
        got: u64,
    },
    /// The pair's seq skipped ahead: the messages in between are
    /// unaccounted for.
    SeqSkipped {
        origin: u64,
        expected: u64,
        got: u64,
    },
    /// An empty record — invalid by construction (remote origins advance
    /// only when messages exist).
    Empty { origin: u64 },
    /// `messages[index].seq` breaks the dense-from-`first_seq` rule.
    NonDense {
        origin: u64,
        index: usize,
        expected: u64,
        got: u64,
    },
    /// A message's `source_hash` is not `remote_source_hash(origin, seq)` —
    /// the canonical id is position-derived, so a mismatch means the record
    /// was not produced by the shared derivation rule.
    SourceHashMismatch { origin: u64, seq: u64 },
    /// `first_seq + index` overflows `u64` — a crafted or corrupt record.
    /// `index` is either a message position (`< messages.len()`, the
    /// density check) or `messages.len()` itself (the one-past cursor
    /// [`RemoteEpochVerifier::observe`] forms after this record verifies).
    /// Checked once, up front, so neither site re-derives the bound.
    SeqOverflow {
        origin: u64,
        first_seq: u64,
        index: usize,
    },
}

impl std::fmt::Display for RemoteEpochFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeqRegressed {
                origin,
                expected,
                got,
            } => write!(
                f,
                "pair (origin {origin}) seq regressed: expected {expected}, got {got} — \
                 a repeated or rewound batch"
            ),
            Self::SeqSkipped {
                origin,
                expected,
                got,
            } => write!(
                f,
                "pair (origin {origin}) skipped {} message(s): expected seq {expected}, got \
                 {got} — the messages in between are unaccounted for",
                got.saturating_sub(*expected)
            ),
            Self::Empty { origin } => write!(
                f,
                "empty remote-epoch record for origin {origin} — invalid by construction"
            ),
            Self::NonDense {
                origin,
                index,
                expected,
                got,
            } => write!(
                f,
                "remote-epoch record for origin {origin} is not dense: messages[{index}] \
                 carries seq {got}, expected {expected}"
            ),
            Self::SourceHashMismatch { origin, seq } => write!(
                f,
                "message (origin {origin}, seq {seq}) carries a source_hash that is not \
                 remote_source_hash(origin, seq)"
            ),
            Self::SeqOverflow {
                origin,
                first_seq,
                index,
            } => write!(
                f,
                "remote-epoch record for origin {origin}: first_seq {first_seq} + index \
                 {index} overflows u64 — a crafted or corrupt record"
            ),
        }
    }
}

/// The inline record checks, split out so they are testable without an
/// engine: pair-seq monotonicity against `expected` (`None` before the first
/// record for this origin — a resumed/late-joining validator legitimately
/// starts mid-pair, mirroring [`crate::epoch_verify::check_sequence`]'s
/// first-epoch exemption) plus record well-formedness.
pub(crate) fn check_remote_epoch(
    expected: Option<u64>,
    rec: &RemoteEpochRecord,
) -> Result<(), RemoteEpochFault> {
    let origin = rec.origin_chain_id;
    if rec.messages.is_empty() {
        return Err(RemoteEpochFault::Empty { origin });
    }
    // `first_seq` is wire data (the canonical-stream record), so its
    // whole declared range must fit in `u64` before anything below reads
    // an individual message's seq against it. This one check covers two
    // sites: every `first_seq + i` the density loop computes for
    // `i < len` (checked here at `index = len`, the largest offset that
    // matters, since a smaller offset cannot overflow if the largest one
    // does not), and the one-past cursor
    // ([`RemoteEpochVerifier::observe`]'s `last_seq() + 1`, which is
    // `first_seq + len`) — the same value. Checking it once here lets
    // both use plain seq arithmetic afterward.
    let len = rec.messages.len() as u64;
    if rec.first_seq.checked_add(len).is_none() {
        return Err(RemoteEpochFault::SeqOverflow {
            origin,
            first_seq: rec.first_seq,
            index: rec.messages.len(),
        });
    }
    if let Some(expected) = expected {
        if rec.first_seq < expected {
            return Err(RemoteEpochFault::SeqRegressed {
                origin,
                expected,
                got: rec.first_seq,
            });
        }
        if rec.first_seq > expected {
            return Err(RemoteEpochFault::SeqSkipped {
                origin,
                expected,
                got: rec.first_seq,
            });
        }
    }
    for (i, msg) in rec.messages.iter().enumerate() {
        // Bounded: `first_seq + len` fits (checked above) and `i < len`,
        // so `first_seq + i` cannot overflow either.
        let want_seq = rec.first_seq + i as u64;
        if msg.seq != want_seq {
            return Err(RemoteEpochFault::NonDense {
                origin,
                index: i,
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
    }
    Ok(())
}

/// Engine-side seam: the [`RemoteEpochObserver`] the destination validator
/// wires in place of the executor's `None`. Inline checks only — see the
/// module docs for what the later phase adds.
pub struct RemoteEpochVerifier {
    /// Per-origin next expected seq (one past the last verified record's
    /// `last_seq`). `BTreeMap`: one interop node hosts every pair (§10's
    /// one-node-not-one-process-per-peer shape).
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

impl RemoteEpochObserver for RemoteEpochVerifier {
    fn observe(&mut self, rec: &RemoteEpochRecord) -> Result<(), ExecutorError> {
        // A verdict recorded elsewhere (write-set/receipt divergence, or —
        // later phase — a deferred content check) lands here on the next
        // record, exactly like EpochVerifier.
        if let Some(reason) = self.divergence.halt_reason("validator halted") {
            return Err(ExecutorError::State(reason));
        }
        let expected = self.next_seq.get(&rec.origin_chain_id).copied();
        if let Err(fault) = check_remote_epoch(expected, rec) {
            metrics::counter_remote_epoch_fault();
            self.divergence
                .record(format!("remote-epoch verification failed: {fault}"));
            return Err(ExecutorError::State(fault.to_string()));
        }
        // Bounded: `check_remote_epoch` already proved `first_seq +
        // messages.len()` fits in `u64` (the `SeqOverflow` check), and
        // `last_seq() + 1` is exactly that value, so this cannot overflow.
        self.next_seq
            .insert(rec.origin_chain_id, rec.last_seq() + 1);
        metrics::counter_remote_epoch_verified();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use kardamom_types::xchain::XChainMessage;

    use super::*;

    fn record(origin: u64, first_seq: u64, n: u64) -> RemoteEpochRecord {
        RemoteEpochRecord {
            origin_chain_id: origin,
            anchor_number: 40,
            anchor_hash: B256::repeat_byte(0x0B),
            first_seq,
            messages: (first_seq..first_seq + n)
                .map(|seq| XChainMessage {
                    source_hash: remote_source_hash(origin, seq),
                    seq,
                    origin_sender: Address::repeat_byte(0xA1),
                    target: Address::repeat_byte(0xB2),
                    value: 0,
                    gas_limit: 100_000,
                    input: bytes::Bytes::default(),
                    callback: None,
                })
                .collect(),
        }
    }

    #[test]
    fn dense_records_verify_and_advance_per_origin() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        // First record per origin: any first_seq (mid-pair resume).
        v.observe(&record(7, 3, 2)).unwrap();
        // Next must continue at 5.
        v.observe(&record(7, 5, 1)).unwrap();
        // A second origin has its own cursor.
        v.observe(&record(9, 0, 4)).unwrap();
        v.observe(&record(9, 4, 1)).unwrap();
        assert!(!div.is_halted());
    }

    #[test]
    fn a_regressed_seq_halts() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 2)).unwrap();
        let err = v.observe(&record(7, 1, 1)).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)), "{err:?}");
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("regressed"));
        // The latch holds: the NEXT record fails too, even a well-formed one.
        assert!(v.observe(&record(9, 0, 1)).is_err());
    }

    #[test]
    fn a_skipped_seq_halts() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 2)).unwrap();
        let err = v.observe(&record(7, 3, 1)).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)));
        assert!(div.reason().unwrap().contains("skipped 1 message(s)"));
    }

    #[test]
    fn record_well_formedness_rules() {
        // Empty record.
        let mut r = record(7, 0, 1);
        r.messages.clear();
        assert_eq!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::Empty { origin: 7 })
        );
        // Non-dense internal seq.
        let mut r = record(7, 0, 3);
        r.messages[2].seq = 5;
        assert!(matches!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::NonDense {
                index: 2,
                expected: 2,
                got: 5,
                ..
            })
        ));
        // A source_hash not derived from (origin, seq).
        let mut r = record(7, 0, 1);
        r.messages[0].source_hash = B256::repeat_byte(0xEE);
        assert!(matches!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::SourceHashMismatch { origin: 7, seq: 0 })
        ));
        // `first_seq` at the very top of `u64`: even one message makes
        // `first_seq + len` overflow. Built directly (not through
        // `record`'s `first_seq..first_seq + n` range, which would
        // overflow constructing the fixture itself).
        let overflowing = RemoteEpochRecord {
            origin_chain_id: 7,
            anchor_number: 40,
            anchor_hash: B256::repeat_byte(0x0B),
            first_seq: u64::MAX,
            messages: vec![XChainMessage {
                source_hash: remote_source_hash(7, u64::MAX),
                seq: u64::MAX,
                origin_sender: Address::repeat_byte(0xA1),
                target: Address::repeat_byte(0xB2),
                value: 0,
                gas_limit: 100_000,
                input: bytes::Bytes::default(),
                callback: None,
            }],
        };
        assert_eq!(
            check_remote_epoch(None, &overflowing),
            Err(RemoteEpochFault::SeqOverflow {
                origin: 7,
                first_seq: u64::MAX,
                index: 1,
            })
        );
    }

    #[test]
    fn cross_origin_ordering_is_not_constrained() {
        // §11: no cross-pair ordering guarantee — interleaved origins each
        // keep their own dense lane.
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 1)).unwrap();
        v.observe(&record(9, 10, 1)).unwrap();
        v.observe(&record(7, 1, 1)).unwrap();
        v.observe(&record(9, 11, 1)).unwrap();
        assert!(!div.is_halted());
    }
}
