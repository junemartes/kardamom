//! Remote-epoch verification — the interop mirror of [`crate::epoch_verify`]
//! (`docs/specs/interop-outbox-messaging-spec.md` §10).
//!
//! Deriving remote epochs is only half the guarantee, exactly as for L1
//! epochs: without a checker, a buggy or malicious watcher/sealer produces a
//! canonical stream whose interop lane nobody can re-derive and nothing
//! notices. This module is the checker's PHASE-1 SKELETON:
//!
//! - **Pair-sequence rules, synchronous.** Per-origin `seq` monotonicity —
//!   dense, no regress, no skip — plus record well-formedness (non-empty,
//!   internally dense, position-derived `source_hash`). These read only local
//!   state, so they run inline on the exec thread and reject BEFORE the
//!   record's messages execute. A violation is a chain fault: divergence
//!   halt, the same posture as [`EpochFault`](crate::epoch_verify::EpochFault).
//! - **Content-vs-origin, NOT YET.** Whether the batch matches what origin
//!   chain A actually sent (re-derivation from A's feed under a §10 posture —
//!   own-validator over DA / signed stream, or a validator attestation
//!   quorum) needs a transport to A and per-pair trust config. That is a
//!   later phase (E2, interop P2: attestation keys + quorum wiring); when it
//!   lands it takes the [`crate::epoch_verify::EpochVerifier`] shape — a
//!   background task with a deferred verdict, observed here on the next
//!   record. Until then a fabricated-but-well-sequenced batch is NOT caught
//!   by this validator alone; it IS caught by any peer running its own
//!   validator of the origin (§10 posture A).

use std::collections::BTreeMap;
use std::sync::Arc;

use kardamom_engine::{ExecutorError, RemoteEpochObserver};
use kardamom_types::xchain::{
    MAX_DATA_BYTES, MAX_MESSAGE_GAS, RemoteEpochRecord, remote_source_hash,
};

use crate::Divergence;
use crate::metrics;

/// Why a remote-epoch record failed the inline checks. Every variant is a
/// chain-level fault — the record is already ON the canonical stream, so
/// disagreeing about it is a consensus fault, not a pair problem (§10's
/// failure-semantics asymmetry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEpochFault {
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
    /// The record's seq range does not fit in u64. The lane cursor
    /// (`last_seq + 1`) would overflow.
    SeqOverflow { origin: u64, first_seq: u64 },
    /// A message carries a nonzero value. v1 delivery is value-free, and
    /// the origin `Outbox` rejects such a send, so the record did not come
    /// from the shared derivation rule.
    ValueNotAllowed { origin: u64, seq: u64, value: u128 },
    /// A message's `gas_limit` is above `MAX_MESSAGE_GAS`, which the origin
    /// `Outbox` rejects.
    GasLimitAboveCap {
        origin: u64,
        seq: u64,
        gas_limit: u64,
    },
    /// A message's `input` is longer than `MAX_DATA_BYTES`, which the origin
    /// `Outbox` rejects.
    DataAboveCap { origin: u64, seq: u64, len: usize },
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
            Self::SeqOverflow { origin, first_seq } => write!(
                f,
                "remote-epoch record for origin {origin} starts at seq {first_seq}; its seq \
                 range overflows u64"
            ),
            Self::ValueNotAllowed { origin, seq, value } => write!(
                f,
                "message (origin {origin}, seq {seq}) carries value {value}; v1 delivery is \
                 value-free"
            ),
            Self::GasLimitAboveCap {
                origin,
                seq,
                gas_limit,
            } => write!(
                f,
                "message (origin {origin}, seq {seq}) gas limit {gas_limit} is above the cap \
                 {MAX_MESSAGE_GAS}"
            ),
            Self::DataAboveCap { origin, seq, len } => write!(
                f,
                "message (origin {origin}, seq {seq}) data length {len} is above the cap \
                 {MAX_DATA_BYTES}"
            ),
        }
    }
}

/// The inline record checks, split out so they are testable without an
/// engine: pair-seq monotonicity against `expected` (`None` before the first
/// record for this origin — a resumed/late-joining validator legitimately
/// starts mid-pair, mirroring [`crate::epoch_verify::check_sequence`]'s
/// first-epoch exemption) plus record well-formedness.
pub fn check_remote_epoch(
    expected: Option<u64>,
    rec: &RemoteEpochRecord,
) -> Result<(), RemoteEpochFault> {
    let origin = rec.origin_chain_id;
    if rec.messages.is_empty() {
        return Err(RemoteEpochFault::Empty { origin });
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
    // The seq range must fit in u64 with room for the next cursor
    // (`last_seq + 1`). This check runs before any per-message arithmetic
    // and before `observe` calls `last_seq()`.
    if rec
        .first_seq
        .checked_add(rec.messages.len() as u64)
        .is_none()
    {
        return Err(RemoteEpochFault::SeqOverflow {
            origin,
            first_seq: rec.first_seq,
        });
    }
    for (i, msg) in rec.messages.iter().enumerate() {
        // Cannot overflow: the range check above covers `first_seq + len`.
        let want_seq =
            rec.first_seq
                .checked_add(i as u64)
                .ok_or(RemoteEpochFault::SeqOverflow {
                    origin,
                    first_seq: rec.first_seq,
                })?;
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
        // The same bounds `derive_remote_epoch` applies on the producer.
        // The origin `Outbox` rejects each of these at send time, so a
        // record that carries one did not come from an honest origin.
        if msg.value != 0 {
            return Err(RemoteEpochFault::ValueNotAllowed {
                origin,
                seq: msg.seq,
                value: msg.value,
            });
        }
        if msg.gas_limit > MAX_MESSAGE_GAS {
            return Err(RemoteEpochFault::GasLimitAboveCap {
                origin,
                seq: msg.seq,
                gas_limit: msg.gas_limit,
            });
        }
        if msg.input.len() > MAX_DATA_BYTES {
            return Err(RemoteEpochFault::DataAboveCap {
                origin,
                seq: msg.seq,
                len: msg.input.len(),
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
        if self.divergence.is_halted() {
            return Err(ExecutorError::State(
                self.divergence
                    .reason()
                    .unwrap_or_else(|| "validator halted".to_string()),
            ));
        }
        let expected = self.next_seq.get(&rec.origin_chain_id).copied();
        if let Err(fault) = check_remote_epoch(expected, rec) {
            metrics::counter_remote_epoch_fault();
            self.divergence
                .record(format!("remote-epoch verification failed: {fault}"));
            return Err(ExecutorError::State(fault.to_string()));
        }
        // `check_remote_epoch` proved `first_seq + len` fits, so this
        // `checked_add` cannot fail. It stays checked so a future change to
        // the checks cannot reintroduce a silent wrap.
        let next = rec
            .last_seq()
            .checked_add(1)
            .ok_or_else(|| ExecutorError::State("remote-epoch lane cursor overflow".to_string()))?;
        self.next_seq.insert(rec.origin_chain_id, next);
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
                    input: Default::default(),
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
    }

    #[test]
    fn outbox_bounds_are_mirrored_on_the_validator() {
        // Value.
        let mut r = record(7, 0, 1);
        r.messages[0].value = 1;
        assert_eq!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::ValueNotAllowed {
                origin: 7,
                seq: 0,
                value: 1
            })
        );
        // Gas limit.
        let mut r = record(7, 0, 1);
        r.messages[0].gas_limit = MAX_MESSAGE_GAS + 1;
        assert_eq!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::GasLimitAboveCap {
                origin: 7,
                seq: 0,
                gas_limit: MAX_MESSAGE_GAS + 1
            })
        );
        // Data length.
        let mut r = record(7, 0, 1);
        r.messages[0].input = bytes::Bytes::from(vec![0xFFu8; MAX_DATA_BYTES + 1]);
        assert_eq!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::DataAboveCap {
                origin: 7,
                seq: 0,
                len: MAX_DATA_BYTES + 1
            })
        );
        // The honest maxima pass.
        let mut r = record(7, 0, 1);
        r.messages[0].gas_limit = MAX_MESSAGE_GAS;
        r.messages[0].input = bytes::Bytes::from(vec![0xFFu8; MAX_DATA_BYTES]);
        assert_eq!(check_remote_epoch(None, &r), Ok(()));
    }

    #[test]
    fn seq_overflow_is_a_fault_not_a_panic() {
        // One message at u64::MAX: the next cursor would overflow.
        let mut r = record(7, 0, 1);
        r.first_seq = u64::MAX;
        r.messages[0].seq = u64::MAX;
        r.messages[0].source_hash = remote_source_hash(7, u64::MAX);
        assert_eq!(
            check_remote_epoch(None, &r),
            Err(RemoteEpochFault::SeqOverflow {
                origin: 7,
                first_seq: u64::MAX
            })
        );
        // Through the verifier: a fault, no panic, and the latch holds.
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        assert!(v.observe(&r).is_err());
        assert!(div.reason().unwrap().contains("overflows"));
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
