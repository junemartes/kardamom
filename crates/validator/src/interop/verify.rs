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

use kardamom_engine::{ExecutorError, ParentStorageReader, RemoteEpochObserver};
use kardamom_types::xchain::{
    INBOX, MAX_DATA_BYTES, MAX_MESSAGE_GAS, RemoteEpochRecord, inbox_next_seq_slot,
    remote_source_hash,
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
/// engine: pair-seq monotonicity against `expected` plus record
/// well-formedness.
///
/// `expected` is never optional. Before the first record for an origin the
/// caller seeds it from `Inbox.nextSeq[origin]` in the parent state, so a
/// resumed or late-joining validator checks its first record against the
/// chain's own lane cursor. The L1 path's first-epoch exemption does not
/// apply here: a skipped lane seq is a permanent hole, and the state holds
/// the cursor that proves it (audit H9).
pub fn check_remote_epoch(expected: u64, rec: &RemoteEpochRecord) -> Result<(), RemoteEpochFault> {
    let origin = rec.origin_chain_id;
    if rec.messages.is_empty() {
        return Err(RemoteEpochFault::Empty { origin });
    }
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
    /// one-node-not-one-process-per-peer shape). An origin not in the map
    /// is seeded from `Inbox.nextSeq[origin]` in the parent state on its
    /// first record.
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
    fn expected_for(
        &mut self,
        origin: u64,
        parent_storage: &ParentStorageReader<'_>,
    ) -> Result<u64, String> {
        if let Some(v) = self.next_seq.get(&origin) {
            return Ok(*v);
        }
        let raw = parent_storage(INBOX, inbox_next_seq_slot(origin))
            .map_err(|e| format!("seed Inbox.nextSeq[{origin}] from the parent state: {e}"))?;
        let seeded = u64::try_from(raw)
            .map_err(|_| format!("Inbox.nextSeq[{origin}] = {raw} does not fit a u64"))?;
        tracing::info!(
            origin,
            next_seq = seeded,
            "remote-epoch verifier seeded the lane cursor from Inbox.nextSeq"
        );
        Ok(seeded)
    }
}

impl RemoteEpochObserver for RemoteEpochVerifier {
    fn observe(
        &mut self,
        rec: &RemoteEpochRecord,
        parent_storage: &ParentStorageReader<'_>,
    ) -> Result<(), ExecutorError> {
        // A verdict recorded elsewhere (write-set/receipt divergence, or —
        // later phase — a deferred content check) lands here on the next
        // record, exactly like EpochVerifier.
        if let Some(reason) = self.divergence.halt_reason("validator halted") {
            return Err(ExecutorError::State(reason));
        }
        let expected = match self.expected_for(rec.origin_chain_id, parent_storage) {
            Ok(v) => v,
            Err(e) => {
                metrics::counter_remote_epoch_fault();
                self.divergence
                    .record(format!("remote-epoch verification failed: {e}"));
                return Err(ExecutorError::State(e));
            }
        };
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
    use alloy_primitives::{Address, B256, U256};
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

    /// A parent state whose `Inbox.nextSeq` is `seeds[origin]` and zero
    /// for every other origin.
    fn parent_with(seeds: &[(u64, u64)]) -> impl Fn(Address, B256) -> Result<U256, String> + '_ {
        move |addr, slot| {
            assert_eq!(addr, INBOX, "only the Inbox is read");
            let v = seeds
                .iter()
                .find(|(origin, _)| inbox_next_seq_slot(*origin) == slot)
                .map(|(_, next)| *next)
                .unwrap_or(0);
            Ok(U256::from(v))
        }
    }

    fn fresh() -> impl Fn(Address, B256) -> Result<U256, String> {
        |_, _| Ok(U256::ZERO)
    }

    #[test]
    fn dense_records_verify_and_advance_per_origin() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        let parent = parent_with(&[(7, 3)]);
        // The first record per origin must start at the seeded cursor: 3
        // for origin 7, 0 for origin 9.
        v.observe(&record(7, 3, 2), &parent).unwrap();
        // Next must continue at 5.
        v.observe(&record(7, 5, 1), &parent).unwrap();
        // A second origin has its own cursor.
        v.observe(&record(9, 0, 4), &parent).unwrap();
        v.observe(&record(9, 4, 1), &parent).unwrap();
        assert!(!div.is_halted());
    }

    /// Audit H9: the first record is never exempt. A validator restored
    /// from a snapshot whose Inbox says 3 halts on a record that starts at 5.
    #[test]
    fn the_first_record_is_checked_against_the_seeded_cursor() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        let parent = parent_with(&[(7, 3)]);
        let err = v.observe(&record(7, 5, 1), &parent).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)), "{err:?}");
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("skipped 2 message(s)"));
    }

    /// The seed is read ONCE per origin. Later records use the in-memory
    /// cursor, so a parent read that changes underneath does not re-seed.
    #[test]
    fn the_seed_is_read_once_per_origin() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 2), &parent_with(&[(7, 0)]))
            .unwrap();
        // A parent that now claims 9 must not move the cursor: 2 is next.
        v.observe(&record(7, 2, 1), &parent_with(&[(7, 9)]))
            .unwrap();
        assert!(!div.is_halted());
    }

    /// A parent read that fails is a fault, never a guessed cursor.
    #[test]
    fn a_failed_seed_read_halts() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        let broken = |_: Address, _: B256| Err::<U256, _>("mdbx: read failed".to_string());
        let err = v.observe(&record(7, 0, 1), &broken).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)));
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("seed Inbox.nextSeq[7]"));
    }

    #[test]
    fn a_regressed_seq_halts() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 2), &fresh()).unwrap();
        let err = v.observe(&record(7, 1, 1), &fresh()).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)), "{err:?}");
        assert!(div.is_halted());
        assert!(div.reason().unwrap().contains("regressed"));
        // The latch holds: the NEXT record fails too, even a well-formed one.
        assert!(v.observe(&record(9, 0, 1), &fresh()).is_err());
    }

    #[test]
    fn a_skipped_seq_halts() {
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        v.observe(&record(7, 0, 2), &fresh()).unwrap();
        let err = v.observe(&record(7, 3, 1), &fresh()).unwrap_err();
        assert!(matches!(err, ExecutorError::State(_)));
        assert!(div.reason().unwrap().contains("skipped 1 message(s)"));
    }

    #[test]
    fn record_well_formedness_rules() {
        // Empty record.
        let mut r = record(7, 0, 1);
        r.messages.clear();
        assert_eq!(
            check_remote_epoch(0, &r),
            Err(RemoteEpochFault::Empty { origin: 7 })
        );
        // Non-dense internal seq.
        let mut r = record(7, 0, 3);
        r.messages[2].seq = 5;
        assert!(matches!(
            check_remote_epoch(0, &r),
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
            check_remote_epoch(0, &r),
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
            check_remote_epoch(u64::MAX, &overflowing),
            Err(RemoteEpochFault::SeqOverflow {
                origin: 7,
                first_seq: u64::MAX,
            })
        );
    }

    #[test]
    fn outbox_bounds_are_mirrored_on_the_validator() {
        // Value.
        let mut r = record(7, 0, 1);
        r.messages[0].value = 1;
        assert_eq!(
            check_remote_epoch(0, &r),
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
            check_remote_epoch(0, &r),
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
            check_remote_epoch(0, &r),
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
        assert_eq!(check_remote_epoch(0, &r), Ok(()));
    }

    #[test]
    fn seq_overflow_is_a_fault_not_a_panic() {
        // One message at u64::MAX, with the lane seeded there: the next
        // cursor would overflow.
        let mut r = record(7, 0, 1);
        r.first_seq = u64::MAX;
        r.messages[0].seq = u64::MAX;
        r.messages[0].source_hash = remote_source_hash(7, u64::MAX);
        assert_eq!(
            check_remote_epoch(u64::MAX, &r),
            Err(RemoteEpochFault::SeqOverflow {
                origin: 7,
                first_seq: u64::MAX
            })
        );
        // Through the verifier: a fault, no panic, and the latch holds.
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        let parent = parent_with(&[(7, u64::MAX)]);
        assert!(v.observe(&r, &parent).is_err());
        assert!(div.reason().unwrap().contains("overflows"));
    }

    #[test]
    fn cross_origin_ordering_is_not_constrained() {
        // §11: no cross-pair ordering guarantee — interleaved origins each
        // keep their own dense lane.
        let div = Divergence::new();
        let mut v = RemoteEpochVerifier::new(div.clone());
        let parent = parent_with(&[(9, 10)]);
        v.observe(&record(7, 0, 1), &parent).unwrap();
        v.observe(&record(9, 10, 1), &parent).unwrap();
        v.observe(&record(7, 1, 1), &parent).unwrap();
        v.observe(&record(9, 11, 1), &parent).unwrap();
        assert!(!div.is_halted());
    }
}
