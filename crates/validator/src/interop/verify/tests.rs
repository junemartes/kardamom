use alloy_primitives::{Address, B256, U256};
use kardamom_types::xchain::{MAX_DATA_BYTES, MAX_MESSAGE_GAS, NonEmptyVec, XChainMessage};

use super::*;

/// Build a record with `n` messages, dense from `first_seq`. `n` must be
/// at least 1: every call site in this module passes a literal `>= 1`.
fn record(origin: u64, first_seq: u64, n: u64) -> RemoteEpochRecord {
    record_with(origin, first_seq, n, |_| {})
}

/// Like [`record`], but runs `edit` on the built messages before they are
/// wrapped into the record's [`NonEmptyVec`]. `NonEmptyVec` exposes no
/// mutable slice, so a test that wants a one-field tweak on an otherwise
/// honest record edits the plain `Vec` here, before construction, instead.
fn record_with(
    origin: u64,
    first_seq: u64,
    n: u64,
    edit: impl FnOnce(&mut [XChainMessage]),
) -> RemoteEpochRecord {
    let build = |seq: u64| XChainMessage {
        source_hash: remote_source_hash(origin, seq),
        seq,
        origin_sender: Address::repeat_byte(0xA1),
        target: Address::repeat_byte(0xB2),
        value: 0,
        gas_limit: 100_000,
        input: bytes::Bytes::default(),
        callback: None,
    };
    let mut msgs: Vec<XChainMessage> = (first_seq..first_seq + n).map(build).collect();
    edit(&mut msgs);
    let mut msgs = msgs.into_iter();
    let first = msgs.next().expect("n >= 1 at every call site");
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 40,
        anchor_hash: B256::repeat_byte(0x0B),
        first_seq,
        messages: NonEmptyVec::new(first, msgs.collect()),
    }
}

/// A minimal [`StateDatabase`] for the parent-state reads `observe`
/// makes: storage is a plain map, everything else is empty, and a read
/// can be told to fail so the "no guessed cursor" path is testable.
#[derive(Debug, Default, Clone)]
struct TestDb {
    values: std::collections::BTreeMap<(Address, B256), U256>,
    fails: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("mock read failed")]
struct TestDbError;

impl kardamom_types::StateError for TestDbError {}

impl StateDatabase for TestDb {
    type Error = TestDbError;

    fn basic(&self, _address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Ok(None)
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        if self.fails {
            return Err(TestDbError);
        }
        Ok(self
            .values
            .get(&(address, key))
            .copied()
            .unwrap_or(U256::ZERO))
    }

    fn code_by_hash(&self, _code_hash: B256) -> Result<bytes::Bytes, Self::Error> {
        Ok(bytes::Bytes::default())
    }

    fn get_receipt(
        &self,
        _pos: kardamom_types::BPosition,
    ) -> Result<Option<kardamom_types::Receipt>, Self::Error> {
        Ok(None)
    }

    fn get_tx_position(
        &self,
        _tx_hash: B256,
    ) -> Result<Option<kardamom_types::BPosition>, Self::Error> {
        Ok(None)
    }
}

/// A parent state whose `Inbox.nextSeq` is `seeds[origin]` and zero
/// for every other origin.
fn parent_with(seeds: &[(u64, u64)]) -> TestDb {
    TestDb {
        values: seeds
            .iter()
            .map(|(origin, next)| ((INBOX, Inbox::next_seq_slot(*origin)), U256::from(*next)))
            .collect(),
        fails: false,
    }
}

fn fresh() -> TestDb {
    TestDb::default()
}

fn failing() -> TestDb {
    TestDb {
        fails: true,
        ..TestDb::default()
    }
}

impl RemoteEpochVerifier {
    /// Test-only convenience: build the empty-delta `ParentState` an
    /// `observe` call needs, from a bare [`TestDb`].
    fn observe_db(&mut self, rec: &RemoteEpochRecord, db: &TestDb) -> Result<(), ExecutorError> {
        let delta = kardamom_engine::delta::PendingDelta::default();
        RemoteEpochObserver::observe(self, rec, &ParentState::new(&delta, None, db))
    }
}

#[test]
fn dense_records_verify_and_advance_per_origin() {
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    let parent = parent_with(&[(7, 3)]);
    // The first record per origin must start at the seeded cursor: 3
    // for origin 7, 0 for origin 9.
    v.observe_db(&record(7, 3, 2), &parent).unwrap();
    // Next must continue at 5.
    v.observe_db(&record(7, 5, 1), &parent).unwrap();
    // A second origin has its own cursor.
    v.observe_db(&record(9, 0, 4), &parent).unwrap();
    v.observe_db(&record(9, 4, 1), &parent).unwrap();
    assert!(!div.is_halted());
}

/// The first record is never exempt. A validator restored from a
/// snapshot whose Inbox says 3 halts on a record that starts at 5.
#[test]
fn the_first_record_is_checked_against_the_seeded_cursor() {
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    let parent = parent_with(&[(7, 3)]);
    let err = v.observe_db(&record(7, 5, 1), &parent).unwrap_err();
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
    v.observe_db(&record(7, 0, 2), &parent_with(&[(7, 0)]))
        .unwrap();
    // A parent that now claims 9 must not move the cursor: 2 is next.
    v.observe_db(&record(7, 2, 1), &parent_with(&[(7, 9)]))
        .unwrap();
    assert!(!div.is_halted());
}

/// A parent read that fails is a fault, never a guessed cursor.
#[test]
fn a_failed_seed_read_halts() {
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    let err = v.observe_db(&record(7, 0, 1), &failing()).unwrap_err();
    assert!(matches!(err, ExecutorError::State(_)));
    assert!(div.is_halted());
    assert!(div.reason().unwrap().contains("seed Inbox.nextSeq[7]"));
}

#[test]
fn a_regressed_seq_halts() {
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    v.observe_db(&record(7, 0, 2), &fresh()).unwrap();
    let err = v.observe_db(&record(7, 1, 1), &fresh()).unwrap_err();
    assert!(matches!(err, ExecutorError::State(_)), "{err:?}");
    assert!(div.is_halted());
    assert!(div.reason().unwrap().contains("regressed"));
    // The latch holds: the NEXT record fails too, even a well-formed one.
    assert!(v.observe_db(&record(9, 0, 1), &fresh()).is_err());
}

#[test]
fn a_skipped_seq_halts() {
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    v.observe_db(&record(7, 0, 2), &fresh()).unwrap();
    let err = v.observe_db(&record(7, 3, 1), &fresh()).unwrap_err();
    assert!(matches!(err, ExecutorError::State(_)));
    assert!(div.reason().unwrap().contains("skipped 1 message(s)"));
}

#[test]
fn record_well_formedness_rules() {
    // Non-dense internal seq.
    let r = record_with(7, 0, 3, |msgs| msgs[2].seq = 5);
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
    let r = record_with(7, 0, 1, |msgs| {
        msgs[0].source_hash = B256::repeat_byte(0xEE);
    });
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
        messages: NonEmptyVec::new(
            XChainMessage {
                source_hash: remote_source_hash(7, u64::MAX),
                seq: u64::MAX,
                origin_sender: Address::repeat_byte(0xA1),
                target: Address::repeat_byte(0xB2),
                value: 0,
                gas_limit: 100_000,
                input: bytes::Bytes::default(),
                callback: None,
            },
            vec![],
        ),
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
    let r = record_with(7, 0, 1, |msgs| msgs[0].value = 1);
    assert_eq!(
        check_remote_epoch(0, &r),
        Err(RemoteEpochFault::Bounds {
            origin: 7,
            fault: BoundsFault::ValueNotAllowed { seq: 0, value: 1 }
        })
    );
    // Gas limit.
    let r = record_with(7, 0, 1, |msgs| msgs[0].gas_limit = MAX_MESSAGE_GAS + 1);
    assert_eq!(
        check_remote_epoch(0, &r),
        Err(RemoteEpochFault::Bounds {
            origin: 7,
            fault: BoundsFault::GasLimitAboveCap {
                seq: 0,
                gas_limit: MAX_MESSAGE_GAS + 1,
                cap: MAX_MESSAGE_GAS,
            }
        })
    );
    // Data length.
    let r = record_with(7, 0, 1, |msgs| {
        msgs[0].input = bytes::Bytes::from(vec![0xFFu8; MAX_DATA_BYTES + 1]);
    });
    assert_eq!(
        check_remote_epoch(0, &r),
        Err(RemoteEpochFault::Bounds {
            origin: 7,
            fault: BoundsFault::DataAboveCap {
                seq: 0,
                len: MAX_DATA_BYTES + 1,
                cap: MAX_DATA_BYTES,
            }
        })
    );
    // The honest maxima pass.
    let r = record_with(7, 0, 1, |msgs| {
        msgs[0].gas_limit = MAX_MESSAGE_GAS;
        msgs[0].input = bytes::Bytes::from(vec![0xFFu8; MAX_DATA_BYTES]);
    });
    assert!(check_remote_epoch(0, &r).is_ok());
}

#[test]
fn seq_overflow_is_a_fault_not_a_panic() {
    // One message at u64::MAX, with the lane seeded there: the next
    // cursor would overflow.
    let mut r = record_with(7, 0, 1, |msgs| {
        msgs[0].seq = u64::MAX;
        msgs[0].source_hash = remote_source_hash(7, u64::MAX);
    });
    r.first_seq = u64::MAX;
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
    assert!(v.observe_db(&r, &parent).is_err());
    assert!(div.reason().unwrap().contains("overflows"));
}

#[test]
fn cross_origin_ordering_is_not_constrained() {
    // No cross-pair ordering guarantee — interleaved origins each keep
    // their own dense lane.
    let div = Divergence::new();
    let mut v = RemoteEpochVerifier::new(div.clone());
    let parent = parent_with(&[(9, 10)]);
    v.observe_db(&record(7, 0, 1), &parent).unwrap();
    v.observe_db(&record(9, 10, 1), &parent).unwrap();
    v.observe_db(&record(7, 1, 1), &parent).unwrap();
    v.observe_db(&record(9, 11, 1), &parent).unwrap();
    assert!(!div.is_halted());
}
