use alloy_primitives::{B256, U256, address};
use kardamom_types::DepositLog;
use kardamom_types::epoch::{LockboxLog, UpgradeLog};

use super::step::{VerifyOutcome, VerifyVerdict};
use super::*;

fn log(number: u64, hash: B256, index: u64, mint: u128) -> LockboxLog {
    LockboxLog::Deposit(DepositLog {
        block_number: number,
        block_hash: hash,
        log_index: index,
        from: address!("00000000000000000000000000000000000000A1"),
        to: address!("00000000000000000000000000000000000000B2"),
        mint,
        gas_limit: 200_000,
        data: alloy_primitives::Bytes::new(),
    })
}

fn upgrade(number: u64, hash: B256, index: u64, feature: u64) -> LockboxLog {
    LockboxLog::Upgrade(UpgradeLog {
        block_number: number,
        block_hash: hash,
        log_index: index,
        feature_id: U256::from(feature),
        activation_timestamp: 0,
    })
}

/// The failure this guards against is subtle and total. If the
/// verifier read only `DepositInitiated` while the watcher wrote both
/// kinds, every upgrade would look like a deposit the producer
/// invented, and every validator would stop the moment the chain was
/// first upgraded.
#[test]
fn an_epoch_carrying_an_upgrade_verifies() {
    let hash = B256::repeat_byte(0x12);
    let logs = vec![log(7, hash, 0, 100), upgrade(7, hash, 1, 1)];
    let epoch = derive_epoch(7, hash, &logs).unwrap();
    assert_eq!(epoch.deposits.len(), 2);
    assert!(epoch.deposits[1].is_system_transaction);
    assert_eq!(compare_against_l1(&epoch, hash, &logs), Ok(()));
}

#[test]
fn a_forged_upgrade_in_the_stream_is_caught() {
    // This closes the attack of a sequencer inserting an upgrade L1
    // never authorized. L1 has only the deposit, so the derived truth
    // differs.
    let hash = B256::repeat_byte(0x13);
    let truth_logs = vec![log(7, hash, 0, 100)];
    let mut epoch = derive_epoch(7, hash, &truth_logs).unwrap();
    let forged = derive_epoch(7, hash, &[upgrade(7, hash, 1, 1)]).unwrap();
    epoch.deposits.push(forged.deposits[0].clone());

    let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
    assert!(
        matches!(err, EpochFault::DepositsMismatch { .. }),
        "got {err:?}"
    );
}

#[test]
fn a_dropped_upgrade_is_caught() {
    // This is the mirror attack: L1 authorized an upgrade, the stream omits it.
    let hash = B256::repeat_byte(0x14);
    let truth_logs = vec![upgrade(7, hash, 0, 1)];
    let mut epoch = derive_epoch(7, hash, &truth_logs).unwrap();
    epoch.deposits.clear();

    let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
    assert!(
        matches!(err, EpochFault::DepositsMismatch { .. }),
        "got {err:?}"
    );
}

#[test]
fn an_upgrade_with_tampered_payload_is_caught() {
    // Same position and count, different feature id: a count
    // comparison alone would let this through.
    let hash = B256::repeat_byte(0x15);
    let truth_logs = vec![upgrade(7, hash, 0, 1)];
    let epoch = derive_epoch(7, hash, &[upgrade(7, hash, 0, 999)]).unwrap();

    let err = compare_against_l1(&epoch, hash, &truth_logs).unwrap_err();
    assert!(
        matches!(err, EpochFault::DepositsMismatch { ref detail, .. }
                 if detail.contains("index 0")),
        "got {err:?}"
    );
}

#[test]
fn an_honest_epoch_verifies() {
    let hash = B256::repeat_byte(0x11);
    let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
    let epoch = derive_epoch(7, hash, &logs).unwrap();
    assert_eq!(compare_against_l1(&epoch, hash, &logs), Ok(()));
}

#[test]
fn a_dropped_deposit_is_caught() {
    // This is the censorship case: L1 recorded two, the chain carries one.
    let hash = B256::repeat_byte(0x22);
    let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
    let mut epoch = derive_epoch(7, hash, &logs).unwrap();
    epoch.deposits.pop();

    let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
    assert!(
        matches!(
            err,
            EpochFault::DepositsMismatch {
                expected: 2,
                got: 1,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn an_injected_deposit_is_caught() {
    let hash = B256::repeat_byte(0x33);
    let logs = vec![log(7, hash, 0, 100)];
    let mut epoch = derive_epoch(7, hash, &logs).unwrap();
    epoch.deposits.push(epoch.deposits[0].clone());

    let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
    assert!(
        matches!(
            err,
            EpochFault::DepositsMismatch {
                expected: 1,
                got: 2,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn a_reordered_epoch_is_caught_despite_matching_counts() {
    // A count comparison would miss this: same deposits, wrong order.
    // Order is part of consensus; it decides the execution results.
    let hash = B256::repeat_byte(0x44);
    let logs = vec![log(7, hash, 0, 100), log(7, hash, 1, 200)];
    let mut epoch = derive_epoch(7, hash, &logs).unwrap();
    epoch.deposits.swap(0, 1);

    let err = compare_against_l1(&epoch, hash, &logs).unwrap_err();
    match err {
        EpochFault::DepositsMismatch {
            expected,
            got,
            detail,
            ..
        } => {
            assert_eq!((expected, got), (2, 2), "counts match; order does not");
            assert!(detail.contains("index 0"), "{detail}");
        }
        other => panic!("expected a deposits mismatch, got {other}"),
    }
}

#[test]
fn a_tampered_amount_is_caught() {
    let hash = B256::repeat_byte(0x55);
    let logs = vec![log(7, hash, 0, 100)];
    let mut epoch = derive_epoch(7, hash, &logs).unwrap();
    epoch.deposits[0].mint += 1_000_000;

    assert!(compare_against_l1(&epoch, hash, &logs).is_err());
}

#[test]
fn an_epoch_naming_the_wrong_l1_block_is_caught() {
    let hash = B256::repeat_byte(0x66);
    let logs = vec![log(7, hash, 0, 100)];
    let epoch = derive_epoch(7, hash, &logs).unwrap();

    let err = compare_against_l1(&epoch, B256::repeat_byte(0x99), &logs).unwrap_err();
    assert_eq!(err, EpochFault::HashMismatch { l1_number: 7 });
}

#[test]
fn an_empty_epoch_verifies_and_an_invented_deposit_in_one_does_not() {
    // Empty epochs are normal and must pass. They are also where a
    // fabricated deposit is easiest to hide.
    let hash = B256::repeat_byte(0x77);
    let epoch = derive_epoch(9, hash, &[]).unwrap();
    assert_eq!(compare_against_l1(&epoch, hash, &[]), Ok(()));

    let mut forged = epoch.clone();
    forged.deposits.push(kardamom_types::Deposit {
        source_hash: B256::repeat_byte(0xEE),
        mint: 1,
        ..Default::default()
    });
    assert!(compare_against_l1(&forged, hash, &[]).is_err());
}

#[test]
fn sequence_rules() {
    // First epoch: anything goes; the producer seeds at the finalized tip.
    assert_eq!(check_sequence(None, 500), Ok(()));
    // The only accepted step is a consecutive one.
    assert_eq!(check_sequence(Some(500), 501), Ok(()));
    assert_eq!(
        check_sequence(Some(500), 500),
        Err(EpochFault::OriginRegressed {
            previous: 500,
            got: 500
        })
    );
    assert_eq!(
        check_sequence(Some(500), 499),
        Err(EpochFault::OriginRegressed {
            previous: 500,
            got: 499
        })
    );
    // A skip means the deposits in between are unaccounted for.
    assert_eq!(
        check_sequence(Some(500), 502),
        Err(EpochFault::OriginSkipped {
            previous: 500,
            got: 502
        })
    );
}

#[test]
fn skipped_origin_message_counts_the_missing_blocks() {
    let f = EpochFault::OriginSkipped {
        previous: 500,
        got: 505,
    };
    assert!(f.to_string().contains("skipped 4 block(s)"), "{f}");
}

#[test]
fn a_missing_block_is_told_apart_from_an_unreachable_l1() {
    // The retry loop's verdict depends on this distinction: "L1 does
    // not have this block" is a statement about the chain (rule 4, a
    // fault). "L1 did not answer" is about the network, a coverage gap.
    let e52 = derive_epoch(52, B256::repeat_byte(0x52), &[]).unwrap();
    let range = [e52];
    let verdict = |msg: &str| Verifier::<FakeL1>::give_up(&range, 8, &anyhow::anyhow!("{msg}"));
    assert!(matches!(
        verdict("L1 provider error: finalized L1 block 52 not found"),
        VerifyVerdict::Fault(EpochFault::BlockBeyondFinality {
            l1_number: 52,
            attempts: 8
        })
    ));
    assert!(matches!(
        verdict("L1 provider error: connection refused"),
        VerifyVerdict::Unverified
    ));
    assert!(matches!(verdict("timed out"), VerifyVerdict::Unverified));
}

#[test]
fn beyond_finality_message_names_the_block_and_the_effort() {
    let f = EpochFault::BlockBeyondFinality {
        l1_number: 52,
        attempts: 8,
    };
    let m = f.to_string();
    assert!(m.contains("52") && m.contains("8 attempts"), "{m}");
}

/// A fake L1 whose blocks are whatever the test says they are. This is
/// the shape a lying or buggy endpoint takes.
#[derive(Clone, Default)]
struct FakeL1 {
    blocks: std::collections::BTreeMap<u64, (B256, B256)>,
}

impl FakeL1 {
    fn ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        self.blocks
            .get(&number)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("finalized L1 block {number} not found"))
    }

    fn of(blocks: &[(u64, B256, B256)]) -> Self {
        Self {
            blocks: blocks.iter().map(|&(n, h, p)| (n, (h, p))).collect(),
        }
    }
}

#[async_trait::async_trait]
impl L1EpochSource for FakeL1 {
    async fn finalized_block_number(&self) -> anyhow::Result<u64> {
        self.blocks
            .keys()
            .next_back()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("no block"))
    }
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        self.ids(number)
    }
    async fn headers(
        &self,
        from: u64,
        to: u64,
    ) -> anyhow::Result<Vec<kardamom_da_watcher::L1Header>> {
        (from..=to)
            .map(|number| {
                let (hash, parent_hash) = self.ids(number)?;
                Ok(kardamom_da_watcher::L1Header {
                    number,
                    hash,
                    parent_hash,
                    timestamp: 0,
                })
            })
            .collect()
    }
    async fn lockbox_logs(
        &self,
        _lockbox: Address,
        _from: u64,
        _to: u64,
    ) -> anyhow::Result<Vec<LockboxLog>> {
        Ok(Vec::new())
    }
}

fn lockbox() -> Address {
    address!("0000000000000000000000000000000000C0DE01")
}

/// A verifier whose anchor is `anchor_l1` and whose logs source is
/// `logs_l1`, with `last` as its last verified epoch.
fn verifier(anchor_l1: FakeL1, logs_l1: FakeL1, last: Option<Anchor>) -> Verifier<FakeL1> {
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let mut v = Verifier::new(
        ContentSources {
            anchor: std::sync::Arc::new(anchor_l1),
            logs: std::sync::Arc::new(logs_l1),
            lockbox: lockbox(),
            max_log_range: std::num::NonZeroU64::new(10).unwrap(),
        },
        std::sync::Arc::new(Divergence::default()),
        rx,
    );
    v.anchor = last;
    v
}

fn h(n: u8) -> B256 {
    B256::repeat_byte(n)
}

/// One chain of blocks 7..=9 with hashes 0x77, 0x88, 0x99.
fn chain() -> FakeL1 {
    FakeL1::of(&[
        (7, h(0x77), h(0x66)),
        (8, h(0x88), h(0x77)),
        (9, h(0x99), h(0x88)),
    ])
}

fn epochs(blocks: &[(u64, B256)]) -> Vec<EpochRecord> {
    blocks
        .iter()
        .map(|&(n, hash)| derive_epoch(n, hash, &[]).unwrap())
        .collect()
}

fn fault(outcome: Result<Anchor, VerifyOutcome>) -> EpochFault {
    match outcome {
        Err(VerifyOutcome::Fault(fault)) => fault,
        Err(VerifyOutcome::Unavailable(e)) => panic!("expected a fault, got {e}"),
        Ok(anchor) => panic!("expected a fault, got {anchor:?}"),
    }
}

/// A range of one chain that ends at the anchor's header verifies in one
/// check, and its last epoch is the new anchor.
#[tokio::test]
async fn a_chained_range_ending_at_the_anchor_verifies() {
    let range = epochs(&[(8, h(0x88)), (9, h(0x99))]);
    let last = Some(Anchor {
        number: 7,
        hash: h(0x77),
    });
    let anchor = verifier(chain(), chain(), last)
        .verify_range(&range)
        .await
        .ok()
        .unwrap();
    assert_eq!(
        anchor,
        Anchor {
            number: 9,
            hash: h(0x99)
        }
    );
}

/// The range's last header is not the light client's: the logs source
/// serves another chain, consistent in itself.
#[tokio::test]
async fn a_range_that_does_not_end_at_the_anchor_is_caught() {
    let forked = FakeL1::of(&[(8, h(0xA8), h(0x77)), (9, h(0xA9), h(0xA8))]);
    let range = epochs(&[(8, h(0xA8)), (9, h(0xA9))]);
    let f = fault(verifier(chain(), forked, None).verify_range(&range).await);
    assert_eq!(f, EpochFault::HashMismatch { l1_number: 9 });
}

/// An epoch that names another hash than its block's is caught, also in
/// the middle of a range.
#[tokio::test]
async fn an_epoch_naming_another_hash_in_the_range_is_caught() {
    let range = epochs(&[(7, h(0x77)), (8, h(0xEE)), (9, h(0x99))]);
    let f = fault(verifier(chain(), chain(), None).verify_range(&range).await);
    assert_eq!(f, EpochFault::HashMismatch { l1_number: 8 });
}

/// The range must descend from the last verified epoch. This is the lie
/// the per-block check cannot see: the numbers are consecutive, but it
/// is not one chain.
#[tokio::test]
async fn a_range_that_does_not_descend_from_the_last_epoch_is_caught() {
    let range = epochs(&[(8, h(0x88)), (9, h(0x99))]);
    let last = Some(Anchor {
        number: 7,
        hash: h(0x70),
    });
    let f = fault(verifier(chain(), chain(), last).verify_range(&range).await);
    assert_eq!(
        f,
        EpochFault::ParentMismatch {
            l1_number: 8,
            expected_parent: h(0x70),
            got_parent: h(0x77),
        }
    );
}

/// After an unverified range the anchor is not the predecessor; the chain
/// check starts at the range, with no false parent mismatch.
#[tokio::test]
async fn chaining_is_skipped_across_a_gap_in_the_anchor() {
    let range = epochs(&[(9, h(0x99))]);
    let last = Some(Anchor {
        number: 7,
        hash: h(0x70),
    });
    assert!(
        verifier(chain(), chain(), last)
            .verify_range(&range)
            .await
            .is_ok()
    );
}

/// A range splits at a gap that a dropped epoch leaves.
#[test]
fn a_gap_splits_the_range() {
    let gathered = epochs(&[(7, h(1)), (8, h(2)), (10, h(3)), (11, h(4))]);
    let runs: Vec<Vec<u64>> = Verifier::<FakeL1>::runs(gathered)
        .iter()
        .map(|run| run.iter().map(|e| e.l1_number).collect())
        .collect();
    assert_eq!(runs, [vec![7, 8], vec![10, 11]]);
}

#[test]
fn parent_mismatch_message_names_both_hashes() {
    let f = EpochFault::ParentMismatch {
        l1_number: 8,
        expected_parent: B256::repeat_byte(0x77),
        got_parent: B256::repeat_byte(0xEE),
    };
    let m = f.to_string();
    assert!(m.contains('8') && m.contains("not one chain"), "{m}");
}

fn epoch(l1_number: u64) -> EpochRecord {
    derive_epoch(l1_number, B256::repeat_byte(0x5A), &[]).unwrap()
}

/// A plain test with no runtime: a verifier that spawned the content task
/// or touched L1 would panic here.
#[test]
fn the_sequence_rules_run_without_an_l1_source() {
    let divergence = Divergence::new();
    let mut verifier = EpochVerifier::new(divergence.clone());
    assert!(verifier.content.is_none());
    assert!(verifier.observe(&epoch(100)).is_ok());
    assert!(verifier.observe(&epoch(101)).is_ok());
    assert!(!divergence.is_halted());
}

#[test]
fn a_skipped_origin_without_an_l1_source_is_a_divergence() {
    let divergence = Divergence::new();
    let mut verifier = EpochVerifier::new(divergence.clone());
    verifier.observe(&epoch(100)).unwrap();

    let skipped = EpochFault::OriginSkipped {
        previous: 100,
        got: 102,
    };
    assert!(matches!(
        verifier.observe(&epoch(102)),
        Err(ExecutorError::State(reason)) if reason == skipped.to_string()
    ));
    assert_eq!(
        divergence.reason(),
        Some(format!("epoch verification failed: {skipped}"))
    );
    // The divergence latches: the missing epoch does not repair it.
    assert!(verifier.observe(&epoch(101)).is_err());
}

#[test]
fn a_regressed_origin_without_an_l1_source_is_a_divergence() {
    let divergence = Divergence::new();
    let mut verifier = EpochVerifier::new(divergence.clone());
    verifier.observe(&epoch(100)).unwrap();

    assert!(verifier.observe(&epoch(100)).is_err());
    assert!(
        divergence
            .reason()
            .is_some_and(|r| r.contains("l1_origin regressed: 100 -> 100")),
        "{:?}",
        divergence.reason()
    );
}

#[tokio::test]
async fn an_l1_source_turns_the_content_check_on() {
    let verifier = EpochVerifier::new(Divergence::new()).with_content_check(
        ContentSources {
            anchor: std::sync::Arc::new(FakeL1::default()),
            logs: std::sync::Arc::new(FakeL1::default()),
            lockbox: lockbox(),
            max_log_range: std::num::NonZeroU64::new(10).unwrap(),
        },
        &tokio::runtime::Handle::current(),
    );
    assert!(verifier.content.is_some());
}
