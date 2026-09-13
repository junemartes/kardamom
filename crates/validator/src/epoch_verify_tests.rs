use alloy_primitives::{B256, U256, address};
use kardamom_types::DepositLog;
use kardamom_types::epoch::{LockboxLog, UpgradeLog};

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
    assert!(is_missing_block(&anyhow::anyhow!(
        "L1 provider error: finalized L1 block 52 not found"
    )));
    assert!(!is_missing_block(&anyhow::anyhow!(
        "L1 provider error: connection refused"
    )));
    assert!(!is_missing_block(&anyhow::anyhow!("timed out")));
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
struct FakeL1 {
    blocks: std::collections::BTreeMap<u64, (B256, B256)>,
}

#[async_trait::async_trait]
impl L1EpochSource for FakeL1 {
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        self.blocks
            .get(&number)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("finalized L1 block {number} not found"))
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

#[tokio::test]
async fn a_properly_chained_pair_of_epochs_verifies() {
    let (h7, h8) = (B256::repeat_byte(0x77), B256::repeat_byte(0x88));
    let l1 = FakeL1 {
        blocks: [(7, (h7, B256::repeat_byte(0x66))), (8, (h8, h7))]
            .into_iter()
            .collect(),
    };
    let e7 = derive_epoch(7, h7, &[]).unwrap();
    let e8 = derive_epoch(8, h8, &[]).unwrap();

    assert!(verify_one(&l1, lockbox(), &e7, None).await.is_ok());
    assert!(
        verify_one(
            &l1,
            lockbox(),
            &e8,
            Some(Anchor {
                number: 7,
                hash: h7
            })
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn an_epoch_that_does_not_descend_from_its_predecessor_is_caught() {
    // This is the lie the per-block check cannot see: block 8 exists,
    // its hash matches what the epoch claims, and its deposits match,
    // but it is not built on block 7. The numbers are consecutive, but
    // it is not one chain.
    let (h7, h8) = (B256::repeat_byte(0x77), B256::repeat_byte(0x88));
    let orphan_parent = B256::repeat_byte(0xEE);
    let l1 = FakeL1 {
        blocks: [(8, (h8, orphan_parent))].into_iter().collect(),
    };
    let e8 = derive_epoch(8, h8, &[]).unwrap();

    // Without an anchor, the epoch passes: there is nothing to chain against.
    assert!(verify_one(&l1, lockbox(), &e8, None).await.is_ok());

    // With an anchor, the break is caught.
    let err = verify_one(
        &l1,
        lockbox(),
        &e8,
        Some(Anchor {
            number: 7,
            hash: h7,
        }),
    )
    .await
    .unwrap_err();
    match err {
        VerifyOutcome::Fault(EpochFault::ParentMismatch {
            l1_number,
            expected_parent,
            got_parent,
        }) => {
            assert_eq!(l1_number, 8);
            assert_eq!(expected_parent, h7);
            assert_eq!(got_parent, orphan_parent);
        }
        VerifyOutcome::Fault(other) => panic!("wrong fault: {other}"),
        VerifyOutcome::Unavailable(e) => panic!("expected a fault, got {e}"),
    }
}

#[tokio::test]
async fn chaining_is_skipped_across_a_gap_in_the_anchor() {
    // After a deferred, unverified epoch, the anchor goes stale, so the
    // next epoch is not the anchor's successor. Chaining must skip
    // rather than report a false parent mismatch. The sequence rules
    // already reject real gaps, and inventing a divergence here would
    // stop a healthy validator over an L1 blip.
    let h9 = B256::repeat_byte(0x99);
    let l1 = FakeL1 {
        blocks: [(9, (h9, B256::repeat_byte(0xAB)))].into_iter().collect(),
    };
    let e9 = derive_epoch(9, h9, &[]).unwrap();

    // The anchor is block 7, this is block 9: not adjacent, so no chain check.
    assert!(
        verify_one(
            &l1,
            lockbox(),
            &e9,
            Some(Anchor {
                number: 7,
                hash: B256::repeat_byte(0x77),
            }),
        )
        .await
        .is_ok()
    );
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
