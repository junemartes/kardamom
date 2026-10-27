use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::RefRefused;
use kardamom_types::{TxErrorReason, TxStageKind};

use super::*;
use crate::resync::SealerRefusal;
use crate::testkit::{EnvelopeSpec, envelope_with};

impl Bench {
    fn assert_guard_retry(reason: &TxErrorReason) {
        let mut b = Self::new(SealerVersion::starting_at(5));
        (0..3).for_each(|nonce| b.send(nonce));
        (0..3).for_each(|nonce| b.refusal(nonce, reason.clone()));
        b.run();
        b.run();
        assert_eq!(b.nonces(), [0, 1, 2, 0, 1, 2]);
        assert!(b.rig.errors().is_empty());
        assert!(b.rig.drops().is_empty());
    }

    fn advance(&mut self, block: u64) {
        self.accounts
            .send(AccountEvent::Rows {
                block,
                rows: vec![],
            })
            .unwrap();
        self.sweep();
    }
    fn refusal(&self, nonce: u64, reason: TxErrorReason) {
        self.accounts
            .send(AccountEvent::Refusal(Box::new(SealerRefusal {
                sender: self.sender(),
                nonce,
                reason,
            })))
            .unwrap();
    }

    fn copy(&mut self, nonce: u64, deadline: u64, offset: i32) {
        let mut envelope = envelope_with(
            &self.signer,
            nonce,
            1,
            EnvelopeSpec {
                real_hash: true,
                ..EnvelopeSpec::default()
            },
        );
        envelope.max_inclusion_block = deadline;
        self.rig.push(TxDataLoc::new(7, pos(offset)), envelope);
    }

    fn past(&self, nonce: u64, deadline: u64) {
        self.refusal(
            nonce,
            TxErrorReason::PastDeadline {
                max_inclusion_block: deadline,
                at_block: deadline + 1,
            },
        );
    }

    fn nonces(&self) -> Vec<u64> {
        self.offers().iter().map(|(nonce, _)| *nonce).collect()
    }
}

#[test]
fn a_refused_copy_cannot_expire_the_loss_window_of_its_replacement() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.copy(0, 100, 64);
    b.run();
    b.send(1);
    b.advance(10);
    b.past(0, 100);
    b.run();
    b.copy(0, 200, 192);
    b.run();
    b.advance(11);
    b.advance(26);
    assert!(
        b.l.admission.unconfirmed_at(b.sender(), 0).is_some(),
        "only the replacement's own loss mark can expire it"
    );
    b.advance(27);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_none());
}

#[test]
fn an_old_lost_suffix_cannot_expire_a_new_sender_run() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.send(0);
    b.advance(2);
    b.send(1);
    b.advance(3);
    b.advance(18);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_none());
    b.copy(0, 200, 256);
    b.run();
    b.copy(1, 200, 320);
    b.run();
    b.advance(19);
    assert!(
        b.l.admission.unconfirmed_at(b.sender(), 0).is_some(),
        "the next old timeout cannot clear a newly accepted run"
    );
    assert!(b.l.admission.unconfirmed_at(b.sender(), 1).is_some());
    b.advance(35);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_none());
}

#[test]
fn a_confirmed_run_cannot_expire_its_replacement_after_rollback() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.send(0);
    b.advance(2);
    b.confirm_first();
    b.accounts.send(AccountEvent::Rollback).unwrap();
    b.accounts
        .send(AccountEvent::Answer {
            address: b.sender(),
            state: AccountState {
                nonce: 0,
                balance: U256::from(10u64).pow(U256::from(24u64)),
                delegated: false,
                tx_idx: 1,
            },
        })
        .unwrap();
    b.copy(0, 200, 128);
    b.run();
    b.advance(3);
    b.advance(18);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_some());
    b.advance(19);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_none());
}

#[test]
fn a_batch_of_da_or_record_refusals_retries_each_run_once_without_new_input() {
    [
        TxErrorReason::DaLag {
            sealed_head: 30,
            posted_head: 10,
            budget_blocks: 8,
        },
        TxErrorReason::RecordLag {
            sealed_index: 50,
            recorded_index: 10,
            budget: 16,
        },
    ]
    .iter()
    .for_each(Bench::assert_guard_retry);
}

#[test]
fn a_delayed_guard_refusal_retries_only_the_still_unconfirmed_suffix() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    b.confirm_first();
    b.refusal(
        1,
        TxErrorReason::DaLag {
            sealed_head: 30,
            posted_head: 10,
            budget_blocks: 8,
        },
    );
    b.run();
    assert_eq!(b.nonces(), [0, 1, 1]);
    b.refusal(
        0,
        TxErrorReason::DaLag {
            sealed_head: 30,
            posted_head: 10,
            budget_blocks: 8,
        },
    );
    b.run();
    assert_eq!(
        b.nonces(),
        [0, 1, 1],
        "a confirmed notice cannot disturb the remaining run"
    );
}

#[test]
fn past_deadline_keeps_the_suffix_until_a_new_copy_fills_the_gap() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    let (verdict_tx, mut verdict_rx) = tokio::sync::mpsc::channel(64);
    b.l.enable_verdicts(verdict_tx);
    (0..3).for_each(|nonce| {
        b.copy(nonce, 100, i32::try_from((nonce + 1) * 64).unwrap());
        b.run();
    });
    assert_eq!(std::iter::from_fn(|| verdict_rx.try_recv().ok()).count(), 3);
    b.past(0, 100);
    b.run();
    assert_eq!(b.nonces(), [0, 1, 2]);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 1).is_none());
    assert_eq!(b.rig.errors().len(), 1);
    assert_eq!(b.rig.errors()[0].nonce, 0);
    assert_eq!(b.rig.drops().len(), 1);
    assert_eq!(
        b.l.depth
            .of(kardamom_types::shard_map::vslot_for(b.sender())),
        2
    );
    assert!(
        verdict_rx.try_recv().is_err(),
        "a later refusal does not answer an acknowledged submit again"
    );
    b.copy(0, 200, 256);
    b.run();
    assert_eq!(b.nonces(), [0, 1, 2, 0, 1, 2]);
    let verdicts: Vec<_> = std::iter::from_fn(|| verdict_rx.try_recv().ok()).collect();
    assert_eq!(verdicts.len(), 1, "only the new copy needs a submit answer");
    assert_eq!(verdicts[0].status.kind(), TxStageKind::Accepted);
}

#[test]
fn a_late_deadline_of_the_old_copy_does_not_remove_its_replacement() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.copy(0, 100, 64);
    b.run();
    b.past(0, 100);
    b.run();
    b.copy(0, 200, 128);
    b.run();
    b.send(1);
    b.past(0, 100);
    b.run();
    assert_eq!(b.nonces(), [0, 0, 1]);
    assert_eq!(
        b.l.admission
            .unconfirmed_at(b.sender(), 0)
            .unwrap()
            .payload
            .max_inclusion_block,
        200
    );
    assert_eq!(b.rig.errors().len(), 1);
    assert_eq!(b.rig.drops().len(), 1);
}

#[test]
fn a_hash_specific_refusal_does_not_remove_another_transaction_at_the_nonce() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.copy(0, 100, 64);
    b.run();
    b.accounts
        .send(AccountEvent::Refused(Box::new(RefRefused {
            canonical_id: B256::repeat_byte(17),
            sender: b.sender(),
            nonce: 0,
            decision_version: 5,
        })))
        .unwrap();
    b.run();
    b.send(1);
    assert_eq!(b.nonces(), [0, 1]);
    assert!(b.l.admission.unconfirmed_at(b.sender(), 0).is_some());
}

#[test]
fn a_late_reference_refusal_keeps_a_copy_that_already_carries_bytes() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.copy(0, 100, 64);
    b.run();
    let hash = b.rig.offers()[0].tx_ref.tx_hash;
    b.accounts
        .send(AccountEvent::Refused(Box::new(RefRefused {
            canonical_id: hash,
            sender: b.sender(),
            nonce: 0,
            decision_version: 5,
        })))
        .unwrap();
    b.run();
    assert!(
        b.l.admission.unconfirmed_at(b.sender(), 0).is_some(),
        "an old missing-bytes notice cannot remove an eligible copy"
    );
    assert!(b.rig.drops().is_empty());
}

#[test]
fn a_terminal_refusal_removes_queued_suffix_offers_before_backpressure_clears() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.copy(1, 100, 64);
    b.run();
    b.copy(2, 100, 128);
    b.run();
    *b.rig.refs.fail_with_backpressure.lock().unwrap() = true;
    b.copy(0, 100, 192);
    assert!(matches!(
        b.l.run_once(&mut b.rig.ports()),
        Err(crate::error::SequencerError::Backpressure)
    ));
    b.past(0, 100);
    *b.rig.refs.fail_with_backpressure.lock().unwrap() = false;
    b.run();
    assert_eq!(b.nonces(), [] as [u64; 0]);
    b.copy(0, 200, 256);
    b.run();
    assert_eq!(b.nonces(), [0, 1, 2]);
}

#[test]
fn a_terminal_notice_cancels_forward_verdicts_from_earlier_account_events() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.copy(1, 100, 64);
    b.run();
    b.copy(2, 100, 128);
    b.run();
    b.confirm_first();
    b.past(1, 100);
    b.run();
    assert!(b.nonces().is_empty());
    assert_eq!(b.rig.errors().len(), 1);
    b.copy(1, 200, 192);
    b.run();
    assert_eq!(b.nonces(), [1, 2]);
}

#[test]
fn a_refused_suffix_stays_bounded_and_every_excess_transaction_gets_a_retry_reason() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    (0..32).for_each(|nonce| {
        b.copy(nonce, 100, i32::try_from((nonce + 1) * 64).unwrap());
        b.run();
    });
    b.past(0, 100);
    b.run();
    let errors = b.rig.errors();
    assert_eq!(
        errors.len(),
        16,
        "one terminal prefix and fifteen excess suffix transactions"
    );
    assert_eq!(b.rig.drops().len(), 16);
    assert_eq!(
        errors
            .iter()
            .filter(|error| matches!(error.reason, TxErrorReason::Evicted { expected_nonce: 0 }))
            .count(),
        15
    );
    b.copy(0, 200, 4096);
    b.run();
    assert_eq!(&b.nonces()[32..], &(0..17).collect::<Vec<_>>());
}

#[test]
fn a_window_full_answer_retains_and_retries_the_whole_sender_run() {
    let mut b = Bench::new(SealerVersion::starting_at(3));
    b.send(0);
    b.send(1);
    b.accounts
        .send(AccountEvent::Retry {
            sender: b.sender(),
            nonce: 1,
        })
        .unwrap();
    b.run();
    assert_eq!(b.nonces(), [0, 1, 0, 1]);
    assert!(b.rig.errors().is_empty());
    assert!(b.rig.drops().is_empty());
}
