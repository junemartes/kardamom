use alloy_primitives::B256;
use kardamom_types::TxErrorReason;

use super::*;

impl Bench {
    fn queue_three(&mut self) {
        (0..3).for_each(|nonce| self.send(nonce));
        assert_eq!(self.offers(), [(0, true)]);
    }

    fn assert_all_ordered(&self) {
        assert!(self.l.outbox.is_empty());
        assert_eq!(self.rig.offers().last().unwrap().guard.nonce, 2);
        assert!(self.rig.errors().is_empty());
        assert!(self.rig.drops().is_empty());
        assert!(
            self.l.admission.unconfirmed_at(self.sender(), 2).is_some(),
            "canonical confirmation is not executed state"
        );
    }

    fn other_sender(&mut self) -> Address {
        let other = signer(22);
        let address = other.address();
        self.accounts
            .send(AccountEvent::Rows {
                block: 2,
                rows: vec![(
                    address,
                    AccountState {
                        nonce: 0,
                        balance: U256::from(10u64).pow(U256::from(24u64)),
                        delegated: false,
                        tx_idx: 2,
                    },
                )],
            })
            .unwrap();
        self.rig.push(
            TxDataLoc::new(8, pos(64)),
            envelope_with(
                &other,
                0,
                1,
                EnvelopeSpec {
                    real_hash: true,
                    ..EnvelopeSpec::default()
                },
            ),
        );
        self.run();
        address
    }

    fn refuse_first(&mut self) {
        self.refusal(
            0,
            TxErrorReason::DaLag {
                sealed_head: 10,
                posted_head: 1,
                budget_blocks: 1,
            },
        );
        self.run();
    }
}

#[test]
fn waiting_for_one_sender_does_not_stop_an_independent_sender() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    let other = b.other_sender();
    let offered: Vec<_> = b
        .rig
        .offers()
        .iter()
        .map(|o| (o.guard.sender, o.guard.nonce))
        .collect();
    assert_eq!(offered, [(b.sender(), 0), (other, 0)]);
    b.canonical(0);
    assert_eq!(b.rig.offers().last().unwrap().guard.sender, other);
    assert_eq!(
        b.l.outbox.len(),
        1,
        "the first sender still waits for its own confirmation"
    );
    let hash = b.rig.offers()[0].tx_ref.tx_hash;
    b.accounts.send(AccountEvent::Canonical(hash)).unwrap();
    b.run();
    assert_eq!(b.rig.offers().last().unwrap().guard.nonce, 1);
    assert_eq!(b.rig.offers().last().unwrap().guard.sender, b.sender());
}

#[test]
fn stale_rows_and_unrelated_hashes_cannot_release_a_sender() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    b.accounts
        .send(AccountEvent::Canonical(B256::repeat_byte(17)))
        .unwrap();
    let stale = AccountState {
        nonce: 8,
        balance: U256::MAX,
        delegated: false,
        tx_idx: 0,
    };
    b.accounts
        .send(AccountEvent::Rows {
            block: 2,
            rows: vec![(b.sender(), stale)],
        })
        .unwrap();
    b.accounts
        .send(AccountEvent::Answer {
            address: b.sender(),
            state: stale,
        })
        .unwrap();
    b.run();
    assert_eq!(b.offers(), [(0, true)]);
    b.canonical(0);
    assert_eq!(b.offers(), [(0, true), (1, true)]);
}

#[test]
fn both_twins_finish_a_delayed_refusal_without_new_client_traffic() {
    let mut twins = [
        Bench::new(SealerVersion::starting_at(5)),
        Bench::new(SealerVersion::starting_at(5)),
    ];
    twins.iter_mut().for_each(Bench::queue_three);
    twins[0].refuse_first();
    assert_eq!(twins[0].offers(), [(0, true), (0, true)]);
    for b in &mut twins {
        b.canonical(0);
    }
    twins[1].refuse_first();
    assert!(
        twins
            .iter()
            .all(|b| b.offers().iter().all(|(nonce, _)| *nonce <= 1))
    );
    for b in &mut twins {
        b.canonical(1);
    }
    for b in &mut twins {
        b.canonical(2);
    }
    twins.iter().for_each(Bench::assert_all_ordered);
}

#[test]
fn rollback_retries_the_prefix_before_its_suffix() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.canonical(0);
    b.send(1);
    b.accounts.send(AccountEvent::Rollback).unwrap();
    b.run();
    assert_eq!(b.offers(), [(0, true), (1, true), (0, true)]);
    b.canonical(0);
    assert_eq!(b.offers(), [(0, true), (1, true), (0, true), (1, true)]);
}

#[test]
fn a_twin_confirmation_before_local_intake_does_not_strand_the_suffix() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    let envelope = envelope_with(
        &b.signer,
        0,
        1,
        EnvelopeSpec {
            real_hash: true,
            ..EnvelopeSpec::default()
        },
    );
    b.accounts
        .send(AccountEvent::Canonical(envelope.tx_hash))
        .unwrap();
    b.send(0);
    assert!(b.offers().is_empty());
    b.send(1);
    assert_eq!(b.offers(), [(1, true)]);
}

#[test]
fn a_retained_confirmation_survives_history_eviction_and_reconnect() {
    let shared = SealerVersion::starting_at(5);
    let mut rules = SealerRules::new(0, shared.clone(), InMemorySidePublisher::default());
    rules.on_session_start(1);
    let mut b = Bench::new(shared);
    (0..3).for_each(|nonce| b.send(nonce));
    b.canonical(0);
    (1..=b.l.cfg.outbox_cap.get()).for_each(|n| {
        b.accounts
            .send(AccountEvent::Canonical(B256::from(U256::from(n))))
            .unwrap();
    });
    b.run();
    rules.on_session_start(2);
    b.run();
    assert_eq!(b.offers(), [(0, true), (1, true), (1, true)]);
    b.canonical(1);
    assert_eq!(b.offers().last(), Some(&(2, true)));
    b.canonical(2);
    assert!(b.l.outbox.is_empty());
    assert!(b.rig.errors().is_empty());
    assert!(b.rig.drops().is_empty());
}

#[test]
fn a_twin_confirmation_of_a_queued_copy_survives_history_eviction() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    (0..3).for_each(|nonce| b.send(nonce));
    let hash = b.l.admission.unconfirmed_at(b.sender(), 1).unwrap().tx_hash;
    b.accounts.send(AccountEvent::Canonical(hash)).unwrap();
    (1..=b.l.cfg.outbox_cap.get()).for_each(|n| {
        b.accounts
            .send(AccountEvent::Canonical(B256::from(U256::from(n))))
            .unwrap();
    });
    b.canonical(0);
    assert_eq!(b.offers(), [(0, true), (2, true)]);
    assert!(
        b.l.admission
            .unconfirmed_at(b.sender(), 1)
            .unwrap()
            .payload
            .canonical
    );
}

#[test]
fn late_terminal_notices_cannot_remove_an_already_canonical_copy() {
    let mut b = Bench::new(SealerVersion::starting_at(4));
    let mut envelope = envelope_with(
        &b.signer,
        0,
        1,
        EnvelopeSpec {
            real_hash: true,
            ..EnvelopeSpec::default()
        },
    );
    envelope.max_inclusion_block = 100;
    b.rig.push(TxDataLoc::new(7, pos(64)), envelope);
    b.run();
    b.send(1);
    b.canonical(0);
    let candidate = b.l.admission.unconfirmed_at(b.sender(), 0).unwrap();
    let hash = candidate.tx_hash;
    let deadline = candidate.payload.max_inclusion_block;
    b.refusal(
        0,
        TxErrorReason::PastDeadline {
            max_inclusion_block: deadline,
            at_block: deadline + 1,
        },
    );
    b.accounts
        .send(AccountEvent::Refused(Box::new(
            kardamom_cluster_adapter::wire::RefRefused {
                canonical_id: hash,
                sender: b.sender(),
                nonce: 0,
                decision_version: 4,
            },
        )))
        .unwrap();
    b.run();
    assert!(
        b.l.admission
            .unconfirmed_at(b.sender(), 0)
            .unwrap()
            .payload
            .canonical
    );
    assert!(b.rig.errors().is_empty());
    assert!(b.rig.drops().is_empty());
    assert_eq!(b.offers(), [(0, false), (1, false), (1, false)]);
    b.canonical(1);
    assert!(b.l.outbox.is_empty());
}

#[test]
fn a_confirmation_after_lookup_release_survives_the_same_event_batch() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.run();
    b.accounts.send(AccountEvent::Rollback).unwrap();
    b.run();
    b.send(0);
    b.send(1);
    assert!(b.offers().is_empty());
    let envelope = envelope_with(
        &b.signer,
        0,
        1,
        EnvelopeSpec {
            real_hash: true,
            ..EnvelopeSpec::default()
        },
    );
    b.accounts
        .send(AccountEvent::Answer {
            address: b.sender(),
            state: AccountState {
                nonce: 0,
                balance: U256::MAX,
                delegated: false,
                tx_idx: 2,
            },
        })
        .unwrap();
    b.accounts
        .send(AccountEvent::Canonical(envelope.tx_hash))
        .unwrap();
    (1..=b.l.cfg.outbox_cap.get()).for_each(|n| {
        b.accounts
            .send(AccountEvent::Canonical(B256::from(U256::from(n))))
            .unwrap();
    });
    b.run();
    assert_eq!(b.offers(), [(1, true)]);
    b.canonical(1);
    assert!(b.l.outbox.is_empty());
}

#[derive(Default)]
struct PartialPublisher {
    offers: Vec<crate::outbound::RefOffer>,
    remaining: usize,
}

impl crate::outbound::TxOrderingRefPublisher for PartialPublisher {
    fn try_publish_ref(
        &mut self,
        offer: &crate::outbound::RefOffer,
    ) -> Result<(), crate::SequencerError> {
        if self.remaining == 0 {
            return Err(crate::SequencerError::Backpressure);
        }
        self.remaining -= 1;
        self.offers.push(offer.clone());
        Ok(())
    }

    fn try_publish_epoch(
        &mut self,
        _: &kardamom_types::EpochRecord,
    ) -> Result<(), crate::SequencerError> {
        unreachable!("this control publishes transaction references only")
    }

    fn try_publish_remote_epoch(
        &mut self,
        _: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), crate::SequencerError> {
        unreachable!("this control publishes transaction references only")
    }
}

#[test]
fn partial_publication_retries_only_the_unpublished_sender_and_holds_its_suffix() {
    let mut b = Bench::new(SealerVersion::starting_at(5));
    b.send(0);
    b.send(1);
    b.other_sender();
    b.l.offer_again_unconfirmed("test reconnect");
    let mut publisher = PartialPublisher {
        remaining: 1,
        ..PartialPublisher::default()
    };
    assert!(matches!(
        b.l.flush(&mut publisher, &mut b.rig.errors),
        Err(crate::SequencerError::Backpressure)
    ));
    assert_eq!(publisher.offers.len(), 1);
    publisher.remaining = 3;
    b.l.flush(&mut publisher, &mut b.rig.errors).unwrap();
    assert_eq!(publisher.offers.len(), 2);
    assert_ne!(
        publisher.offers[0].guard.sender,
        publisher.offers[1].guard.sender
    );
    assert!(publisher.offers.iter().all(|offer| offer.guard.nonce == 0));
    publisher.offers.iter().for_each(|offer| {
        b.accounts
            .send(AccountEvent::Canonical(offer.tx_ref.tx_hash))
            .unwrap();
    });
    assert!(b.l.apply_accounts());
    b.l.flush(&mut publisher, &mut b.rig.errors).unwrap();
    assert_eq!(publisher.offers.len(), 3);
    assert_eq!(publisher.offers[2].guard.sender, b.sender());
    assert_eq!(publisher.offers[2].guard.nonce, 1);
    assert!(b.rig.errors().is_empty());
    assert!(b.rig.drops().is_empty());
}
