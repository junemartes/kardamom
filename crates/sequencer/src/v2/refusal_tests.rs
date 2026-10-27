use super::*;
use crate::resync::{ResyncChannel, ResyncConfig, SealerRefusal};
use kardamom_types::TxErrorReason;

#[test]
fn a_da_lag_refusal_reports_every_unconfirmed_transaction_the_filter_forgets() {
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    bench.send(1);
    let reason = TxErrorReason::DaLag {
        sealed_head: 100,
        posted_head: 1,
        budget_blocks: 10,
    };
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: reason.clone(),
        })
        .unwrap();
    bench.run();
    let errors = bench.rig.errors();
    assert_eq!(errors.iter().map(|e| e.nonce).collect::<Vec<_>>(), [0, 1]);
    assert!(errors.iter().all(|e| e.reason == reason));
    assert!(bench.l.admission.unconfirmed().next().is_none());
    bench.send(0);
    bench.send(1);
    assert_eq!(bench.offers(), [(0, true), (1, true), (0, true), (1, true)]);
}

#[test]
fn a_late_refusal_of_a_confirmed_nonce_cannot_remove_the_remaining_run() {
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    bench.send(1);
    bench.confirm_first();
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: TxErrorReason::DaLag {
                sealed_head: 100,
                posted_head: 1,
                budget_blocks: 10,
            },
        })
        .unwrap();
    bench.run();
    assert!(bench.rig.errors().is_empty());
    assert_eq!(
        bench
            .l
            .admission
            .unconfirmed()
            .map(|c| c.nonce)
            .collect::<Vec<_>>(),
        [1]
    );
}

#[test]
fn a_past_deadline_answer_names_only_its_original_offer() {
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    let deadline = bench.rig.offers()[0].guard.max_inclusion_block;
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: TxErrorReason::PastDeadline {
                max_inclusion_block: deadline - 32,
                at_block: 10,
            },
        })
        .unwrap();
    bench.run();
    assert!(bench.rig.errors().is_empty());
    assert_eq!(bench.l.admission.unconfirmed().count(), 1);
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: TxErrorReason::PastDeadline {
                max_inclusion_block: deadline,
                at_block: 10,
            },
        })
        .unwrap();
    bench.run();
    assert_eq!(bench.rig.errors().len(), 1);
    assert!(bench.l.admission.unconfirmed().next().is_none());
}

#[test]
fn a_record_lag_refusal_removes_backpressured_offers_before_they_can_escape() {
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    *bench.rig.refs.fail_with_backpressure.lock().unwrap() = true;
    bench.queue(1);
    assert!(matches!(
        bench.l.run_once(&mut bench.rig.ports()),
        Err(crate::error::SequencerError::Backpressure)
    ));
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: TxErrorReason::RecordLag {
                sealed_index: 100,
                recorded_index: 1,
                budget: 10,
            },
        })
        .unwrap();
    *bench.rig.refs.fail_with_backpressure.lock().unwrap() = false;
    bench.run();
    assert_eq!(bench.offers(), [(0, true)]);
    assert_eq!(
        bench
            .rig
            .errors()
            .iter()
            .map(|e| e.nonce)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    bench.send(0);
    bench.send(1);
    assert_eq!(bench.offers(), [(0, true), (0, true), (1, true)]);
}

#[test]
fn a_paused_loop_reports_refusals_and_leaves_new_input_archived_until_resume() {
    use kardamom_obs::halt::{HaltCause, HaltRef};
    use kardamom_obs::lifecycle::Lifecycle;
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let life = Lifecycle::new(Some("sequencer"));
    bench.l.enable_pause(life.subscribe());
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    bench.send(1);
    life.follow(Some(HaltRef::sealer(HaltCause::DaLag)));
    channel
        .deadline_tx
        .send(SealerRefusal {
            sender: bench.sender(),
            nonce: 0,
            reason: TxErrorReason::DaLag {
                sealed_head: 100,
                posted_head: 1,
                budget_blocks: 10,
            },
        })
        .unwrap();
    bench.queue(0);
    bench.queue(1);
    bench.run();
    assert_eq!(bench.rig.errors().len(), 2);
    assert_eq!(bench.rig.tx_data.queue.len(), 2);
    assert_eq!(bench.offers(), [(0, true), (1, true)]);
    life.follow(None);
    bench.run();
    assert_eq!(bench.offers(), [(0, true), (1, true), (0, true), (1, true)]);
}

#[test]
fn a_full_terminal_channel_cannot_lose_a_paused_senders_unanswered_run() {
    use kardamom_obs::halt::{HaltCause, HaltRef};
    use kardamom_obs::lifecycle::Lifecycle;
    let mut bench = Bench::new(SealerVersion::starting_at(5));
    let life = Lifecycle::new(Some("sequencer"));
    bench.l.enable_pause(life.subscribe());
    let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    bench.l.enable_sealer_refusals(channel.controller);
    bench.send(0);
    let other = signer(12);
    bench
        .accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(
                other.address(),
                AccountState {
                    nonce: 0,
                    balance: U256::from(10u64).pow(U256::from(24u64)),
                    delegated: false,
                    tx_idx: 1,
                },
            )],
        })
        .unwrap();
    for nonce in 0..2 {
        bench.rig.push(
            TxDataLoc::new(7, pos(4096 + i32::try_from(nonce).unwrap() * 64)),
            signed_envelope(&other, nonce, 1),
        );
    }
    bench.run();
    life.follow(Some(HaltRef::sealer(HaltCause::DaLag)));
    let reason = TxErrorReason::DaLag {
        sealed_head: 100,
        posted_head: 1,
        budget_blocks: 10,
    };
    let flood = SealerRefusal {
        sender: bench.sender(),
        nonce: 0,
        reason: reason.clone(),
    };
    let queued = std::iter::repeat_with(|| channel.deadline_tx.try_send(flood.clone()))
        .take_while(Result::is_ok)
        .count();
    assert_eq!(Some(queued), channel.deadline_tx.capacity());
    assert!(matches!(
        channel.deadline_tx.try_send(SealerRefusal {
            sender: other.address(),
            nonce: 0,
            reason,
        }),
        Err(crossbeam_channel::TrySendError::Full(_))
    ));
    for block in [100, 200] {
        bench.empty_block(block);
    }
    assert_eq!(
        bench.l.admission.unconfirmed_for(other.address()).count(),
        2
    );
    life.follow(None);
    bench.run();
    let nonces: Vec<_> = bench
        .rig
        .offers()
        .into_iter()
        .filter(|offer| offer.guard.sender == other.address())
        .map(|offer| offer.guard.nonce)
        .collect();
    assert_eq!(nonces, [0, 1, 0, 1]);
    assert!(
        bench
            .rig
            .errors()
            .iter()
            .all(|error| error.sender != other.address())
    );
}
