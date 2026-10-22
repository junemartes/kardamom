use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, U256};
use kardamom_types::shard_map::vslot_for;
use kardamom_types::{TxDataLoc, TxEnvelope, TxErrorReason, TxStageKind};

use super::{AccountEvent, AdmissionLoop, V2Config, VerdictOut};
use crate::admission::{AccountState, AdmissionConfig};
use crate::sealer_rules::{SealerRules, SealerVersion};
use crate::testkit::{Rig, one_partition_cfg, pos, signed_envelope, signer};

fn cfg() -> V2Config {
    V2Config {
        partition: 0,
        admission: AdmissionConfig {
            max_parked: NonZeroUsize::new(16).unwrap(),
            max_forwarded: NonZeroUsize::new(64).unwrap(),
            loss_blocks: NonZeroU64::new(16).unwrap(),
            park_width: NonZeroU64::new(1 << 17).unwrap(),
            park_ttl: Duration::from_secs(4),
            relook_after: Duration::from_millis(500),
            max_senders: NonZeroUsize::new(1 << 16).unwrap(),
        },
        sweep_every: Duration::from_millis(50),
        outbox_cap: NonZeroUsize::new(4).unwrap(),
    }
}

/// A loop over the one-partition test configuration, the sender side of
/// its account channel, and the receiver side of its lookup queue.
fn new_loop() -> (
    AdmissionLoop,
    crossbeam_channel::Sender<AccountEvent>,
    tokio::sync::mpsc::Receiver<Address>,
) {
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let (lookups_tx, lookups_rx) = tokio::sync::mpsc::channel(16);
    let vslots = one_partition_cfg().vslot_set().unwrap();
    (
        AdmissionLoop::new(cfg(), vslots, accounts_rx, lookups_tx),
        accounts_tx,
        lookups_rx,
    )
}

fn rich(nonce: u64, tx_idx: u64) -> AccountState {
    AccountState {
        nonce,
        balance: U256::from(10u64).pow(U256::from(24u64)),
        delegated: false,
        tx_idx,
    }
}

#[test]
fn a_known_sender_goes_out_and_gets_its_offered_status() {
    let s = signer(1);
    let (mut l, accounts, _) = new_loop();
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(64)), signed_envelope(&s, 0, 1));
    assert!(l.run_once(&mut rig.ports()).unwrap());
    let offers = rig.offers();
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0].guard.sender, s.address());
    assert_eq!(offers[0].tx_ref.tx_data_session_id, 7);
    assert!(
        rig.statuses()
            .iter()
            .any(|st| st.kind() == TxStageKind::Offered)
    );
}

#[test]
fn an_unknown_sender_waits_for_its_lookup() {
    let s = signer(2);
    let (mut l, accounts, mut lookups) = new_loop();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(64)), signed_envelope(&s, 0, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert!(rig.offers().is_empty());
    assert_eq!(lookups.try_recv().unwrap(), s.address());
    accounts
        .send(AccountEvent::Answer {
            address: s.address(),
            state: rich(0, 1),
        })
        .unwrap();
    l.run_once(&mut rig.ports()).unwrap();
    assert_eq!(rig.offers().len(), 1);
}

#[test]
fn a_sender_that_cannot_pay_gets_insufficient_funds() {
    let s = signer(3);
    let (mut l, accounts, _) = new_loop();
    let poor = AccountState {
        balance: U256::from(1u64),
        ..rich(0, 1)
    };
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), poor)],
        })
        .unwrap();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(64)), signed_envelope(&s, 0, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert!(rig.offers().is_empty());
    let errors = rig.errors();
    assert_eq!(errors.len(), 1);
    assert!(matches!(
        errors[0].reason,
        TxErrorReason::InsufficientFunds { have: 1, .. }
    ));
}

#[test]
fn a_sealer_on_version_four_gets_the_tagged_position() {
    let s = signer(4);
    let (mut l, accounts, _) = new_loop();
    let shared = SealerVersion::default();
    let mut rules = SealerRules::new(0, shared.clone());
    rules.on_session_start(1);
    rules.on_sealer_info(1, 4);
    l.enable_positioned_refs(shared);
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(256)), signed_envelope(&s, 0, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert_eq!(
        rig.offers()[0].guard.max_inclusion_block,
        kardamom_cluster_adapter::wire::POSITION_TAG | pos(256).as_index()
    );
}

/// A sealer on decision version 5 takes each offer with the bytes of its
/// transaction: the archived envelope that the ingress published.
#[test]
fn a_sealer_on_version_five_gets_the_bytes() {
    let s = signer(5);
    let (mut l, accounts, _) = new_loop();
    let shared = SealerVersion::default();
    let mut rules = SealerRules::new(0, shared.clone());
    rules.on_session_start(1);
    rules.on_sealer_info(1, 5);
    l.enable_positioned_refs(shared);
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    let envelope = signed_envelope(&s, 0, 1);
    rig.push(TxDataLoc::new(7, pos(256)), envelope.clone());
    l.run_once(&mut rig.ports()).unwrap();
    let carried = rig.offers()[0].envelope.clone().expect("the bytes");
    assert_eq!(
        carried.as_ref(),
        kardamom_log::codec::encode(&envelope).unwrap().as_slice()
    );
}

/// While the sealer takes no offer, the loop takes no envelope: the offer
/// that waits holds the intake. The offers go out in order once the sealer
/// takes them.
#[test]
fn a_back_pressured_sealer_holds_the_intake_and_keeps_the_order() {
    let s = signer(5);
    let (mut l, accounts, _) = new_loop();
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    for n in 0..6u64 {
        let at = pos(64 * (i32::try_from(n).unwrap() + 1));
        rig.push(TxDataLoc::new(7, at), signed_envelope(&s, n, n));
    }
    *rig.refs.fail_with_backpressure.lock().unwrap() = true;
    (0..6).for_each(|_| {
        let _ = l.run_once(&mut rig.ports());
    });
    assert_eq!(
        rig.tx_data.queue.len(),
        5,
        "the intake stops after the first refused offer"
    );
    *rig.refs.fail_with_backpressure.lock().unwrap() = false;
    (0..6).for_each(|_| {
        l.run_once(&mut rig.ports()).unwrap();
    });
    let nonces: Vec<u64> = rig.offers().iter().map(|o| o.guard.nonce).collect();
    assert_eq!(nonces, [0, 1, 2, 3, 4, 5]);
}

#[test]
fn an_envelope_of_another_shard_is_skipped() {
    let s = signer(6);
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let (lookups_tx, _lookups_rx) = tokio::sync::mpsc::channel(16);
    let mut l = AdmissionLoop::new(
        cfg(),
        kardamom_types::shard_map::VslotSet::default(),
        accounts_rx,
        lookups_tx,
    );
    accounts_tx
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(64)), signed_envelope(&s, 0, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert!(rig.offers().is_empty());
    assert!(rig.errors().is_empty());
}

/// A transaction that parks counts in the depth of its vslot until its
/// verdict.
#[test]
fn a_parked_transaction_counts_in_the_depth_of_its_vslot() {
    let s = signer(7);
    let vslot = vslot_for(s.address());
    let (mut l, accounts, _) = new_loop();
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(0, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(7, pos(64)), signed_envelope(&s, 1, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert_eq!(l.depth.of(vslot), 1, "nonce 1 waits for nonce 0");
    rig.push(TxDataLoc::new(7, pos(128)), signed_envelope(&s, 0, 1));
    l.run_once(&mut rig.ports()).unwrap();
    assert_eq!(l.depth.of(vslot), 0);
    assert_eq!(rig.offers().len(), 2);
}

/// The verdict of each transaction goes to the ingress that published it:
/// accepted when the filter forwards it or holds it for a gap, rejected
/// with the reason. A transaction that waits for a lookup gets its verdict
/// with the answer.
#[test]
fn each_transaction_gets_one_verdict_at_admission() {
    let s = signer(8);
    let cold = signer(9);
    let (mut l, accounts, _) = new_loop();
    let (verdicts_tx, mut verdicts) = tokio::sync::mpsc::channel(16);
    l.enable_verdicts(verdicts_tx);
    accounts
        .send(AccountEvent::Rows {
            block: 1,
            rows: vec![(s.address(), rich(2, 1))],
        })
        .unwrap();
    let mut rig = Rig::default();
    // Nonce 2 goes out, 4 waits for 3, 1 is below the chain, and the cold
    // sender waits for its lookup.
    for (who, nonce, ingress) in [(&s, 2u64, 3u16), (&s, 4, 5), (&s, 1, 6), (&cold, 0, 7)] {
        let mut env = signed_envelope(who, nonce, 1);
        env.correlation_id = TxEnvelope::pack_correlation_id(ingress, 40 + nonce);
        let at = pos(64 * (i32::try_from(nonce).unwrap() + 1) + i32::from(ingress) * 640);
        rig.push(TxDataLoc::new(7, at), env);
        l.run_once(&mut rig.ports()).unwrap();
    }
    l.run_once(&mut rig.ports()).unwrap();
    let got =
        |verdicts: &mut tokio::sync::mpsc::Receiver<VerdictOut>| -> Vec<(u8, u64, TxStageKind)> {
            std::iter::from_fn(|| verdicts.try_recv().ok())
                .map(|v| {
                    let (_, nonce) = v.status.identity().unwrap();
                    (v.ingress, nonce, v.status.kind())
                })
                .collect()
        };
    assert_eq!(
        got(&mut verdicts),
        [
            (3, 2, TxStageKind::Accepted),
            (5, 4, TxStageKind::Accepted),
            (6, 1, TxStageKind::Rejected),
        ],
        "the cold sender waits for its lookup"
    );

    accounts
        .send(AccountEvent::Answer {
            address: cold.address(),
            state: rich(0, 1),
        })
        .unwrap();
    // Nonce 3 closes the gap: 3 gets its verdict, 4 goes out with none.
    let mut env = signed_envelope(&s, 3, 1);
    env.correlation_id = TxEnvelope::pack_correlation_id(4, 43);
    rig.push(TxDataLoc::new(7, pos(64 * 40)), env);
    l.run_once(&mut rig.ports()).unwrap();
    l.run_once(&mut rig.ports()).unwrap();
    assert_eq!(
        got(&mut verdicts),
        [(7, 0, TxStageKind::Accepted), (4, 3, TxStageKind::Accepted)]
    );
    assert_eq!(rig.offers().len(), 4, "2, the cold 0, 3 and 4");
}
