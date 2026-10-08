use std::time::Duration;

use alloy_primitives::B256;
use kardamom_cluster_adapter::gateway::OfferOutcome;
use kardamom_cluster_adapter::gateway::fakes::FakeIngress;
use kardamom_types::epoch_delivery::PUBLISH_WINDOW;

use super::fakes::ScriptedEpochs;
use super::*;
use crate::outbound::cluster::ClusterRefPublisher;
use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

fn epoch(n: u64, deposits: usize) -> EpochRecord {
    EpochRecord {
        l1_number: n,
        l1_hash: B256::left_padding_from(&n.to_be_bytes()),
        deposits: (0..deposits)
            .map(|i| kardamom_types::Deposit {
                source_hash: B256::repeat_byte(0xD0 + u8::try_from(i).unwrap()),
                mint: 100 + u128::try_from(i).unwrap(),
                ..Default::default()
            })
            .collect(),
    }
}

/// A subscription that holds the epochs of `origins`, in that order.
fn feed(origins: impl IntoIterator<Item = u64>) -> ScriptedEpochs {
    let sub = ScriptedEpochs::default();
    origins
        .into_iter()
        .for_each(|n| sub.push(BPosition::default(), epoch(n, 0)));
    sub
}

#[test]
fn forwards_the_epoch_verbatim() {
    // The sequencer must not re-derive or reorder anything. It orders
    // exactly what the watcher derived from L1. Otherwise the producer
    // and verifier could disagree.
    let mut sub = ScriptedEpochs::default();
    let mut pubr = InMemoryTxOrderingRefPublisher::default();
    let e = epoch(100, 3);
    sub.push(BPosition::default(), e.clone());

    assert!(EpochPump::new().1.relay(&mut sub, &mut pubr).unwrap());

    let got = pubr.epochs.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0], e);
}

#[test]
fn empty_epochs_are_forwarded_too() {
    // A depositless epoch is the one most tempting to drop. Without
    // testing it, the no-skipping rule is not enforced.
    let mut sub = feed([101]);
    let mut pubr = InMemoryTxOrderingRefPublisher::default();

    assert!(EpochPump::new().1.relay(&mut sub, &mut pubr).unwrap());
    assert_eq!(pubr.epochs.lock().unwrap().len(), 1);
}

/// Idle, closed, backpressure-holds-the-record, retry-does-not-poll-
/// past-the-held-record, and relay-after-backpressure-clears: the
/// contract every `ScriptedQueue<T>`-backed pump shares.
#[test]
fn epoch_pump_honors_the_shared_contract() {
    crate::fakes::pump_contract::run(
        &epoch(102, 1),
        &epoch(103, 0),
        || EpochPump::new().1,
        EpochPump::is_held,
    );
}

/// The sealer's origin rule over the frames a [`FakeIngress`] accepted:
/// it loses the offers of `lose` once each, absorbs a copy, refuses a
/// gap, and confirms the rest. Every answer goes to the pump in egress
/// order, as the egress feed sends it.
struct FakeSealer {
    origin: u64,
    seen: usize,
    lose: Vec<u64>,
    signals: OriginSignalTx,
}

impl FakeSealer {
    /// Read the frames accepted since the last call, in order.
    fn order(&mut self, ingress: &FakeIngress) {
        let sent = ingress.accepted();
        sent[self.seen..]
            .iter()
            .for_each(|frame| self.offer(u64::from_le_bytes(frame[33..41].try_into().unwrap())));
        self.seen = sent.len();
    }

    fn offer(&mut self, n: u64) {
        let lost = self.lose.iter().position(|l| *l == n);
        let signal = match lost {
            Some(at) => {
                self.lose.remove(at);
                None
            }
            None if n <= self.origin => None,
            None if n != self.origin + 1 => Some(OriginSignal::Gap {
                expected: self.origin + 1,
            }),
            None => {
                self.origin = n;
                Some(OriginSignal::Confirmed(n))
            }
        };
        if let Some(s) = signal {
            self.signals.send(s).unwrap();
        }
    }
}

/// The origins of the frames a [`FakeIngress`] accepted, in order.
fn offered(ingress: &FakeIngress) -> Vec<u64> {
    ingress
        .accepted()
        .iter()
        .map(|f| u64::from_le_bytes(f[33..41].try_into().unwrap()))
        .collect()
}

/// Relay until the pump reports no work.
fn drain<S: EpochSubscriber, P: TxOrderingRefPublisher>(
    pump: &mut EpochPump,
    sub: &mut S,
    publ: &mut P,
) {
    while pump.relay(sub, publ).unwrap() {}
}

/// The ingress accepts 101 and loses it, as across a leader kill. The
/// sealer refuses 102 with the expected origin 101, and the pump offers
/// 101 and 102 again. Each confirmed epoch leaves the queue.
#[test]
fn an_accepted_epoch_the_sealer_lost_is_offered_again_from_the_expected_origin() {
    let (tx, mut pump) = EpochPump::new();
    let ingress = FakeIngress::new();
    let mut publ = ClusterRefPublisher::new(ingress.clone());
    let mut sealer = FakeSealer {
        origin: 99,
        seen: 0,
        lose: vec![101],
        signals: tx,
    };
    let mut sub = feed([100, 101, 102]);

    drain(&mut pump, &mut sub, &mut publ);
    assert_eq!(offered(&ingress), [100, 101, 102]);
    assert_eq!(
        pump.unconfirmed.len(),
        3,
        "an accepted offer is not ordered"
    );

    sealer.order(&ingress);
    drain(&mut pump, &mut sub, &mut publ);
    assert_eq!(offered(&ingress), [100, 101, 102, 101, 102]);
    assert_eq!(pump.unconfirmed.len(), 2, "the boundary confirmed 100 only");

    sealer.order(&ingress);
    drain(&mut pump, &mut sub, &mut publ);
    assert_eq!(sealer.origin, 102, "the sealer ordered 100, 101, 102");
    assert_eq!(
        pump.unconfirmed.len(),
        0,
        "the boundaries confirmed every epoch"
    );
    assert_eq!(offered(&ingress).len(), 5, "nothing else is offered");
}

/// A twin's offer can order an epoch before this pump offers it. The
/// confirm removes it, and the pump does not offer it.
#[test]
fn a_confirmed_epoch_is_removed_before_its_offer() {
    let (tx, mut pump) = EpochPump::new();
    let mut publ = InMemoryTxOrderingRefPublisher::default();
    let mut sub = feed([100]);
    *publ.fail_with_backpressure.lock().unwrap() = true;
    assert!(matches!(
        pump.relay(&mut sub, &mut publ),
        Err(SequencerError::Backpressure)
    ));

    tx.send(OriginSignal::Confirmed(100)).unwrap();
    *publ.fail_with_backpressure.lock().unwrap() = false;

    assert!(!pump.relay(&mut sub, &mut publ).unwrap());
    assert!(publ.epochs.lock().unwrap().is_empty());
    assert_eq!(pump.unconfirmed.len(), 0);
}

/// A full queue stops the pump before it polls. Nothing is skipped: the
/// next epoch stays in the subscription. A confirm frees the queue.
#[test]
fn an_overflow_halts_and_polls_nothing() {
    let (tx, mut pump) = EpochPump::new();
    let mut publ = InMemoryTxOrderingRefPublisher::default();
    let first = 1_000_u64;
    let count = u64::try_from(MAX_UNCONFIRMED_EPOCHS).unwrap();
    let mut sub = feed(first..=first + count);

    (0..MAX_UNCONFIRMED_EPOCHS).for_each(|_| assert!(pump.relay(&mut sub, &mut publ).unwrap()));
    let err = pump.relay(&mut sub, &mut publ).unwrap_err();

    assert!(
        matches!(err, SequencerError::EpochQueueFull { held, oldest: 1_000 } if held == MAX_UNCONFIRMED_EPOCHS),
        "{err}"
    );
    assert_eq!(
        err.halt().unwrap().cause,
        kardamom_obs::halt::HaltCause::OriginGap
    );
    assert_eq!(
        sub.len(),
        1,
        "the epoch past the bound stays in the subscription"
    );

    tx.send(OriginSignal::Confirmed(first)).unwrap();
    assert!(pump.relay(&mut sub, &mut publ).unwrap());
    assert_eq!(sub.len(), 0);
    assert_eq!(
        publ.epochs.lock().unwrap().last().unwrap().l1_number,
        first + count
    );
}

/// After a restart, the pump does not hold the epoch the sealer expects.
/// It stops offering and, past the grace, reports the gap until a twin's
/// offer fills it: the boundary that confirms the missing epoch ends the
/// stall.
#[test]
fn a_missing_epoch_stalls_until_a_boundary_confirms_it() {
    let (tx, pump) = EpochPump::new();
    let mut pump = pump.with_gap_grace(Duration::ZERO);
    let mut publ = InMemoryTxOrderingRefPublisher::default();
    let mut sub = feed([105, 106]);
    assert!(pump.relay(&mut sub, &mut publ).unwrap());

    tx.send(OriginSignal::Gap { expected: 104 }).unwrap();
    let err = pump.relay(&mut sub, &mut publ).unwrap_err();
    assert!(
        matches!(err, SequencerError::OriginGapUnfilled { expected: 104 }),
        "{err}"
    );
    assert_eq!(publ.epochs.lock().unwrap().len(), 1, "106 is not offered");
    assert_eq!(pump.unconfirmed.len(), 2, "106 is kept");

    tx.send(OriginSignal::Confirmed(104)).unwrap();
    drain(&mut pump, &mut sub, &mut publ);
    let origins: Vec<u64> = publ
        .epochs
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.l1_number)
        .collect();
    assert_eq!(origins, [105, 105, 106], "the pump offers again from 105");
}

/// A da-watcher that publishes again from an older block fills the gap:
/// the missing epoch takes its place in L1 order, and the pump offers
/// from it. An epoch the sealer ordered already is dropped.
#[test]
fn a_missing_epoch_that_arrives_again_is_offered_in_order() {
    let (tx, pump) = EpochPump::new();
    let mut pump = pump.with_gap_grace(Duration::ZERO);
    let mut publ = InMemoryTxOrderingRefPublisher::default();
    let mut sub = feed([105]);
    tx.send(OriginSignal::Confirmed(102)).unwrap();
    drain(&mut pump, &mut sub, &mut publ);
    tx.send(OriginSignal::Gap { expected: 104 }).unwrap();
    assert!(pump.relay(&mut sub, &mut publ).is_err());

    for n in [102, 103, 104, 105] {
        sub.push(BPosition::default(), epoch(n, 0));
    }
    assert!(
        pump.relay(&mut sub, &mut publ).is_err(),
        "102 is ordered already"
    );
    assert!(
        pump.relay(&mut sub, &mut publ).is_err(),
        "103 is not the gap"
    );
    drain(&mut pump, &mut sub, &mut publ);

    let origins: Vec<u64> = publ
        .epochs
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.l1_number)
        .collect();
    assert_eq!(origins, [105, 103, 104, 105]);
}

/// A sequencer restarts and loses the unconfirmed epoch 104 from its
/// queue. Within the grace, the gap is a wait, not a halt: the pump
/// offers nothing and reports no error. The da-watcher publishes its
/// unconfirmed epochs 104 and 105 again, and the pump offers them in
/// order.
#[test]
fn within_the_grace_a_missing_epoch_waits_for_the_republish() {
    let (tx, mut pump) = EpochPump::new();
    let mut publ = InMemoryTxOrderingRefPublisher::default();
    tx.send(OriginSignal::Confirmed(103)).unwrap();
    let mut sub = feed([105]);
    drain(&mut pump, &mut sub, &mut publ);
    tx.send(OriginSignal::Gap { expected: 104 }).unwrap();

    assert!(!pump.relay(&mut sub, &mut publ).unwrap(), "a wait, no halt");
    assert!(!pump.relay(&mut sub, &mut publ).unwrap(), "still a wait");

    sub.push(BPosition::default(), epoch(104, 0));
    sub.push(BPosition::default(), epoch(105, 0));
    drain(&mut pump, &mut sub, &mut publ);
    let origins: Vec<u64> = publ
        .epochs
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.l1_number)
        .collect();
    assert_eq!(origins, [105, 104, 105]);
    assert!(pump.missing.is_none());
}

/// One da-watcher window of unconfirmed epochs fits in the queue, so the
/// da-watcher's re-publish never fills it.
#[test]
fn a_publish_window_fits_in_the_queue() {
    assert!(PUBLISH_WINDOW.get() < MAX_UNCONFIRMED_EPOCHS);
}

#[test]
fn not_connected_keeps_the_epoch_for_the_next_offer() {
    let (_tx, mut pump) = EpochPump::new();
    let ingress = FakeIngress::new();
    ingress.set_outcome(OfferOutcome::NotConnected);
    let mut publ = ClusterRefPublisher::new(ingress.clone());
    let mut sub = feed([100]);

    assert!(pump.relay(&mut sub, &mut publ).is_err());
    ingress.set_outcome(OfferOutcome::Accepted);
    assert!(pump.relay(&mut sub, &mut publ).unwrap());
    assert_eq!(offered(&ingress), [100]);
}
