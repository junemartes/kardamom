//! A terminal sealer refusal frees the refused nonce.
//!
//! The sealer refuses a ref past its inclusion deadline (or on a DA lag or
//! a record lag) before its contiguity guard, so its expected nonce for
//! the sender stays at the refused nonce. The sequencer must do the same:
//! a resubmit of the refused nonce publishes, and the later refs of the
//! sender follow it. If the sequencer keeps its floor past the refused
//! nonce, the resubmit is a past nonce (`DuplicatedTx`) and every later
//! ref gets a contiguity reject for good.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{TxDataLoc, TxErrorReason};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::resync::{FloorUpdate, ResyncChannel, ResyncConfig, SealerRefusal};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, pos, signed_envelope, signer};

/// A sequencer with the resync channels open, and the three sender
/// halves the egress thread feeds in production.
struct Sealed {
    seq: Sequencer,
    rig: Rig,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
    reject_tx: crossbeam_channel::Sender<(Address, u64, u64)>,
    deadline_tx: crossbeam_channel::Sender<SealerRefusal>,
    /// The next free `tx_data` offset.
    offset: i32,
}

impl Sealed {
    fn new(cfg: SequencerConfig) -> Self {
        let mut seq = Sequencer::new(cfg).unwrap();
        let ResyncChannel {
            controller,
            floor_tx,
            reject_tx,
            deadline_tx,
            ..
        } = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
        seq.enable_resync(controller);
        Self {
            seq,
            rig: Rig::default(),
            floor_tx,
            reject_tx,
            deadline_tx,
            offset: 0,
        }
    }

    /// The client submits `nonce`, and the sequencer takes one step.
    fn submit(&mut self, s: &PrivateKeySigner, nonce: u64) {
        let at = pos(self.offset);
        self.offset += 64;
        self.rig.push(
            TxDataLoc::new(0, at),
            signed_envelope(s, nonce, u64::try_from(self.offset).unwrap()),
        );
        self.step();
    }

    fn step(&mut self) {
        self.rig.step(&mut self.seq).unwrap();
    }

    /// The nonces of every offer after the first `skip`, in offer order.
    fn offered_nonces(&self, skip: usize) -> Vec<u64> {
        self.rig
            .offers()
            .iter()
            .skip(skip)
            .map(|o| o.guard.nonce)
            .collect()
    }

    /// The sealer refuses `nonce` past its deadline, and answers each of
    /// `later` with a contiguity reject that expects `nonce`.
    fn refuse(&self, s: Address, nonce: u64, later: &[u64]) {
        self.deadline_tx
            .send(SealerRefusal {
                sender: s,
                nonce,
                reason: TxErrorReason::PastDeadline {
                    max_inclusion_block: 64,
                    at_block: 65,
                },
            })
            .unwrap();
        for n in later {
            self.reject_tx.send((s, *n, nonce)).unwrap();
        }
    }

    fn errors_of(&self, nonce: u64) -> Vec<TxErrorReason> {
        self.rig
            .errors()
            .into_iter()
            .filter(|e| e.nonce == nonce)
            .map(|e| e.reason)
            .collect()
    }
}

/// The sequence of the stuck sender: the executors are down, so no
/// receipt comes; the deadline passes; the sealer refuses nonce 1 and
/// rejects nonces 2 and 3. The resubmit of nonce 1 publishes, nonces 2
/// and 3 follow it, and a later nonce 4 publishes too.
#[test]
fn a_refused_nonce_is_free_for_the_resubmit() {
    let s = signer(21);
    let mut sealed = Sealed::new(one_partition_cfg());
    (0..4).for_each(|n| sealed.submit(&s, n));
    assert_eq!(sealed.offered_nonces(0), vec![0, 1, 2, 3]);

    sealed.refuse(s.address(), 1, &[2, 3]);
    sealed.step();
    sealed.step();
    assert_eq!(
        sealed.errors_of(1),
        vec![TxErrorReason::PastDeadline {
            max_inclusion_block: 64,
            at_block: 65,
        }],
        "the client hears of the refusal"
    );
    assert_eq!(
        sealed.offered_nonces(4),
        Vec::<u64>::new(),
        "nonces 2 and 3 wait for nonce 1 and do not republish"
    );

    sealed.submit(&s, 1);
    assert_eq!(
        sealed.offered_nonces(4),
        vec![1, 2, 3],
        "the resubmit publishes, and the parked run drains behind it"
    );
    assert_eq!(
        sealed.rig.offers()[4].tx_ref.tx_data_position,
        pos(256),
        "the resubmit publishes the new envelope"
    );

    sealed.submit(&s, 4);
    assert_eq!(sealed.offered_nonces(7), vec![4]);
    assert!(
        !sealed
            .rig
            .errors()
            .iter()
            .any(|e| matches!(e.reason, TxErrorReason::DuplicatedTx { .. })),
        "no submit is a past nonce"
    );
}

/// A client that does not resubmit the refused nonce: the later refs wait
/// on a gap that only the client can fill, so they expire after `tx_ttl`
/// and their clients hear of it.
#[test]
fn the_later_refs_expire_without_a_resubmit() {
    let s = signer(22);
    let mut sealed = Sealed::new(SequencerConfig {
        tx_ttl_ms: NonZeroU64::new(20).unwrap(),
        ..one_partition_cfg()
    });
    (0..3).for_each(|n| sealed.submit(&s, n));
    sealed.refuse(s.address(), 0, &[1, 2]);
    sealed.step();

    std::thread::sleep(Duration::from_millis(40));
    sealed.step();
    let expired = [1, 2].map(|n| sealed.errors_of(n));
    assert_eq!(
        expired,
        [1, 2].map(|_| vec![TxErrorReason::Expired { expected_nonce: 0 }]),
        "each later ref expires once"
    );
    assert_eq!(sealed.offered_nonces(3), Vec::<u64>::new());
}

/// A receipt proves that nonce 1 executed. A refusal of a late copy of it
/// frees nothing: the floor stays, a resubmit of nonce 1 is a proven
/// duplicate, and the next nonce publishes.
#[test]
fn a_refusal_of_a_proven_ref_keeps_the_floor() {
    let s = signer(23);
    let mut sealed = Sealed::new(one_partition_cfg());
    (0..3).for_each(|n| sealed.submit(&s, n));
    sealed
        .floor_tx
        .send(FloorUpdate::executed(s.address(), 1))
        .unwrap();
    sealed.step();

    sealed.refuse(s.address(), 1, &[]);
    sealed.submit(&s, 1);
    sealed.submit(&s, 3);
    assert_eq!(
        sealed.offered_nonces(3),
        vec![3],
        "the floor stays past the executed nonce"
    );
}
