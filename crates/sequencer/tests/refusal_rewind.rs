//! A terminal sealer refusal never leaves a sender stuck.
//!
//! The sealer refuses a ref past its inclusion deadline (or on a DA lag or
//! a record lag) before its contiguity guard, so its expected nonce for
//! the sender stays at the refused nonce. It also refuses a late copy of a
//! ref it ordered long ago, so a refusal alone says nothing about the
//! nonce. The sequencer moves its floor back only to the expected nonce
//! that a contiguity reject names, and only when it holds no ref at that
//! nonce. A resubmit at a refused nonce below the floor is offered again,
//! and the sealer decides.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{TxDataLoc, TxErrorReason};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::resync::{ResyncChannel, ResyncConfig, SealerRefusal};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, pos, signed_envelope, signer};

/// The reason every refusal in these tests carries. It names the
/// deadline of the test envelopes, so it names their copies.
const LATE: TxErrorReason = TxErrorReason::PastDeadline {
    max_inclusion_block: u64::MAX,
    at_block: 65,
};

/// A sequencer with the resync channels open, and the sender halves the
/// egress thread feeds in production.
struct Sealed {
    seq: Sequencer,
    rig: Rig,
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
            reject_tx,
            deadline_tx,
            ..
        } = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
        seq.enable_resync(controller);
        Self {
            seq,
            rig: Rig::default(),
            reject_tx,
            deadline_tx,
            offset: 0,
        }
    }

    /// A sequencer whose parked refs expire after 20 ms.
    fn short_ttl() -> Self {
        Self::new(SequencerConfig {
            tx_ttl_ms: NonZeroU64::new(20).unwrap(),
            ..one_partition_cfg()
        })
    }

    /// The client submits `nonce`, and the sequencer takes one step.
    fn submit(&mut self, s: &PrivateKeySigner, nonce: u64) {
        self.submit_with_deadline(s, nonce, u64::MAX);
    }

    /// [`Self::submit`] with the deadline `deadline` on the envelope.
    fn submit_with_deadline(&mut self, s: &PrivateKeySigner, nonce: u64, deadline: u64) {
        let at = pos(self.offset);
        self.offset += 64;
        let mut envelope = signed_envelope(s, nonce, u64::try_from(self.offset).unwrap());
        envelope.max_inclusion_block = deadline;
        self.rig.push(TxDataLoc::new(0, at), envelope);
        self.step();
    }

    fn step(&mut self) {
        self.rig.step(&mut self.seq).unwrap();
    }

    /// One step while the publisher pushes back.
    fn step_blocked(&mut self) {
        assert!(self.rig.step(&mut self.seq).is_err(), "the publish blocks");
    }

    /// Wait past the 20 ms `tx_ttl` of [`Self::short_ttl`], and step.
    fn step_after_ttl(&mut self) {
        std::thread::sleep(Duration::from_millis(40));
        self.step();
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

    /// The sealer refuses each of `nonces` past its deadline.
    fn refuse(&self, s: Address, nonces: &[u64]) {
        for nonce in nonces {
            self.deadline_tx
                .send(SealerRefusal {
                    sender: s,
                    nonce: *nonce,
                    reason: LATE,
                })
                .unwrap();
        }
    }

    /// The sealer answers each of `nonces` with a contiguity reject that
    /// names `expected`.
    fn reject(&self, s: Address, nonces: &[u64], expected: u64) {
        for nonce in nonces {
            self.reject_tx.send((s, *nonce, expected)).unwrap();
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

    /// Every error reason, without the refusals.
    fn errors_but_refusals(&self) -> Vec<TxErrorReason> {
        self.rig
            .errors()
            .into_iter()
            .map(|e| e.reason)
            .filter(|r| *r != LATE)
            .collect()
    }
}

/// The sequence of the stuck sender: the executors are down, so no
/// receipt comes; the deadline passes; the sealer refuses nonce 1 and
/// rejects nonces 2 and 3 with `expected=1`. The resubmit of nonce 1
/// publishes, nonces 2 and 3 follow it, and a later nonce 4 publishes too.
#[test]
fn a_refused_nonce_is_free_for_the_resubmit() {
    let s = signer(21);
    let mut sealed = Sealed::new(one_partition_cfg());
    (0..4).for_each(|n| sealed.submit(&s, n));
    assert_eq!(sealed.offered_nonces(0), vec![0, 1, 2, 3]);

    sealed.refuse(s.address(), &[1]);
    sealed.reject(s.address(), &[2, 3], 1);
    sealed.step();
    sealed.step();
    assert_eq!(sealed.errors_of(1), vec![LATE], "the client hears of it");
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
    assert_eq!(sealed.errors_but_refusals(), vec![], "no past nonce");
}

/// A record-lag halt refuses the whole tail 1 to 3, and no contiguity
/// reject comes. The floor stays, nothing parks, and each resubmit is
/// offered again.
#[test]
fn a_refused_tail_is_offered_again_on_resubmit() {
    let s = signer(22);
    let mut sealed = Sealed::short_ttl();
    (0..4).for_each(|n| sealed.submit(&s, n));
    sealed.refuse(s.address(), &[1, 2, 3]);
    sealed.step();
    sealed.step_after_ttl();
    assert_eq!(sealed.offered_nonces(4), Vec::<u64>::new());

    (1..4).for_each(|n| sealed.submit(&s, n));
    sealed.submit(&s, 4);
    assert_eq!(sealed.offered_nonces(4), vec![1, 2, 3, 4]);
    assert_eq!(sealed.errors_but_refusals(), vec![], "nothing expires");
}

/// The executors are down past the horizon. The sealer ordered nonces 0
/// to 3 long ago, and no receipt came. The confirm sweep republishes
/// them, and the sealer refuses each late copy. The floor stays, a new
/// nonce publishes at once, and nothing expires. A resubmit of a refused
/// nonce is offered, and the sealer answers it as a past nonce.
#[test]
fn a_late_copy_of_an_ordered_ref_keeps_the_floor() {
    let s = signer(23);
    let mut sealed = Sealed::short_ttl();
    (0..4).for_each(|n| sealed.submit(&s, n));
    sealed.seq.set_confirm_timeout_ms(0);
    sealed.step();
    assert_eq!(sealed.offered_nonces(4), vec![0, 1, 2, 3], "the sweep");
    sealed.seq.set_confirm_timeout_ms(60_000);

    sealed.refuse(s.address(), &[0, 1, 2, 3]);
    sealed.step();
    sealed.submit(&s, 4);
    assert_eq!(sealed.offered_nonces(8), vec![4], "no stall");

    sealed.submit(&s, 2);
    assert_eq!(sealed.offered_nonces(9), vec![2], "the resubmit is offered");
    sealed.reject(s.address(), &[2], 5);
    sealed.step_after_ttl();
    sealed.submit(&s, 5);
    assert_eq!(sealed.offered_nonces(10), vec![5], "the floor stays at 5");
    assert_eq!(sealed.errors_but_refusals(), vec![], "nothing expires");
}

/// The twins of a shard get the same sealer answers in a different
/// order: twin A gets the refusal first, twin B gets the contiguity
/// rejects first, republishes, and gets the answers to the republish. Both
/// end in the same state, and both publish the same refs on the resubmit.
#[test]
fn twins_with_different_answer_orders_converge() {
    let s = signer(24);
    let mut a = Sealed::new(one_partition_cfg());
    let mut b = Sealed::new(one_partition_cfg());
    for twin in [&mut a, &mut b] {
        (0..4).for_each(|n| twin.submit(&s, n));
    }

    a.refuse(s.address(), &[1]);
    a.reject(s.address(), &[2, 3], 1);
    a.step();

    b.reject(s.address(), &[2, 3], 1);
    b.step();
    b.step();
    assert_eq!(b.offered_nonces(4), vec![1, 2, 3], "B rewinds the gap");
    b.refuse(s.address(), &[1, 1]);
    b.reject(s.address(), &[2, 3], 1);
    b.step();

    assert_eq!(a.offered_nonces(4), Vec::<u64>::new());
    assert_eq!(b.offered_nonces(7), Vec::<u64>::new());

    a.submit(&s, 1);
    b.submit(&s, 1);
    assert_eq!(a.offered_nonces(4), vec![1, 2, 3]);
    assert_eq!(b.offered_nonces(7), vec![1, 2, 3]);
    let resubmit = |twin: &Sealed, at: usize| twin.rig.offers()[at].tx_ref.tx_data_position;
    assert_eq!(resubmit(&a, 4), pos(256), "A publishes the resubmit");
    assert_eq!(resubmit(&b, 7), pos(256), "B publishes the resubmit");
    assert_eq!(a.errors_but_refusals(), vec![]);
    assert_eq!(b.errors_but_refusals(), vec![]);
}

/// The sealer refuses nonce 0 and rejects 1 to 3, so 1 to 3 park. Then
/// the refusals of 1 to 3 arrive. Their parked copies leave the buffer:
/// nothing expires later, and the resubmit of 0 publishes 0 alone.
#[test]
fn a_refused_batch_leaves_nothing_parked() {
    let s = signer(25);
    let mut sealed = Sealed::short_ttl();
    (0..4).for_each(|n| sealed.submit(&s, n));
    sealed.refuse(s.address(), &[0]);
    sealed.reject(s.address(), &[1, 2, 3], 0);
    sealed.step();
    sealed.refuse(s.address(), &[1, 2, 3]);
    sealed.step_after_ttl();
    assert_eq!(sealed.errors_but_refusals(), vec![], "nothing expires");

    sealed.submit(&s, 0);
    assert_eq!(sealed.offered_nonces(4), vec![0]);
}

/// A client that does not resubmit the freed nonce: the later refs wait
/// on a gap that only the client can fill, so they expire after `tx_ttl`
/// and their clients hear of it.
#[test]
fn the_later_refs_expire_without_a_resubmit() {
    let s = signer(26);
    let mut sealed = Sealed::short_ttl();
    (0..3).for_each(|n| sealed.submit(&s, n));
    sealed.refuse(s.address(), &[0]);
    sealed.reject(s.address(), &[1, 2], 0);
    sealed.step();
    sealed.step_after_ttl();
    let expired = [1, 2].map(|n| sealed.errors_of(n));
    assert_eq!(
        expired,
        [1, 2].map(|_| vec![TxErrorReason::Expired { expected_nonce: 0 }]),
        "each later ref expires once"
    );
    assert_eq!(sealed.offered_nonces(3), Vec::<u64>::new());
}

/// Back-pressure keeps the swept refs 0 to 3 in the buffer, with the
/// floor at 0, when the refusal of an earlier copy of 0 arrives. The
/// buffered 0 stays: it republishes when the publisher recovers, and the
/// refs 1 to 3 do not wait for good behind a hole at the floor.
#[test]
fn a_refusal_leaves_a_rebuffered_ref_to_republish() {
    let s = signer(27);
    let mut sealed = Sealed::short_ttl();
    (0..4).for_each(|n| sealed.submit(&s, n));
    *sealed.rig.refs.fail_with_backpressure.lock().unwrap() = true;
    sealed.seq.set_confirm_timeout_ms(0);
    sealed.step_blocked();
    sealed.seq.set_confirm_timeout_ms(60_000);

    sealed.refuse(s.address(), &[0]);
    sealed.step_blocked();
    *sealed.rig.refs.fail_with_backpressure.lock().unwrap() = false;
    sealed.step();
    assert_eq!(sealed.offered_nonces(4), vec![0, 1, 2, 3], "the republish");

    sealed.refuse(s.address(), &[0]);
    sealed.reject(s.address(), &[1, 2, 3], 0);
    sealed.step();
    sealed.step_after_ttl();
    let expired = [1, 2, 3].map(|n| sealed.errors_of(n));
    assert_eq!(
        expired,
        [1, 2, 3].map(|_| vec![TxErrorReason::Expired { expected_nonce: 0 }]),
        "without a resubmit of 0, the clients of 1 to 3 hear back"
    );
}

/// The refs 1 to 3 park behind the freed nonce 0. The client resubmits 2
/// with a new deadline, which replaces the parked copy. A late refusal of
/// the old copy of 2 does not remove the resubmit, and the resubmit of 0
/// publishes the whole run.
#[test]
fn a_late_refusal_spares_the_resubmit_that_replaced_its_copy() {
    let s = signer(28);
    let mut sealed = Sealed::new(one_partition_cfg());
    (0..4).for_each(|n| sealed.submit(&s, n));
    sealed.refuse(s.address(), &[0]);
    sealed.reject(s.address(), &[1, 2, 3], 0);
    sealed.step();

    sealed.submit_with_deadline(&s, 2, 1_000);
    sealed.refuse(s.address(), &[2]);
    sealed.step();
    sealed.submit(&s, 0);
    assert_eq!(sealed.offered_nonces(4), vec![0, 1, 2, 3]);
    assert_eq!(
        sealed.rig.offers()[6].guard.max_inclusion_block,
        1_000,
        "nonce 2 publishes the resubmit"
    );
}
