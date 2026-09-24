//! Driver-level tests for the receipt-floor resync filter. A skip happens
//! only with receipt proof, and only in resync mode. Everything unproven
//! publishes (sole-survivor safety). Receipt floors unstick a
//! cold-rejoined replica's buffered run, without ever publishing a
//! canonical gap.

use kardamom_types::TxDataLoc;

use kardamom_sequencer::resync::{FloorUpdate, ResyncChannel, ResyncConfig};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{
    Rig, one_partition_cfg, pos, signed_envelope as signed_tx_envelope, signer,
};

/// A sequencer with resync enabled. Returns the floor-update sender.
/// The controller starts in resync mode (the startup trigger), which is
/// exactly the state these tests exercise.
type ResyncTestRig = (
    Sequencer,
    crossbeam_channel::Sender<FloorUpdate>,
    crossbeam_channel::Sender<(alloy_primitives::Address, u64, u64)>,
);

fn resync_sequencer_with_rejects() -> ResyncTestRig {
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();
    let ResyncChannel {
        controller,
        floor_tx,
        reject_tx,
        ..
    } = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    seq.enable_resync(controller);
    (seq, floor_tx, reject_tx)
}

fn resync_sequencer() -> (Sequencer, crossbeam_channel::Sender<FloorUpdate>) {
    let (seq, floor_tx, _reject_tx) = resync_sequencer_with_rejects();
    (seq, floor_tx)
}

#[test]
fn receipt_proven_nonce_is_skipped_unproven_published() {
    let s = signer(1);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 1, 10));
    rig.push(TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 2, 11));
    let (mut seq, floor_tx) = resync_sequencer();

    // A receipt for nonce 1 exists (the twin covered it), so the floor
    // advances to 2. (Nonce-0 receipts are never evidence, since they
    // cannot be told apart from deposits, so this scenario starts at
    // nonce 1.) The floor advances the state machine before the stale
    // envelope is processed, so the envelope lands on the `Past` path,
    // but as a receipt-proven skip: no publish, and no DuplicatedTx
    // notice. The transaction executed, so reporting it as a duplicate
    // to ingress would be spurious.
    floor_tx
        .send(FloorUpdate::executed(s.address(), 1))
        .unwrap();

    rig.step(&mut seq).unwrap();
    rig.step(&mut seq).unwrap();

    let refs = rig.refs();
    assert_eq!(refs.len(), 1, "nonce 1 skipped (proven), nonce 2 published");
    assert_eq!(refs[0].tx_data_position, pos(64));
    assert!(
        rig.errors().is_empty(),
        "a receipt-proven skip is not a client error"
    );
}

#[test]
fn sole_survivor_publishes_everything() {
    // The twin is dead, so there are no receipts and no floors. Resync
    // mode must publish the full backlog. No accepted transaction is ever
    // dropped on inference.
    let s = signer(2);
    let mut rig = Rig::default();
    for n in 0..3u64 {
        rig.push(
            TxDataLoc::new(0, pos(64 * i32::try_from(n).unwrap())),
            signed_tx_envelope(&s, n, n),
        );
    }
    let (mut seq, _floor_tx) = resync_sequencer();

    for _ in 0..3 {
        rig.step(&mut seq).unwrap();
    }
    assert_eq!(rig.refs().len(), 3);
}

#[test]
fn receipt_floor_unsticks_cold_rejoin_buffer() {
    // A cold-restarted replica (floors hydrate at 0) sees only live
    // traffic at nonces 5,6. The twin ordered 0..=4 before the restart, so
    // the buffered run can never become contiguous from 0. A receipt for
    // nonce 4 advances the floor to 5, and the run drains.
    let s = signer(3);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 5, 50));
    rig.push(TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 6, 51));
    let (mut seq, floor_tx) = resync_sequencer();

    // Both envelopes buffer as future (expected = 0, cold hydration).
    rig.step(&mut seq).unwrap();
    rig.step(&mut seq).unwrap();
    assert!(rig.refs().is_empty(), "stuck behind the gap");

    // Execution evidence arrives. Nonce 4 is receipted, so the floor becomes 5.
    floor_tx
        .send(FloorUpdate::executed(s.address(), 4))
        .unwrap();

    // On the next iteration, the floor advances the state machine. The
    // buffered run 5,6 becomes contiguous and publishes. Floor 5 does not
    // prove that 5 and 6 executed, so the resync filter lets them through.
    rig.step(&mut seq).unwrap();
    let refs = rig.refs();
    assert_eq!(refs.len(), 2, "buffered run drained after floor advance");
}

/// An `Accepted` offer is not a commit. Published refs stay in the
/// unconfirmed ledger, and are rewound and republished when no receipt
/// confirms them within the timeout. A receipt at or above the nonce
/// (cumulative per sender) retires them permanently.
#[test]
fn unconfirmed_refs_republish_until_receipt_confirms() {
    let s = signer(4);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 0, 40));
    rig.push(TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 1, 41));
    let (mut seq, floor_tx) = resync_sequencer();

    // Publish both refs at the default 15 second timeout. No republish churn.
    rig.step(&mut seq).unwrap();
    rig.step(&mut seq).unwrap();
    assert_eq!(rig.refs().len(), 2);

    // Confirm nonce 1. This is cumulative per sender, and retires both
    // (0 and 1).
    floor_tx
        .send(FloorUpdate::executed(s.address(), 1))
        .unwrap();
    rig.step(&mut seq).unwrap();

    // With timeout 0, anything still unconfirmed would republish now.
    seq.set_confirm_timeout_ms(0);
    rig.step(&mut seq).unwrap();
    assert_eq!(rig.refs().len(), 2, "confirmed refs must never re-publish");

    // A third, never-confirmed ref. With timeout 0, every iteration
    // rewinds and republishes it. This is the offer-is-not-commit
    // guarantee made recoverable.
    rig.push(TxDataLoc::new(0, pos(128)), signed_tx_envelope(&s, 2, 42));
    rig.step(&mut seq).unwrap(); // publish #3
    rig.step(&mut seq).unwrap(); // republish #3
    let n = rig.refs().len();
    assert!(n >= 4, "unconfirmed ref must re-publish (got {n})");

    // Confirming it stops the churn.
    floor_tx
        .send(FloorUpdate::executed(s.address(), 2))
        .unwrap();
    rig.step(&mut seq).unwrap();
    let stable = rig.refs().len();
    rig.step(&mut seq).unwrap();
    assert_eq!(rig.refs().len(), stable);
}

/// A contiguity reject with a nonce below expected proves the ref
/// already committed (the sealer's per-sender expected nonce is past it).
/// The unconfirmed entry is dropped, like a receipt confirmation. Without
/// the drop, a ref with no confirming receipt (nonce-0 receipts cannot be
/// told apart from deposits, so they never confirm) would republish on
/// every confirm timeout forever, once its dedup entry ages out.
#[test]
fn committed_proof_reject_retires_unconfirmed_entry() {
    let s = signer(5);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 0, 60));
    let (mut seq, _floor_tx, reject_tx) = resync_sequencer_with_rejects();

    // Publish the sender's only transaction (nonce 0). No receipt will
    // ever confirm it, since nonce-0 receipts are excluded. So with
    // timeout 0 it republishes on every iteration: the infinite loop
    // this fix closes.
    rig.step(&mut seq).unwrap();
    assert_eq!(rig.refs().len(), 1);
    seq.set_confirm_timeout_ms(0);
    rig.step(&mut seq).unwrap();
    assert!(rig.refs().len() >= 2, "unconfirmed nonce-0 churns");

    // The sealer answers a republish with a committed-proof reject
    // (nonce 0 below expected 1). The entry retires permanently.
    reject_tx.send((s.address(), 0, 1)).unwrap();
    rig.step(&mut seq).unwrap();
    let stable = rig.refs().len();
    rig.step(&mut seq).unwrap();
    assert_eq!(
        rig.refs().len(),
        stable,
        "committed-proof reject must stop the republish loop"
    );
}
