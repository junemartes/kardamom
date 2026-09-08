//! Driver-level tests for `Sequencer::run_once` and `run`.

use kardamom_types::TxDataLoc;
use std::num::NonZeroU32;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::partition::PartitionCount;
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_sequencer::testkit::{
    Rig, one_partition_cfg, pos, signed_envelope as signed_tx_envelope, signer,
};

#[test]
fn match_publishes_ref() {
    let s = signer(1);
    let env = signed_tx_envelope(&s, 0, 7);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), env);
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();

    assert!(rig.step(&mut seq).unwrap());
    let refs = rig.refs();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].shard_id, 0);
    assert_eq!(refs[0].tx_data_position, pos(0));
    assert!(rig.errors().is_empty());
}

#[test]
fn past_nonce_emits_duplicate_notification() {
    let s = signer(2);
    let env0 = signed_tx_envelope(&s, 0, 100);
    let env0_dup = signed_tx_envelope(&s, 0, 200);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), env0);
    rig.push(TxDataLoc::new(0, pos(64)), env0_dup);
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();

    rig.step(&mut seq).unwrap();
    rig.step(&mut seq).unwrap();

    assert_eq!(rig.refs().len(), 1, "only first nonce-0 published");
    let errs = rig.errors();
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].nonce, 0);
    assert_eq!(errs[0].sender, s.address());
    assert!(matches!(
        errs[0].reason,
        kardamom_sequencer::TxErrorReason::DuplicatedTx { expected_nonce: 1 }
    ));
}

#[test]
fn future_nonce_buffered_then_drained() {
    let s = signer(3);
    let env0 = signed_tx_envelope(&s, 0, 100);
    let env1 = signed_tx_envelope(&s, 1, 101);
    let mut rig = Rig::default();
    // Out of order: nonce 1 first.
    rig.push(TxDataLoc::new(0, pos(0)), env1);
    rig.push(TxDataLoc::new(0, pos(64)), env0);
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();

    rig.step(&mut seq).unwrap();
    assert_eq!(rig.refs().len(), 0, "nonce 1 buffered");

    rig.step(&mut seq).unwrap();
    let refs = rig.refs();
    assert_eq!(refs.len(), 2, "nonce 0 publishes + drains buffered nonce 1");
    // Both refs land in nonce order: nonce 0 at pos(64), nonce 1 at pos(0).
    assert_eq!(refs[0].tx_data_position, pos(64));
    assert_eq!(refs[1].tx_data_position, pos(0));
}

#[test]
fn b_backpressure_rewinds_state_and_retry_succeeds() {
    let s = signer(4);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), env);
    *rig.refs.fail_with_backpressure.lock().unwrap() = true;
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();

    let r = rig.step(&mut seq);
    assert!(matches!(
        r,
        Err(kardamom_sequencer::SequencerError::Backpressure)
    ));
    assert_eq!(rig.refs().len(), 0, "B never accepted the ref");

    // Recover B. The next run_once drains the rebuffered nonce 0 onto B.
    *rig.refs.fail_with_backpressure.lock().unwrap() = false;
    assert!(rig.step(&mut seq).unwrap());
    let refs = rig.refs();
    assert_eq!(refs.len(), 1, "drain-pending republishes the rewound ref");
    assert_eq!(refs[0].tx_data_position, pos(0));
}

#[test]
fn run_once_returns_false_when_empty() {
    let mut rig = Rig::default();
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();
    assert!(!rig.step(&mut seq).unwrap());
}

/// `seed`'s signer's envelope, kept only if its address routes to a
/// shard other than `cfg.partition_index`. For `wrong_shard_message_skipped`.
fn other_shard_envelope(cfg: &SequencerConfig, seed: u64) -> Option<kardamom_types::TxEnvelope> {
    let s = signer(seed);
    let routed = cfg.partition_count.index_of(s.address());
    (routed != cfg.partition_index).then(|| signed_tx_envelope(&s, 0, 1))
}

#[test]
fn wrong_shard_message_skipped() {
    let cfg = SequencerConfig {
        partition_count: PartitionCount::new(NonZeroU32::new(8).unwrap()),
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    };
    let mut seq = Sequencer::new(cfg.clone()).unwrap();

    // Find a signer whose address routes to a shard != 0.
    let env = (1u64..)
        .find_map(|seed| other_shard_envelope(&cfg, seed))
        .expect("some seed routes to a shard other than 0");

    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), env);
    assert!(rig.step(&mut seq).unwrap());
    assert_eq!(rig.refs().len(), 0, "wrong-shard envelope ignored");
}

#[test]
fn run_loops_until_shutdown_signaled() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(cfg).unwrap();
    let mut rig = Rig::default();
    let shutdown = Shutdown::new();
    shutdown.signal();
    let result = seq.run(&mut rig.ports(), &shutdown);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn run_returns_when_channel_a_disconnected() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(cfg).unwrap();
    let mut rig = Rig::default();
    rig.tx_data.disconnected = true;
    let shutdown = Shutdown::new();
    let result = seq.run(&mut rig.ports(), &shutdown);
    assert!(result.is_ok(), "{result:?}");
}

// CI first-record audit: the degenerate single-transaction-after-idle
// case. The smoke test sends exactly one transaction. If the retry of a
// backpressured ref were gated on fresh ingress, which never comes, that
// lone ref would wedge forever. The retry must come from run_once's
// drain-pending sweep alone, across multiple consecutive backpressured
// passes, and publish exactly once.
#[test]
fn single_tx_after_idle_survives_repeated_backpressure_without_new_ingress() {
    let s = signer(40);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut rig = Rig::default();
    rig.push(TxDataLoc::new(0, pos(0)), env);
    *rig.refs.fail_with_backpressure.lock().unwrap() = true;
    let mut seq = Sequencer::new(one_partition_cfg()).unwrap();

    // The ingress pass hits backpressure. Then several ingress-empty
    // passes keep hitting backpressure. Each must rewind and keep the
    // ref pending.
    for _ in 0..4 {
        let r = rig.step(&mut seq);
        assert!(
            matches!(r, Err(kardamom_sequencer::SequencerError::Backpressure)),
            "backpressured pass must surface Backpressure, got {r:?}"
        );
        assert!(rig.refs().is_empty(), "nothing accepted yet");
    }

    // The publisher recovers. The ref publishes from drain-pending, with
    // no new ingress, exactly once.
    *rig.refs.fail_with_backpressure.lock().unwrap() = false;
    assert!(rig.step(&mut seq).unwrap());
    let refs = rig.refs();
    assert_eq!(refs.len(), 1, "the lone ref publishes exactly once");
    assert_eq!(refs[0].tx_data_position, pos(0));
    // And the machine is idle again (no duplicate retry).
    assert!(!rig.step(&mut seq).unwrap());
    assert_eq!(rig.refs().len(), 1);
}
