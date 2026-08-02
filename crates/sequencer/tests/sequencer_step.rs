//! Driver-level tests for `Sequencer::run_once` + `run`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedTxData;
use kardamom_sequencer::outbound::fakes::{
    InMemoryTxErrorPublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_tx_envelope(
    signer: &PrivateKeySigner,
    nonce: u64,
    correlation_id: u64,
) -> kardamom_log::TxFrame {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let mut tx_mut = tx;
    let sig = signer.sign_transaction_sync(&mut tx_mut).unwrap();
    let alloy_env: ConsensusEnvelope = tx_mut.into_signed(sig).into();
    let mut buf = Vec::with_capacity(256);
    alloy_env.encode(&mut buf);
    kardamom_log::TxFrame::from_owned(&TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: signer.address(),
        tx_hash: Default::default(),
    })
    .expect("encode test envelope")
}

fn one_partition_cfg() -> SequencerConfig {
    SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    }
}

/// Monotonically synthesize a `BPosition` for the in-memory subscription so
/// every observed envelope has a distinct A-position (the way Aeron-archive
/// fragment offsets do in production).
fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

#[test]
fn match_publishes_ref() {
    let s = signer(1);
    let env = signed_tx_envelope(&s, 0, 7);
    let mut channel_a = ScriptedTxData::default();
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());

    assert!(seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].shard_id, 0);
    assert_eq!(refs[0].tx_data_position, pos(0));
    assert!(rc.errors.lock().unwrap().is_empty());
}

#[test]
fn past_nonce_emits_duplicate_notification() {
    let s = signer(2);
    let env0 = signed_tx_envelope(&s, 0, 100);
    let env0_dup = signed_tx_envelope(&s, 0, 200);
    let mut channel_a = ScriptedTxData::default();
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env0));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), env0_dup));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());

    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();

    assert_eq!(
        b.refs.lock().unwrap().len(),
        1,
        "only first nonce-0 published"
    );
    let errs = rc.errors.lock().unwrap();
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
    let mut channel_a = ScriptedTxData::default();
    // Out of order: nonce 1 first.
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env1));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), env0));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());

    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(b.refs.lock().unwrap().len(), 0, "nonce 1 buffered");

    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 2, "nonce 0 publishes + drains buffered nonce 1");
    // Both refs land in nonce order: nonce 0 at pos(64), nonce 1 at pos(0).
    assert_eq!(refs[0].tx_data_position, pos(64));
    assert_eq!(refs[1].tx_data_position, pos(0));
}

#[test]
fn b_backpressure_rewinds_state_and_retry_succeeds() {
    let s = signer(4);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut channel_a = ScriptedTxData::default();
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    *b.fail_with_backpressure.lock().unwrap() = true;
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());

    let r = seq.run_once(&mut channel_a, &mut b, &mut rc);
    assert!(matches!(
        r,
        Err(kardamom_sequencer::SequencerError::Backpressure)
    ));
    assert_eq!(b.refs.lock().unwrap().len(), 0, "B never accepted the ref");

    // Recover B; the next run_once drains the rebuffered nonce 0 onto B.
    *b.fail_with_backpressure.lock().unwrap() = false;
    assert!(seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1, "drain-pending republishes the rewound ref");
    assert_eq!(refs[0].tx_data_position, pos(0));
}

#[test]
fn run_once_returns_false_when_empty() {
    let mut channel_a = ScriptedTxData::default();
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());
    assert!(!seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
}

#[test]
fn wrong_shard_message_skipped() {
    let cfg = SequencerConfig {
        partition_count: 8,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    };
    let mut seq = Sequencer::new(cfg.clone());

    // Find a signer whose address routes to a shard != 0.
    let mut seed = 1u64;
    let env = loop {
        let s = signer(seed);
        let p = kardamom_sequencer::partition::partition_for(s.address(), cfg.partition_count);
        if p != cfg.partition_index {
            break signed_tx_envelope(&s, 0, 1);
        }
        seed += 1;
    };

    let mut channel_a = ScriptedTxData::default();
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    assert!(seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
    assert_eq!(
        b.refs.lock().unwrap().len(),
        0,
        "wrong-shard envelope ignored"
    );
}

#[test]
fn run_loops_until_shutdown_signaled() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(cfg);
    let mut channel_a = ScriptedTxData::default();
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let shutdown = Shutdown::from_atomic(Arc::new(AtomicBool::new(true)));
    let result = seq.run(&mut channel_a, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn run_returns_when_channel_a_disconnected() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(cfg);
    let mut channel_a = ScriptedTxData {
        disconnected: true,
        ..Default::default()
    };
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let shutdown = Shutdown::new();
    let result = seq.run(&mut channel_a, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}

// CI first-record audit: the DEGENERATE single-tx-after-idle case. The smoke
// test sends exactly ONE tx; if the retry of a backpressured ref were gated on
// fresh ingress (which never comes), that lone ref would wedge forever. The
// retry must be driven by run_once's drain-pending sweep alone, across
// MULTIPLE consecutive backpressured passes, and publish exactly once.
#[test]
fn single_tx_after_idle_survives_repeated_backpressure_without_new_ingress() {
    let s = signer(40);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut channel_a = ScriptedTxData::default();
    channel_a.queue.push_back((TxDataLoc::new(0, pos(0)), env));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    *b.fail_with_backpressure.lock().unwrap() = true;
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(one_partition_cfg());

    // Ingress pass hits backpressure; then several ingress-EMPTY passes keep
    // hitting backpressure — each must rewind and keep the ref pending.
    for _ in 0..4 {
        let r = seq.run_once(&mut channel_a, &mut b, &mut rc);
        assert!(
            matches!(r, Err(kardamom_sequencer::SequencerError::Backpressure)),
            "backpressured pass must surface Backpressure, got {r:?}"
        );
        assert!(b.refs.lock().unwrap().is_empty(), "nothing accepted yet");
    }

    // Publisher recovers: the ref publishes from drain-pending with NO new
    // ingress, exactly once.
    *b.fail_with_backpressure.lock().unwrap() = false;
    assert!(seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1, "the lone ref publishes exactly once");
    assert_eq!(refs[0].tx_data_position, pos(0));
    drop(refs);
    // And the machine is idle again (no duplicate retry).
    assert!(!seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap());
    assert_eq!(b.refs.lock().unwrap().len(), 1);
}
