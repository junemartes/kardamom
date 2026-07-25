//! Driver-level tests for the receipt-floor resync filter
//! (docs/agents/sequencer-lag-resync-spec.md): skips happen ONLY with
//! receipt proof and only in resync mode; everything unproven publishes
//! (sole-survivor safety); receipt floors unstick a cold-rejoined replica's
//! buffered run without ever publishing a canonical gap.

use std::sync::Arc;

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
use kardamom_sequencer::resync::{FloorUpdate, ResyncConfig, resync_channel};
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testing::FakeStateDatabase;

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_tx_envelope(signer: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
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
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: signer.address(),
        tx_hash: Default::default(),
    }
}

fn one_partition_cfg() -> SequencerConfig {
    SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    }
}

fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

/// A sequencer with resync enabled; returns the floor-update sender. The
/// controller starts in resync mode (startup trigger), which is exactly the
/// state these tests exercise.
fn resync_sequencer() -> (
    Sequencer<FakeStateDatabase>,
    std::sync::mpsc::Sender<FloorUpdate>,
) {
    let mut seq = Sequencer::new(one_partition_cfg(), Arc::new(FakeStateDatabase::new()));
    let (controller, floor_tx, _watermark) = resync_channel(ResyncConfig::default(), 0);
    seq.enable_resync(controller);
    (seq, floor_tx)
}

#[test]
fn receipt_proven_nonce_is_skipped_unproven_published() {
    let s = signer(1);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 0, 10)));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 1, 11)));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, floor_tx) = resync_sequencer();

    // A receipt for nonce 0 exists (twin covered it) → floor 1. The floor
    // advances the state machine BEFORE the stale envelope is processed, so
    // the skip happens via the ordinary Past/DuplicatedTx path — never a
    // publish of a proven-executed nonce.
    floor_tx
        .send(FloorUpdate {
            sender: s.address(),
            executed_nonce: 0,
        })
        .unwrap();

    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();

    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1, "nonce 0 skipped (proven), nonce 1 published");
    assert_eq!(refs[0].tx_data_position, pos(64));
    let errs = rc.errors.lock().unwrap();
    assert_eq!(
        errs.len(),
        1,
        "the skip surfaces as DuplicatedTx to ingress"
    );
    assert_eq!(errs[0].nonce, 0);
    assert!(matches!(
        errs[0].reason,
        kardamom_sequencer::TxErrorReason::DuplicatedTx { expected_nonce: 1 }
    ));
}

#[test]
fn sole_survivor_publishes_everything() {
    // Twin dead ⇒ no receipts ⇒ no floors: resync mode must publish the
    // FULL backlog — no accepted tx is ever dropped on inference.
    let s = signer(2);
    let mut channel_a = ScriptedTxData::default();
    for n in 0..3u64 {
        channel_a.queue.push_back((
            TxDataLoc::new(0, pos(64 * n as i32)),
            signed_tx_envelope(&s, n, n),
        ));
    }
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, _floor_tx) = resync_sequencer();

    for _ in 0..3 {
        seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    }
    assert_eq!(b.refs.lock().unwrap().len(), 3);
}

#[test]
fn receipt_floor_unsticks_cold_rejoin_buffer() {
    // F02.1 partial closure: a cold-restarted replica (floors hydrate at 0)
    // sees only live traffic at nonces 5,6 — twin ordered 0..=4 before the
    // restart, so the buffered run can never become contiguous from 0. A
    // receipt for nonce 4 advances the floor to 5 and the run drains.
    let s = signer(3);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 5, 50)));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 6, 51)));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, floor_tx) = resync_sequencer();

    // Both envelopes buffer as future (expected = 0, cold hydration).
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert!(b.refs.lock().unwrap().is_empty(), "stuck behind the gap");

    // Execution evidence arrives: nonce 4 receipted → floor 5.
    floor_tx
        .send(FloorUpdate {
            sender: s.address(),
            executed_nonce: 4,
        })
        .unwrap();

    // Next iteration: floor advances the state machine, the buffered run
    // 5,6 becomes contiguous and publishes. Floor 5 does NOT prove 5/6
    // executed, so the resync filter lets them through.
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 2, "buffered run drained after floor advance");
}
