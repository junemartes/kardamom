//! Driver-level tests for the receipt-floor resync filter
//! (docs/agents/sequencer-lag-resync-spec.md): skips happen ONLY with
//! receipt proof and only in resync mode; everything unproven publishes
//! (sole-survivor safety); receipt floors unstick a cold-rejoined replica's
//! buffered run without ever publishing a canonical gap.

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

fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

/// A sequencer with resync enabled; returns the floor-update sender. The
/// controller starts in resync mode (startup trigger), which is exactly the
/// state these tests exercise.
type ResyncTestRig = (
    Sequencer,
    std::sync::mpsc::Sender<FloorUpdate>,
    std::sync::mpsc::Sender<(alloy_primitives::Address, u64, u64)>,
);

fn resync_sequencer_with_rejects() -> ResyncTestRig {
    let mut seq = Sequencer::new(one_partition_cfg());
    let (controller, floor_tx, reject_tx, _watermark) = resync_channel(ResyncConfig::default(), 0);
    seq.enable_resync(controller);
    (seq, floor_tx, reject_tx)
}

fn resync_sequencer() -> (Sequencer, std::sync::mpsc::Sender<FloorUpdate>) {
    let (seq, floor_tx, _reject_tx) = resync_sequencer_with_rejects();
    (seq, floor_tx)
}

#[test]
fn receipt_proven_nonce_is_skipped_unproven_published() {
    let s = signer(1);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 1, 10)));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 2, 11)));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, floor_tx) = resync_sequencer();

    // A receipt for nonce 1 exists (twin covered it) → floor 2. (Nonce-0
    // receipts are never evidence — deposit-indistinguishable — so the
    // scenario starts at nonce 1.) The floor advances the state machine
    // BEFORE the stale envelope is processed, so the envelope lands on the
    // `Past` path — but as a receipt-PROVEN skip: no publish, and NO
    // DuplicatedTx notice (the tx executed; reporting it as a duplicate to
    // ingress would be spurious — and growing dropped_past here broke the
    // load harness's seq_clean verdict).
    floor_tx
        .send(FloorUpdate {
            sender: s.address(),
            executed_nonce: 1,
            invalid_skip: false,
        })
        .unwrap();

    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();

    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1, "nonce 1 skipped (proven), nonce 2 published");
    assert_eq!(refs[0].tx_data_position, pos(64));
    assert!(
        rc.errors.lock().unwrap().is_empty(),
        "a receipt-proven skip is not a client error"
    );
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
            invalid_skip: false,
        })
        .unwrap();

    // Next iteration: floor advances the state machine, the buffered run
    // 5,6 becomes contiguous and publishes. Floor 5 does NOT prove 5/6
    // executed, so the resync filter lets them through.
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 2, "buffered run drained after floor advance");
}

/// #85: an `Accepted` offer is NOT a commit — published refs stay in the
/// unconfirmed ledger and are rewound + re-published when no receipt
/// confirms them within the timeout; a receipt at/above the nonce
/// (cumulative per sender) retires them permanently.
#[test]
fn unconfirmed_refs_republish_until_receipt_confirms() {
    let s = signer(4);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 0, 40)));
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(64)), signed_tx_envelope(&s, 1, 41)));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, floor_tx) = resync_sequencer();

    // Publish both refs at the default (15s) timeout — no republish churn.
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(b.refs.lock().unwrap().len(), 2);

    // Confirm nonce 1: cumulative per sender, retires BOTH (0 and 1).
    floor_tx
        .send(FloorUpdate {
            sender: s.address(),
            executed_nonce: 1,
            invalid_skip: false,
        })
        .unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();

    // Timeout 0: were anything still unconfirmed it would republish NOW.
    seq.set_confirm_timeout_ms(0);
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(
        b.refs.lock().unwrap().len(),
        2,
        "confirmed refs must never re-publish"
    );

    // A third, never-confirmed ref: with timeout 0 every iteration rewinds
    // and re-publishes it — offer-is-not-commit made recoverable.
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(128)), signed_tx_envelope(&s, 2, 42)));
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap(); // publish #3
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap(); // republish #3
    let n = b.refs.lock().unwrap().len();
    assert!(n >= 4, "unconfirmed ref must re-publish (got {n})");

    // Confirming it stops the churn.
    floor_tx
        .send(FloorUpdate {
            sender: s.address(),
            executed_nonce: 2,
            invalid_skip: false,
        })
        .unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    let stable = b.refs.lock().unwrap().len();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(b.refs.lock().unwrap().len(), stable);
}

/// #85 fix B: a contiguity reject with `nonce < expected` proves the ref
/// already committed (the sealer's per-sender expected nonce is past it) —
/// the unconfirmed entry is dropped like a receipt confirmation. Without the
/// drop, a ref with no confirming receipt (nonce-0: deposit-indistinguishable
/// receipts never confirm) republishes every confirm-timeout FOREVER once
/// its dedup entry ages out (observed live: smoke-gate accounts).
#[test]
fn committed_proof_reject_retires_unconfirmed_entry() {
    let s = signer(5);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((TxDataLoc::new(0, pos(0)), signed_tx_envelope(&s, 0, 60)));
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let (mut seq, _floor_tx, reject_tx) = resync_sequencer_with_rejects();

    // Publish the sender's ONLY tx (nonce 0). No receipt will ever confirm
    // it (nonce-0 receipts are excluded), so with timeout 0 it republishes
    // on every iteration — the infinite loop this fix closes.
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(b.refs.lock().unwrap().len(), 1);
    seq.set_confirm_timeout_ms(0);
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert!(
        b.refs.lock().unwrap().len() >= 2,
        "unconfirmed nonce-0 churns"
    );

    // The sealer answers a republish with a committed-proof reject
    // (nonce 0 < expected 1): the entry retires permanently.
    reject_tx.send((s.address(), 0, 1)).unwrap();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    let stable = b.refs.lock().unwrap().len();
    seq.run_once(&mut channel_a, &mut b, &mut rc).unwrap();
    assert_eq!(
        b.refs.lock().unwrap().len(),
        stable,
        "committed-proof reject must stop the republish loop"
    );
}
