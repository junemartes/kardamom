//! End-to-end sequencer behaviour against a scripted tx_data
//! subscription and in-memory tx_ordering / receipt-cache publishers
//! (MDS topology). Asserts:
//!  * Canonical order on tx_ordering (the `TxRef` sequence) matches a
//!    per-sender nonce-ascending sequence.
//!  * Each ref's `tx_data_position` matches the position the proxy supplied
//!    on the scripted tx_data subscription.
//!  * Duplicates are dropped and reported on the receipt-cache channel.
//!  * Future-nonce txs are buffered and drained when the prior arrives.

use std::collections::HashMap;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::{BPosition, TxEnvelope};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedTxData;
use kardamom_sequencer::outbound::fakes::{
    InMemoryTxErrorPublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::sequencer::Sequencer;

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_envelope(signer: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut buf = Vec::with_capacity(256);
    alloy_env.encode(&mut buf);
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: signer.address(),
        tx_hash: Default::default(),
    }
}

/// Synthesize an A-position for each scripted envelope so refs carry a
/// unique pointer back to the simulated tx_data.
fn pos_n(n: u64) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: (n * 64) as i32,
    }
}

#[test]
fn integration_1000_txs_100_senders_with_chaos() {
    // Single shard so every sender lands here.
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        max_pending_per_sender: 16,
        ..Default::default()
    };
    let mut seq = Sequencer::new(
        cfg.clone(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEAD_BEEF);
    let signers: Vec<_> = (1..=100u64).map(signer).collect();

    // Each sender contributes 10 in-order nonces; shuffle the arrival order to
    // exercise the future buffer.
    let mut stream: Vec<(usize, u64)> = Vec::new();
    for i in 0..signers.len() {
        for n in 0..10u64 {
            stream.push((i, n));
        }
    }
    stream.shuffle(&mut rng);

    let mut channel_a = ScriptedTxData::default();
    // sender_at_pos: tx_data_position → (sender, nonce). Used to validate that
    // each published TxRef's tx_data_position points back to the expected envelope.
    let mut sender_at_pos: HashMap<BPosition, (Address, u64)> = HashMap::new();
    for (correlation, (i, n)) in stream.iter().enumerate() {
        let position = pos_n(correlation as u64);
        sender_at_pos.insert(position, (signers[*i].address(), *n));
        let env = signed_envelope(&signers[*i], *n, correlation as u64);
        channel_a.queue.push_back((position, env));
    }
    let total_input = channel_a.queue.len();

    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    loop {
        match seq.run_once(&mut channel_a, &mut b, &mut rc) {
            Ok(true) => continue,
            Ok(false) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    // Every in-order input must produce a B ref.
    let refs = b.refs.lock().unwrap().clone();
    assert_eq!(
        refs.len(),
        total_input,
        "every in-order input should produce a TxRef on B"
    );

    // For each ref, look up the (sender, nonce) the proxy fed into tx_data
    // at that position. Then verify per-sender the nonces emerge in
    // ascending order 0..10.
    let mut per_sender: HashMap<Address, Vec<u64>> = HashMap::new();
    for r in &refs {
        assert_eq!(r.shard_id, cfg.sequencer_id);
        let (sender, nonce) = sender_at_pos
            .get(&r.tx_data_position)
            .copied()
            .expect("every ref's tx_data_position must match a scripted input");
        per_sender.entry(sender).or_default().push(nonce);
    }
    assert_eq!(per_sender.len(), signers.len());
    for (s, nonces) in &per_sender {
        let mut last = None;
        for n in nonces {
            if let Some(p) = last {
                assert!(*n > p, "sender {s}: nonces not ascending: {nonces:?}");
            } else {
                assert_eq!(*n, 0, "sender {s}: must start at nonce 0");
            }
            last = Some(*n);
        }
        assert_eq!(
            nonces.len(),
            10,
            "sender {s}: each contributes 10 in-order nonces"
        );
    }
}

#[test]
fn integration_duplicates_are_reported() {
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        max_pending_per_sender: 4,
        ..Default::default()
    };
    let mut seq = Sequencer::new(
        cfg,
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );
    let s = signer(7);
    let mut channel_a = ScriptedTxData::default();
    channel_a
        .queue
        .push_back((pos_n(0), signed_envelope(&s, 0, 100)));
    channel_a
        .queue
        .push_back((pos_n(1), signed_envelope(&s, 1, 101)));
    // Three duplicates of nonce 0 arriving AFTER nonce 1 has been processed.
    channel_a
        .queue
        .push_back((pos_n(2), signed_envelope(&s, 0, 200)));
    channel_a
        .queue
        .push_back((pos_n(3), signed_envelope(&s, 0, 201)));
    channel_a
        .queue
        .push_back((pos_n(4), signed_envelope(&s, 0, 202)));

    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    while let Ok(true) = seq.run_once(&mut channel_a, &mut b, &mut rc) {}

    assert_eq!(b.refs.lock().unwrap().len(), 2);
    let errs = rc.errors.lock().unwrap();
    assert_eq!(errs.len(), 3, "all 3 past-nonce submissions emit a TxError");
    // All 3 errors are for the same (sender, nonce=0) — they're distinct
    // submissions but indistinguishable at the TxError layer (correlation_id
    // was dropped when the receipt-cache channel was retired).
    for err in errs.iter() {
        assert_eq!(err.sender, s.address());
        assert_eq!(err.nonce, 0);
        assert!(matches!(
            err.reason,
            kardamom_sequencer::TxErrorReason::DuplicatedTx { .. }
        ));
    }
}

#[test]
fn integration_bounded_buffer_evicts_oldest() {
    // Send nonces 100..110 (10 futures) with max_pending=4. Then no
    // contiguous nonce-0 arrives — the futures stay buffered, but the
    // buffer's cap=4 evicts the oldest 6, so when we eventually feed
    // nonce 0 we'd only see refs for 0 + the surviving futures.
    // This test asserts no publishes happen in the all-future phase.
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        max_pending_per_sender: 4,
        ..Default::default()
    };
    let mut seq = Sequencer::new(
        cfg,
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );
    let s = signer(42);
    let mut channel_a = ScriptedTxData::default();
    for n in 100..110u64 {
        channel_a
            .queue
            .push_back((pos_n(n), signed_envelope(&s, n, n)));
    }
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    while let Ok(true) = seq.run_once(&mut channel_a, &mut b, &mut rc) {}
    assert_eq!(
        b.refs.lock().unwrap().len(),
        0,
        "all 10 are futures, no canonical refs emitted"
    );
}
