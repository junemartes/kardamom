//! Driver-level tests for `Sequencer::run_once` + `run`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::TxEnvelope;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedIngress;
use kardamom_sequencer::outbound::fakes::{
    InMemoryChannelAPublisher, InMemoryChannelBRefPublisher, InMemoryReceiptCachePublisher,
};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};

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

#[test]
fn match_dual_writes_once() {
    let s = signer(1);
    let env = signed_tx_envelope(&s, 0, 7);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    assert!(seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
    assert_eq!(a.published.lock().unwrap().len(), 1);
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].shard_id, 0);
    assert!(rc.duplicates.lock().unwrap().is_empty());
}

#[test]
fn past_nonce_emits_duplicate_notification() {
    let s = signer(2);
    let env0 = signed_tx_envelope(&s, 0, 100);
    let env0_dup = signed_tx_envelope(&s, 0, 200);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env0);
    ingress.queue.push_back(env0_dup);
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap();
    seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap();

    assert_eq!(a.published.lock().unwrap().len(), 1);
    assert_eq!(b.refs.lock().unwrap().len(), 1);
    let dups = rc.duplicates.lock().unwrap();
    assert_eq!(dups.len(), 1);
    assert_eq!(dups[0].correlation_id, 200);
    assert_eq!(dups[0].nonce, 0);
    assert_eq!(dups[0].sender, s.address());
}

#[test]
fn future_nonce_buffered_then_drained() {
    let s = signer(3);
    let env0 = signed_tx_envelope(&s, 0, 100);
    let env1 = signed_tx_envelope(&s, 1, 101);
    let mut ingress = ScriptedIngress::default();
    // Out of order: 1 first.
    ingress.queue.push_back(env1);
    ingress.queue.push_back(env0);
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap();
    assert_eq!(a.published.lock().unwrap().len(), 0);
    assert_eq!(b.refs.lock().unwrap().len(), 0);

    seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap();
    assert_eq!(a.published.lock().unwrap().len(), 2);
    assert_eq!(b.refs.lock().unwrap().len(), 2);
}

#[test]
fn a_backpressure_rewinds_state_and_retry_succeeds() {
    let s = signer(4);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut a = InMemoryChannelAPublisher::new(0);
    *a.fail_with_backpressure.lock().unwrap() = true;
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    let r = seq.run_once(&mut ingress, &mut a, &mut b, &mut rc);
    assert!(matches!(
        r,
        Err(kardamom_sequencer::SequencerError::Backpressure)
    ));
    assert_eq!(a.published.lock().unwrap().len(), 0);
    assert_eq!(b.refs.lock().unwrap().len(), 0);

    // Recover A; the next run_once drains the rebuffered nonce 0 onto both
    // channels, then the call after processes the fresh ingress nonce 1.
    *a.fail_with_backpressure.lock().unwrap() = false;
    let env1 = signed_tx_envelope(&s, 1, 101);
    ingress.queue.push_back(env1);
    assert!(seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
    assert_eq!(
        a.published.lock().unwrap().len(),
        1,
        "drain-pending publishes 0 to A"
    );
    assert_eq!(
        b.refs.lock().unwrap().len(),
        1,
        "drain-pending publishes 0's ref to B"
    );
    assert!(seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
    assert_eq!(a.published.lock().unwrap().len(), 2);
    assert_eq!(b.refs.lock().unwrap().len(), 2);
}

#[test]
fn b_backpressure_after_a_publish_orphans_a_and_rewinds() {
    // Documented behaviour: when channel B back-pressures after channel A
    // has already accepted the envelope, the A entry is an orphan (no ref
    // will ever point at it). The state machine still rewinds and the
    // retry publishes a *fresh* A entry + matching B ref.
    let s = signer(5);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    *b.fail_with_backpressure.lock().unwrap() = true;
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    let r = seq.run_once(&mut ingress, &mut a, &mut b, &mut rc);
    assert!(matches!(
        r,
        Err(kardamom_sequencer::SequencerError::Backpressure)
    ));
    assert_eq!(
        a.published.lock().unwrap().len(),
        1,
        "first attempt left an orphan envelope on A"
    );
    assert_eq!(b.refs.lock().unwrap().len(), 0, "B never got the ref");

    // Recover B; the retry publishes a *new* A entry plus a matching B ref.
    *b.fail_with_backpressure.lock().unwrap() = false;
    assert!(seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
    assert_eq!(
        a.published.lock().unwrap().len(),
        2,
        "retry adds a fresh A entry (the first one is the orphan)"
    );
    let refs = b.refs.lock().unwrap();
    assert_eq!(refs.len(), 1, "B ref now points at the retry's A entry");
    // The orphan and the live entry are byte-identical (it's the same
    // envelope, retried). What distinguishes them is the *position* on A:
    // the orphan sits at offset 0, the retry at offset orphan.len(). The
    // ref must point at the retry's position (non-zero offset).
    let orphan_len = a.published.lock().unwrap()[0].len() as i32;
    assert_ne!(
        refs[0].position_a.term_offset, 0,
        "ref must point past the orphan, not at it"
    );
    assert_eq!(
        refs[0].position_a.term_offset, orphan_len,
        "ref position must be exactly after the orphan entry"
    );
    // And the resolved bytes must materialize to the same envelope the
    // sequencer accepted.
    let fetched = a.fetch(refs[0].position_a).expect("ref resolves");
    assert_eq!(fetched.len(), orphan_len as usize);
}

#[test]
fn run_once_returns_false_when_empty() {
    let mut ingress = ScriptedIngress::default();
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = Sequencer::new(
        one_partition_cfg(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );
    assert!(!seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
}

#[test]
fn wrong_partition_message_skipped() {
    let cfg = SequencerConfig {
        partition_count: 8,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    };
    let mut seq = Sequencer::new(
        cfg.clone(),
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );

    // Find a signer whose address routes to a partition != 0.
    let mut seed = 1u64;
    let env = loop {
        let s = signer(seed);
        let p = kardamom_sequencer::partition::partition_for(s.address(), cfg.partition_count);
        if p != cfg.partition_index {
            break signed_tx_envelope(&s, 0, 1);
        }
        seed += 1;
    };

    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    assert!(seq.run_once(&mut ingress, &mut a, &mut b, &mut rc).unwrap());
    assert_eq!(a.published.lock().unwrap().len(), 0);
    assert_eq!(b.refs.lock().unwrap().len(), 0);
}

#[test]
fn run_loops_until_shutdown_signaled() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(
        cfg,
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );
    let mut ingress = ScriptedIngress::default();
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let shutdown = Shutdown::from_atomic(Arc::new(AtomicBool::new(true)));
    let result = seq.run(&mut ingress, &mut a, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn run_returns_when_ingress_disconnected() {
    let cfg = one_partition_cfg();
    let mut seq = Sequencer::new(
        cfg,
        std::sync::Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new()),
    );
    let mut ingress = ScriptedIngress {
        disconnected: true,
        ..Default::default()
    };
    let mut a = InMemoryChannelAPublisher::new(0);
    let mut b = InMemoryChannelBRefPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let shutdown = Shutdown::new();
    let result = seq.run(&mut ingress, &mut a, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}
