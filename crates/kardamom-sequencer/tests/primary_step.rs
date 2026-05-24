//! Driver-level tests for `PrimarySequencer::run_once` + `run`.

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
use kardamom_sequencer::outbound::fakes::{InMemoryBPublisher, InMemoryReceiptCachePublisher};
use kardamom_sequencer::primary::{PrimarySequencer, Shutdown};

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
        ..Default::default()
    }
}

#[test]
fn match_publishes_once() {
    let s = signer(1);
    let env = signed_tx_envelope(&s, 0, 7);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(one_partition_cfg());

    assert!(seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
    assert_eq!(b.published.lock().unwrap().len(), 1);
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
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(one_partition_cfg());

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();

    assert_eq!(b.published.lock().unwrap().len(), 1);
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
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(one_partition_cfg());

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    assert_eq!(b.published.lock().unwrap().len(), 0);

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    assert_eq!(b.published.lock().unwrap().len(), 2);
}

#[test]
fn backpressure_rewinds_state_and_retry_succeeds() {
    let s = signer(4);
    let env = signed_tx_envelope(&s, 0, 100);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(env);
    let mut b = InMemoryBPublisher::default();
    *b.fail_with_backpressure.lock().unwrap() = true;
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(one_partition_cfg());

    let r = seq.run_once(&mut ingress, &mut b, &mut rc);
    assert!(matches!(
        r,
        Err(kardamom_sequencer::SequencerError::Backpressure)
    ));
    assert_eq!(b.published.lock().unwrap().len(), 0);

    // Recover B; run_once first drains the rebuffered nonce 0 (from the
    // pending buffer), then the next call processes the new ingress nonce 1.
    *b.fail_with_backpressure.lock().unwrap() = false;
    let env1 = signed_tx_envelope(&s, 1, 101);
    ingress.queue.push_back(env1);
    assert!(seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
    assert_eq!(
        b.published.lock().unwrap().len(),
        1,
        "drain-pending publishes 0"
    );
    assert!(seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
    assert_eq!(
        b.published.lock().unwrap().len(),
        2,
        "then ingress publishes 1"
    );
}

#[test]
fn run_once_returns_false_when_empty() {
    let mut ingress = ScriptedIngress::default();
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(one_partition_cfg());
    assert!(!seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
}

#[test]
fn wrong_partition_message_skipped() {
    let cfg = SequencerConfig {
        partition_count: 8,
        partition_index: 0,
        ..Default::default()
    };
    let mut seq = PrimarySequencer::new(cfg.clone());

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
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    assert!(seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
    assert_eq!(b.published.lock().unwrap().len(), 0);
}

#[test]
fn run_loops_until_shutdown_signaled() {
    let cfg = one_partition_cfg();
    let mut seq = PrimarySequencer::new(cfg);
    let mut ingress = ScriptedIngress::default();
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let shutdown = Shutdown::from_atomic(Arc::new(AtomicBool::new(true)));
    let result = seq.run(&mut ingress, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn run_returns_when_ingress_disconnected() {
    let cfg = one_partition_cfg();
    let mut seq = PrimarySequencer::new(cfg);
    let mut ingress = ScriptedIngress {
        disconnected: true,
        ..Default::default()
    };
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let shutdown = Shutdown::new();
    let result = seq.run(&mut ingress, &mut b, &mut rc, shutdown);
    assert!(result.is_ok(), "{result:?}");
}
