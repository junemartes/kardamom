//! Chaos test: primary fails mid-stream, standby is promoted. Asserts:
//!   - No nonce gap on B for the affected sender.
//!   - No duplicate (sender, nonce) on B.

use std::sync::{Arc, Mutex};

use alloy_consensus::transaction::Transaction;
use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_log::codec;
use kardamom_types::TxEnvelope;

use kardamom_sequencer::DuplicateNotification;
use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::{BMessage, BReplaySource, IngressSource};
use kardamom_sequencer::outbound::{BPublisher, ReceiptCachePublisher};
use kardamom_sequencer::primary::PrimarySequencer;
use kardamom_sequencer::standby::HotStandbyTailer;

#[derive(Default, Clone)]
struct SharedB {
    frames: Arc<Mutex<Vec<Vec<u8>>>>,
    decoded_for_replay: Arc<Mutex<Vec<BMessage>>>,
}

impl BPublisher for SharedB {
    fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError> {
        self.frames.lock().unwrap().push(frame_bytes.to_vec());
        // Decode the rkyv-archived TxEnvelope to project a BMessage::Tx so the
        // standby (which only sees BMessage) can replay it.
        let env: TxEnvelope = codec::materialize::<TxEnvelope>(frame_bytes)
            .map_err(|e| SequencerError::MalformedFrame(e.to_string()))?;
        let nonce = ConsensusEnvelope::decode(&mut env.raw_tx.as_ref())
            .map_err(|e| SequencerError::MalformedFrame(e.to_string()))?
            .nonce();
        self.decoded_for_replay.lock().unwrap().push(BMessage::Tx {
            sender: env.sender,
            nonce,
        });
        Ok(())
    }
}

#[derive(Clone)]
struct SharedBSubscription {
    inner: Arc<Mutex<Vec<BMessage>>>,
    cursor: Arc<Mutex<usize>>,
}

impl BReplaySource for SharedBSubscription {
    fn poll(&mut self) -> Result<Option<BMessage>, SequencerError> {
        let v = self.inner.lock().unwrap();
        let mut c = self.cursor.lock().unwrap();
        if *c >= v.len() {
            return Ok(None);
        }
        let m = v[*c].clone();
        *c += 1;
        Ok(Some(m))
    }
}

#[derive(Default, Clone)]
struct NullReceiptCache;

impl ReceiptCachePublisher for NullReceiptCache {
    fn publish_duplicate(&mut self, _: DuplicateNotification) {}
}

#[derive(Default)]
struct VecIngress {
    q: Vec<TxEnvelope>,
}

impl IngressSource for VecIngress {
    fn poll(&mut self) -> Result<Option<TxEnvelope>, SequencerError> {
        if self.q.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.q.remove(0)))
        }
    }
}

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_envelope(s: &PrivateKeySigner, n: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce: n,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut buf = Vec::with_capacity(256);
    alloy_env.encode(&mut buf);
    TxEnvelope {
        correlation_id: n,
        raw_tx: Bytes::from(buf),
        sender: s.address(),
        tx_hash: Default::default(),
    }
}

#[test]
fn standby_takeover_no_gap_no_duplicate() {
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        max_pending_per_sender: 8,
        role: SequencerRole::Primary,
        ..Default::default()
    };
    let mut primary = PrimarySequencer::new(cfg.clone());
    let standby_cfg = SequencerConfig {
        role: SequencerRole::Standby,
        ..cfg.clone()
    };
    let mut standby = HotStandbyTailer::new(standby_cfg);

    let signer1 = signer(1);
    let mut ingress_p = VecIngress::default();
    for n in 0u64..20 {
        ingress_p.q.push(signed_envelope(&signer1, n));
    }

    let shared_b = SharedB::default();
    let standby_sub = SharedBSubscription {
        inner: shared_b.decoded_for_replay.clone(),
        cursor: Arc::new(Mutex::new(0)),
    };
    let mut b_pub = shared_b.clone();
    let mut rc = NullReceiptCache;

    // Drive primary for 10 messages, then simulate a crash.
    for _ in 0..10 {
        primary
            .run_once(&mut ingress_p, &mut b_pub, &mut rc)
            .unwrap();
    }

    // Replay everything primary has published into the standby.
    let mut standby_src = standby_sub.clone();
    while standby.run_once(&mut standby_src).unwrap() {}
    assert_eq!(standby.next_nonce(signer1.address()), 10);

    // Promote standby: hand its state to a brand-new primary.
    let inherited = standby.into_state();
    let mut promoted = PrimarySequencer::with_state(cfg, inherited);

    // Remaining 10 ingress messages flow into the promoted primary.
    let mut ingress_promoted = VecIngress::default();
    for n in 10u64..20 {
        ingress_promoted.q.push(signed_envelope(&signer1, n));
    }
    while promoted
        .run_once(&mut ingress_promoted, &mut b_pub, &mut rc)
        .unwrap()
    {}

    // Decode every B frame and assert canonical order.
    let frames = shared_b.frames.lock().unwrap();
    let mut nonces: Vec<u64> = frames
        .iter()
        .map(|bytes| {
            let env: TxEnvelope = codec::materialize::<TxEnvelope>(bytes).unwrap();
            assert_eq!(env.sender, signer1.address());
            ConsensusEnvelope::decode(&mut env.raw_tx.as_ref())
                .unwrap()
                .nonce()
        })
        .collect();
    let unique: std::collections::HashSet<_> = nonces.iter().copied().collect();
    assert_eq!(unique.len(), nonces.len(), "duplicate publication on B");
    nonces.sort();
    assert_eq!(nonces, (0u64..20).collect::<Vec<_>>(), "gap on B");
}
