//! Chaos test: primary fails mid-stream, standby is promoted. Asserts:
//!   - No nonce gap on B for the affected sender.
//!   - No duplicate (sender, nonce) on B.
//!
//! In the D-Sh12 split architecture (spec §2.3) the sequencer dual-writes:
//! full `TxEnvelope`s to channel A and tiny `TxRef`s to channel B. The
//! standby tails B and replays each ref into its nonce map by looking up
//! the underlying envelope on the appropriate channel A. This test wires
//! the in-memory A+B fakes to a small adapter that does exactly that
//! lookup before handing a `BMessage` to the standby.

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
use kardamom_sequencer::outbound::ReceiptCachePublisher;
use kardamom_sequencer::outbound::fakes::{
    InMemoryChannelAPublisher, InMemoryChannelBRefPublisher,
};
use kardamom_sequencer::primary::PrimarySequencer;
use kardamom_sequencer::standby::HotStandbyTailer;

/// Adapter: tail the in-memory channel-B ref log and, for each ref, fetch
/// the envelope from the matching channel A in-memory fake, decode the
/// nonce, and yield a `BMessage::Tx`. Mirrors what a real standby will do
/// once it owns a real `ChannelBSubscriber` + per-A `ChannelASubscriber`
/// handles.
#[derive(Clone)]
struct ChannelBRefReplay {
    refs: Arc<Mutex<Vec<kardamom_types::TxRef>>>,
    a_publishers: Vec<InMemoryChannelAPublisher>,
    cursor: Arc<Mutex<usize>>,
}

impl BReplaySource for ChannelBRefReplay {
    fn poll(&mut self) -> Result<Option<BMessage>, SequencerError> {
        let v = self.refs.lock().unwrap();
        let mut c = self.cursor.lock().unwrap();
        if *c >= v.len() {
            return Ok(None);
        }
        let r = v[*c];
        *c += 1;
        let a = self
            .a_publishers
            .iter()
            .find(|p| p.sequencer_id == r.sequencer_id)
            .ok_or_else(|| {
                SequencerError::MalformedFrame(format!(
                    "TxRef sequencer_id {} not in known A set",
                    r.sequencer_id
                ))
            })?;
        let bytes = a.fetch(r.position_a).ok_or_else(|| {
            SequencerError::MalformedFrame(format!(
                "channel A[{}] missing entry at {:?}",
                r.sequencer_id, r.position_a
            ))
        })?;
        let env: TxEnvelope = codec::materialize::<TxEnvelope>(&bytes)
            .map_err(|e| SequencerError::MalformedFrame(e.to_string()))?;
        let nonce = ConsensusEnvelope::decode(&mut env.raw_tx.as_ref())
            .map_err(|e| SequencerError::MalformedFrame(e.to_string()))?
            .nonce();
        Ok(Some(BMessage::Tx {
            sender: env.sender,
            nonce,
        }))
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
        sequencer_id: 0,
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

    let mut a_pub = InMemoryChannelAPublisher::new(cfg.sequencer_id);
    let mut b_pub = InMemoryChannelBRefPublisher::default();
    let standby_src = ChannelBRefReplay {
        refs: b_pub.refs.clone(),
        a_publishers: vec![a_pub.clone()],
        cursor: Arc::new(Mutex::new(0)),
    };
    let mut rc = NullReceiptCache;

    // Drive primary for 10 messages, then simulate a crash.
    for _ in 0..10 {
        primary
            .run_once(&mut ingress_p, &mut a_pub, &mut b_pub, &mut rc)
            .unwrap();
    }

    // Replay everything primary has published into the standby.
    let mut standby_src_replay = standby_src.clone();
    while standby.run_once(&mut standby_src_replay).unwrap() {}
    assert_eq!(standby.next_nonce(signer1.address()), 10);

    // Promote standby: hand its state to a brand-new primary.
    let inherited = standby.into_state();
    let mut promoted = PrimarySequencer::with_state(cfg.clone(), inherited);

    // Remaining 10 ingress messages flow into the promoted primary. The
    // promoted primary keeps publishing onto the same channel A + B
    // (single-sequencer cluster in this test).
    let mut ingress_promoted = VecIngress::default();
    for n in 10u64..20 {
        ingress_promoted.q.push(signed_envelope(&signer1, n));
    }
    while promoted
        .run_once(&mut ingress_promoted, &mut a_pub, &mut b_pub, &mut rc)
        .unwrap()
    {}

    // Walk the canonical B order, resolve each ref against channel A, and
    // assert exactly-once + dense ordering for sender 1.
    let refs = b_pub.refs.lock().unwrap().clone();
    let mut nonces: Vec<u64> = refs
        .iter()
        .map(|r| {
            assert_eq!(r.sequencer_id, cfg.sequencer_id);
            let bytes = a_pub.fetch(r.position_a).expect("A lookup must succeed");
            let env: TxEnvelope = codec::materialize::<TxEnvelope>(&bytes).unwrap();
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
