//! M=4 sequencers each dual-writing onto its own per-sequencer channel A
//! plus the shared canonical channel B (D-Sh12 / spec §2.3).
//!
//! Asserts the system-level invariants that motivate the split:
//!  * Every `TxRef` on channel B resolves to a unique `TxEnvelope` on the
//!    referenced sequencer's channel A (cross-A correctness).
//!  * The per-sender nonce sequence reconstructed from the canonical-B
//!    arrival order is strictly ascending and dense for every sender
//!    (canonical-ordering correctness across multiple writers).
//!  * Each sequencer only writes refs with `sequencer_id == self.id`
//!    (no cross-sequencer leakage; per-A isolation invariant).
//!  * The total ref count equals the total input count (no drops, no
//!    duplicates in the in-memory backpressure-free harness).
//!
//! The in-memory `InMemoryChannelBRefPublisher` is `Clone` and routes all
//! handles to one shared `Vec<TxRef>` — that gives us the same "single
//! canonical B stream observed in arrival order" semantics the real Aeron
//! concurrent-publisher provides. Real-Aeron concurrent-pub interleaving
//! is exercised in the `docker-e2e` test.

use std::collections::HashMap;

use alloy_consensus::transaction::Transaction;
use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_log::codec;
use kardamom_types::TxEnvelope;
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedIngress;
use kardamom_sequencer::outbound::fakes::{
    InMemoryChannelAPublisher, InMemoryChannelBRefPublisher, InMemoryReceiptCachePublisher,
};
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::primary::PrimarySequencer;

const M: u32 = 4;
// Per the task spec: each of the M=4 sequencers dual-writes 100 txs. We
// split that 100 across `SENDERS_PER_PARTITION` distinct senders so the
// per-sender nonce-sequence assertion is non-trivial; each sender
// contributes `TX_PER_SENDER` in-order nonces. 100 txs per sequencer ×
// M=4 sequencers = 400 canonical entries on B.
const SENDERS_PER_PARTITION: usize = 4;
const TX_PER_SENDER: u64 = 25;

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_envelope(s: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
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
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: s.address(),
        tx_hash: Default::default(),
    }
}

/// Find `n` signers whose addresses route to partition `target`.
fn find_signers_for_partition(target: u32, n: usize, seed_start: u64) -> Vec<PrivateKeySigner> {
    let mut out = Vec::with_capacity(n);
    let mut seed = seed_start;
    while out.len() < n {
        let s = signer(seed);
        if partition_for(s.address(), M) == target {
            out.push(s);
        }
        seed += 1;
    }
    out
}

#[test]
fn m_eq_4_sequencers_dual_write_round_trip() {
    // Build M sequencers, each with its own per-A publisher and one shared
    // channel-B ref publisher (clones share state by design — that's how
    // we model the single canonical stream).
    let b = InMemoryChannelBRefPublisher::default();
    let mut sequencers: Vec<PrimarySequencer> = Vec::with_capacity(M as usize);
    let mut a_pubs: Vec<InMemoryChannelAPublisher> = Vec::with_capacity(M as usize);
    let mut b_pubs: Vec<InMemoryChannelBRefPublisher> = Vec::with_capacity(M as usize);
    let mut rcs: Vec<InMemoryReceiptCachePublisher> = Vec::with_capacity(M as usize);
    let mut ingresses: Vec<ScriptedIngress> = (0..M).map(|_| ScriptedIngress::default()).collect();

    for i in 0u32..M {
        let cfg = SequencerConfig {
            partition_count: M,
            partition_index: i,
            sequencer_id: i as u8,
            // Generous buffer relative to per-sender shuffled depth
            // (TX_PER_SENDER nonces) so the future-buffer never overflows
            // under the shuffled ingress order.
            max_pending_per_sender: TX_PER_SENDER as usize * 2,
            ..Default::default()
        };
        sequencers.push(PrimarySequencer::new(cfg));
        a_pubs.push(InMemoryChannelAPublisher::new(i as u8));
        b_pubs.push(b.clone());
        rcs.push(InMemoryReceiptCachePublisher::default());
    }

    // For each partition, generate `SENDERS_PER_PARTITION` distinct
    // senders contributing `TX_PER_SENDER` in-order nonces each. Shuffle
    // the per-partition ingress order to exercise the future-nonce buffer.
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0FFEE);
    let mut all_signers_per_part: Vec<Vec<PrivateKeySigner>> = Vec::with_capacity(M as usize);
    let mut seed_cursor = 1u64;
    for i in 0u32..M {
        let signers = find_signers_for_partition(i, SENDERS_PER_PARTITION, seed_cursor);
        seed_cursor += 10_000;
        all_signers_per_part.push(signers);
    }

    let mut correlation: u64 = 0;
    for i in 0u32..M {
        let mut stream: Vec<(usize, u64)> = Vec::new();
        for si in 0..SENDERS_PER_PARTITION {
            for n in 0..TX_PER_SENDER {
                stream.push((si, n));
            }
        }
        stream.shuffle(&mut rng);
        for (si, n) in stream {
            let env = signed_envelope(&all_signers_per_part[i as usize][si], n, correlation);
            ingresses[i as usize].queue.push_back(env);
            correlation += 1;
        }
    }

    let total_input = (M as usize) * SENDERS_PER_PARTITION * TX_PER_SENDER as usize;
    assert_eq!(correlation as usize, total_input);

    // Drive every sequencer to drain in round-robin. The shared channel B
    // accumulates refs in actual call order — that's the canonical L2
    // order under the in-memory fake (real Aeron is the same; the
    // concurrent-pub CAS cursor serialises offers into a single byte
    // order).
    loop {
        let mut any = false;
        for i in 0..M as usize {
            match sequencers[i].run_once(
                &mut ingresses[i],
                &mut a_pubs[i],
                &mut b_pubs[i],
                &mut rcs[i],
            ) {
                Ok(true) => any = true,
                Ok(false) => {}
                Err(e) => panic!("sequencer {i}: unexpected error {e:?}"),
            }
        }
        if !any {
            break;
        }
    }

    // Cross-sequencer invariants on the shared B log.
    let refs = b.refs.lock().unwrap().clone();
    assert_eq!(
        refs.len(),
        total_input,
        "every accepted tx must produce exactly one TxRef on B"
    );

    // Invariant: each sequencer wrote only refs with its own sequencer_id.
    let mut per_seq_refs: HashMap<u8, usize> = HashMap::new();
    for r in &refs {
        *per_seq_refs.entry(r.sequencer_id).or_default() += 1;
    }
    for i in 0u8..M as u8 {
        let n = per_seq_refs.get(&i).copied().unwrap_or(0);
        assert_eq!(
            n,
            SENDERS_PER_PARTITION * TX_PER_SENDER as usize,
            "sequencer {i} should own exactly its slice's worth of refs"
        );
    }

    // Invariant: each per-A archive holds exactly the envelope count its
    // sequencer wrote refs for. (M parallel A-streams, no cross-leakage.)
    for (i, a) in a_pubs.iter().enumerate() {
        let n = a.published.lock().unwrap().len();
        assert_eq!(
            n,
            SENDERS_PER_PARTITION * TX_PER_SENDER as usize,
            "channel A[{i}] should hold its own slice's envelopes"
        );
    }

    // Walk the canonical B order, resolve every ref against the
    // appropriate channel A by sequencer_id, decode the nonce, and assert
    // per-sender nonce sequences are strictly ascending and dense from 0.
    let mut per_sender_nonces: HashMap<Address, Vec<u64>> = HashMap::new();
    for r in &refs {
        let a = &a_pubs[r.sequencer_id as usize];
        let bytes = a
            .fetch(r.position_a)
            .expect("every TxRef must resolve to an envelope on its channel A");
        let env: TxEnvelope =
            codec::materialize::<TxEnvelope>(&bytes).expect("decode TxEnvelope from A");

        // The sequencer that owns this ref must agree with the partition
        // router on this sender — cross-sequencer routing correctness.
        assert_eq!(
            partition_for(env.sender, M),
            r.sequencer_id as u32,
            "sender's partition must match the referencing sequencer"
        );

        let nonce = ConsensusEnvelope::decode(&mut env.raw_tx.as_ref())
            .expect("decode alloy env")
            .nonce();
        per_sender_nonces.entry(env.sender).or_default().push(nonce);
    }

    let expected_senders = (M as usize) * SENDERS_PER_PARTITION;
    assert_eq!(
        per_sender_nonces.len(),
        expected_senders,
        "every distinct sender must appear in the canonical log"
    );
    for (s, nonces) in &per_sender_nonces {
        assert_eq!(
            nonces.len(),
            TX_PER_SENDER as usize,
            "sender {s}: must contribute {TX_PER_SENDER} canonical entries"
        );
        let mut prev: Option<u64> = None;
        for n in nonces {
            if let Some(p) = prev {
                assert!(
                    *n > p,
                    "sender {s}: canonical nonces not strictly ascending: {nonces:?}"
                );
            } else {
                assert_eq!(*n, 0, "sender {s}: must start at nonce 0");
            }
            prev = Some(*n);
        }
        // Density: last - first + 1 must equal len (no gaps).
        let first = nonces.first().copied().unwrap();
        let last = nonces.last().copied().unwrap();
        assert_eq!(
            last - first + 1,
            nonces.len() as u64,
            "sender {s}: canonical nonce sequence not dense: {nonces:?}"
        );
    }

    // No duplicates were reported (in-order ingress, no past nonces).
    for (i, rc) in rcs.iter().enumerate() {
        assert!(
            rc.duplicates.lock().unwrap().is_empty(),
            "sequencer {i} should not have reported duplicates"
        );
    }
}
