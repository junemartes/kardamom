//! M=4 sequencers each subscribed to their own tx_data and racing to
//! publish refs onto a shared canonical tx_ordering (MDS topology —
//! /).
//!
//! Asserts the system-level invariants of the split:
//!  * Each ref on tx_ordering has `shard_id` matching the producing
//!    sequencer's shard (no cross-shard leakage).
//!  * The per-sender nonce sequence reconstructed from the canonical B
//!    arrival order is strictly ascending and dense.
//!  * The total ref count equals the total input count (no drops, no
//!    duplicate publishes in the in-memory backpressure-free harness).
//!
//! The in-memory `InMemoryTxOrderingRefPublisher` is `Clone` and routes
//! all handles to one shared `Vec<TxRef>` — modelling the "single
//! canonical B stream observed in arrival order" semantics Aeron's
//! concurrent-publisher provides on real channels.

use std::collections::HashMap;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedTxData;
use kardamom_sequencer::outbound::fakes::{
    InMemoryTxErrorPublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::sequencer::Sequencer;

const M: u32 = 4;
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
fn m_eq_4_sequencers_publish_canonical_refs() {
    // Build M sequencers, each subscribed to its own tx_data. They all
    // share one `InMemoryTxOrderingRefPublisher` (cloning shares the
    // underlying Vec — same canonical B stream).
    let b = InMemoryTxOrderingRefPublisher::default();
    let mut sequencers: Vec<Sequencer> = Vec::with_capacity(M as usize);
    let mut b_pubs: Vec<InMemoryTxOrderingRefPublisher> = Vec::with_capacity(M as usize);
    let mut rcs: Vec<InMemoryTxErrorPublisher> = Vec::with_capacity(M as usize);
    let mut channels_a: Vec<ScriptedTxData> = (0..M).map(|_| ScriptedTxData::default()).collect();
    // sender_at_pos[(shard, pos)] → (sender, nonce). Used to validate the
    // canonical-B refs resolve to expected envelopes.
    let mut sender_at_pos: HashMap<(u8, BPosition), (Address, u64)> = HashMap::new();

    for i in 0u32..M {
        let cfg = SequencerConfig {
            partition_count: M,
            partition_index: i,
            sequencer_id: i as u8,
            max_pending_per_sender: TX_PER_SENDER as usize * 2,
            ..Default::default()
        };
        sequencers.push(Sequencer::new(cfg));
        b_pubs.push(b.clone());
        rcs.push(InMemoryTxErrorPublisher::default());
    }

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
        for (offset, (si, n)) in stream.into_iter().enumerate() {
            // Each shard has its own A-position space. Use (shard_id, offset)
            // to derive a unique position per envelope.
            let position = BPosition {
                term_id: 0,
                term_offset: (offset as i32) * 64,
            };
            let env = signed_envelope(&all_signers_per_part[i as usize][si], n, correlation);
            sender_at_pos.insert(
                (i as u8, position),
                (all_signers_per_part[i as usize][si].address(), n),
            );
            channels_a[i as usize]
                .queue
                .push_back((TxDataLoc::new(0, position), env));
            correlation += 1;
        }
    }

    let total_input = (M as usize) * SENDERS_PER_PARTITION * TX_PER_SENDER as usize;
    assert_eq!(correlation as usize, total_input);

    // Drive every sequencer to drain in round-robin.
    loop {
        let mut any = false;
        for i in 0..M as usize {
            match sequencers[i].run_once(&mut channels_a[i], &mut b_pubs[i], &mut rcs[i]) {
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

    // Invariant: each sequencer wrote only refs with its own shard_id.
    let mut per_seq_refs: HashMap<u8, usize> = HashMap::new();
    for r in &refs {
        *per_seq_refs.entry(r.shard_id).or_default() += 1;
    }
    for i in 0u8..M as u8 {
        let n = per_seq_refs.get(&i).copied().unwrap_or(0);
        assert_eq!(
            n,
            SENDERS_PER_PARTITION * TX_PER_SENDER as usize,
            "sequencer {i} should own exactly its shard's worth of refs"
        );
    }

    // Walk the canonical B order, resolve every ref against the scripted
    // tx_data input by (shard, tx_data_position), and assert per-sender nonce
    // sequences are strictly ascending and dense from 0.
    let mut per_sender_nonces: HashMap<Address, Vec<u64>> = HashMap::new();
    for r in &refs {
        let (sender, nonce) = sender_at_pos
            .get(&(r.shard_id, r.tx_data_position))
            .copied()
            .expect("every TxRef must resolve to a scripted (shard, position) input");

        // The sequencer that owns this ref must agree with the shard router
        // on this sender — cross-shard routing correctness.
        assert_eq!(
            partition_for(sender, M),
            r.shard_id as u32,
            "sender's shard must match the referencing sequencer"
        );

        per_sender_nonces.entry(sender).or_default().push(nonce);
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
        let first = nonces.first().copied().unwrap();
        let last = nonces.last().copied().unwrap();
        assert_eq!(
            last - first + 1,
            nonces.len() as u64,
            "sender {s}: canonical nonce sequence not dense: {nonces:?}"
        );
    }

    // No tx_errors were emitted (in-order tx_data reads, no past nonces).
    for (i, rc) in rcs.iter().enumerate() {
        assert!(
            rc.errors.lock().unwrap().is_empty(),
            "sequencer {i} should not have emitted TxErrors"
        );
    }
}
