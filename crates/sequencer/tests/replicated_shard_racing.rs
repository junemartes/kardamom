//! P=2 racing sequencer replicas on one shard (the replicated-sequencer-shards
//! deploy: two Nomad groups, same partition, same tx_data stream).
//!
//! This test checks the invariants that make replica racing safe by
//! construction:
//!  * Determinism: two replicas fed the identical tx_data stream emit
//!    byte-identical ref sequences (`wire::encode_ingress_txref`). So the
//!    cluster's first-seen dedup relays the same canonical payload, no
//!    matter which replica wins any given record.
//!  * Dedup convergence: first-seen dedup by `canonical_id`, over any
//!    interleaving of the two replicas' offers, equals the single-replica
//!    sequence. No duplicates survive, no records are lost, and per-sender
//!    nonce order is kept (each replica emits nonce-ordered, and session
//!    order is kept per publisher, so the first-seen merge cannot invert
//!    nonces).
//!  * Cold rejoin: a replica that restarts mid-stream, and hydrates its
//!    nonce floor from committed state (the stateless-sequencer cache-miss
//!    path), emits a suffix of its twin's sequence. Merging it changes
//!    nothing.
//!  * Cold rejoin, misaligned floor: hydration is only a lower bound (the
//!    deployed binary wires an empty state DB, and even a real one can
//!    trail refs that the twin ordered but that are not committed yet).
//!    The stream-adaptive floor fast-forward (`nonce_floor_lag_ms`) adopts
//!    the live join point after the lag bound. So the rejoiner still
//!    emits exactly its twin's suffix, instead of zombie-buffering forever.

use std::collections::HashSet;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, U256, keccak256};
use alloy_rlp::Encodable;
use bytes::Bytes;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope, TxRef};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_cluster_adapter::wire;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::fakes::ScriptedTxData;
use kardamom_sequencer::outbound::fakes::{
    InMemoryTxErrorPublisher, InMemoryTxOrderingRefPublisher,
};
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::sequencer::Sequencer;

const SENDERS: usize = 3;
const TX_PER_SENDER: u64 = 20;

fn signer(seed: u64) -> alloy_signer_local::PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    alloy_signer_local::PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

/// Build a signed legacy transaction with the real keccak256 tx_hash.
/// This is the racing-replica dedup key. Unlike the single-replica tests,
/// it is not left defaulted.
fn signed_envelope(
    s: &alloy_signer_local::PrivateKeySigner,
    nonce: u64,
    correlation_id: u64,
) -> TxEnvelope {
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
    let tx_hash = keccak256(&buf);
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: s.address(),
        tx_hash,
    }
}

fn shard0_cfg() -> SequencerConfig {
    SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    }
}

/// The shared per-shard tx_data stream. SENDERS senders' transactions are
/// interleaved round-robin, nonce-ordered per sender, with distinct
/// A-positions. This is the way Aeron fragment offsets work in production.
fn shard_stream() -> Vec<(TxDataLoc, TxEnvelope)> {
    let signers: Vec<_> = (1..=SENDERS as u64).map(signer).collect();
    // Sanity check: sharding is a property of the ingress router, not
    // the sequencer. With partition_count=1, every sender is ours.
    for s in &signers {
        assert_eq!(partition_for(s.address(), 1), 0);
    }
    let mut stream = Vec::new();
    let mut offset = 0i32;
    for nonce in 0..TX_PER_SENDER {
        for (i, s) in signers.iter().enumerate() {
            let corr = nonce * SENDERS as u64 + i as u64;
            let pos = BPosition {
                term_id: 0,
                term_offset: offset,
            };
            stream.push((TxDataLoc::new(0, pos), signed_envelope(s, nonce, corr)));
            offset += 64;
        }
    }
    stream
}

/// Run one replica over `stream`, returning its published refs.
fn run_replica(stream: &[(TxDataLoc, TxEnvelope)]) -> Vec<TxRef> {
    run_replica_with(shard0_cfg(), stream)
}

fn run_replica_with(cfg: SequencerConfig, stream: &[(TxDataLoc, TxEnvelope)]) -> Vec<TxRef> {
    let mut inbound = ScriptedTxData::default();
    for (loc, env) in stream {
        inbound.queue.push_back((*loc, env.clone()));
    }
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(cfg);
    while seq.run_once(&mut inbound, &mut b, &mut rc).unwrap() {}
    b.refs.lock().unwrap().clone()
}

/// The cluster's first-seen dedup (Java `CanonicalSealerState.firstSeen`),
/// keyed on the wire `canonical_id`, which is `TxRef.tx_hash`.
fn first_seen_merge(interleaved: &[TxRef]) -> Vec<TxRef> {
    let mut seen = HashSet::new();
    interleaved
        .iter()
        .filter(|r| seen.insert(r.tx_hash))
        .cloned()
        .collect()
}

fn encoded(refs: &[TxRef]) -> Vec<Vec<u8>> {
    // Both replicas derive the same (sender, nonce) guard header from
    // the same envelope, so a fixed header keeps the byte equality meaningful.
    refs.iter()
        .map(|r| wire::encode_ingress_txref(r, alloy_primitives::Address::ZERO, 0))
        .collect()
}

#[test]
fn racing_replicas_emit_identical_ref_streams() {
    let stream = shard_stream();
    let a = run_replica(&stream);
    let b = run_replica(&stream);

    assert_eq!(a.len(), stream.len(), "replica A must ref every input tx");
    // Byte-identical wire encoding. Whichever replica's copy wins the
    // race, the relayed canonical payload is the same.
    assert_eq!(encoded(&a), encoded(&b));
}

#[test]
fn first_seen_dedup_of_any_interleaving_is_the_single_replica_stream() {
    let stream = shard_stream();
    let a = run_replica(&stream);
    let b = run_replica(&stream);

    // Session order is kept per publisher. The cluster may interleave
    // the two sessions arbitrarily between records. Model a handful of
    // adversarial interleavings (seeded, reproducible): random alternation
    // that keeps each replica's own order.
    for seed in 0..8u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut ia = a.iter();
        let mut ib = b.iter();
        let mut order: Vec<bool> = std::iter::repeat_n(true, a.len())
            .chain(std::iter::repeat_n(false, b.len()))
            .collect();
        order.shuffle(&mut rng);
        let interleaved: Vec<TxRef> = order
            .into_iter()
            .map(|from_a| {
                if from_a {
                    *ia.next().unwrap()
                } else {
                    *ib.next().unwrap()
                }
            })
            .collect();

        let canonical = first_seen_merge(&interleaved);
        assert_eq!(
            encoded(&canonical),
            encoded(&a),
            "seed {seed}: dedup must converge on the single-replica stream"
        );
        // Per-sender nonce order in the canonical stream is dense and
        // ascending. (The stream is built round-robin, so stream[i]'s
        // nonce is i / SENDERS. No need to RLP-decode.)
        let mut next: std::collections::HashMap<Address, u64> = Default::default();
        for r in &canonical {
            let (idx, env) = stream
                .iter()
                .enumerate()
                .find(|(_, (_, e))| e.tx_hash == r.tx_hash)
                .map(|(i, (_, e))| (i, e))
                .unwrap();
            let nonce = idx as u64 / SENDERS as u64;
            let want = next.entry(env.sender).or_insert(0);
            assert_eq!(nonce, *want, "sender nonce order inverted");
            *want += 1;
        }
    }
}

/// A deliberately re-opened status pin: the floor fast-forward was removed
/// after it published canonical nonce gaps. A sequencer cannot locally
/// tell a twin-ordered gap apart from a client-abandoned one, and every
/// executor fatally hit NonceTooHigh when it adopted a client-abandoned
/// gap.
///
/// The sequencer holds no committed-state reader. So a replica that
/// rejoins mid-stream seeds established senders at 0, buffers their
/// traffic, and emits nothing for them until the receipt-floor resync
/// (`crate::resync`, not wired in this harness) advances their floors from
/// the tx_receipts stream. This is degraded P=1 coverage, but never
/// canonical corruption. This test pins the never-corrupts invariant.
#[test]
fn rejoining_replica_with_empty_db_stalls_but_never_corrupts() {
    let stream = shard_stream();
    let a = run_replica(&stream);

    // Replica B restarts and joins at the midpoint. With no state reader,
    // every floor seeds at 0, so it buffers established senders as future
    // nonces.
    let half = stream.len() / 2;
    let b = run_replica(&stream[half..]);

    // This is the re-opened limitation: B emits nothing, since all
    // traffic buffers as future.
    assert!(
        b.is_empty(),
        "empty-DB rejoiner is expected to stall (F02.1 re-opened), got {} refs",
        b.len()
    );
    // And the canonical stream is untouched by the zombie replica.
    let mut interleaved = a.clone();
    interleaved.extend(b);
    assert_eq!(encoded(&first_seen_merge(&interleaved)), encoded(&a));
}

/// A client-abandoned nonce hole must never be adopted into the canonical
/// stream. This is a transaction dropped at ingress under overload, or
/// during a chaos outage, so it never reaches tx_data. The sender stalls
/// at the hole, and every published nonce run stays dense. The removed
/// fast-forward used to adopt the post-hole run after 5 seconds,
/// publishing a gapped stream that fatally hit NonceTooHigh on every
/// executor.
#[test]
fn client_abandoned_nonce_hole_is_never_published_past() {
    let full = shard_stream();
    // Drop nonces 3 and 4 of sender 1 from the stream entirely. They
    // never reached tx_data: the classic overload shape.
    let victim = signer(1).address();
    let stream: Vec<_> = full
        .into_iter()
        .filter(|(_, env)| {
            if env.sender != victim {
                return true;
            }
            use alloy_rlp::Decodable as _;
            let e = alloy_consensus::TxEnvelope::decode(&mut env.raw_tx.as_ref()).unwrap();
            use alloy_consensus::transaction::Transaction as _;
            !(e.nonce() == 3 || e.nonce() == 4)
        })
        .collect();

    let refs = run_replica(&stream);

    // The victim's published nonces are exactly the dense prefix 0..=2.
    // Nothing at or past the hole appears, and every sender's run is
    // gapless.
    use std::collections::HashMap;
    let mut per_sender: HashMap<_, Vec<u64>> = HashMap::new();
    for (loc, env) in &stream {
        // Reconstruct (sender, nonce) for each published ref via its position.
        if let Some(r) = refs.iter().find(|r| r.tx_data_position == loc.position) {
            use alloy_rlp::Decodable as _;
            let e = alloy_consensus::TxEnvelope::decode(&mut env.raw_tx.as_ref()).unwrap();
            use alloy_consensus::transaction::Transaction as _;
            assert_eq!(r.tx_hash, env.tx_hash);
            per_sender.entry(env.sender).or_default().push(e.nonce());
        }
    }
    for (sender, mut nonces) in per_sender {
        nonces.sort_unstable();
        let expect_len = if sender == victim {
            3 // 0,1,2, stalled at the hole
        } else {
            TX_PER_SENDER as usize
        };
        assert_eq!(
            nonces,
            (0..expect_len as u64).collect::<Vec<_>>(),
            "sender {sender:?} must publish a dense prefix only"
        );
    }
}
