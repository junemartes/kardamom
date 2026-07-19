//! P=2 racing sequencer replicas on ONE shard (the replicated-sequencer-shards
//! deploy: two Nomad groups, same partition, same tx_data stream).
//!
//! Asserts the invariants that make replica racing safe by construction:
//!  * **Determinism** — two replicas fed the identical tx_data stream emit
//!    byte-identical ref sequences (`wire::encode_ingress_txref`), so the
//!    cluster's first-seen dedup relays the same canonical payload no matter
//!    which replica wins any given record.
//!  * **Dedup convergence** — first-seen dedup by `canonical_id` over ANY
//!    interleaving of the two replicas' offers equals the single-replica
//!    sequence: no duplicates survive, no records are lost, and per-sender
//!    nonce order is preserved (each replica emits nonce-ordered, session
//!    order is preserved per publisher, so the first-seen merge cannot invert
//!    nonces).
//!  * **Cold rejoin** — a replica that (re)starts mid-stream and hydrates its
//!    nonce floor from committed state (the stateless-sequencer cache-miss
//!    path) emits a suffix of its twin's sequence; merging it changes nothing.
//!  * **Cold rejoin, misaligned floor** — hydration is only a *lower bound*
//!    (the deployed binary wires an empty state DB, and even a real one can
//!    trail refs the twin ordered but that aren't committed yet). The
//!    stream-adaptive floor fast-forward (`nonce_floor_lag_ms`) adopts the
//!    live join point after the lag bound, so the rejoiner still emits
//!    exactly its twin's suffix instead of zombie-buffering forever.

use std::collections::HashSet;
use std::sync::Arc;

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
use kardamom_sequencer::testing::FakeStateDatabase;

const SENDERS: usize = 3;
const TX_PER_SENDER: u64 = 20;

fn signer(seed: u64) -> alloy_signer_local::PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    alloy_signer_local::PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

/// Signed legacy tx with the REAL keccak256 tx_hash (the racing-replica dedup
/// key), unlike the single-replica tests that leave it defaulted.
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

/// The shared per-shard tx_data stream: SENDERS senders' txs interleaved
/// round-robin, nonce-ordered per sender, with distinct A-positions (the way
/// Aeron fragment offsets are in production).
fn shard_stream() -> Vec<(TxDataLoc, TxEnvelope)> {
    let signers: Vec<_> = (1..=SENDERS as u64).map(signer).collect();
    // Sanity: sharding is a property of the ingress router, not the
    // sequencer; with partition_count=1 every sender is ours.
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
fn run_replica(stream: &[(TxDataLoc, TxEnvelope)], db: Arc<FakeStateDatabase>) -> Vec<TxRef> {
    run_replica_with(shard0_cfg(), stream, db)
}

fn run_replica_with(
    cfg: SequencerConfig,
    stream: &[(TxDataLoc, TxEnvelope)],
    db: Arc<FakeStateDatabase>,
) -> Vec<TxRef> {
    let mut inbound = ScriptedTxData::default();
    for (loc, env) in stream {
        inbound.queue.push_back((*loc, env.clone()));
    }
    let mut b = InMemoryTxOrderingRefPublisher::default();
    let mut rc = InMemoryTxErrorPublisher::default();
    let mut seq = Sequencer::new(cfg, db);
    while seq.run_once(&mut inbound, &mut b, &mut rc).unwrap() {}
    b.refs.lock().unwrap().clone()
}

/// The cluster's first-seen dedup (Java `CanonicalSealerState.firstSeen`,
/// keyed on the wire `canonical_id` = `TxRef.tx_hash`).
fn first_seen_merge(interleaved: &[TxRef]) -> Vec<TxRef> {
    let mut seen = HashSet::new();
    interleaved
        .iter()
        .filter(|r| seen.insert(r.tx_hash))
        .cloned()
        .collect()
}

fn encoded(refs: &[TxRef]) -> Vec<Vec<u8>> {
    refs.iter().map(wire::encode_ingress_txref).collect()
}

#[test]
fn racing_replicas_emit_identical_ref_streams() {
    let stream = shard_stream();
    let a = run_replica(&stream, Arc::new(FakeStateDatabase::new()));
    let b = run_replica(&stream, Arc::new(FakeStateDatabase::new()));

    assert_eq!(a.len(), stream.len(), "replica A must ref every input tx");
    // Byte-identical wire encoding: whichever replica's copy wins the race,
    // the relayed canonical payload is the same.
    assert_eq!(encoded(&a), encoded(&b));
}

#[test]
fn first_seen_dedup_of_any_interleaving_is_the_single_replica_stream() {
    let stream = shard_stream();
    let a = run_replica(&stream, Arc::new(FakeStateDatabase::new()));
    let b = run_replica(&stream, Arc::new(FakeStateDatabase::new()));

    // Session order is preserved per publisher; the cluster may interleave
    // the two sessions arbitrarily BETWEEN records. Model a handful of
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
        // Per-sender nonce order in the canonical stream is dense-ascending.
        // (Stream construction is round-robin, so stream[i]'s nonce is
        // i / SENDERS — no need to RLP-decode.)
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

#[test]
fn cold_rejoining_replica_emits_a_suffix_and_changes_nothing() {
    let stream = shard_stream();
    let a = run_replica(&stream, Arc::new(FakeStateDatabase::new()));

    // Replica B (re)starts after the first half was already ordered and
    // committed: it joins the live stream at the midpoint and hydrates each
    // sender's nonce floor from committed state (cache-miss path).
    let half = stream.len() / 2; // round-robin ⇒ every sender at TX_PER_SENDER/2
    let db = FakeStateDatabase::new();
    for s in (1..=SENDERS as u64).map(signer) {
        db.set_nonce(s.address(), TX_PER_SENDER / 2);
    }
    let b = run_replica(&stream[half..], Arc::new(db));

    // B's output is exactly A's suffix…
    assert_eq!(encoded(&b), encoded(&a[half..]));
    // …so merging it into the canonical stream (A first — its records were
    // ordered while B was down) adds nothing.
    let mut interleaved = a.clone();
    interleaved.extend(b);
    assert_eq!(encoded(&first_seen_merge(&interleaved)), encoded(&a));
}

/// F02.1 status pin (RE-OPENED, deliberate): the floor fast-forward was
/// REMOVED after it published canonical nonce GAPS (CI run 29687514869: all
/// three executors fatally NonceTooHigh'd in every shard — a sequencer
/// cannot locally tell a twin-ordered gap from a client-abandoned one). A
/// replica that rejoins mid-stream with an EMPTY state DB therefore buffers
/// established senders and emits NOTHING for them — degraded P=1 coverage,
/// but never canonical corruption. This test pins BOTH properties so a
/// future fix must consciously flip the first assertion, and can never
/// regress the second.
#[test]
fn rejoining_replica_with_empty_db_stalls_but_never_corrupts() {
    let stream = shard_stream();
    let a = run_replica(&stream, Arc::new(FakeStateDatabase::new()));

    // Replica B restarts and joins at the midpoint with an empty state DB
    // (production wiring: every floor hydrates at 0).
    let half = stream.len() / 2;
    let b = run_replica(&stream[half..], Arc::new(FakeStateDatabase::new()));

    // Re-opened limitation: B emits nothing (all traffic buffers as future).
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

/// CI round-4 regression (the executor-killing bug): a CLIENT-ABANDONED
/// nonce hole — txs dropped at ingress under overload / a chaos outage, so
/// they never reach tx_data — must NEVER be adopted into the canonical
/// stream. The sender stalls at the hole; every published nonce run stays
/// dense. (The removed fast-forward adopted the post-hole run after 5s,
/// publishing a gapped stream that fatally NonceTooHigh'd every executor.)
#[test]
fn client_abandoned_nonce_hole_is_never_published_past() {
    let full = shard_stream();
    // Drop nonces 3 and 4 of sender 1 from the stream entirely (they never
    // reached tx_data): the classic overload shape.
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

    let refs = run_replica(&stream, Arc::new(FakeStateDatabase::new()));

    // The victim's published nonces are exactly the dense prefix 0..=2 —
    // nothing at or past the hole — and every sender's run is gapless.
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
            3 // 0,1,2 — stalled at the hole
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
