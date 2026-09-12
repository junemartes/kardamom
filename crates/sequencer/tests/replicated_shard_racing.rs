//! P=2 racing sequencer replicas on one shard (the replicated-sequencer-shards
//! deploy: two Nomad groups, same partition, same `tx_data` stream).
//!
//! This test checks the invariants that make replica racing safe by
//! construction:
//!  * Determinism: two replicas fed the identical `tx_data` stream emit
//!    byte-identical ref sequences (`wire::encode_ingress_txref`). So the
//!    cluster's first-seen dedup relays the same canonical payload, no
//!    matter which replica wins any given record.
//!  * Dedup convergence: first-seen dedup by `canonical_id`, over any
//!    interleaving of the two replicas' offers, equals the single-replica
//!    sequence. No duplicates survive, no records are lost, and per-sender
//!    nonce order is kept (each replica emits nonce-ordered, and session
//!    order is kept per publisher, so the first-seen merge cannot invert
//!    nonces).
//!  * Cold rejoin: a replica that restarts mid-stream holds no
//!    committed-state reader, so it seeds established senders at 0 and
//!    buffers their traffic. It stalls (degraded P=1 coverage) until the
//!    receipt-floor resync advances their floors, and never corrupts the
//!    canonical stream in the meantime.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

use alloy_consensus::transaction::Transaction as _;
use alloy_primitives::Address;
use alloy_rlp::Decodable as _;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope, TxRef};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_cluster_adapter::wire;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::partition::PartitionCount;
use kardamom_sequencer::testkit::{
    EnvelopeSpec, drive_to_idle, envelope_with, one_partition_cfg, signer,
};

const SENDERS: usize = 3;
const TX_PER_SENDER: u64 = 20;

/// Build a signed legacy transaction with the real keccak256 `tx_hash`.
/// This is the racing-replica dedup key. Unlike the single-replica tests,
/// it is not left defaulted.
fn signed_envelope(s: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    envelope_with(
        s,
        nonce,
        correlation_id,
        EnvelopeSpec {
            real_hash: true,
            ..Default::default()
        },
    )
}

/// The shared per-shard `tx_data` stream. SENDERS senders' transactions are
/// interleaved round-robin, nonce-ordered per sender, with distinct
/// A-positions. This is the way Aeron fragment offsets work in production.
fn shard_stream() -> Vec<(TxDataLoc, TxEnvelope)> {
    let signers: Vec<_> = (1..=SENDERS as u64).map(signer).collect();
    // Sanity check: sharding is a property of the ingress router, not
    // the sequencer. With partition_count=1, every sender is ours.
    for s in &signers {
        assert_eq!(
            PartitionCount::new(NonZeroU32::new(1).unwrap()).index_of(s.address()),
            0
        );
    }
    // The cartesian product of (nonce, sender-index), in the same
    // row-major order the original nested loop walked: `offset` there
    // was just `64 * corr`, since it incremented once per (nonce, i)
    // pair starting at 0.
    (0..TX_PER_SENDER)
        .flat_map(|nonce| (0..signers.len()).map(move |i| (nonce, i)))
        .map(|(nonce, i)| {
            let corr = nonce * SENDERS as u64 + i as u64;
            let pos = BPosition {
                term_id: 0,
                term_offset: i32::try_from(corr * 64).unwrap(),
            };
            (
                TxDataLoc::new(0, pos),
                signed_envelope(&signers[i], nonce, corr),
            )
        })
        .collect()
}

/// Run one replica over `stream`, returning its published refs.
fn run_replica(stream: &[(TxDataLoc, TxEnvelope)]) -> Vec<TxRef> {
    run_replica_with(one_partition_cfg(), stream)
}

fn run_replica_with(cfg: SequencerConfig, stream: &[(TxDataLoc, TxEnvelope)]) -> Vec<TxRef> {
    drive_to_idle(cfg, stream).0
}

/// The cluster's first-seen dedup (Java `CanonicalSealerState.firstSeen`),
/// keyed on the wire `canonical_id`, which is `TxRef.tx_hash`.
fn first_seen_merge(interleaved: &[TxRef]) -> Vec<TxRef> {
    let mut seen = HashSet::new();
    interleaved
        .iter()
        .filter(|r| seen.insert(r.tx_hash))
        .copied()
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
        check_interleaving(seed, &a, &b, &stream);
    }
}

/// One [`first_seen_dedup_of_any_interleaving_is_the_single_replica_stream`]
/// seed: shuffle `a` and `b` (keeping each replica's own session order),
/// merge by first-seen `canonical_id`, and check the result converges on
/// the single-replica stream with dense, ascending per-sender nonces.
fn check_interleaving(seed: u64, a: &[TxRef], b: &[TxRef], stream: &[(TxDataLoc, TxEnvelope)]) {
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
        encoded(a),
        "seed {seed}: dedup must converge on the single-replica stream"
    );
    // Per-sender nonce order in the canonical stream is dense and
    // ascending. (The stream is built round-robin, so stream[i]'s
    // nonce is i / SENDERS. No need to RLP-decode.)
    let mut next: std::collections::HashMap<Address, u64> = std::collections::HashMap::default();
    for r in &canonical {
        assert_dense_ascending_nonce(stream, &mut next, r);
    }
}

/// One canonical ref's nonce must be the next dense, ascending nonce for
/// its sender. (The stream is built round-robin, so `stream[i]`'s nonce
/// is `i / SENDERS`. No need to RLP-decode.)
fn assert_dense_ascending_nonce(
    stream: &[(TxDataLoc, TxEnvelope)],
    next: &mut std::collections::HashMap<Address, u64>,
    r: &TxRef,
) {
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

/// A deliberately re-opened status pin: the floor fast-forward was removed
/// after it published canonical nonce gaps. A sequencer cannot locally
/// tell a twin-ordered gap apart from a client-abandoned one, and every
/// executor fatally hit `NonceTooHigh` when it adopted a client-abandoned
/// gap.
///
/// The sequencer holds no committed-state reader. So a replica that
/// rejoins mid-stream seeds established senders at 0, buffers their
/// traffic, and emits nothing for them until the receipt-floor resync
/// (`crate::resync`, not wired in this harness) advances their floors from
/// the `tx_receipts` stream. This is degraded P=1 coverage, but never
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
/// during a chaos outage, so it never reaches `tx_data`. The sender stalls
/// at the hole, and every published nonce run stays dense: nothing past
/// the hole is ever published.
/// Reconstruct `(sender, nonce)` for `loc`/`env`'s published ref, if it
/// published, and record it in `per_sender`. For
/// `client_abandoned_nonce_hole_is_never_published_past`'s loop.
fn record_sender_nonce(
    refs: &[TxRef],
    loc: &TxDataLoc,
    env: &TxEnvelope,
    per_sender: &mut HashMap<Address, Vec<u64>>,
) {
    let Some(r) = refs.iter().find(|r| r.tx_data_position == loc.position) else {
        return;
    };
    let e = alloy_consensus::TxEnvelope::decode(&mut env.raw_tx.as_ref()).unwrap();
    assert_eq!(r.tx_hash, env.tx_hash);
    per_sender.entry(env.sender).or_default().push(e.nonce());
}

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
            let e = alloy_consensus::TxEnvelope::decode(&mut env.raw_tx.as_ref()).unwrap();
            !(e.nonce() == 3 || e.nonce() == 4)
        })
        .collect();

    let refs = run_replica(&stream);

    // The victim's published nonces are exactly the dense prefix 0..=2.
    // Nothing at or past the hole appears, and every sender's run is
    // gapless.
    let mut per_sender: HashMap<_, Vec<u64>> = HashMap::new();
    for (loc, env) in &stream {
        record_sender_nonce(&refs, loc, env, &mut per_sender);
    }
    for (sender, nonces) in per_sender {
        assert_dense_prefix(sender, nonces, victim);
    }
}

/// `sender`'s published nonces must be the dense prefix `0..expect_len`:
/// 3 (stalled at the hole) for `victim`, the full run otherwise. For
/// `client_abandoned_nonce_hole_is_never_published_past`'s loop.
fn assert_dense_prefix(sender: Address, mut nonces: Vec<u64>, victim: Address) {
    nonces.sort_unstable();
    let expect_len = if sender == victim {
        3 // 0,1,2, stalled at the hole
    } else {
        usize::try_from(TX_PER_SENDER).unwrap()
    };
    assert_eq!(
        nonces,
        (0..expect_len as u64).collect::<Vec<_>>(),
        "sender {sender:?} must publish a dense prefix only"
    );
}
