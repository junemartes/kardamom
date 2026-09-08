//! M=4 sequencers, each subscribed to its own `tx_data` and racing to
//! publish refs onto a shared canonical `tx_ordering` (MDS topology).
//!
//! This test checks the system-level invariants of the split:
//!  * Each ref on `tx_ordering` has a `shard_id` that matches the producing
//!    sequencer's shard (no cross-shard leakage).
//!  * The per-sender nonce sequence, rebuilt from the canonical B arrival
//!    order, is strictly ascending and dense.
//!  * The total ref count equals the total input count (no drops, no
//!    duplicate publishes in the in-memory, backpressure-free harness).
//!
//! The in-memory `InMemoryTxOrderingRefPublisher` is `Clone`, and routes
//! all handles to one shared `Vec<TxRef>`. This models the "single
//! canonical B stream, observed in arrival order" semantics that Aeron's
//! concurrent publisher gives on real channels.

use std::collections::HashMap;
use std::num::NonZeroU32;

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{BPosition, TxDataLoc, TxRef};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::outbound::fakes::InMemoryTxOrderingRefPublisher;
use kardamom_sequencer::partition::PartitionCount;
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{Rig, signed_envelope, signer};

const M: u32 = 4;
const SENDERS_PER_PARTITION: usize = 4;
const TX_PER_SENDER: u64 = 25;
#[allow(
    clippy::cast_possible_truncation,
    reason = "small compile-time constant; always fits in usize"
)]
const TX_PER_SENDER_USIZE: usize = TX_PER_SENDER as usize;

fn m_partitions() -> PartitionCount {
    PartitionCount::new(NonZeroU32::new(M).unwrap())
}

/// `(shard, tx_data_position) -> (sender, nonce)`, resolving a canonical
/// ref back to its scripted input.
type SenderAtPos = HashMap<(u8, BPosition), (Address, u64)>;

fn find_signers_for_partition(target: u32, n: usize, seed_start: u64) -> Vec<PrivateKeySigner> {
    (seed_start..)
        .map(signer)
        .filter(|s| m_partitions().index_of(s.address()) == target)
        .take(n)
        .collect()
}

/// The M sequencers under test, each paired with its own [`Rig`] (its own
/// `tx_data` and `tx_errors`, but every rig's `refs` is a clone of the
/// shared canonical B publisher, `b`).
struct Harness {
    b: InMemoryTxOrderingRefPublisher,
    sequencers: Vec<Sequencer>,
    rigs: Vec<Rig>,
}

fn build_harness() -> Harness {
    let b = InMemoryTxOrderingRefPublisher::default();
    let mut sequencers = Vec::with_capacity(M as usize);
    let mut rigs = Vec::with_capacity(M as usize);

    for i in 0u32..M {
        let cfg = SequencerConfig {
            partition_count: m_partitions(),
            partition_index: i,
            sequencer_id: u8::try_from(i).unwrap(),
            max_pending_per_sender: TX_PER_SENDER_USIZE * 2,
            ..Default::default()
        };
        sequencers.push(Sequencer::new(cfg).unwrap());
        rigs.push(Rig {
            refs: b.clone(),
            ..Default::default()
        });
    }
    Harness {
        b,
        sequencers,
        rigs,
    }
}

/// Accumulates envelopes loaded across all M per-shard streams: the
/// running correlation id, and the `(shard, position) -> (sender,
/// nonce)` map the assertions resolve canonical refs against.
struct StreamLoader {
    sender_at_pos: SenderAtPos,
    correlation: u64,
}

impl StreamLoader {
    fn new() -> Self {
        Self {
            sender_at_pos: HashMap::new(),
            correlation: 0,
        }
    }

    /// Load one shard's shuffled `(sender, nonce)` stream into `channel`.
    /// The cartesian product of senders and nonces replaces a nested
    /// loop; this method is the "inner loop as a method" for the outer
    /// per-shard loop in [`load_input_streams`].
    fn load_shard(
        &mut self,
        shard: u8,
        signers: &[PrivateKeySigner],
        rig: &mut Rig,
        rng: &mut rand::rngs::StdRng,
    ) {
        let mut stream: Vec<(usize, u64)> = (0..SENDERS_PER_PARTITION)
            .flat_map(|si| (0..TX_PER_SENDER).map(move |n| (si, n)))
            .collect();
        stream.shuffle(rng);
        for (offset, (si, n)) in stream.into_iter().enumerate() {
            // Each shard has its own A-position space. Use (shard_id, offset)
            // to derive a unique position per envelope.
            let position = BPosition {
                term_id: 0,
                term_offset: i32::try_from(offset).unwrap() * 64,
            };
            let signer = &signers[si];
            let env = signed_envelope(signer, n, self.correlation);
            self.sender_at_pos
                .insert((shard, position), (signer.address(), n));
            rig.push(TxDataLoc::new(0, position), env);
            self.correlation += 1;
        }
    }
}

/// Build M shuffled per-shard input streams and load them into
/// `channels_a`. Returns the total envelope count and a
/// `(shard, position) -> (sender, nonce)` map the assertions resolve
/// canonical refs against.
fn load_input_streams(rigs: &mut [Rig], rng: &mut rand::rngs::StdRng) -> (u64, SenderAtPos) {
    let all_signers_per_part: Vec<Vec<PrivateKeySigner>> = (0..M)
        .map(|i| find_signers_for_partition(i, SENDERS_PER_PARTITION, 1 + u64::from(i) * 10_000))
        .collect();

    let mut loader = StreamLoader::new();
    for (i, (signers, rig)) in all_signers_per_part.iter().zip(rigs).enumerate() {
        loader.load_shard(u8::try_from(i).unwrap(), signers, rig, rng);
    }
    (loader.correlation, loader.sender_at_pos)
}

impl Harness {
    /// One round-robin pass: try every sequencer once. Returns whether
    /// any of them did productive work. The helper method for the inner
    /// loop of [`Harness::drain_round_robin`].
    fn step_all(&mut self) -> bool {
        let mut any = false;
        for (i, (seq, rig)) in self
            .sequencers
            .iter_mut()
            .zip(self.rigs.iter_mut())
            .enumerate()
        {
            match rig.step(seq) {
                Ok(true) => any = true,
                Ok(false) => {}
                Err(e) => panic!("sequencer {i}: unexpected error {e:?}"),
            }
        }
        any
    }

    /// Drive every sequencer to drain, in round-robin, until none has work.
    fn drain_round_robin(&mut self) {
        while self.step_all() {}
    }
}

/// Each sequencer wrote only refs with its own `shard_id`, and owns
/// exactly its shard's worth of refs.
fn assert_shard_ownership(refs: &[TxRef]) {
    let mut per_seq_refs: HashMap<u8, usize> = HashMap::new();
    for r in refs {
        *per_seq_refs.entry(r.shard_id).or_default() += 1;
    }
    for i in 0u8..u8::try_from(M).unwrap() {
        let n = per_seq_refs.get(&i).copied().unwrap_or(0);
        assert_eq!(
            n,
            SENDERS_PER_PARTITION * TX_PER_SENDER_USIZE,
            "sequencer {i} should own exactly its shard's worth of refs"
        );
    }
}

/// Walk the canonical B order. Resolve every ref against the scripted
/// `tx_data` input by `(shard, tx_data_position)`. Check that per-sender
/// nonce sequences are strictly ascending and dense from 0.
fn assert_dense_nonce_sequences(refs: &[TxRef], sender_at_pos: &SenderAtPos) {
    let mut per_sender_nonces: HashMap<Address, Vec<u64>> = HashMap::new();
    for r in refs {
        let (sender, nonce) = sender_at_pos
            .get(&(r.shard_id, r.tx_data_position))
            .copied()
            .expect("every TxRef must resolve to a scripted (shard, position) input");

        // The sequencer that owns this ref must agree with the shard router
        // on this sender. This checks cross-shard routing correctness.
        assert_eq!(
            m_partitions().index_of(sender),
            u32::from(r.shard_id),
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
            TX_PER_SENDER_USIZE,
            "sender {s}: must contribute {TX_PER_SENDER} canonical entries"
        );
        assert_eq!(nonces[0], 0, "sender {s}: must start at nonce 0");
        assert!(
            nonces.windows(2).all(|w| w[1] > w[0]),
            "sender {s}: canonical nonces not strictly ascending: {nonces:?}"
        );
        let first = nonces.first().copied().unwrap();
        let last = nonces.last().copied().unwrap();
        assert_eq!(
            last - first + 1,
            nonces.len() as u64,
            "sender {s}: canonical nonce sequence not dense: {nonces:?}"
        );
    }
}

#[test]
fn m_eq_4_sequencers_publish_canonical_refs() {
    // Build M sequencers, each subscribed to its own tx_data. They all
    // share one `InMemoryTxOrderingRefPublisher`. Cloning it shares the
    // underlying vector, so it is the same canonical B stream.
    let mut harness = build_harness();

    let mut rng = rand::rngs::StdRng::seed_from_u64(0x00C0_FFEE);
    let (correlation, sender_at_pos) = load_input_streams(&mut harness.rigs, &mut rng);

    let total_input = (M as usize) * SENDERS_PER_PARTITION * TX_PER_SENDER_USIZE;
    assert_eq!(usize::try_from(correlation).unwrap(), total_input);

    harness.drain_round_robin();

    // Cross-sequencer invariants on the shared B log.
    let refs = harness.b.refs.lock().unwrap().clone();
    assert_eq!(
        refs.len(),
        total_input,
        "every accepted tx must produce exactly one TxRef on B"
    );

    assert_shard_ownership(&refs);
    assert_dense_nonce_sequences(&refs, &sender_at_pos);

    // No tx_errors were emitted (in-order tx_data reads, no past nonces).
    for (i, rig) in harness.rigs.iter().enumerate() {
        assert!(
            rig.errors().is_empty(),
            "sequencer {i} should not have emitted TxErrors"
        );
    }
}
