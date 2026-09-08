//! Allocation profile of the sequencer's per-tx core loop.
//! This test is ignored by default. Run it explicitly:
//!
//!   cargo test -p kardamom-sequencer --test `alloc_profile` --release -- \
//!     --ignored --nocapture
//!
//! This test drives `Sequencer::run_once`: frame ingest from the `tx_data`
//! subscription, RLP nonce decode, per-sender nonce state-machine advance,
//! and `TxRef` build and batch publish onto `tx_ordering`. It runs
//! in-process against the crate's in-memory fakes (no Aeron, no cluster),
//! under the DHAT heap profiler. It prints allocs/tx, bytes/tx, and
//! wall/tx, and writes dhat-heap-sequencer.json (per-callsite data,
//! viewable with `dh_view.html`) next to this crate's Cargo.toml.
//!
//! The inputs mirror `benches/throughput.rs`: RLP-encoded signed legacy
//! transactions (about 240 bytes raw, with 128 bytes of calldata), and 64
//! senders round-robined with strictly sequential nonces. So every
//! envelope takes the steady-state Matched-to-publish path, with no
//! future-nonce buffering and no backpressure.
//!
//! This test does not measure the receipt-floor resync controller or the
//! unconfirmed-ledger bookkeeping. Both are disabled (`resync: None`, as
//! in the criterion bench and the IPC dev-run wiring). The `tx_ordering`
//! publisher is the in-memory fake, so the real cluster offer path (rkyv
//! frame encode and Aeron offer) is out of scope here.

use std::num::NonZeroU64;

use alloy_signer_local::PrivateKeySigner;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{EnvelopeSpec, Rig, envelope_with, one_partition_cfg, signer};
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const SENDERS: usize = 64;
/// Nonces per sender to process before the profiler starts. This warms
/// the per-sender nonce map and the publisher's ref vector.
const WARMUP_NONCES: u64 = 16;
/// Nonces per sender inside the measured window: 64 x 80 = 5,120 txs.
const MEASURED_NONCES: u64 = 80;
/// Calldata padding so the raw frame lands in the realistic 200-400 byte
/// range. An empty-input transfer is about 110 bytes, and undercounts the
/// RLP decode cost.
const CALLDATA_BYTES: usize = 128;

/// Build a signed legacy transaction, wrapped the way the proxy publishes
/// it onto `tx_data`: RLP `raw_tx`, plus a proxy-stamped `sender` and
/// `tx_hash`. The sequencer trusts both fields and never recovers or
/// hashes them.
///
/// Unlike `testkit::signed_envelope`'s default: realistic calldata padding
/// (see `CALLDATA_BYTES`) and the real `tx_hash`, since this profile
/// mirrors the production RLP-decode and dedup-key cost, not just the
/// nonce-check state machine.
fn signed_envelope(s: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    envelope_with(
        s,
        nonce,
        correlation_id,
        EnvelopeSpec {
            gas_limit: 100_000,
            calldata_len: CALLDATA_BYTES,
            real_hash: true,
        },
    )
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "this profiling test turns small, deterministic counters into printed ratios (allocs/tx, bytes/tx, ktx/s); the f64 conversions are inherently approximate reporting, not correctness-sensitive"
)]
#[test]
#[ignore = "profiling run — invoke explicitly with --ignored"]
fn sequencer_core_loop_allocation_profile() {
    let signers: Vec<_> = (1..=SENDERS as u64).map(signer).collect();

    // Round-robin senders with strictly sequential per-sender nonces.
    // This is the realistic steady-state arrival order. Load warmup nonces
    // first, then measured ones, into the scripted subscription before the
    // profiled window starts.
    let total_nonces = WARMUP_NONCES + MEASURED_NONCES;
    let mut rig = Rig::default();
    // The cartesian product of nonce and sender, in the same row-major
    // order the original nested loop walked, with `i` (the original
    // manual counter) recovered from `enumerate`.
    for (i, (nonce, s)) in (0..total_nonces)
        .flat_map(|nonce| signers.iter().map(move |s| (nonce, s)))
        .enumerate()
    {
        let i = i as u64;
        let loc = TxDataLoc::new(0, BPosition::from_index(i));
        rig.push(loc, signed_envelope(s, nonce, i));
    }
    let warmup_total = SENDERS as u64 * WARMUP_NONCES;
    let measured_total = SENDERS as u64 * MEASURED_NONCES;

    let mut seq = Sequencer::new(SequencerConfig {
        max_pending_per_sender: 16,
        ..one_partition_cfg()
    })
    .unwrap();

    // Warm up before the measured window. Every envelope is in order, so
    // each productive `run_once` call ingests one envelope and publishes its ref.
    for _ in 0..warmup_total {
        assert!(rig.step(&mut seq).expect("warmup"));
    }
    assert_eq!(
        rig.refs().len() as u64,
        warmup_total,
        "warmup must publish exactly one ref per envelope"
    );
    // Pre-reserve the fake publisher's ref vector. This stops its growth
    // doubling from affecting the measured counts. The real publisher holds
    // no such vector.
    rig.refs
        .refs
        .lock()
        .unwrap()
        .reserve(measured_total as usize + 16);

    // This is the measured window, under DHAT. It runs the production
    // `run_once` path: poll, shard check, RLP nonce decode, nonce state
    // machine, TxRef build, batch publish. The final iteration is the
    // empty-queue idle poll (returns false), and it has little effect.
    // `Rig::step` builds its `Ports` on the stack (three references, no
    // heap allocation), so routing through it here does not skew the
    // measured counts below.
    let profiler = dhat::Profiler::builder().build();
    let stats0 = dhat::HeapStats::get();
    let t0 = std::time::Instant::now();
    while rig.step(&mut seq).expect("run") {}
    let wall = t0.elapsed();
    let stats = dhat::HeapStats::get();

    assert_eq!(
        rig.refs().len() as u64,
        warmup_total + measured_total,
        "every measured envelope must publish exactly one ref"
    );
    assert!(
        rig.errors().is_empty(),
        "no tx errors expected on the in-order path"
    );

    // SENDERS and MEASURED_NONCES are both nonzero compile-time constants,
    // so their product is provably nonzero. `NonZeroU64` states that
    // invariant at the divisor, instead of a bare `u64` that merely
    // happens to be nonzero today.
    let n = NonZeroU64::new(measured_total)
        .expect("SENDERS * MEASURED_NONCES is a nonzero compile-time product");
    let allocs = stats.total_blocks - stats0.total_blocks;
    let bytes = stats.total_bytes - stats0.total_bytes;
    println!("==================== SEQUENCER ALLOCATION PROFILE ({n} txs) ====================");
    println!("allocs/tx:      {:.2}", allocs as f64 / n.get() as f64);
    println!("bytes/tx:       {:.0}", bytes as f64 / n.get() as f64);
    println!("peak heap:      {:.2} MB", stats.max_bytes as f64 / 1e6);
    println!(
        "wall/tx:        {:.2} us",
        wall.as_micros() as f64 / n.get() as f64
    );
    // A very fast run can measure a wall time of exactly 0s (coarse clock
    // resolution), which would otherwise divide-by-zero into a
    // meaningless "inf ktx/s" line.
    match wall.as_secs_f64() {
        secs if secs > 0.0 => {
            println!("implied 1-core: {:.0} ktx/s", n.get() as f64 / secs / 1e3);
        }
        _ => println!("implied 1-core: too fast to measure"),
    }
    drop(profiler); // writes dhat-heap.json with per-callsite data
    let _ = std::fs::rename("dhat-heap.json", "dhat-heap-sequencer.json");
}
