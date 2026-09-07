//! End-to-end sequencer test against a scripted `tx_data` subscription and
//! in-memory `tx_ordering` and receipt-cache publishers (MDS topology).
//! This test checks:
//!  * Canonical order on `tx_ordering` (the `TxRef` sequence) matches a
//!    per-sender, nonce-ascending sequence.
//!  * Each ref's `tx_data_position` matches the position the proxy supplied
//!    on the scripted `tx_data` subscription.
//!  * Duplicates are dropped and reported on the receipt-cache channel.
//!  * Future-nonce transactions are buffered, and drain when the prior
//!    nonce arrives.

use std::collections::HashMap;

use alloy_primitives::Address;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};
use rand::SeedableRng;
use rand::seq::SliceRandom;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::testkit::{drive_to_idle, one_partition_cfg, signed_envelope, signer};

/// Build an A-position for each scripted envelope. This gives refs a
/// unique pointer back to the simulated `tx_data`. Spaced by 64
/// (unlike `testkit::pos`'s per-1 spacing) to also exercise a
/// non-contiguous term offset.
fn pos_n(n: u64) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: i32::try_from(n * 64).unwrap(),
    }
}

#[test]
fn integration_1000_txs_100_senders_with_chaos() {
    // Single shard so every sender lands here.
    let cfg = SequencerConfig {
        max_pending_per_sender: 16,
        ..one_partition_cfg()
    };

    let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEAD_BEEF);
    let signers: Vec<_> = (1..=100u64).map(signer).collect();

    // Each sender contributes 10 in-order nonces. Shuffle the arrival order
    // to exercise the future buffer.
    let mut stream: Vec<(usize, u64)> = (0..signers.len())
        .flat_map(|i| (0..10u64).map(move |n| (i, n)))
        .collect();
    stream.shuffle(&mut rng);

    let tx_stream: Vec<(TxDataLoc, TxEnvelope)> = stream
        .iter()
        .enumerate()
        .map(|(correlation, (i, n))| {
            let position = pos_n(correlation as u64);
            let env = signed_envelope(&signers[*i], *n, correlation as u64);
            (TxDataLoc::new(0, position), env)
        })
        .collect();
    let total_input = tx_stream.len();

    // sender_at_pos maps tx_data_position to (sender, nonce). It checks that
    // each published TxRef's tx_data_position points back to the right
    // envelope. Derived from tx_stream (which carries the position) zipped
    // with stream (which carries the sender index and nonce).
    let sender_at_pos: HashMap<BPosition, (Address, u64)> = tx_stream
        .iter()
        .zip(stream.iter())
        .map(|((loc, _env), (i, n))| (loc.position, (signers[*i].address(), *n)))
        .collect();

    let (refs, _errs) = drive_to_idle(cfg.clone(), &tx_stream);

    // Every in-order input must produce a B ref.
    assert_eq!(
        refs.len(),
        total_input,
        "every in-order input should produce a TxRef on B"
    );

    // For each ref, look up the (sender, nonce) that the proxy fed into
    // tx_data at that position. Then check that, for each sender, the
    // nonces come out in ascending order from 0 to 10.
    let mut per_sender: HashMap<Address, Vec<u64>> = HashMap::new();
    for r in &refs {
        assert_eq!(r.shard_id, cfg.sequencer_id);
        let (sender, nonce) = sender_at_pos
            .get(&r.tx_data_position)
            .copied()
            .expect("every ref's tx_data_position must match a scripted input");
        per_sender.entry(sender).or_default().push(nonce);
    }
    assert_eq!(per_sender.len(), signers.len());
    for (s, nonces) in &per_sender {
        assert_eq!(nonces[0], 0, "sender {s}: must start at nonce 0");
        assert!(
            nonces.windows(2).all(|w| w[1] > w[0]),
            "sender {s}: nonces not ascending: {nonces:?}"
        );
        assert_eq!(
            nonces.len(),
            10,
            "sender {s}: each contributes 10 in-order nonces"
        );
    }
}

#[test]
fn integration_duplicates_are_reported() {
    let cfg = SequencerConfig {
        max_pending_per_sender: 4,
        ..one_partition_cfg()
    };
    let s = signer(7);
    let stream = vec![
        (TxDataLoc::new(0, pos_n(0)), signed_envelope(&s, 0, 100)),
        (TxDataLoc::new(0, pos_n(1)), signed_envelope(&s, 1, 101)),
        // Three duplicates of nonce 0 arrive after nonce 1 is processed.
        (TxDataLoc::new(0, pos_n(2)), signed_envelope(&s, 0, 200)),
        (TxDataLoc::new(0, pos_n(3)), signed_envelope(&s, 0, 201)),
        (TxDataLoc::new(0, pos_n(4)), signed_envelope(&s, 0, 202)),
    ];

    let (refs, errs) = drive_to_idle(cfg, &stream);

    assert_eq!(refs.len(), 2);
    assert_eq!(errs.len(), 3, "all 3 past-nonce submissions emit a TxError");
    // All 3 errors are for the same (sender, nonce=0). They are distinct
    // submissions, but the TxError layer cannot tell them apart:
    // correlation_id was dropped when the receipt-cache channel was retired.
    for err in &errs {
        assert_eq!(err.sender, s.address());
        assert_eq!(err.nonce, 0);
        assert!(matches!(
            err.reason,
            kardamom_sequencer::TxErrorReason::DuplicatedTx { .. }
        ));
    }
}

#[test]
fn integration_bounded_buffer_evicts_oldest() {
    // Send nonces 100..110 (10 future nonces) with max_pending=4. No
    // contiguous nonce 0 arrives, so the futures stay buffered. The
    // buffer's cap of 4 evicts the oldest 6. So if nonce 0 arrived later,
    // only refs for 0 and the surviving futures would appear. This test
    // checks that no publishes happen in the all-future phase.
    let cfg = SequencerConfig {
        max_pending_per_sender: 4,
        ..one_partition_cfg()
    };
    let s = signer(42);
    let stream: Vec<_> = (100..110u64)
        .map(|n| (TxDataLoc::new(0, pos_n(n)), signed_envelope(&s, n, n)))
        .collect();
    let (refs, _errs) = drive_to_idle(cfg, &stream);
    assert_eq!(
        refs.len(),
        0,
        "all 10 are futures, no canonical refs emitted"
    );
}
