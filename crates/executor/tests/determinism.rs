//! Determinism conformance. Two executor instances, driven by the same
//! input, must produce byte-identical `tx_receipts` output (every
//! `tx_hash` and every `write_set_hash` matches). No state-root
//! assertion: the executor does not emit a state-root commitment yet.
//!
//! The topology is M=1 `tx_data`, plus one `tx_ordering`, with refs
//! joined through the executor's `JoinBuffer`. Determinism does not
//! depend on the demux shape. It depends on canonical ordering, which
//! `tx_ordering` preserves.

use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::actor::fixtures::{ChannelHarness, HarnessInput};
use kardamom_engine::{CMessage, ExecutorConfig, MockStateDatabase};

mod common;
use common::{Corpus, TxDataVec, TxOrderingVec};

const QUEUE_DEPTH_128: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// Build 3 blocks' worth (5 txs each) of `tx_data`/`tx_ordering` input,
/// in the order [`ChannelHarness::run`] needs it.
fn populate(signer: &PrivateKeySigner) -> (TxDataVec, TxOrderingVec) {
    let to = address!("00000000000000000000000000000000DEAD0001");
    let mut corpus = Corpus::new(signer, to);
    for blk in 1..=3u64 {
        corpus.send_block(5, blk);
    }
    (corpus.tx_data, corpus.tx_ordering)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "each call constructs a fresh signer and has no further use for it"
)]
fn run_one(signer: PrivateKeySigner) -> Vec<CMessage> {
    let from = signer.address();
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let (tx_data, tx_ordering) = populate(&signer);
    let cfg = ExecutorConfig {
        chain_id: NonZeroU64::MIN,
        receipt_queue_depth: QUEUE_DEPTH_128,
        ..Default::default()
    };
    let outcome = ChannelHarness::run(HarnessInput {
        cfg,
        tx_data,
        tx_ordering,
        snap,
    });
    outcome.result.expect("ok");
    outcome.receipts
}

#[test]
fn two_replicas_produce_byte_identical_c_stream() {
    let signer_a =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    let signer_b =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    assert_eq!(signer_a.address(), signer_b.address());

    let a = run_one(signer_a);
    let b = run_one(signer_b);

    assert_eq!(a.len(), b.len());
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        assert_message_pair_matches(i, x, y);
    }
}

/// Assert that `x` and `y`, the two replicas' `c_stream` messages at
/// index `i`, are the same variant with equal content.
fn assert_message_pair_matches(i: usize, x: &CMessage, y: &CMessage) {
    match (x, y) {
        (CMessage::Receipt(rx), CMessage::Receipt(ry)) => {
            assert_eq!(rx, ry, "receipt mismatch at idx {i}");
        }
        (CMessage::BlockBoundary(bx), CMessage::BlockBoundary(by)) => {
            assert_eq!(bx, by, "boundary mismatch at idx {i}");
        }
        _ => panic!("type mismatch at idx {i}"),
    }
}
