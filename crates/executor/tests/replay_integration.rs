//! Integration test: feed a synthetic stream of txs and boundaries into
//! an `Executor`, and check the `tx_receipts` output against expectation.
//!
//! The topology is single-sequencer (M=1). Envelopes push onto a fake
//! `tx_data`. Tiny `TxRef` records and a `BlockBoundaryStart` push onto
//! fake channel B, in the same canonical order. The executor's M+1
//! readers join the two streams through the in-process `JoinBuffer`.

use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use alloy_primitives::{B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::actor::fixtures::{ChannelHarness, HarnessInput};
use kardamom_engine::{BlockBoundary, CMessage, ExecutorConfig, MockStateDatabase};

mod common;
use common::{Corpus, TxDataVec, TxOrderingVec};

const QUEUE_DEPTH_64: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Build `tx_data`/`tx_ordering` input for 10 transfers across 3
/// boundaries: 4 txs then boundary block 1, 3 txs then boundary block
/// 2, 3 txs then boundary block 3. Returns each tx's hash, in send
/// order.
fn build_replay_corpus(
    signer: &PrivateKeySigner,
    to: alloy_primitives::Address,
) -> (TxDataVec, TxOrderingVec, Vec<B256>) {
    let mut corpus = Corpus::new(signer, to);
    for (n_txs, blk) in [(4u64, 1u64), (3, 2), (3, 3)] {
        corpus.send_block(n_txs, blk);
    }
    (corpus.tx_data, corpus.tx_ordering, corpus.expected_hashes)
}

/// The `c_stream`'s tallies: receipt count, boundary count, and every
/// receipt's `tx_hash`, in arrival order.
#[derive(Default)]
struct Collected {
    receipts: usize,
    boundaries: usize,
    got_hashes: Vec<B256>,
}

impl Collected {
    /// Fold one `c_stream` message into the tallies. Asserts that a
    /// receipt succeeded and a boundary's block number falls inside
    /// the expected range: both are fixture bugs, not runtime
    /// conditions to recover from.
    fn record(&mut self, msg: CMessage) {
        match msg {
            CMessage::Receipt(r) => {
                assert!(r.status, "tx {} should succeed", self.receipts);
                assert_ne!(r.write_set_hash, B256::ZERO);
                self.got_hashes.push(r.tx_hash);
                self.receipts += 1;
            }
            CMessage::BlockBoundary(b) => {
                // BlockBoundary has no state_root_commitment field yet.
                // This destructure checks only the slim three-field shape.
                let BlockBoundary {
                    block_number,
                    end_tx_idx: _,
                    l2_timestamp: _,
                    l1_origin: _,
                } = b;
                assert!((1..=3).contains(&block_number));
                self.boundaries += 1;
            }
        }
    }
}

/// Fold every message in `receipts` into one [`Collected`] tally.
fn drain_c_stream(receipts: Vec<CMessage>) -> Collected {
    let mut collected = Collected::default();
    for msg in receipts {
        collected.record(msg);
    }
    collected
}

#[test]
fn replay_10_txs_across_3_blocks_yields_expected_c_stream() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    // Shared MockStateDatabase. The writer applies each block's delta
    // back into it, so the next block's snapshot reflects the previous
    // block's writes. This matches the production libmdbx semantics.
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();

    let (tx_data, tx_ordering, expected_hashes) = build_replay_corpus(&signer, to);
    let cfg = ExecutorConfig {
        chain_id: NonZeroU64::MIN,
        receipt_queue_depth: QUEUE_DEPTH_64,
        ..Default::default()
    };
    let outcome = ChannelHarness::run(HarnessInput {
        cfg,
        tx_data,
        tx_ordering,
        snap,
    });
    outcome.result.expect("exec ok");

    let collected = drain_c_stream(outcome.receipts);

    assert_eq!(collected.receipts, 10);
    assert_eq!(collected.boundaries, 3);
    // Every receipt's tx_hash must equal the inbound envelope's
    // tx_hash, byte-for-byte, in the same order. The executor never
    // recomputes it; the executor only passes it through.
    assert_eq!(collected.got_hashes, expected_hashes);
}
