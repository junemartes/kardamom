//! Differential test: the actor's receipt for each tx must match a
//! naive single-threaded `revm` loop's receipt for the same tx.
//!
//! The corpus covers transfers, a contract `SSTORE`, and a revert.
//!
//! The topology is M=1 `tx_data`, plus one `tx_ordering`. The demux
//! shape does not affect determinism.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices here are bounded by the small fixed test corpus, never near a truncation or wrap boundary"
)]

use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes as AlloyBytes, U256, address};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::CacheDB;
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::Bytecode;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use kardamom_engine::actor::fixtures::{ChannelHarness, HarnessInput, LegacyTx};
use kardamom_engine::executor::SnapshotRef;
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, ExecutorConfig, MockStateDatabase, TxEnvelope,
    TxOrderingMessage, TxRef,
};

const QUEUE_DEPTH_8: NonZeroUsize = NonZeroUsize::new(8).unwrap();

/// A `tx_data` input stream: [`HarnessInput::tx_data`]'s type.
type TxDataVec = Vec<(BPosition, TxEnvelope)>;
/// A `tx_ordering` input stream: [`HarnessInput::tx_ordering`]'s type.
type TxOrderingVec = Vec<(BPosition, TxOrderingMessage)>;

// Minimal: PUSH1 0x42; PUSH1 0x00; SSTORE; STOP
const SSTORE_42_AT_0: [u8; 6] = [0x60, 0x42, 0x60, 0x00, 0x55, 0x00];
// PUSH1 0x00; PUSH1 0x00; REVERT
const REVERT_CODE: [u8; 5] = [0x60, 0x00, 0x60, 0x00, 0xfd];

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// One transaction and the sender address it commits under, for
/// [`naive_reference`] and the actor comparison in
/// `actor_receipts_match_naive_reference`.
#[derive(Clone)]
struct TxWithSender {
    env: TxEnvelope,
    sender: Address,
}

/// One transaction's outcome: whether it succeeded, and its gas used.
/// [`naive_reference`] and the actor path both produce a `Vec` of
/// these, compared entry by entry.
#[derive(Debug, PartialEq, Eq)]
struct TxResult {
    status: bool,
    gas_used: u64,
}

/// Run one transaction from `entry` against `cache`, and return its
/// outcome.
fn run_one_naive(
    cache: &mut CacheDB<SnapshotRef<'_, MockStateDatabase>>,
    entry: &TxWithSender,
) -> TxResult {
    use alloy_consensus::Transaction;
    let mut slice: &[u8] = entry.env.raw_tx.as_ref();
    let env = alloy_consensus::TxEnvelope::decode_2718(&mut slice).expect("decode raw_tx");
    let tx_env = TxEnv {
        caller: entry.sender,
        chain_id: env.chain_id(),
        nonce: env.nonce(),
        gas_limit: env.gas_limit(),
        value: env.value(),
        data: env.input().clone(),
        kind: match env.to() {
            Some(a) => TxKind::Call(a),
            None => TxKind::Create,
        },
        gas_price: env.gas_price().unwrap_or_else(|| env.max_fee_per_gas()),
        ..Default::default()
    };
    #[allow(
        clippy::field_reassign_with_default,
        reason = "CfgEnv has many fields; building the default then setting chain_id is clearer than a full literal"
    )]
    let cfg: CfgEnv = {
        let mut c = CfgEnv::default();
        c.chain_id = 1;
        c
    };
    let blk = BlockEnv {
        number: U256::from(1u64),
        timestamp: U256::from(1_700_000_000u64),
        gas_limit: 30_000_000,
        basefee: 0,
        prevrandao: Some(alloy_primitives::B256::default()),
        ..Default::default()
    };
    let mut evm = Context::mainnet()
        .with_db(cache)
        .with_block(blk)
        .with_cfg(cfg)
        .build_mainnet();
    let r = evm.transact_commit(tx_env).expect("commit");
    TxResult {
        status: matches!(r, ExecutionResult::Success { .. }),
        gas_used: r.gas().tx_gas_used(),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "each call constructs a fresh snapshot and has no further use for it"
)]
fn naive_reference(snap: MockStateDatabase, txs: &[TxWithSender]) -> Vec<TxResult> {
    let snap_ref = SnapshotRef { inner: &snap };
    let mut cache: CacheDB<SnapshotRef<'_, MockStateDatabase>> = CacheDB::new(snap_ref);
    txs.iter()
        .map(|entry| run_one_naive(&mut cache, entry))
        .collect()
}

/// Two snapshots of the same fixture accounts (a funded sender, an
/// `SSTORE` contract, and a reverting contract), plus the
/// three-transaction corpus every diff-reference test runs. The
/// reference path mutates its `CacheDB` in place; a second snapshot
/// keeps that out of the actor's reads.
struct DiffFixture {
    snap_ref: MockStateDatabase,
    snap_actor: MockStateDatabase,
    pairs: Vec<TxWithSender>,
}

fn build_diff_fixture() -> DiffFixture {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let sstore_addr = address!("00000000000000000000000000000000000ABC55");
    let revert_addr = address!("00000000000000000000000000000000000ABCFD");

    let sstore_code = AlloyBytes::from_static(&SSTORE_42_AT_0);
    let revert_code = AlloyBytes::from_static(&REVERT_CODE);
    let sstore_hash = Bytecode::new_raw(sstore_code.clone()).hash_slow();
    let revert_hash = Bytecode::new_raw(revert_code.clone()).hash_slow();

    let build_snap = || {
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .account(sstore_addr, U256::ZERO, 1, sstore_hash)
            .account(revert_addr, U256::ZERO, 1, revert_hash)
            .code(sstore_hash, Bytes::copy_from_slice(sstore_code.as_ref()))
            .code(revert_hash, Bytes::copy_from_slice(revert_code.as_ref()))
            .build()
    };

    let txs: [TxEnvelope; 3] = [
        LegacyTx {
            chain_id: 1,
            to,
            nonce: 0,
            value: 10,
            gas_limit: 21_000,
            gas_price: 0,
            ..Default::default()
        }
        .sign(&signer),
        LegacyTx {
            chain_id: 1,
            to: sstore_addr,
            nonce: 1,
            value: 0,
            gas_limit: 100_000,
            gas_price: 0,
            ..Default::default()
        }
        .sign(&signer),
        LegacyTx {
            chain_id: 1,
            to: revert_addr,
            nonce: 2,
            value: 0,
            gas_limit: 100_000,
            gas_price: 0,
            ..Default::default()
        }
        .sign(&signer),
    ];
    let pairs: Vec<TxWithSender> = txs
        .iter()
        .cloned()
        .map(|env| TxWithSender { env, sender: from })
        .collect();

    DiffFixture {
        snap_ref: build_snap(),
        snap_actor: build_snap(),
        pairs,
    }
}

/// Append `entry`'s `tx_data` record and `tx_ordering` ref, at index
/// `i`, to `tx_data`/`tx_ordering`.
fn publish_one(
    tx_data: &mut TxDataVec,
    tx_ordering: &mut TxOrderingVec,
    i: usize,
    entry: &TxWithSender,
) {
    let tx_data_position = bpos((i as i32) * 200);
    let tx_hash = entry.env.tx_hash;
    tx_data.push((tx_data_position, entry.env.clone()));
    tx_ordering.push((
        bpos(i as i32),
        TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
    ));
}

/// Build `tx_data`/`tx_ordering` input for `pairs`, then the closing
/// boundary.
fn build_replay_input(pairs: &[TxWithSender]) -> (TxDataVec, TxOrderingVec) {
    let mut tx_data = Vec::new();
    let mut tx_ordering = Vec::new();
    pairs
        .iter()
        .enumerate()
        .for_each(|(i, entry)| publish_one(&mut tx_data, &mut tx_ordering, i, entry));
    tx_ordering.push((
        bpos(pairs.len() as i32),
        TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            // end_tx_idx equals the cumulative count of canonical records
            // (the number of txs applied), encoded with bpos (the same as
            // BPosition::from_index here).
            end_tx_idx: bpos(pairs.len() as i32),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }),
    ));
    (tx_data, tx_ordering)
}

/// `m`'s receipt as a [`TxResult`], or `None` if `m` is a boundary.
fn receipt_to_result(m: CMessage) -> Option<TxResult> {
    match m {
        CMessage::Receipt(r) => Some(TxResult {
            status: r.status,
            gas_used: r.gas_used,
        }),
        CMessage::BlockBoundary(_) => None,
    }
}

#[test]
fn actor_receipts_match_naive_reference() {
    let fixture = build_diff_fixture();
    let reference = naive_reference(fixture.snap_ref, &fixture.pairs);

    let (tx_data, tx_ordering) = build_replay_input(&fixture.pairs);
    let cfg = ExecutorConfig {
        chain_id: NonZeroU64::MIN,
        receipt_queue_depth: QUEUE_DEPTH_8,
        ..Default::default()
    };
    let outcome = ChannelHarness::run(HarnessInput {
        cfg,
        tx_data,
        tx_ordering,
        snap: fixture.snap_actor,
    });
    outcome.result.expect("ok");

    let actor: Vec<TxResult> = outcome
        .receipts
        .into_iter()
        .filter_map(receipt_to_result)
        .collect();

    assert_eq!(actor.len(), reference.len());
    for (i, (a, r)) in actor.iter().zip(reference.iter()).enumerate() {
        assert_eq!(a, r, "diff at idx {i}: actor={a:?} reference={r:?}");
    }
}
