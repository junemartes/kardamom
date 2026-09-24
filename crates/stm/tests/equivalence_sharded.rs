//! Sharded admission's byte-identical check.
//!
//! Dependency discovery split across cell-space shards must produce the
//! same bytes as the serial feed. The shapes that matter are the ones
//! where a shard boundary could hide an edge: a hot single-cell chain
//! (all transactions in one shard), transfers (two cells per
//! transaction, usually in different shards), and racing lying-stats
//! repetitions.
#![allow(
    clippy::cast_possible_truncation,
    reason = "fixture indices (signer count, tx count) stay far below u8/u32::MAX in every test"
)]

mod common;

use alloy_primitives::{Address, TxKind, U256, keccak256};
use common::*;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs};
use kardamom_stm::execute::{StmOutcome, execute_block_sequential};
use kardamom_types::{BPosition, TxEnvelope};

/// Run one block through the sharded pool with `shards` admission
/// shards and `workers` worker threads.
fn run_sharded(
    database: &MockStateDatabase,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
    stats: &Stats,
    workers: usize,
    shards: usize,
) -> StmOutcome {
    kardamom_stm::execute::with_pool(
        kardamom_stm::execute::PoolConfig {
            workers: nz(workers),
            admit_shards: std::num::NonZeroUsize::new(shards),
            ..Default::default()
        },
        |pool| {
            pool.run_block(
                vec![database.clone(); workers],
                PendingDelta::new(),
                env(),
                recs,
                stats,
            )
            .unwrap()
        },
    )
}

/// Hot chain: 24 increments of one slot. Every transaction's conflict
/// cell lands in the same shard, so one shard carries the whole chain.
fn hot_chain_matches_across_shards(
    sg: &[alloy_signer_local::PrivateKeySigner],
    database: &MockStateDatabase,
) {
    let recs = records(counter_chain(&sg[..3], 8));
    let seq = execute_block_sequential(database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for (shards, workers) in shard_worker_pairs([1, 2, 3, 4], [1, 4]) {
        let out = run_sharded(database, &recs, &stats, workers, shards);
        assert_eq!(out.wounds, 0, "ordered chain must not wound (k={shards})");
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("sharded chain k={shards} w={workers}"),
        );
    }
}

/// Transfers: two cells per transaction, so most transactions span
/// shards, and the per-batch guard is what keeps them from dispatching
/// early.
fn transfers_match_across_shards(
    sg: &[alloy_signer_local::PrivateKeySigner],
    database: &MockStateDatabase,
) {
    let recs2 = records(transfer_block(sg));
    let seq2 = execute_block_sequential(database, None, env(), &recs2).unwrap();
    for (shards, workers) in shard_worker_pairs([2, 3], [1, 2, 4]) {
        let out = run_sharded(database, &recs2, &Stats::default(), workers, shards);
        assert_identical(
            &seq2,
            &out.receipts,
            &out.delta,
            &format!("sharded transfers k={shards} w={workers}"),
        );
    }
}

/// Table pressure: a block big enough to fill the per-shard tables. The
/// 24-transaction cases above pass even with a broken probe walk, since
/// they never fill a table. This case catches a mis-sized shard table:
/// the bug it was written for made `upsert` spin forever at k=3.
fn table_pressure_matches_across_shards(
    sg: &[alloy_signer_local::PrivateKeySigner],
    database: &MockStateDatabase,
) {
    let many: Vec<TxEnvelope> = (0..1500u64)
        .map(|n| {
            let s = &sg[(n % 4) as usize];
            tx(
                s,
                n / 4,
                TxKind::Call(sg[((n + 1) % 4) as usize].address()),
                1,
                &[],
            )
        })
        .collect();
    let recs_many = records(many);
    let seq_many = execute_block_sequential(database, None, env(), &recs_many).unwrap();
    for shards in [2, 3, 5] {
        let out = run_sharded(database, &recs_many, &Stats::default(), 4, shards);
        assert_identical(
            &seq_many,
            &out.receipts,
            &out.delta,
            &format!("sharded pressure k={shards}"),
        );
    }
}

/// Lying stats: mispredicted footprints race, so validation and repair
/// must still land on identical bytes under sharding.
fn lying_stats_match_across_shards(
    sg: &[alloy_signer_local::PrivateKeySigner],
    database: &MockStateDatabase,
) {
    let lying = {
        let obs: Vec<TxObs> = (0..4)
            .map(|i| {
                let sender = Address::with_last_byte(i as u8 + 0x10);
                let mut buf = [0u8; 64];
                buf[..32]
                    .copy_from_slice(&U256::from_be_slice(sender.as_slice()).to_be_bytes::<32>());
                buf[32..].copy_from_slice(&U256::from(3u8).to_be_bytes::<32>());
                TxObs {
                    index: i,
                    block: 1,
                    sender,
                    to: Some(COUNTER),
                    selector: Some(COUNTER_SEL),
                    args: Vec::new(),
                    gas: 30_000,
                    has_value: false,
                    reads: Vec::new(),
                    writes: vec![Cell::Slot(COUNTER, keccak256(buf))],
                }
            })
            .collect();
        Stats::learn(&obs)
    };
    let recs3 = records(counter_block(sg, 0));
    let seq3 = execute_block_sequential(database, None, env(), &recs3).unwrap();
    for rep in 0..25 {
        let out = run_sharded(database, &recs3, &lying, 4, 3);
        assert_identical(
            &seq3,
            &out.receipts,
            &out.delta,
            &format!("sharded lying rep={rep}"),
        );
    }
}

#[test]
fn sharded_admission_byte_identical() {
    let sg = signers(4);
    let database = db(&sg);
    hot_chain_matches_across_shards(&sg, &database);
    transfers_match_across_shards(&sg, &database);
    table_pressure_matches_across_shards(&sg, &database);
    lying_stats_match_across_shards(&sg, &database);
}
