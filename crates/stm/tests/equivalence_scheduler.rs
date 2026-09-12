//! DAG construction, the fixed-worker gate, and the FIFO/bag
//! schedulers: pins their mechanics down, plus the byte-identical
//! check on top.
#![allow(
    clippy::cast_possible_truncation,
    reason = "fixture indices (signer count, tx count) stay far below u8/u32::MAX in every test"
)]

mod common;

use alloy_primitives::{Address, B256, TxKind, U256, keccak256};
use common::*;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs};
use kardamom_stm::execute::{execute_block_sequential, execute_block_stm};
use kardamom_types::{BPosition, TxEnvelope};

#[test]
fn base_delta_layer_is_visible() {
    // Block 2 semantics: the pre-block delta (block 1's writes) must be
    // the block-input view for both engines.
    let sg = signers(2);
    let database = db(&sg);
    let e = env();
    let b1 = records(vec![tx(&sg[0], 0, TxKind::Call(sg[1].address()), 700, &[])]);
    let (_, d1) = execute_block_sequential(&database, None, e, &b1).unwrap();
    let e2 = env_at(2);
    let b2 = records(vec![
        tx(&sg[1], 0, TxKind::Call(sg[0].address()), 600, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[1].address()), 100, &[]),
    ]);
    let seq = execute_block_sequential(&database, Some(&d1), e2, &b2).unwrap();
    let out = execute_block_stm(&database, Some(&d1), e2, &b2, &Stats::default(), nz(4)).unwrap();
    assert!(!out.fallback);
    assert_identical(&seq, &out.receipts, &out.delta, "base layer");
}

/// The scheduler's structural invariant: every admitted transaction
/// occupies exactly one node and leaves the graph exactly once.
/// Registering twice is unreachable through the public API, because the
/// local index comes from the session's own counter, so no caller can
/// name an occupied slot. Leaving twice is counted rather than assumed:
/// `double_exit` must be zero, since a second exit would strand every
/// edge registered in between and hang the block.

#[test]
fn each_tx_occupies_one_node_and_exits_once() {
    let sg = signers(4);
    let database = db(&sg);
    // Same sender repeatedly (one chain), distinct senders (independent),
    // and cold calls (barriers): every admission path in one block.
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 10, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[2].address()), 10, &[]),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(sg[3].address()), 10, &[]),
        tx(&sg[3], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[0], 2, TxKind::Call(sg[3].address()), 10, &[]),
    ];
    let n = envs.len();
    let recs = records(envs);
    let out = run_pool(&database, &recs);
    assert_eq!(out.receipts.len(), n, "one receipt per admitted tx");
    assert_eq!(
        out.dispatch.iter().sum::<u32>(),
        n as u32,
        "each tx dispatched exactly once: {:?}",
        out.dispatch
    );
    assert_eq!(out.double_exit, 0, "no tx may leave the graph twice");
}

/// Every admitted transaction leaves the graph, so a block always drains.
/// If a future change ever strands an edge, `seal` fail-stops with
/// diagnostics instead of freezing the execution thread. This test pins
/// the healthy path: a block with barriers, chains, and independent work
/// drains promptly.

#[test]
fn every_block_drains() {
    let sg = signers(4);
    let database = db(&sg);
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 100, &[]),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL), // cold ⇒ barrier
        tx(&sg[2], 0, TxKind::Call(sg[3].address()), 50, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[2].address()), 25, &[]),
        tx(&sg[3], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL), // second barrier
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let started = std::time::Instant::now();
    let out = run_pool(&database, &recs);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "block took {:?} — the watchdog would have fired",
        started.elapsed()
    );
    assert_eq!(out.cold, 2);
    assert_identical(&seq, &out.receipts, &out.delta, "drain");
}

/// The pool must never make things worse. Plain transfers cost far less
/// per transaction than parallel execution costs to coordinate. Once the
/// pool measures that, it declines the block and runs it sequentially.
/// The result is still byte-identical, because declining routes through
/// the sequential executor itself, not a second implementation of it.
///
/// The threshold is injected rather than left at its measured default.
/// A machine-load-dependent test is flaky: the gate is a timing
/// decision, and a test that depends on how loaded the machine is
/// would pass alone and fail in a parallel test run.

#[test]
fn cheap_blocks_are_declined_and_still_match() {
    use kardamom_stm::execute::{PoolConfig, with_pool};
    let sg = signers(6);
    let database = db(&sg);
    let first = records(
        (0..6usize)
            .map(|i| tx(&sg[i], 0, TxKind::Call(sg[(i + 1) % 6].address()), 100, &[]))
            .collect(),
    );
    let second = records(
        (0..6usize)
            .map(|i| tx(&sg[i], 1, TxKind::Call(sg[(i + 2) % 6].address()), 10, &[]))
            .collect(),
    );

    let (r1, d1) = execute_block_sequential(&database, None, env(), &first).unwrap();
    let seq2 = execute_block_sequential(&database, Some(&d1), env(), &second).unwrap();

    let (declined, learned, receipts, delta) = with_pool(
        PoolConfig {
            workers: nz(4),
            prune_batch: nz(8),
            // No amount of work per transaction is ever "worth it", so the
            // only block that runs in parallel is the one taken before any
            // measurement exists.
            parallel_worth_ns: u64::MAX,
            ..Default::default()
        },
        |pool| {
            let out1 = pool
                .run_block(
                    vec![database.clone(); 4],
                    PendingDelta::new(),
                    env(),
                    &first,
                    &Stats::default(),
                )
                .expect("first block");
            assert!(!out1.declined, "a fresh pool has nothing to decline on");
            assert_identical(&(r1.clone(), d1.clone()), &out1.receipts, &out1.delta, "b1");

            let out2 = pool
                .run_block(
                    vec![database.clone(); 4],
                    out1.delta.clone(),
                    env(),
                    &second,
                    &Stats::default(),
                )
                .expect("second block");
            (out2.declined, out2.learned_tx_ns, out2.receipts, out2.delta)
        },
    );

    assert!(declined, "the pool should have declined rather than lose");
    assert_identical(&seq2, &receipts, &delta, "declined block");
    // The trap door: a declined block must still teach the pool what a
    // transaction costs. Otherwise one cheap block disables the engine
    // for the rest of the run.
    assert!(
        learned > 0,
        "declining stopped the measurement — the gate can never reopen"
    );
}

/// With the gate wide open, the pool always executes in parallel, so the
/// decline path is a policy choice, not a silent change in behavior.

#[test]
fn an_open_gate_never_declines() {
    use kardamom_stm::execute::{PoolConfig, with_pool};
    let sg = signers(6);
    let database = db(&sg);
    let block = records(
        (0..6usize)
            .map(|i| tx(&sg[i], 0, TxKind::Call(sg[(i + 1) % 6].address()), 100, &[]))
            .collect(),
    );
    let seq = execute_block_sequential(&database, None, env(), &block).unwrap();

    with_pool(
        PoolConfig {
            workers: nz(4),
            prune_batch: nz(8),
            parallel_worth_ns: 0,
            ..Default::default()
        },
        |pool| {
            // Round 0 also checks against the sequential oracle; every
            // later round only re-checks the gate stays open.
            let out0 = pool
                .run_block(
                    vec![database.clone(); 4],
                    PendingDelta::new(),
                    env(),
                    &block,
                    &Stats::default(),
                )
                .expect("block");
            assert!(!out0.declined, "round 0 declined with the gate open");
            assert_identical(&seq, &out0.receipts, &out0.delta, "open gate");
            let mut base = out0.delta.clone();
            for round in 1..3 {
                let out = pool
                    .run_block(
                        vec![database.clone(); 4],
                        base.clone(),
                        env(),
                        &block,
                        &Stats::default(),
                    )
                    .expect("block");
                assert!(!out.declined, "round {round} declined with the gate open");
                base = out.delta.clone();
            }
        },
    );
}

/// One `hot_chain_streams_through_the_fifo` attempt at `workers`.
/// Asserts correctness (no wound, one hot domain, byte-identical),
/// unconditionally; returns whether this attempt also hit the eager
/// streaming shape (fifo-covered, few edges), which only some attempts
/// reach when the host descheduled the feed thread.
///
/// This test pins the FIFO scheduler's mechanics (eager coverage,
/// single-worker domains). The bag scheduler has neither; it is pinned
/// by `bag_hot_chain_byte_identical`.
fn eager_chain_shape_once(
    workers: usize,
    database: &MockStateDatabase,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
    seq: &(Vec<kardamom_types::Receipt>, PendingDelta),
    stats: &Stats,
) -> bool {
    let out = kardamom_stm::execute::with_pool(
        kardamom_stm::execute::PoolConfig {
            workers: nz(workers),
            scheduler: kardamom_stm::execute::Scheduler::Fifo(
                kardamom_stm::execute::FifoOptions::default(),
            ),
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
    );
    assert_eq!(out.wounds, 0, "an ordered chain must never wound");
    assert_eq!(
        out.dispatch.iter().filter(|c| **c > 0).count(),
        1,
        "one hot domain must land on one worker: {:?}",
        out.dispatch
    );
    assert_identical(
        seq,
        &out.receipts,
        &out.delta,
        &format!("eager chain w={workers}"),
    );
    // The point of eager mode: links seen pending on the same worker
    // are FIFO-covered, not edged. That is 23 counter links plus
    // sender links, minus whatever completed at admission.
    out.fifo_covered >= 20 && out.edges <= 4
}

/// The classification asserts inside [`eager_chain_shape_once`] need
/// the streaming shape: the feed admits links while their predecessors
/// are still queued. Workers legitimately outrun the feed when the
/// host deschedules the feed thread. Predecessors then complete before
/// admission (the engine's "p already finished and published, no edge
/// needed" path), and both counters degrade with no engine fault.
/// Correctness is asserted on every attempt; the streaming shape is
/// asserted on at least one of 20.
fn check_eager_chain(
    workers: usize,
    database: &MockStateDatabase,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
    seq: &(Vec<kardamom_types::Receipt>, PendingDelta),
    stats: &Stats,
) {
    let shaped = (0..20).any(|_| eager_chain_shape_once(workers, database, recs, seq, stats));
    assert!(
        shaped,
        "20 attempts, workers outran the feed every time (w={workers})"
    );
}

/// Eager chain mode: a hot domain's chain must stream into its owner's
/// FIFO at admission, ordered by queue position instead of by edges, and
/// still be byte-identical. The read-modify-write counter makes any
/// ordering mistake visible in state, since the final count and every
/// intermediate receipt depend on execution order, so this cannot pass
/// by luck.
#[test]
fn hot_chain_streams_through_the_fifo() {
    let sg = signers(3);
    let database = db(&sg);
    // 24 increments of one slot: a single 24-link chain interleaved with
    // three 8-link sender chains, all hashing to the same worker.
    let recs = records(counter_chain(&sg, 8));
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for workers in [1, 4] {
        check_eager_chain(workers, &database, &recs, &seq, &stats);
    }
    let key = (COUNTER, B256::ZERO);
    assert_eq!(seq.1.storage.get(&key), Some(&U256::from(24u64)));
}

/// Bag scheduler (flag-gated v1): one shared runnable set, no
/// per-worker queues, no stealing, no eager coverage. Every shape that
/// pins the FIFO scheduler must stay byte-identical under the bag too:
/// chains (every dependency is an edge), racing lying-stats repetitions,
/// and plain transfers, across worker counts.

#[test]
fn bag_scheduler_byte_identical() {
    let sg = signers(4);
    let database = db(&sg);
    let run_bag = |recs: &[(TxIndex, BPosition, TxEnvelope)], stats: &Stats, workers: usize| {
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers: nz(workers),
                scheduler: kardamom_stm::execute::Scheduler::Bag,
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
    };
    // Chained counter (canonical order is the only right answer).
    let recs = records(counter_chain(&sg, 8));
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for workers in [1, 2, 4] {
        let out = run_bag(&recs, &stats, workers);
        assert_eq!(out.wounds, 0, "ordered chain must never wound (bag)");
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("bag chain w={workers}"),
        );
    }
    // Lying stats: racing increments, wound/repair must stay identical.
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
    let recs2 = records(counter_block(&sg, 0));
    let seq2 = execute_block_sequential(&database, None, env(), &recs2).unwrap();
    for rep in 0..25 {
        let out = run_bag(&recs2, &lying, 4);
        assert_identical(
            &seq2,
            &out.receipts,
            &out.delta,
            &format!("bag lying rep={rep}"),
        );
    }
    // Transfers with value flows.
    let recs3 = records(vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 500, &[]),
        tx(&sg[1], 0, TxKind::Call(sg[2].address()), 300, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[2].address()), 100, &[]),
        tx(&sg[2], 0, TxKind::Call(sg[3].address()), 200, &[]),
    ]);
    let seq3 = execute_block_sequential(&database, None, env(), &recs3).unwrap();
    for workers in [1, 4] {
        let out = run_bag(&recs3, &Stats::default(), workers);
        assert_identical(
            &seq3,
            &out.receipts,
            &out.delta,
            &format!("bag transfers w={workers}"),
        );
    }
}

/// The bag scheduler on the same hot chain: no coverage, no
/// single-worker domain, only edges and chain-local hand-off. Must stay
/// byte-identical with zero wounds at every worker count.

#[test]
fn bag_hot_chain_byte_identical() {
    let sg = signers(3);
    let database = db(&sg);
    let recs = records(counter_chain(&sg, 8));
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for (workers, rep) in shard_worker_pairs([1, 2, 4], [0, 1, 2, 3, 4]) {
        let out = execute_block_stm(&database, None, env(), &recs, &stats, nz(workers)).unwrap();
        assert_eq!(out.wounds, 0, "an ordered chain must never wound (bag)");
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("bag hot chain w={workers} rep={rep}"),
        );
    }
    let key = (COUNTER, B256::ZERO);
    assert_eq!(seq.1.storage.get(&key), Some(&U256::from(24u64)));
}
