//! Byte-identical output across worker counts and stat quality: the
//! basic A/B shape, no scheduler or pipeline mechanics.
#![allow(
    clippy::cast_possible_truncation,
    reason = "fixture indices (signer count, tx count) stay far below u8/u32::MAX in every test"
)]

mod common;

use alloy_primitives::{B256, TxKind, U256};
use common::*;
use kardamom_footprint::classifier::Stats;
use kardamom_stm::execute::{execute_block_sequential, execute_block_stm};

#[test]
fn transfers_byte_identical_across_worker_counts() {
    let sg = signers(4);
    let database = db(&sg);
    // Interleaved same-sender chains and cross transfers, with value
    // flows that depend on order within a chain.
    let recs = records(transfer_block(&sg));
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    for workers in [1, 2, 4, 8] {
        let out = execute_block_stm(
            &database,
            None,
            env(),
            &recs,
            &Stats::default(),
            nz(workers),
        )
        .unwrap();
        assert!(!out.fallback, "tier-1 transfers must not fall back");
        assert_eq!(out.cold, 0);
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("transfers w={workers}"),
        );
    }
}

/// Regression for the defect where `predict_us` and `commit_fold_us`
/// always reported zero: the `Metrics` atomics behind them were never
/// written. `push_tx` now times the decode and predict phases directly,
/// and `block_tail` times its own fold. A block with enough transactions
/// must report non-zero for both.

#[test]
fn predict_and_commit_fold_time_are_measured() {
    let sg = signers(4);
    let database = db(&sg);
    let envs: Vec<_> = (0..40)
        .map(|i| {
            let s = &sg[i % sg.len()];
            tx(
                s,
                (i / sg.len()) as u64,
                TxKind::Call(sg[(i + 1) % sg.len()].address()),
                1,
                &[],
            )
        })
        .collect();
    let recs = records(envs);
    let out = execute_block_stm(&database, None, env(), &recs, &Stats::default(), nz(4)).unwrap();
    assert!(
        !out.declined,
        "block must run parallel to exercise the tail fold"
    );
    assert!(
        out.predict_us > 0,
        "predict_us must report the feed thread's prediction time, not zero"
    );
    assert!(
        out.commit_fold_us > 0,
        "commit_fold_us must report the tail thread's fold time, not zero"
    );
    assert!(out.decode_us > 0, "decode_us must report real decode time");
}

#[test]
fn trained_contention_chains_and_matches() {
    let sg = signers(3);
    let database = db(&sg);
    // Three read-modify-write increments of the same slot from distinct
    // senders. The canonical order is the only correct result (1, 2, 3).
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for workers in [1, 4] {
        let out = execute_block_stm(&database, None, env(), &recs, &stats, nz(workers)).unwrap();
        assert!(
            !out.fallback,
            "trained fixed-slot conflict must schedule as a chain, not convict"
        );
        // This shows the counter chain exists: all three contending
        // transactions hash to the same domain, so they land on one
        // worker queue and execute in canonical FIFO order. No
        // cross-thread coordination is needed for this common conflict.
        assert_eq!(
            out.dispatch.iter().filter(|c| **c > 0).count(),
            1,
            "same-domain txs must share one thread: {:?}",
            out.dispatch
        );
        assert_eq!(out.wounds, 0, "an ordered domain must never wound");
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("counter w={workers}"),
        );
    }
    // Final state check against the semantics: slot 0 must equal 3.
    let key = (COUNTER, B256::ZERO);
    assert_eq!(seq.1.storage.get(&key), Some(&U256::from(3u64)));
}

#[test]
fn cold_calls_are_barriers_and_match() {
    let sg = signers(3);
    let database = db(&sg);
    // No stats at all: the counter calls are cold (their selector was
    // never seen). Barriers serialize them at their canonical positions.
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(sg[2].address()), 100, &[]),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let out = execute_block_stm(&database, None, env(), &recs, &Stats::default(), nz(4)).unwrap();
    assert_eq!(out.cold, 2);
    assert!(!out.fallback, "barriers order cold txs — no conviction");
    assert_identical(&seq, &out.receipts, &out.delta, "cold barriers");
}

#[test]
fn wrongly_trained_stats_still_produce_identical_bytes() {
    // Stats falsely claim the counter writes a sender-derived slot (it
    // actually writes fixed slot 0), so the predictor sees the
    // transactions as independent and races become possible. Validation
    // and fallback must keep the bytes identical on every repetition.
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = lying_stats();
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[3], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let mut fallbacks = 0;
    for rep in 0..25 {
        let out = execute_block_stm(&database, None, env(), &recs, &lying_stats, nz(4)).unwrap();
        if out.fallback {
            fallbacks += 1;
        }
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("lying stats rep={rep}"),
        );
    }
    // Not asserted, since a fast machine may win every race, but visible
    // when it happens: the fallback path itself produced identical bytes
    // above.
    eprintln!("lying-stats reps: {fallbacks}/25 fell back");
}

#[test]
fn deploy_then_call_in_one_block_matches() {
    let sg = signers(2);
    let database = db(&sg);
    // Init code that deploys the SLOAD runtime [60 00 54 00]:
    // PUSH4 runtime, PUSH1 0, MSTORE, PUSH1 4, PUSH1 28, RETURN.
    let init: &[u8] = &[
        0x63, 0x60, 0x00, 0x54, 0x00, 0x60, 0x00, 0x52, 0x60, 0x04, 0x60, 0x1c, 0xf3,
    ];
    let created = sg[0].address().create(0);
    let envs = vec![
        tx(&sg[0], 0, TxKind::Create, 0, init),
        // A different sender calls the just-created contract: the
        // in-block deploy-then-call shape. The predictor cannot see the
        // dependency, since tier-2 stats do not exist for a brand-new
        // address, so validation must catch any race.
        tx(&sg[1], 0, TxKind::Call(created), 0, &[]),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    assert!(
        seq.0[0].status && seq.0[1].status,
        "setup: both must succeed"
    );
    for rep in 0..10 {
        let out =
            execute_block_stm(&database, None, env(), &recs, &Stats::default(), nz(4)).unwrap();
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("deploy-call rep={rep}"),
        );
    }
}
