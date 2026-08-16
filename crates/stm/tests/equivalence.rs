//! THE P2 invariant (spec "Invariants" #1): byte-identical receipts +
//! delta vs sequential execution — same write-set hashes (accumulator
//! fixup included), same cumulative gas, same logs — regardless of
//! schedule quality, worker count, or interleaving. Prediction quality
//! may only ever cost throughput (fallback), never bytes.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, TxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs};
use kardamom_stm::execute::{execute_block_sequential, execute_block_stm};
use kardamom_types::{BPosition, TxEnvelope};

const CHAIN_ID: u64 = 412346;
/// SLOAD s0, PUSH1 1, ADD, PUSH1 0, SSTORE, STOP — an RMW counter: the
/// result depends on execution order, so any ordering bug changes bytes.
const COUNTER: Address = Address::with_last_byte(0xC0);
const COUNTER_CODE: [u8; 10] = [0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55, 0x00];
const COUNTER_SEL: [u8; 4] = [0xAA, 0xBB, 0xCC, 0xDD];

fn signers(n: usize) -> Vec<PrivateKeySigner> {
    // Deterministic dev keys: index-derived, test-only.
    (0..n)
        .map(|i| {
            let mut seed = [0u8; 32];
            seed[31] = i as u8 + 1;
            PrivateKeySigner::from_bytes(&B256::from(seed)).unwrap()
        })
        .collect()
}

fn tx(signer: &PrivateKeySigner, nonce: u64, to: TxKind, value: u64, input: &[u8]) -> TxEnvelope {
    let mut t = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1_000_000_000, // nonzero: every tx credits the fee sink
        gas_limit: 500_000,
        to,
        value: U256::from(value),
        input: AlloyBytes::copy_from_slice(input),
    };
    let sig = signer.sign_transaction_sync(&mut t).unwrap();
    let env = alloy_consensus::TxEnvelope::Legacy(t.into_signed(sig));
    let mut raw = Vec::new();
    env.encode_2718(&mut raw);
    TxEnvelope {
        correlation_id: 0,
        raw_tx: bytes::Bytes::from(raw),
        sender: signer.address(),
        tx_hash: *env.tx_hash(),
    }
}

fn db(signers: &[PrivateKeySigner]) -> MockStateDatabase {
    let counter_hash = keccak256(COUNTER_CODE);
    let mut b = MockStateDatabase::builder()
        .account(COUNTER, U256::ZERO, 1, counter_hash)
        .code(counter_hash, COUNTER_CODE.to_vec().into());
    for s in signers {
        b = b.account(s.address(), U256::from(10u128.pow(18)), 0, B256::ZERO);
    }
    b.build()
}

fn env() -> ExecEnv {
    ExecEnv {
        chain_id: CHAIN_ID,
        block_number: 1,
        l2_timestamp: 1_700_000_000,
    }
}

fn records(envs: Vec<TxEnvelope>) -> Vec<(TxIndex, BPosition, TxEnvelope)> {
    envs.into_iter()
        .enumerate()
        .map(|(i, e)| (TxIndex(i as u64), BPosition::from_index(i as u64), e))
        .collect()
}

fn assert_identical(
    seq: &(Vec<kardamom_types::Receipt>, PendingDelta),
    stm_receipts: &[kardamom_types::Receipt],
    stm_delta: &PendingDelta,
    label: &str,
) {
    assert_eq!(
        seq.0, stm_receipts,
        "{label}: receipts must be byte-identical"
    );
    assert_eq!(
        seq.1.accounts, stm_delta.accounts,
        "{label}: delta accounts must match"
    );
    assert_eq!(
        seq.1.storage, stm_delta.storage,
        "{label}: delta storage must match"
    );
    assert_eq!(seq.1.code, stm_delta.code, "{label}: delta code must match");
}

/// Stats that KNOW the counter selector writes the fixed slot 0 — the
/// trained/predicted-conflict path (chained, no fallback expected).
fn counter_stats() -> Stats {
    let obs: Vec<TxObs> = (0..4)
        .map(|i| TxObs {
            index: i,
            block: 1,
            sender: Address::with_last_byte(i as u8 + 0x10),
            to: Some(COUNTER),
            selector: Some(COUNTER_SEL),
            args: Vec::new(),
            gas: 30_000,
            has_value: false,
            reads: vec![Cell::Slot(COUNTER, B256::ZERO)],
            writes: vec![Cell::Slot(COUNTER, B256::ZERO)],
        })
        .collect();
    Stats::learn(&obs)
}

#[test]
fn transfers_byte_identical_across_worker_counts() {
    let sg = signers(4);
    let database = db(&sg);
    // Interleaved same-sender chains + cross transfers, value flows that
    // depend on order within a chain.
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 500, &[]),
        tx(&sg[1], 0, TxKind::Call(sg[2].address()), 300, &[]),
        tx(&sg[2], 0, TxKind::Call(sg[3].address()), 200, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[2].address()), 100, &[]),
        tx(&sg[3], 0, TxKind::Call(sg[0].address()), 50, &[]),
        tx(&sg[1], 1, TxKind::Call(sg[3].address()), 25, &[]),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    for workers in [1, 2, 4, 8] {
        let out =
            execute_block_stm(&database, None, env(), &recs, &Stats::default(), workers).unwrap();
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

#[test]
fn trained_contention_chains_and_matches() {
    let sg = signers(3);
    let database = db(&sg);
    // Three RMW increments of the SAME slot from distinct senders — the
    // canonical order is the only correct result (1, 2, 3).
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for workers in [1, 4] {
        let out = execute_block_stm(&database, None, env(), &recs, &stats, workers).unwrap();
        assert!(
            !out.fallback,
            "trained fixed-slot conflict must schedule as a chain, not convict"
        );
        // The new expression of "the counter chain exists": all three
        // contending txs hash to the SAME domain, so they land on ONE
        // worker queue and execute in canonical FIFO order — no
        // cross-thread coordination needed for the common conflict.
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
    // Final state check against the semantics: slot0 == 3.
    let key = (COUNTER, B256::ZERO);
    assert_eq!(seq.1.storage.get(&key), Some(&U256::from(3u64)));
}

#[test]
fn cold_calls_are_barriers_and_match() {
    let sg = signers(3);
    let database = db(&sg);
    // No stats at all: the counter calls are COLD (selector never seen) —
    // barriers serialize them at their canonical positions.
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(sg[2].address()), 100, &[]),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let out = execute_block_stm(&database, None, env(), &recs, &Stats::default(), 4).unwrap();
    assert_eq!(out.cold, 2);
    assert!(!out.fallback, "barriers order cold txs — no conviction");
    assert_identical(&seq, &out.receipts, &out.delta, "cold barriers");
}

#[test]
fn wrongly_trained_stats_still_produce_identical_bytes() {
    // Stats claim the counter writes a SENDER-DERIVED slot (false: it
    // writes fixed slot 0) ⇒ predicted-independent ⇒ races are possible;
    // validation + fallback must keep the bytes identical on every rep.
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = {
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
        let out = execute_block_stm(&database, None, env(), &recs, &lying_stats, 4).unwrap();
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
    // Not asserted — a fast machine may win every race — but visible when
    // it happens: the fallback path itself produced identical bytes above.
    eprintln!("lying-stats reps: {fallbacks}/25 fell back");
}

#[test]
fn deploy_then_call_in_one_block_matches() {
    let sg = signers(2);
    let database = db(&sg);
    // Init code deploying the SLOAD runtime [60 00 54 00]:
    // PUSH4 runtime, PUSH1 0, MSTORE, PUSH1 4, PUSH1 28, RETURN.
    let init: &[u8] = &[
        0x63, 0x60, 0x00, 0x54, 0x00, 0x60, 0x00, 0x52, 0x60, 0x04, 0x60, 0x1c, 0xf3,
    ];
    let created = sg[0].address().create(0);
    let envs = vec![
        tx(&sg[0], 0, TxKind::Create, 0, init),
        // A different sender calls the just-created contract — the
        // in-block deploy-then-call shape (the burst-block lesson). The
        // predictor cannot see the dependency (tier-2 stats don't exist
        // for a brand-new address); validation must catch any race.
        tx(&sg[1], 0, TxKind::Call(created), 0, &[]),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    assert!(
        seq.0[0].status && seq.0[1].status,
        "setup: both must succeed"
    );
    for rep in 0..10 {
        let out = execute_block_stm(&database, None, env(), &recs, &Stats::default(), 4).unwrap();
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("deploy-call rep={rep}"),
        );
    }
}

#[test]
fn base_delta_layer_is_visible() {
    // Block 2 semantics: the pre-block delta (block 1's writes) must be
    // the block-input view for both engines.
    let sg = signers(2);
    let database = db(&sg);
    let e = env();
    let b1 = records(vec![tx(&sg[0], 0, TxKind::Call(sg[1].address()), 700, &[])]);
    let (_, d1) = execute_block_sequential(&database, None, e, &b1).unwrap();
    let e2 = ExecEnv {
        block_number: 2,
        ..e
    };
    let b2 = records(vec![
        tx(&sg[1], 0, TxKind::Call(sg[0].address()), 600, &[]),
        tx(&sg[0], 1, TxKind::Call(sg[1].address()), 100, &[]),
    ]);
    let seq = execute_block_sequential(&database, Some(&d1), e2, &b2).unwrap();
    let out = execute_block_stm(&database, Some(&d1), e2, &b2, &Stats::default(), 4).unwrap();
    assert!(!out.fallback);
    assert_identical(&seq, &out.receipts, &out.delta, "base layer");
}

/// The scheduler's structural invariant: every admitted tx occupies
/// exactly ONE node and leaves the graph exactly ONCE. Registering twice
/// is unreachable through the public API (the local index comes from the
/// session's own counter, so no caller can name an occupied slot), and
/// leaving twice is counted rather than assumed — `double_exit` must be
/// zero, because a second exit would strand every edge registered in
/// between and hang the block.
#[test]
fn each_tx_occupies_one_node_and_exits_once() {
    use kardamom_stm::execute::{PoolConfig, with_pool};
    let sg = signers(4);
    let database = db(&sg);
    // Same sender repeatedly (one chain), distinct senders (independent),
    // and cold calls (barriers) — every admission path in one block.
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
    let out = with_pool(
        PoolConfig {
            workers: 4,
            prune_batch: 8,
            ..Default::default()
        },
        |pool| {
            pool.run_block(
                vec![database.clone(); 4],
                PendingDelta::new(),
                env(),
                &recs,
                &Stats::default(),
            )
            .expect("block must drain")
        },
    );
    assert_eq!(out.receipts.len(), n, "one receipt per admitted tx");
    assert_eq!(
        out.dispatch.iter().sum::<u32>(),
        n as u32,
        "each tx dispatched exactly once: {:?}",
        out.dispatch
    );
    assert_eq!(out.double_exit, 0, "no tx may leave the graph twice");
}

/// Every admitted tx leaves the graph, so a block always drains — and if a
/// future change ever strands an edge, `seal` fail-stops with diagnostics
/// instead of freezing the exec thread. This pins the healthy path: a
/// block with barriers, chains and independent work drains promptly.
#[test]
fn every_block_drains() {
    use kardamom_stm::execute::{PoolConfig, with_pool};
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
    let out = with_pool(
        PoolConfig {
            workers: 4,
            prune_batch: 8,
            ..Default::default()
        },
        |pool| {
            pool.run_block(
                vec![database.clone(); 4],
                PendingDelta::new(),
                env(),
                &recs,
                &Stats::default(),
            )
            .expect("block must drain")
        },
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "block took {:?} — the watchdog would have fired",
        started.elapsed()
    );
    assert_eq!(out.cold, 2);
    assert_identical(&seq, &out.receipts, &out.delta, "drain");
}

/// The pool must never be a pessimization. Plain transfers cost far less
/// per transaction than parallel execution costs to coordinate, so once
/// the pool has MEASURED that, it declines the block and runs it
/// sequentially — and the result is still byte-identical, because
/// declining routes through the sequential executor itself rather than a
/// second implementation of it.
///
/// The threshold is INJECTED rather than left at its measured default:
/// the gate is a timing decision, and a test that depends on how loaded
/// the machine is would pass alone and fail in a parallel test run — as
/// the first version of this test did.
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
            workers: 4,
            prune_batch: 8,
            // No amount of work per tx is ever "worth it" — so the only
            // block that runs in parallel is the one taken before any
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
    // The trap door: a declined block MUST still teach the pool what a
    // transaction costs, or one cheap block disables the engine for the
    // rest of the run.
    assert!(
        learned > 0,
        "declining stopped the measurement — the gate can never reopen"
    );
}

/// With the gate wide open the pool always executes in parallel, so the
/// decline path is a policy and not a silent behaviour change.
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
            workers: 4,
            prune_batch: 8,
            parallel_worth_ns: 0,
            ..Default::default()
        },
        |pool| {
            let mut base = PendingDelta::new();
            for round in 0..3 {
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
                if round == 0 {
                    assert_identical(&seq, &out.receipts, &out.delta, "open gate");
                }
                base = out.delta.clone();
            }
        },
    );
}

/// EAGER CHAIN MODE: a hot domain's chain must stream into its owner's
/// FIFO at admission — ordered by queue position, not by edges — and
/// still be byte-identical. The RMW counter makes any ordering mistake
/// visible in state (the final count and every intermediate receipt
/// depend on execution order), so this cannot pass by luck.
#[test]
fn hot_chain_streams_through_the_fifo() {
    let sg = signers(3);
    let database = db(&sg);
    // 24 increments of one slot: a single 24-link chain interleaved with
    // three 8-link sender chains, all hashing to the same worker.
    let envs: Vec<TxEnvelope> = (0..8u64)
        .flat_map(|n| sg.iter().map(move |s| (s, n)).collect::<Vec<_>>())
        .map(|(s, n)| tx(s, n, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
        .collect();
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();
    let stats = counter_stats();
    for workers in [1, 4] {
        // The classification asserts below need the STREAMING shape: the
        // feed admitting links while their predecessors are still queued.
        // Workers legitimately outrun the feed when the host deschedules
        // the feed thread — predecessors then complete before admission
        // (the engine's "p already finished and published — no edge
        // needed" path) and both counters degrade with no engine fault.
        // Correctness is asserted on EVERY attempt; the streaming shape
        // on at least one.
        let mut shaped = false;
        for _attempt in 0..20 {
            let out = execute_block_stm(&database, None, env(), &recs, &stats, workers).unwrap();
            assert_eq!(out.wounds, 0, "an ordered chain must never wound");
            assert_eq!(
                out.dispatch.iter().filter(|c| **c > 0).count(),
                1,
                "one hot domain must land on one worker: {:?}",
                out.dispatch
            );
            assert_identical(
                &seq,
                &out.receipts,
                &out.delta,
                &format!("eager chain w={workers}"),
            );
            // The point of eager mode: links seen PENDING on the same
            // worker are FIFO-covered, not edged — 23 counter links +
            // sender links, minus whatever completed at admission.
            if out.fifo_covered >= 20 && out.edges <= 4 {
                shaped = true;
                break;
            }
        }
        assert!(
            shaped,
            "20 attempts, workers outran the feed every time (w={workers})"
        );
    }
    let key = (COUNTER, B256::ZERO);
    assert_eq!(seq.1.storage.get(&key), Some(&U256::from(24u64)));
}

/// STREAMING RELEASE (spec P3): `submit_streaming` ships the folded
/// delta before receipts — and, speculatively, before the validation
/// verdict. Invariants pinned here, per rep and mode:
/// - wounds == 0  ⇒ exactly ONE release, not corrected;
/// - wounds  > 0  ⇒ speculative mode sends TWO (stale speculative, then
///   corrected), conservative sends ONE (already-final);
/// - the LAST release always byte-equals the outcome's delta;
/// - receipts + delta stay byte-identical to sequential in every case.
///
/// The lying-stats generator makes wounds actually fire across reps, so
/// the correction leg is exercised for real, not vacuously.
#[test]
fn streaming_release_and_wound_correction() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = {
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
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[3], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();

    for speculative in [true, false] {
        let mut wound_reps = 0usize;
        let mut reps = 0usize;
        // Hunt for the wound leg: races are timing-dependent, and a fast
        // machine may win every one (the sibling lying-stats test's
        // note) — so rep until a wound is seen or the budget runs out,
        // asserting the protocol on every rep either way.
        for rep in 0..200 {
            let (out, releases) = kardamom_stm::execute::with_pool(
                kardamom_stm::execute::PoolConfig {
                    workers: 4,
                    ..Default::default()
                },
                |pool| {
                    let mut sess = pool
                        .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                        .unwrap();
                    for (t, p, e) in &recs {
                        sess.push_tx(*t, *p, e.clone()).unwrap();
                    }
                    let (dtx, drx) = std::sync::mpsc::channel();
                    let ticket = sess.submit_streaming(dtx, speculative).unwrap();
                    let out = ticket.wait().unwrap();
                    let releases: Vec<kardamom_stm::execute::DeltaRelease> =
                        drx.try_iter().collect();
                    (out, releases)
                },
            );
            let label = format!("streaming spec={speculative} rep={rep}");
            assert_identical(&seq, &out.receipts, &out.delta, &label);
            if out.wounds == 0 {
                assert_eq!(releases.len(), 1, "{label}: clean block, one release");
                assert!(!releases[0].corrected, "{label}: clean release");
            } else {
                wound_reps += 1;
                if speculative {
                    assert_eq!(
                        releases.len(),
                        2,
                        "{label}: wound must re-issue the release"
                    );
                    assert!(!releases[0].corrected);
                    assert!(releases[1].corrected, "{label}: second release corrects");
                } else {
                    assert_eq!(
                        releases.len(),
                        1,
                        "{label}: conservative releases once, post-verdict"
                    );
                    assert!(!releases[0].corrected);
                }
            }
            let last = releases.last().expect("at least one release");
            assert_eq!(
                last.delta.accounts, out.delta.accounts,
                "{label}: final release delta == outcome delta (accounts)"
            );
            assert_eq!(
                last.delta.storage, out.delta.storage,
                "{label}: final release delta == outcome delta (storage)"
            );
            assert_eq!(
                last.delta.code, out.delta.code,
                "{label}: final release delta == outcome delta (code)"
            );
            reps = rep + 1;
            if wound_reps > 0 && rep >= 24 {
                break; // wound leg exercised and a full base run done
            }
        }
        // Not asserted — a fast machine may win every race — but loud
        // when the correction leg went unexercised.
        eprintln!("streaming spec={speculative}: {wound_reps}/{reps} reps wounded");
    }
}

/// THE P3b ADVERSARIAL CASE (spec "Wound-abort adversarial"): block 2
/// is built, fed, and submitted with DEFERRED layers while block 1
/// still executes, binds on block 1's speculative release, and runs
/// while block 1 is still validating. When the lying-stats race fires a wound in block
/// 1, the speculative release was wrong — the consumer aborts block 2,
/// rebuilds it on the `corrected` release, and both blocks must come
/// out byte-identical to the sequential chain. When no wound fires,
/// block 2's speculative run IS the answer — asserted identical too,
/// which is what makes the gamble sound: either way, bytes.
#[test]
fn speculative_pipeline_wound_aborts_and_recovers() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = {
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
    // Block 1: the wound-prone racing increments. Block 2: four MORE
    // increments from the same senders — its every receipt depends on
    // block 1's final counter value, so a stale layer cannot pass the
    // byte-identical assert.
    let recs1 = records(
        (0..4)
            .map(|i| tx(&sg[i], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
    let recs2 = records(
        (0..4)
            .map(|i| tx(&sg[i], 1, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
    let seq1 = execute_block_sequential(&database, None, env(), &recs1).unwrap();
    let env2 = ExecEnv {
        block_number: 2,
        ..env()
    };
    let seq2 = execute_block_sequential(&database, Some(&seq1.1), env2, &recs2).unwrap();

    let mut wound_reps = 0usize;
    let mut reps = 0usize;
    for rep in 0..200 {
        let label = format!("spec-pipeline rep={rep}");
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers: 4,
                ..Default::default()
            },
            |pool| {
                let submit_block2 = |layer: std::sync::Arc<PendingDelta>| {
                    let mut sess = pool
                        .begin_block_layered(
                            vec![database.clone(); 4],
                            PendingDelta::new(),
                            vec![layer],
                            env2,
                            &lying_stats,
                        )
                        .unwrap();
                    for (t, p, e) in &recs2 {
                        sess.push_tx(*t, *p, e.clone()).unwrap();
                    }
                    let (d2tx, _d2rx) = std::sync::mpsc::channel();
                    sess.submit_streaming(d2tx, true).unwrap()
                };
                // Block 1: streaming speculative.
                let mut sess1 = pool
                    .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                    .unwrap();
                for (t, p, e) in &recs1 {
                    sess1.push_tx(*t, *p, e.clone()).unwrap();
                }
                let (d1tx, d1rx) = std::sync::mpsc::channel();
                let ticket1 = sess1.submit_streaming(d1tx, true).unwrap();
                // THE PRODUCTION SEQUENCING (late-bound layers): block 2
                // is built, fed, and SUBMITTED while block 1 still
                // executes — before its read base exists. Workers gate
                // on the bind.
                let (mut sess2, binder2) = pool
                    .begin_block_deferred(
                        vec![database.clone(); 4],
                        PendingDelta::new(),
                        env2,
                        &lying_stats,
                    )
                    .unwrap();
                for (t, p, e) in &recs2 {
                    sess2.push_tx(*t, *p, e.clone()).unwrap();
                }
                let (d2tx, _d2rx) = std::sync::mpsc::channel();
                let ticket2 = sess2.submit_streaming(d2tx, true).unwrap();
                // The SPECULATIVE release: block 1 is still validating.
                let rel1 = d1rx.recv().expect("speculative release");
                assert!(!rel1.corrected, "{label}: first release is speculative");
                binder2.bind(vec![rel1.delta.clone()]).unwrap();
                // Block 1's verdict.
                let out1 = ticket1.wait().unwrap();
                assert_identical(&seq1, &out1.receipts, &out1.delta, &label);
                if out1.wounds == 0 {
                    // The gamble held: block 2's speculative run is final.
                    let out2 = ticket2.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                    assert!(d1rx.try_recv().is_err(), "{label}: no extra release");
                } else {
                    wound_reps += 1;
                    // The release was WRONG. Unwind block 2 entirely:
                    // abort, discard its ticket (Ok or Err — either is
                    // garbage), rebuild on the corrected delta.
                    let corrected = d1rx.recv().expect("corrected release");
                    assert!(corrected.corrected, "{label}: wound re-issues");
                    pool.abort_active();
                    let _ = ticket2.wait();
                    let ticket2b = submit_block2(corrected.delta.clone());
                    let out2 = ticket2b.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                }
            },
        );
        reps = rep + 1;
        if wound_reps > 0 && rep >= 24 {
            break;
        }
    }
    eprintln!("spec-pipeline: {wound_reps}/{reps} reps wounded");
}

/// A deferred session whose consumer never binds must not hang: abort
/// resolves its ticket (error or stale-Ok), loudly and promptly.
#[test]
fn deferred_never_bound_aborts_cleanly() {
    let sg = signers(2);
    let database = db(&sg);
    let recs = records(vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 5, &[]),
        tx(&sg[1], 0, TxKind::Call(sg[0].address()), 3, &[]),
    ]);
    kardamom_stm::execute::with_pool(
        kardamom_stm::execute::PoolConfig {
            workers: 2,
            ..Default::default()
        },
        |pool| {
            let stats = Stats::default();
            let (mut sess, _binder) = pool
                .begin_block_deferred(
                    vec![database.clone(); 2],
                    PendingDelta::new(),
                    env(),
                    &stats,
                )
                .unwrap();
            for (t, p, e) in &recs {
                sess.push_tx(*t, *p, e.clone()).unwrap();
            }
            let (dtx, _drx) = std::sync::mpsc::channel();
            let ticket = sess.submit_streaming(dtx, true).unwrap();
            pool.abort_active();
            let r = ticket.wait();
            assert!(
                r.is_err(),
                "never-bound block must resolve to the abort error"
            );
        },
    );
}

/// MV-AS-LAYER (spec P3b): block 2 binds on block 1's EARLY release —
/// the mv cache itself, shipped before block 1's fold, hash, or
/// validation ran. Wound in block 1 ⇒ the corrected DELTA release
/// arrives, block 2 aborts and rebuilds on the delta layer (never the
/// stale mv). Byte-identical to the sequential chain in every path.
#[test]
fn mv_as_layer_pipeline_wound_aborts_and_recovers() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = {
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
    let recs1 = records(
        (0..4)
            .map(|i| tx(&sg[i], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
    let recs2 = records(
        (0..4)
            .map(|i| tx(&sg[i], 1, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
    let seq1 = execute_block_sequential(&database, None, env(), &recs1).unwrap();
    let env2 = ExecEnv {
        block_number: 2,
        ..env()
    };
    let seq2 = execute_block_sequential(&database, Some(&seq1.1), env2, &recs2).unwrap();

    let mut wound_reps = 0usize;
    let mut reps = 0usize;
    for rep in 0..200 {
        let label = format!("mv-layer rep={rep}");
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers: 4,
                ..Default::default()
            },
            |pool| {
                // Block 1: early mv release + fold delta release.
                let mut sess1 = pool
                    .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                    .unwrap();
                for (t, p, e) in &recs1 {
                    sess1.push_tx(*t, *p, e.clone()).unwrap();
                }
                let (mv1_tx, mv1_rx) = std::sync::mpsc::channel();
                let (d1tx, d1rx) = std::sync::mpsc::channel();
                let ticket1 = sess1.submit_streaming_mv(mv1_tx, d1tx).unwrap();
                // Block 2: deferred, submitted before block 1's release.
                let (mut sess2, binder2) = pool
                    .begin_block_deferred(
                        vec![database.clone(); 4],
                        PendingDelta::new(),
                        env2,
                        &lying_stats,
                    )
                    .unwrap();
                for (t, p, e) in &recs2 {
                    sess2.push_tx(*t, *p, e.clone()).unwrap();
                }
                let (d2tx, _d2rx) = std::sync::mpsc::channel();
                let ticket2 = sess2.submit_streaming(d2tx, true).unwrap();
                // The EARLY release: block 1's mv, pre-fold, pre-verdict.
                let rel1 = mv1_rx.recv().expect("early mv release");
                binder2
                    .bind_with(
                        vec![rel1.mv.clone()],
                        Vec::new(),
                        Some(rel1.sink_final.clone()),
                    )
                    .unwrap();
                let out1 = ticket1.wait().unwrap();
                assert_identical(&seq1, &out1.receipts, &out1.delta, &label);
                if out1.wounds == 0 {
                    let out2 = ticket2.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                } else {
                    wound_reps += 1;
                    // The mv layer is stale. Unwind block 2 onto the
                    // CORRECTED delta.
                    let first = d1rx.recv().expect("first delta release");
                    let corrected = if first.corrected {
                        first
                    } else {
                        d1rx.recv().expect("corrected delta release")
                    };
                    assert!(corrected.corrected, "{label}: wound re-issues");
                    pool.abort_active();
                    let _ = ticket2.wait();
                    let mut sess = pool
                        .begin_block_layered(
                            vec![database.clone(); 4],
                            PendingDelta::new(),
                            vec![corrected.delta.clone()],
                            env2,
                            &lying_stats,
                        )
                        .unwrap();
                    for (t, p, e) in &recs2 {
                        sess.push_tx(*t, *p, e.clone()).unwrap();
                    }
                    let (d2btx, _d2brx) = std::sync::mpsc::channel();
                    let out2 = sess.submit_streaming(d2btx, true).unwrap().wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                }
            },
        );
        reps = rep + 1;
        if wound_reps > 0 && rep >= 24 {
            break;
        }
    }
    eprintln!("mv-layer: {wound_reps}/{reps} reps wounded");
}

/// BAG SCHEDULER (flag-gated v1): one shared runnable set, no
/// per-worker queues, no stealing, no eager coverage — every shape that
/// pins the FIFO scheduler must stay byte-identical under the bag too:
/// chains (every dependency an edge), racing lying-stats reps, and
/// plain transfers, across worker counts.
#[test]
fn bag_scheduler_byte_identical() {
    let sg = signers(4);
    let database = db(&sg);
    let run_bag = |recs: &[(TxIndex, BPosition, TxEnvelope)], stats: &Stats, workers: usize| {
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers,
                bag_scheduler: true,
                ..Default::default()
            },
            |pool| {
                pool.run_block(
                    vec![database.clone(); workers.max(1)],
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
    let recs = records(
        (0..8u64)
            .flat_map(|n| sg.iter().map(move |s| (s, n)).collect::<Vec<_>>())
            .map(|(s, n)| tx(s, n, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
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
    let recs2 = records(
        (0..4)
            .map(|i| tx(&sg[i], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
            .collect(),
    );
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
