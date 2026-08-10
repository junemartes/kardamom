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
