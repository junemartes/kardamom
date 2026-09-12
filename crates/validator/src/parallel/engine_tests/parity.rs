//! The parity property: parallel seeded batches must produce
//! byte-identical state to sequential execution, including for a
//! conflicting workload (one sender, dependent nonces, shared recipient)
//! where every tx depends on its predecessor. Seeding, not ordering, is
//! what makes that safe. Also covers deposits, cross-chain deliveries,
//! and the K > 1 quantized chunk views.

use alloy_primitives::{B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::PendingDelta;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::error::ExecutorError;
use kardamom_engine::state::MockStateDatabase;

use crate::parallel::{BlockInputs, ClaimIndex, execute_block_parallel};

use super::fixtures::{
    call_tx, dep, env, fund, funded, honest_claims, nz, nz16, seq_capture, seq_delta, test_pool,
    tx, xchain,
};

#[test]
fn parallel_batches_equal_sequential_on_a_fully_dependent_chain() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000AA");
    let snap = funded(&signer);
    // 12 txs from one sender: maximal conflict, since each reads the
    // balance and nonce the previous tx wrote.
    let txs: Vec<BufferedRecord> = (0..12).map(|i| tx(&signer, to, i, 1_000 + i, i)).collect();

    let claims = honest_claims(&snap, &txs);
    let expected = seq_delta(&snap, &txs);

    for batch_size in [1usize, 5, 10] {
        assert_parity_at(batch_size, &snap, &txs, &claims, &expected);
    }
}

/// Execute `txs` in parallel batches of `batch_size` and assert the
/// result is byte-identical to `expected` (the sequential delta), with
/// monotonic per-block cumulative gas.
fn assert_parity_at(
    batch_size: usize,
    snap: &MockStateDatabase,
    txs: &[BufferedRecord],
    claims: &ClaimIndex,
    expected: &PendingDelta,
) {
    let inputs = BlockInputs {
        snapshot: snap,
        parent: None,
        txs,
        claims,
        env: env(),
        granularity: nz16(1),
    };
    let out = execute_block_parallel(&test_pool(), &inputs, nz(batch_size))
        .unwrap_or_else(|e| panic!("batch_size {batch_size}: {e:?}"));
    assert_eq!(
        out.delta.accounts, expected.accounts,
        "batch_size {batch_size}: account state must equal sequential"
    );
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.receipts.len(), txs.len());
    // Block-cumulative gas must be monotonic and match the total.
    let total: u64 = out.receipts.iter().map(|r| r.gas_used).sum();
    assert_eq!(out.receipts.last().unwrap().cumulative_gas_used, total);
    assert!(
        out.receipts
            .windows(2)
            .all(|w| w[0].cumulative_gas_used < w[1].cumulative_gas_used)
    );
}

/// K = 20 end-to-end: quantized wire claims and chunk-aligned batches
/// must be parity-identical to sequential, and a forged chunk claim
/// must stop the process, naming the chunk. This exercises the same-view
/// invariant: both sides pass through the shared `quantize()`.
#[test]
fn quantized_claims_verify_with_aligned_batches() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000CC");
    let snap = funded(&signer);
    let txs: Vec<BufferedRecord> = (0..47).map(|i| tx(&signer, to, i, 100 + i, i)).collect();

    // The executor's view: per-tx capture, then the shared quantize.
    let (expected, bal) = seq_capture(&snap, None, &txs, false);
    let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 20);
    let claims = ClaimIndex::from_alloy(&quantized);

    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &txs,
            claims: &claims,
            env: env(),
            granularity: nz16(20),
        },
        nz(8),
    )
    .expect("quantized parity");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.batches, 3, "47 txs at K=20 -> 3 aligned chunks");

    // Forge a chunk-2 claim: this must stop the process, naming the chunk.
    let mut forged = claims.clone();
    if let Some(w) = forged.balance.get_mut(&to)
        && let Some(e) = w.iter_mut().find(|(i, _)| *i == 2)
    {
        e.1 += U256::from(999u64);
    }
    let err = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &txs,
            claims: &forged,
            env: env(),
            granularity: nz16(20),
        },
        nz(8),
    )
    .expect_err("forged chunk claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("chunk 2"), "must name the chunk: {msg}");
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// Under the pipelined commit, the snapshot can be K blocks stale.
/// Block 2's txs must observe block 1's writes through the parent layer.
#[test]
fn parent_layer_bridges_the_uncommitted_gap() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000DD");
    let snap = funded(&signer);

    // Block 1: nonces 0..3, executed and folded into a parent layer, but
    // never committed to the snapshot. Under StaticSnapshotSource
    // semantics, the mock snapshot still says nonce 0.
    let b1: Vec<BufferedRecord> = (0..4).map(|i| tx(&signer, to, i, 100, i)).collect();
    let claims1 = honest_claims(&snap, &b1);
    let out1 = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &b1,
            claims: &claims1,
            env: env(),
            granularity: nz16(1),
        },
        nz(2),
    )
    .expect("block 1");
    let parent = out1.delta.clone();

    // Block 2: nonces 4..7. Against the bare snapshot, every tx is a
    // nonce-mismatch skip. With the parent layer, they execute.
    let b2: Vec<BufferedRecord> = (4..8).map(|i| tx(&signer, to, i, 100, i)).collect();
    // Build block-2 claims through the same capture path, with a parent,
    // so every record must execute given the parent.
    let (delta, bal) = seq_capture(&snap, Some(&parent), &b2, true);
    let claims2 = ClaimIndex::from_alloy(&bal.into_alloy_bal());

    // Without the parent: the stale-state bug means every tx skips.
    let stale = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &b2,
            claims: &claims2,
            env: env(),
            granularity: nz16(1),
        },
        nz(2),
    );
    assert!(
        stale.is_err(),
        "without the parent layer the block must diverge (skips vs claims)"
    );

    // With the parent: byte-identical to the sequential-with-parent run.
    let out2 = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: Some(&parent),
            txs: &b2,
            claims: &claims2,
            env: env(),
            granularity: nz16(1),
        },
        nz(2),
    )
    .expect("block 2 with parent");
    assert_eq!(out2.delta.accounts, delta.accounts);
    assert!(out2.receipts.iter().all(|r| r.status));
}

#[test]
fn empty_block_is_a_no_op() {
    let snap = MockStateDatabase::builder().build();
    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &[],
            claims: &ClaimIndex::default(),
            env: env(),
            granularity: nz16(1),
        },
        nz(5),
    )
    .unwrap();
    assert!(out.receipts.is_empty() && out.batches == 0);
}

/// A deposit's mint must be claimed in the BAL as a balance write,
/// because later batches seed the recipient's balance from it.
#[test]
fn deposit_mint_seeds_later_batches() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let to = address!("00000000000000000000000000000000000000EE");
    // d starts at zero balance, so only the mint can fund its txs.
    let snap = MockStateDatabase::builder()
        .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
        .build();
    // The CALL-type deposit bumps d's nonce to 1 (revm bumps the caller
    // nonce for `is_call` deposits too), so the spends start at nonce 1.
    let records: Vec<BufferedRecord> = vec![
        dep(d, Some(to), 10u128.pow(18), Vec::new(), 0),
        tx(&signer, to, 1, 1_000, 1),
        tx(&signer, to, 2, 1_000, 2),
    ];

    let claims = honest_claims(&snap, &records);
    // The mint must be visible as a balance claim at the deposit's index.
    assert!(
        claims.balance_seed(d, 2).is_some(),
        "deposit mint must be claimed in the BAL (balance change at index 1)"
    );
    let expected = seq_delta(&snap, &records);

    // batch_size 1: the spends run in batches seeded only from claims.
    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &records,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(1),
    )
    .expect("deposit-seeded parallel block");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 3);
    // The deposit's receipt survives the parallel path intact.
    assert_eq!(out.receipts[0].tx_type, kardamom_types::TX_TYPE_DEPOSIT);
    assert!(out.receipts.iter().all(|r| r.status));
}

/// A CREATE deposit's bytecode is a code claim. A later batch calling
/// the deposited contract must seed the bytes, or the call runs against
/// empty code and does nothing.
#[test]
fn create_deposit_code_seeds_later_calls() {
    let signer = PrivateKeySigner::random();
    let l1_sender = address!("00000000000000000000000000000000000000F1");
    let snap = funded(&signer);
    // Initcode returning runtime `PUSH1 1 PUSH1 0 SSTORE STOP`: every
    // call writes slot 0 = 1.
    let initcode = alloy_primitives::hex::decode("656001600055006000526006601af3").unwrap();
    // CREATE address: keccak(rlp(from, nonce=0)).
    let contract = l1_sender.create(0);
    let records: Vec<BufferedRecord> = vec![
        dep(l1_sender, None, 0, initcode, 0),
        call_tx(&signer, contract, 0, 1),
    ];

    let claims = honest_claims(&snap, &records);
    assert!(
        claims.code_seed(contract, 2).is_some(),
        "CREATE deposit bytecode must be claimed in the BAL"
    );
    let expected = seq_delta(&snap, &records);
    // The call's SSTORE proves it executed real code.
    assert_eq!(
        expected.storage.get(&(contract, B256::ZERO)),
        Some(&U256::from(1u64)),
        "sequential call must hit the deployed contract"
    );

    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &records,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(1),
    )
    .expect("CREATE-deposit-seeded parallel block");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 2);
}

/// Cross-chain deliveries participate in the whole-block path like
/// deposits: buffered, dispatched through the scope-native path, claims
/// verified where produced, writes seeding later batches. Parallel must
/// equal sequential with a 0x7D record mid-block, the `BufferedRecord::XChain`
/// arm.
#[test]
fn xchain_records_execute_in_parallel_blocks() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000F4");
    let snap = funded(&signer);
    let origin = 424_242u64;
    // tx, xchain delivery, tx, xchain delivery, tx — deliveries at
    // bal indices 2 and 4, so with batch_size 1 their claims must seed
    // the following batches.
    let records: Vec<BufferedRecord> = vec![
        tx(&signer, to, 0, 100, 0),
        xchain(origin, 0, to, 1),
        tx(&signer, to, 1, 100, 2),
        xchain(origin, 1, to, 3),
        tx(&signer, to, 2, 100, 4),
    ];

    let claims = honest_claims(&snap, &records);
    let expected = seq_delta(&snap, &records);

    for batch_size in [1usize, 2, 5] {
        assert_xchain_parity_at(batch_size, &snap, &records, &claims, &expected, origin);
    }
}

/// Execute `records` in parallel batches of `batch_size`, assert the
/// result matches `expected` (the sequential delta), and check that both
/// 0x7D delivery receipts land at their expected index with the right
/// identity.
fn assert_xchain_parity_at(
    batch_size: usize,
    snap: &MockStateDatabase,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    expected: &PendingDelta,
    origin: u64,
) {
    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: snap,
            parent: None,
            txs: records,
            claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(batch_size),
    )
    .unwrap_or_else(|e| panic!("batch_size {batch_size}: {e:?}"));
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.receipts.len(), records.len());
    // The deliveries surface as 0x7D receipts keyed by their remote
    // source hash, exactly as on the streaming path.
    for (seq, idx) in [(0u64, 1usize), (1, 3)] {
        let r = &out.receipts[idx];
        assert_eq!(r.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert_eq!(
            r.tx_hash,
            kardamom_types::xchain::remote_source_hash(origin, seq)
        );
        assert_eq!(r.from, kardamom_types::xchain::xchain_tx_sender(origin));
        assert_eq!(r.to, Some(kardamom_types::xchain::INBOX));
    }
}

/// Deposits inside a K > 1 chunk: chunk-aligned batches with a deposit
/// mid-chunk verify and match sequential execution. The deposit's claims
/// are quantized through the same shared ladder as tx claims.
#[test]
fn quantized_chunk_containing_a_deposit_verifies() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let filler = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000F2");
    let snap = fund(
        MockStateDatabase::builder().account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY),
        filler.address(),
    )
    .build();
    // Chunk 1 (K=4): 3 filler txs plus the deposit at bal index 4, so the
    // deposit is chunk-final for d's balance. Chunk 2's spends seed the
    // mint from the deposit's claim alone. The CALL deposit bumps d's
    // nonce to 1, so spends run nonces 1..4.
    let mut records: Vec<BufferedRecord> = (0..3u64).map(|i| tx(&filler, to, i, 100, i)).collect();
    records.push(dep(d, Some(to), 10u128.pow(18), Vec::new(), 3));
    for i in 4..8u64 {
        records.push(tx(&signer, to, i - 3, 500, i));
    }

    // Executor view: per-record capture, then the shared quantize at K=4.
    let (expected, bal) = seq_capture(&snap, None, &records, true);
    let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 4);
    let claims = ClaimIndex::from_alloy(&quantized);

    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &records,
            claims: &claims,
            env: env(),
            granularity: nz16(4),
        },
        nz(3),
    )
    .expect("deposit-in-chunk quantized parity");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 2, "8 records at K=4 -> 2 aligned chunks");
}
