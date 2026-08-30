//! Parity and fail-stop tests for the seeded parallel engine
//! (`engine.rs`): parallel batches vs sequential ground truth, forged
//! claims, deposits, and the K > 1 quantized chunk views.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, TxKind, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::{Executor, execute_deposit_tx};
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::{BPosition, BlockBoundaryStart, Deposit, StateDatabase, TxEnvelope};

use super::engine::execute_block_parallel_scoped;
use super::{ClaimIndex, execute_block_parallel};

fn tx(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64, i: u64) -> BufferedRecord {
    let inner = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 100_000,
        to: TxKind::Call(to),
        value: U256::from(value),
        input: Default::default(),
    };
    let mut m = inner;
    let sig = signer.sign_transaction_sync(&mut m).unwrap();
    let env: alloy_consensus::TxEnvelope = m.into_signed(sig).into();
    let mut raw = Vec::new();
    alloy_eips::eip2718::Encodable2718::encode_2718(&env, &mut raw);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
        envelope: TxEnvelope {
            correlation_id: i,
            raw_tx: raw.into(),
            sender: signer.address(),
            tx_hash: alloy_primitives::B256::repeat_byte(i as u8 + 1),
        },
    }
}

/// A zero-value call: [`tx`] with `value = 0`, kept for call-shaped
/// readability at use sites.
fn call_tx(signer: &PrivateKeySigner, to: Address, nonce: u64, i: u64) -> BufferedRecord {
    tx(signer, to, nonce, 0, i)
}

fn dep(from: Address, to: Option<Address>, mint: u128, input: Vec<u8>, i: u64) -> BufferedRecord {
    BufferedRecord::Deposit {
        tx_idx: TxIndex(i),
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
        deposit: Deposit {
            source_hash: B256::repeat_byte(0xd0u8.wrapping_add(i as u8)),
            from,
            to,
            mint,
            value: U256::ZERO,
            gas_limit: 1_000_000,
            is_system_transaction: false,
            input: input.into(),
        },
    }
}

fn env() -> ExecEnv {
    ExecEnv::new(
        1,
        &BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
    )
}

/// One record through the FREE executor path (tx or deposit; a fresh
/// scope per call) — deliberately NOT the exec core's
/// `execute_record_in_scope`, so the parity tests compare the shared
/// production dispatch against an independently-constructed reference
/// instead of against itself.
fn exec_record<S: StateDatabase>(
    snap: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    rec: &BufferedRecord,
    i: u64,
    cumulative: u64,
    bal: Option<&mut revm::state::bal::Bal>,
) -> (kardamom_types::Receipt, kardamom_engine::delta::WriteSet) {
    let bal = bal.map(|b| (b, i + 1));
    match rec {
        BufferedRecord::Tx {
            tx_idx,
            envelope,
            position,
        } => Executor::execute_once(
            snap,
            parent,
            delta,
            env(),
            *tx_idx,
            *position,
            envelope,
            i,
            cumulative,
            bal,
        )
        .expect("seq execute"),
        BufferedRecord::Deposit {
            tx_idx,
            deposit,
            position,
        } => execute_deposit_tx(
            snap,
            parent,
            delta,
            env(),
            *tx_idx,
            *position,
            deposit,
            i,
            cumulative,
            bal,
        )
        .expect("seq deposit"),
    }
}

/// THE sequential-capture fixture (previously hand-rolled at four sites):
/// execute `records` in order through the free executor path with Bal
/// capture, folding writes as it goes. Returns the folded delta (the
/// sequential ground truth) and the captured `Bal` (the claim source).
/// `assert_status` additionally requires every record to execute.
fn seq_capture<S: StateDatabase>(
    snap: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    assert_status: bool,
) -> (PendingDelta, revm::state::bal::Bal) {
    let mut bal = revm::state::bal::Bal::new();
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    for (i, rec) in records.iter().enumerate() {
        let (r, ws) = exec_record(
            snap,
            parent,
            &delta,
            rec,
            i as u64,
            cumulative,
            Some(&mut bal),
        );
        if assert_status {
            assert!(r.status, "record {i} must execute");
        }
        cumulative = r.cumulative_gas_used;
        delta.apply(ws);
    }
    (delta, bal)
}

/// Build a claim index by SEQUENTIALLY executing the block through the
/// executor's REAL capture path (`execute_tx` / `execute_deposit_tx` →
/// revm `Bal`), exactly as the live executor produces claims. The first
/// version of this fixture hand-rolled claims from WriteSets — symmetric
/// with a verification bug (per-field vs whole-triple attribution), so
/// both passed while live traffic diverged on every transfer. The
/// fixture and the producer must share code, not shape.
fn honest_claims<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> ClaimIndex {
    let (_, bal) = seq_capture(snap, None, records, false);
    ClaimIndex::from_alloy(&bal.into_alloy_bal())
}

fn seq_delta<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> PendingDelta {
    seq_capture(snap, None, records, false).0
}

/// THE parity property: parallel seeded batches must produce byte-identical
/// state to sequential execution — including for a CONFLICTING workload
/// (one sender, dependent nonces, shared recipient) where every tx depends
/// on its predecessor. Seeding, not ordering, is what makes that safe.
/// One small pool per test: the production strategy holds a persistent
/// pool; tests mint a fresh one so each case is isolated.
fn test_pool() -> kardamom_stm::pool::WorkerPool {
    kardamom_stm::pool::WorkerPool::new(4, Vec::new())
}

#[test]
fn parallel_batches_equal_sequential_on_a_fully_dependent_chain() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000AA");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();
    // 12 txs from ONE sender: maximal conflict (each reads the balance and
    // nonce the previous tx wrote).
    let txs: Vec<BufferedRecord> = (0..12).map(|i| tx(&signer, to, i, 1_000 + i, i)).collect();

    let claims = honest_claims(&snap, &txs);
    let expected = seq_delta(&snap, &txs);

    for batch_size in [1usize, 5, 10] {
        let out = execute_block_parallel(
            &test_pool(),
            &snap,
            None,
            &txs,
            &claims,
            env(),
            batch_size,
            1,
        )
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
        for w in out.receipts.windows(2) {
            assert!(w[0].cumulative_gas_used < w[1].cumulative_gas_used);
        }
    }
}

/// A FORGED claim must fail-stop at the batch that produces it — this is
/// what makes seeding from unverified claims sound.
#[test]
fn a_forged_claim_fails_stop_at_its_producing_batch() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000BB");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();
    let txs: Vec<BufferedRecord> = (0..8).map(|i| tx(&signer, to, i, 500, i)).collect();

    let mut claims = honest_claims(&snap, &txs);
    // Tamper: inflate the recipient's claimed balance at tx 6 (batch 2 of
    // 5-tx batches) — the executor claiming a state it did not compute.
    let bogus = claims.balance.get_mut(&to).expect("recipient claims");
    if let Some(entry) = bogus.iter_mut().find(|(i, _)| *i == 6) {
        entry.1 += U256::from(1_000_000u64);
    }

    let err = execute_block_parallel(&test_pool(), &snap, None, &txs, &claims, env(), 5, 1)
        .expect_err("a forged claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("tx 6"), "must name the producing tx: {msg}");
            assert!(
                msg.contains("balance"),
                "must name the mismatching item: {msg}"
            );
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// K = 20 end-to-end: quantized wire claims + chunk-aligned batches
/// must be parity-identical to sequential, and a forged CHUNK claim
/// must fail-stop naming the chunk. Exercises the same-view invariant:
/// both sides pass through the shared quantize().
#[test]
fn quantized_claims_verify_with_aligned_batches() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000CC");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();
    let txs: Vec<BufferedRecord> = (0..47).map(|i| tx(&signer, to, i, 100 + i, i)).collect();

    // The executor's view: per-tx capture, then the SHARED quantize.
    let (expected, bal) = seq_capture(&snap, None, &txs, false);
    let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 20);
    let claims = ClaimIndex::from_alloy(&quantized);

    let out = execute_block_parallel(&test_pool(), &snap, None, &txs, &claims, env(), 8, 20)
        .expect("quantized parity");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.batches, 3, "47 txs at K=20 -> 3 aligned chunks");

    // Forge a chunk-2 claim: must fail-stop naming the chunk.
    let mut forged = claims.clone();
    if let Some(w) = forged.balance.get_mut(&to)
        && let Some(e) = w.iter_mut().find(|(i, _)| *i == 2)
    {
        e.1 += U256::from(999u64);
    }
    let err = execute_block_parallel(&test_pool(), &snap, None, &txs, &forged, env(), 8, 20)
        .expect_err("forged chunk claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("chunk 2"), "must name the chunk: {msg}")
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// THE depth-K regression: under the pipelined commit the snapshot can
/// be K blocks stale — block 2's txs must observe block 1's writes via
/// the PARENT layer. The first DeFi gate diverged exactly here: the
/// hook dropped the parent, the validator saw stale nonces, and skipped
/// txs the executor had executed.
#[test]
fn parent_layer_bridges_the_uncommitted_gap() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000DD");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();

    // Block 1: nonces 0..3, executed and folded into a parent layer —
    // but NEVER committed to the snapshot (StaticSnapshotSource
    // semantics: the mock snapshot still says nonce 0).
    let b1: Vec<BufferedRecord> = (0..4).map(|i| tx(&signer, to, i, 100, i)).collect();
    let claims1 = honest_claims(&snap, &b1);
    let out1 = execute_block_parallel(&test_pool(), &snap, None, &b1, &claims1, env(), 2, 1)
        .expect("block 1");
    let parent = out1.delta.clone();

    // Block 2: nonces 4..7. Against the bare snapshot every tx is a
    // nonce-mismatch skip; with the parent layer they execute.
    let b2: Vec<BufferedRecord> = (4..8).map(|i| tx(&signer, to, i, 100, i)).collect();
    // Build block-2 claims through the same capture path, WITH parent
    // (every record must execute given the parent).
    let (delta, bal) = seq_capture(&snap, Some(&parent), &b2, true);
    let claims2 = ClaimIndex::from_alloy(&bal.into_alloy_bal());

    // WITHOUT parent: the stale-state bug — every tx skips.
    let stale = execute_block_parallel(&test_pool(), &snap, None, &b2, &claims2, env(), 2, 1);
    assert!(
        stale.is_err(),
        "without the parent layer the block must diverge (skips vs claims)"
    );

    // WITH parent: byte-identical to the sequential-with-parent run.
    let out2 = execute_block_parallel(
        &test_pool(),
        &snap,
        Some(&parent),
        &b2,
        &claims2,
        env(),
        2,
        1,
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
        &snap,
        None,
        &[],
        &ClaimIndex::default(),
        env(),
        5,
        1,
    )
    .unwrap();
    assert!(out.receipts.is_empty() && out.batches == 0);
}

/// THE deposit-emission regression: a deposit's mint MUST be claimed in
/// the BAL as a balance write, because later batches seed the recipient's
/// balance from it. Before the fix, `record_writeset_into_bal` fabricated
/// only a differing nonce and revm's per-FIELD classification silently
/// dropped the balance claim — batch 2 seeded the pre-mint balance and
/// every spend of minted funds diverged.
#[test]
fn deposit_mint_seeds_later_batches() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let to = address!("00000000000000000000000000000000000000EE");
    // D starts at ZERO balance: only the mint can fund its txs.
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

    // batch_size 1: the spends run in batches seeded ONLY from claims.
    let out = execute_block_parallel(&test_pool(), &snap, None, &records, &claims, env(), 1, 1)
        .expect("deposit-seeded parallel block");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 3);
    // The deposit's receipt survives the parallel path intact.
    assert_eq!(out.receipts[0].tx_type, kardamom_types::TX_TYPE_DEPOSIT);
    assert!(out.receipts.iter().all(|r| r.status));
}

/// A CREATE deposit's bytecode is a CODE claim: a later batch calling
/// the deposited contract must seed the BYTES, or the call no-ops
/// against empty code (the cross-chunk CREATE-then-CALL class, deposit
/// edition).
#[test]
fn create_deposit_code_seeds_later_calls() {
    let signer = PrivateKeySigner::random();
    let l1_sender = address!("00000000000000000000000000000000000000F1");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();
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

    let out = execute_block_parallel(&test_pool(), &snap, None, &records, &claims, env(), 1, 1)
        .expect("CREATE-deposit-seeded parallel block");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 2);
}

/// Deposits inside a K > 1 chunk: chunk-aligned batches with a deposit
/// mid-chunk verify and match sequential (the deposit's claims are
/// quantized through the same shared ladder as tx claims).
#[test]
fn quantized_chunk_containing_a_deposit_verifies() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let filler = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000F2");
    let snap = MockStateDatabase::builder()
        .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
        .account(
            filler.address(),
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .build();
    // Chunk 1 (K=4): 3 filler txs + the deposit at bal index 4, so the
    // deposit is CHUNK-FINAL for d's balance — chunk 2's spends seed the
    // mint from the deposit's claim alone, and no same-chunk tx re-claim
    // can mask a missing deposit emission. (The CALL deposit bumps d's
    // nonce to 1; spends run nonces 1..4.)
    let mut records: Vec<BufferedRecord> = (0..3u64).map(|i| tx(&filler, to, i, 100, i)).collect();
    records.push(dep(d, Some(to), 10u128.pow(18), Vec::new(), 3));
    for i in 4..8u64 {
        records.push(tx(&signer, to, i - 3, 500, i));
    }

    // Executor view: per-record capture, then the SHARED quantize at K=4.
    let (expected, bal) = seq_capture(&snap, None, &records, true);
    let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 4);
    let claims = ClaimIndex::from_alloy(&quantized);

    let out = execute_block_parallel(&test_pool(), &snap, None, &records, &claims, env(), 3, 4)
        .expect("deposit-in-chunk quantized parity");
    assert_eq!(out.delta.accounts, expected.accounts);
    assert_eq!(out.delta.storage, expected.storage);
    assert_eq!(out.batches, 2, "8 records at K=4 -> 2 aligned chunks");
}

/// A forged claim about a DEPOSIT fails-stop like any other — deposits
/// get no special trust.
#[test]
fn a_forged_deposit_claim_fails_stop() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let to = address!("00000000000000000000000000000000000000F3");
    let snap = MockStateDatabase::builder()
        .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
        .build();
    let records: Vec<BufferedRecord> = vec![
        dep(d, Some(to), 10u128.pow(18), Vec::new(), 0),
        tx(&signer, to, 1, 1_000, 1),
    ];
    let mut claims = honest_claims(&snap, &records);
    // Tamper the deposit's claimed mint (bal index 1).
    let w = claims.balance.get_mut(&d).expect("mint claim");
    let entry = w.iter_mut().find(|(i, _)| *i == 1).expect("index 1");
    entry.1 += U256::from(7u64);

    let err = execute_block_parallel(&test_pool(), &snap, None, &records, &claims, env(), 1, 1)
        .expect_err("forged deposit claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("tx 1"), "must name the producing unit: {msg}");
            assert!(msg.contains("balance"), "must name the field: {msg}");
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// A/B: the pooled dispatch must be BYTE-IDENTICAL to the scope-spawn
/// reference on the same compositions — receipts, delta, and (on a forged
/// claim) the error text. The pool changes only WHERE batches run.
#[test]
fn pooled_dispatch_matches_scoped_reference() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            from,
            U256::from(10u128.pow(18)),
            0,
            revm::primitives::KECCAK_EMPTY,
        )
        .build();
    let txs: Vec<BufferedRecord> = (0..23).map(|i| tx(&signer, to, i, 100 + i, i)).collect();
    // Per-tx capture once; at K > 1 the wire claims are chunk-collapsed
    // through the SHARED quantize, exactly as the executor publishes them.
    let (_, bal) = seq_capture(&snap, None, &txs, false);
    let raw = bal.into_alloy_bal();

    for (batch_size, granularity) in [(5usize, 1u16), (8, 1), (8, 8)] {
        let claims = ClaimIndex::from_alloy(&kardamom_engine::bal_ladder::quantize(
            raw.clone(),
            granularity,
        ));
        let pooled = execute_block_parallel(
            &test_pool(),
            &snap,
            None,
            &txs,
            &claims,
            env(),
            batch_size,
            granularity,
        )
        .expect("pooled");
        let scoped = execute_block_parallel_scoped(
            &snap,
            None,
            &txs,
            &claims,
            env(),
            batch_size,
            granularity,
        )
        .expect("scoped");
        assert_eq!(
            pooled.batches, scoped.batches,
            "bs={batch_size} k={granularity}"
        );
        assert_eq!(
            pooled.receipts, scoped.receipts,
            "bs={batch_size} k={granularity}"
        );
        assert_eq!(
            pooled.delta.accounts, scoped.delta.accounts,
            "bs={batch_size} k={granularity}"
        );
        assert_eq!(
            pooled.delta.storage, scoped.delta.storage,
            "bs={batch_size} k={granularity}"
        );
    }
}

/// Real-mdbx equivalence: the pooled path with PER-WORKER SNAPSHOT FORKS
/// (`fork_view`) produces byte-identical output to sequential execution
/// against the same mdbx snapshot. This is the state-backed proof for the
/// shared-read-txn fix — mock tests cannot exercise the fork path's mdbx
/// anchor semantics.
#[test]
fn pooled_with_forks_matches_sequential_on_mdbx() {
    use kardamom_engine::stateless::execute_block;

    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let dir = tempfile::tempdir().expect("tmpdir");
    let env_ = kardamom_state::StateEnvBuilder::new(dir.path())
        .durability(kardamom_state::Durability::SafeNoSync)
        .open()
        .expect("open state env");
    // Fund the sender at genesis so transfers execute.
    kardamom_state::seed_genesis(
        &env_,
        &[kardamom_types::AccountChange {
            address: from,
            nonce: 0,
            balance: U256::from(10u128.pow(18)),
            code_hash: revm::primitives::KECCAK_EMPTY,
        }],
        &[],
    )
    .expect("seed genesis");
    let mut writer = kardamom_state::StateWriter::spawn(env_).expect("spawn writer");
    let snap = writer.snapshot_rx.recv().expect("genesis snapshot");
    // Sanity: forks mint (writer quiescent at genesis).
    assert!(
        snap.fork_view().is_some(),
        "fork must mint at a quiet anchor"
    );

    let txs: Vec<BufferedRecord> = (0..17).map(|i| tx(&signer, to, i, 50 + i, i)).collect();
    let claims = honest_claims(&snap, &txs);

    let sequential = execute_block(&snap, None, &txs, env()).expect("sequential");
    let pooled = execute_block_parallel(&test_pool(), &snap, None, &txs, &claims, env(), 8, 1)
        .expect("pooled with forks");

    assert_eq!(pooled.receipts, sequential.receipts);
    assert_eq!(pooled.delta.accounts, sequential.delta.accounts);
    assert_eq!(pooled.delta.storage, sequential.delta.storage);
    drop(snap);
    writer.shutdown().expect("writer shutdown");
}
