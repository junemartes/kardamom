//! This is an offline reproducer for the deterministic K=20 DeFi
//! divergence.
//!
//! Three separate cluster deploys produced the same divergence: in
//! chunk 2 (transactions 21 through 40), vault/slot0 was claimed as a
//! nonzero value but recomputed as absent. This is a deterministic
//! workload point, not chaos. This sweep runs many deterministic block
//! compositions of the real bench contracts through
//! `execute_block_parallel` at K=20 and K=1, and compares each against
//! sequential execution through the executor's exact capture path. Any
//! composition that diverges is the repro.

use alloy_primitives::{Address, U256};
use kardamom_bench::load::defi::{DefiContracts, deployment_txs, pregenerate_defi};
use kardamom_bench::load::plan::PlannedTx;
use kardamom_bench::mnemonic;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::Executor;
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};
use kardamom_validator::parallel::{ClaimIndex, execute_block_parallel};

const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";
const CHAIN_ID: u64 = 412_346;
const SENDERS: u32 = 15;

fn env_for(block: u64) -> ExecEnv {
    ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: block,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000 + block * 2,
            l1_origin: 0,
        },
    )
}

fn envelope(t: &PlannedTx, sender: Address, i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: t.raw.clone().into(),
        sender,
        tx_hash: t.hash,
    }
}

/// This is xorshift, for deterministic composition shuffling with no
/// external RNG dependency.
fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// The shared pool for the sweep. It is persistent, like production.
fn repro_pool() -> &'static kardamom_stm::pool::WorkerPool {
    use std::sync::OnceLock;
    static POOL: OnceLock<kardamom_stm::pool::WorkerPool> = OnceLock::new();
    POOL.get_or_init(|| kardamom_stm::pool::WorkerPool::new(4, Vec::new()))
}

#[test]
fn k20_defi_parallel_matches_sequential_across_compositions() {
    let signers = mnemonic::derive_signers(ANVIL_PHRASE, SENDERS).unwrap();
    let mut b = MockStateDatabase::builder();
    for s in &signers {
        b = b.account(
            s.signer.address(),
            U256::from(10u128.pow(21)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    let snap = b.build();
    let (deploys, contracts) = deployment_txs(&signers, CHAIN_ID, 0, 1_000_000_000).unwrap();
    let _: DefiContracts = contracts;
    let queues = pregenerate_defi(&signers, CHAIN_ID, &contracts, 120, 0, 1_000_000_000).unwrap();

    // Block 1 is setup: the deploys and every sender's seed operation,
    // executed in order. This becomes the parent layer for the repro blocks.
    let mut parent = PendingDelta::new();
    let mut cumulative = 0u64;
    let mut gi = 0u64;
    {
        let env = env_for(1);
        for d in &deploys {
            let (r, ws) = Executor::execute_once(
                &snap,
                None,
                &parent,
                env,
                TxIndex(gi),
                BPosition::from_index(gi),
                &envelope(d, signers[0].signer.address(), gi),
                gi,
                cumulative,
                None,
            )
            .expect("deploy");
            assert!(r.status);
            cumulative = r.cumulative_gas_used;
            parent.apply(ws);
            gi += 1;
        }
        for (si, q) in queues.iter().enumerate() {
            let t = &q[0]; // This is the seed() operation.
            let (r, ws) = Executor::execute_once(
                &snap,
                None,
                &parent,
                env,
                TxIndex(gi),
                BPosition::from_index(gi),
                &envelope(t, signers[si].signer.address(), gi),
                gi,
                cumulative,
                None,
            )
            .expect("seed");
            assert!(r.status);
            cumulative = r.cumulative_gas_used;
            parent.apply(ws);
            gi += 1;
        }
    }

    // Sweep: many deterministic compositions of blocks starting at 2.
    // Interleave the senders' operation queues under different shuffles,
    // 60 transactions per block, and check K=20 and K=1 parallel against
    // sequential for each.
    let mut next_op_idx = vec![1usize; queues.len()]; // A per-sender queue cursor.
    let mut rng: u64 = 0x00C0FFEE_D15EA5E5;
    let mut parent = parent;
    for block in 2..14u64 {
        let env = env_for(block);
        // Composition: weighted round-robin with random skips.
        let mut txs: Vec<(usize, &PlannedTx)> = Vec::new();
        while txs.len() < 60 {
            let si = (xs(&mut rng) % queues.len() as u64) as usize;
            let idx = next_op_idx[si];
            if idx >= queues[si].len() {
                continue;
            }
            next_op_idx[si] += 1;
            txs.push((si, &queues[si][idx]));
        }
        // Each sender's order within the block must stay nonce-monotonic.
        // The composition above guarantees this, since the cursor only advances.

        // This computes the sequential truth and the executor's claims,
        // through per-transaction capture.
        let mut seq_delta = PendingDelta::new();
        let mut bal = revm::state::bal::Bal::new();
        let mut cum = 0u64;
        let mut statuses = Vec::new();
        for (i, (si, t)) in txs.iter().enumerate() {
            let (r, ws) = Executor::execute_once(
                &snap,
                Some(&parent),
                &seq_delta,
                env,
                TxIndex(i as u64),
                BPosition::from_index(i as u64),
                &envelope(t, signers[*si].signer.address(), i as u64),
                i as u64,
                cum,
                Some((&mut bal, (i + 1) as u64)),
            )
            .expect("seq");
            cum = r.cumulative_gas_used;
            statuses.push(r.status);
            seq_delta.apply(ws);
        }
        let alloy_bal = bal.into_alloy_bal();

        let block_txs: Vec<BufferedRecord> = txs
            .iter()
            .enumerate()
            .map(|(i, (_si, t))| BufferedRecord::Tx {
                tx_idx: TxIndex(i as u64),
                position: BPosition::from_index(i as u64),
                envelope: envelope(t, signers[txs[i].0].signer.address(), i as u64),
            })
            .collect();

        for k in [1u16, 20] {
            let q = kardamom_engine::bal_ladder::quantize(alloy_bal.clone(), k);
            let claims = ClaimIndex::from_alloy(&q);
            let out = execute_block_parallel(repro_pool(), &snap,
                Some(&parent),
                &block_txs,
                &claims,
                env,
                8,
                k,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "REPRO block {block} K={k}: {e:?}\n  statuses: {statuses:?}\n  rng composition block {block}"
                )
            });
            assert_eq!(
                out.delta.accounts, seq_delta.accounts,
                "block {block} K={k}: account state mismatch"
            );
            assert_eq!(
                out.delta.storage, seq_delta.storage,
                "block {block} K={k}: storage mismatch"
            );
        }

        parent.merge_from(&seq_delta);
    }
}

/// This is the burst-block case that produced the live divergence. A
/// stall makes the sealer pack the whole backlog into one giant block,
/// so the contract deployments land in chunk 1 and the calls land in
/// later chunks: a CREATE, then a CALL, across a chunk boundary within
/// one block.
///
/// The original spec excluded code from attribution, reasoning that
/// the account-entry dependency orders it. That reasoning is correct
/// for the wave-DAG model, but wrong for the seeded model: batch 2
/// never waits for batch 1. So without code claims, its seed has
/// account entries but empty bytecode, every contract call becomes a
/// no-op, and verification reports "recomputed absent".
#[test]
fn create_then_call_across_chunks_in_one_block() {
    let signers = mnemonic::derive_signers(ANVIL_PHRASE, SENDERS).unwrap();
    let mut b = MockStateDatabase::builder();
    for s in &signers {
        b = b.account(
            s.signer.address(),
            U256::from(10u128.pow(21)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    let snap = b.build();
    let (deploys, contracts) = deployment_txs(&signers, CHAIN_ID, 0, 1_000_000_000).unwrap();
    let queues = pregenerate_defi(&signers, CHAIN_ID, &contracts, 8, 0, 1_000_000_000).unwrap();

    // One block: the deploys (bal 1-3), all seeds, then two rounds of
    // operations. This spans multiple K=20 chunks, with the CREATEs in
    // chunk 1.
    let mut txs: Vec<(usize, &PlannedTx)> = deploys.iter().map(|d| (0usize, d)).collect();
    for (si, q) in queues.iter().enumerate() {
        txs.push((si, &q[0])); // This is the seed() operation.
    }
    for round in 1..3 {
        for (si, q) in queues.iter().enumerate() {
            txs.push((si, &q[round]));
        }
    }
    assert!(txs.len() > 40, "must span >2 chunks at K=20: {}", txs.len());

    let env = env_for(2);
    let mut seq_delta = PendingDelta::new();
    let mut bal = revm::state::bal::Bal::new();
    let mut cum = 0u64;
    for (i, (si, t)) in txs.iter().enumerate() {
        let (r, ws) = Executor::execute_once(
            &snap,
            None,
            &seq_delta,
            env,
            TxIndex(i as u64),
            BPosition::from_index(i as u64),
            &envelope(t, signers[*si].signer.address(), i as u64),
            i as u64,
            cum,
            Some((&mut bal, (i + 1) as u64)),
        )
        .expect("seq");
        assert!(r.status, "tx {i} must execute sequentially");
        cum = r.cumulative_gas_used;
        seq_delta.apply(ws);
    }
    let alloy_bal = bal.into_alloy_bal();

    let block_txs: Vec<BufferedRecord> = txs
        .iter()
        .enumerate()
        .map(|(i, (_si, t))| BufferedRecord::Tx {
            tx_idx: TxIndex(i as u64),
            position: BPosition::from_index(i as u64),
            envelope: envelope(t, signers[txs[i].0].signer.address(), i as u64),
        })
        .collect();

    for k in [20u16, 1] {
        let q = kardamom_engine::bal_ladder::quantize(alloy_bal.clone(), k);
        let claims = ClaimIndex::from_alloy(&q);
        let out = execute_block_parallel(repro_pool(), &snap, None, &block_txs, &claims, env, 8, k)
            .unwrap_or_else(|e| panic!("K={k} create-then-call diverged: {e:?}"));
        assert_eq!(out.delta.storage, seq_delta.storage, "K={k}: storage");
        assert_eq!(out.delta.accounts, seq_delta.accounts, "K={k}: accounts");
    }
}
