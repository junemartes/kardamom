//! This test executes the embedded DeFi bench contracts on the real
//! kardamom engine.
//!
//! It gates between "forge compiled it" and a full cluster deploy.
//! Every operation family, meaning deploy, seed, swap, vault deposit
//! and withdraw, and CLOB place and cancel, must produce a success
//! receipt with contract-shaped gas and a non-empty write set on
//! `execute_tx` with `MockStateDatabase`, exactly as the executor
//! will run them.

use alloy_primitives::{Address, U256};
use kardamom_bench::load::defi::{deployment_txs, pregenerate_defi};
use kardamom_bench::load::plan::PlannedTx;
use kardamom_bench::mnemonic;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::Executor;
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};

const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";
const CHAIN_ID: u64 = 412_346;

fn envelope(tx: &PlannedTx, sender: Address, i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: tx.raw.clone().into(),
        sender,
        tx_hash: tx.hash,
    }
}

#[test]
fn defi_workload_executes_on_the_engine() {
    let signers = mnemonic::derive_signers(ANVIL_PHRASE, 3).unwrap();
    let addr0 = signers[0].signer.address();
    let addr1 = signers[1].signer.address();
    let mut snap_builder = MockStateDatabase::builder();
    for s in &signers {
        snap_builder = snap_builder.account(
            s.signer.address(),
            U256::from(10u128.pow(21)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    let snap = snap_builder.build();

    let (deploys, contracts) = deployment_txs(&signers, CHAIN_ID, 0, 1_000_000_000).unwrap();
    let queues = pregenerate_defi(&signers, CHAIN_ID, &contracts, 40, 0, 1_000_000_000).unwrap();

    let env = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
    );
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    let mut i = 0u64;
    let mut run = |tx: &PlannedTx, sender: Address, delta: &mut PendingDelta, cum: &mut u64| {
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            delta,
            env,
            TxIndex(i),
            BPosition::from_index(i),
            &envelope(tx, sender, i),
            i,
            *cum,
            None,
        )
        .unwrap_or_else(|e| panic!("tx {i} (sender {sender:?} nonce {}): {e:?}", tx.nonce));
        *cum = receipt.cumulative_gas_used;
        delta.apply(ws.clone());
        i += 1;
        (receipt, ws)
    };

    // Deployments: expect three contract accounts with code.
    let mut deploy_gas = 0;
    for d in &deploys {
        let (r, ws) = run(d, addr0, &mut delta, &mut cumulative);
        assert!(r.status, "deploy failed: {r:?}");
        assert!(r.contract_address.is_some());
        assert!(!ws.code.is_empty(), "deploy must write code");
        deploy_gas += r.gas_used;
    }
    assert!(
        deploy_gas > 300_000,
        "creates are code-write heavy: {deploy_gas}"
    );

    // Run the head of each sender's queue: seed, then mixed operations.
    let mut op_gas = Vec::new();
    for (si, queue) in queues.iter().enumerate() {
        let sender = signers[si].signer.address();
        for tx in queue.iter().take(24) {
            let (r, ws) = run(tx, sender, &mut delta, &mut cumulative);
            assert!(
                r.status,
                "op reverted (sender {si} nonce {}): {r:?}",
                tx.nonce
            );
            assert!(!ws.accounts.is_empty());
            op_gas.push(r.gas_used);
        }
    }

    // Check the gas profile: these are contract calls, not transfers.
    // Swap, vault, and CLOB operations must average well above 21k gas,
    // with heavy tail operations, such as cold-slot CLOB places, above 100k.
    let avg = op_gas.iter().sum::<u64>() / op_gas.len() as u64;
    let max = *op_gas.iter().max().unwrap();
    assert!(avg > 30_000, "avg gas {avg} too low for a contract mix");
    assert!(
        max > 100_000,
        "max gas {max} — no storage-heavy op executed?"
    );

    // The pool's hot reserve slots must move from their constructor
    // values. This proves the swap path actually swaps.
    let r0 = delta
        .storage
        .get(&(contracts.pool, alloy_primitives::B256::ZERO));
    assert!(r0.is_some(), "reserve0 untouched — swaps never executed");
    assert_ne!(
        *r0.unwrap(),
        U256::from(10u128.pow(24)),
        "reserve0 unchanged from constructor"
    );
    let _ = addr1;
}
