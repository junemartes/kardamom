//! This test executes the embedded `DeFi` bench contracts on the real
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
use kardamom_bench::load::plan::{PlannedTx, TxPlanParams};
use kardamom_bench::signers::SignerSet;
use kardamom_bench::stm::{BlockAt, SeqExec};
use kardamom_engine::delta::PendingDelta;
use kardamom_types::BPosition;

use kardamom_bench::ANVIL_MNEMONIC as ANVIL_PHRASE;
const CHAIN_ID: u64 = 412_346;

/// Assert one `(Receipt, WriteSet)` run's receipt succeeded, with
/// `context` in the panic message. Returns the pair unchanged, for the
/// caller's own per-family checks.
fn assert_one_ok(
    run: (kardamom_types::Receipt, kardamom_engine::delta::WriteSet),
    context: impl std::fmt::Display,
) -> (kardamom_types::Receipt, kardamom_engine::delta::WriteSet) {
    let (r, ws) = run;
    assert!(r.status, "{context}: {r:?}");
    (r, ws)
}

#[test]
fn defi_workload_executes_on_the_engine() {
    let signers = SignerSet::derive(ANVIL_PHRASE, 3).unwrap();
    let addr0 = signers[0].signer.address();
    let addr1 = signers[1].signer.address();
    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);

    let plan_params = TxPlanParams {
        chain_id: CHAIN_ID,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let dep = deployment_txs(&signers, plan_params).unwrap();
    let queues = pregenerate_defi(&signers, &dep.contracts, 40, plan_params).unwrap();

    let env = BlockAt(0).env(CHAIN_ID);
    let mut exec = SeqExec::new(&snap, None, env).expect("open scope");
    let mut delta = PendingDelta::new();
    let mut i = 0u64;
    let mut run = |tx: &PlannedTx, sender: Address, delta: &mut PendingDelta| {
        let slot = kardamom_engine::exec_types::TxSlot {
            tx_idx: kardamom_engine::TxIndex(i),
            tx_position: BPosition::from_index(i),
            tx_index_in_block: i,
            cumulative_gas_used_before: 0,
        };
        let out = exec
            .run(slot, &tx.to_envelope(sender, i), None)
            .unwrap_or_else(|e| panic!("tx {i} (sender {sender:?} nonce {}): {e:?}", tx.nonce));
        delta.apply(out.write_set.clone());
        i += 1;
        (out.receipt, out.write_set)
    };

    // Deployments: expect three contract accounts with code.
    let mut deploy_gas = 0;
    for d in &dep.txs {
        let (r, ws) = assert_one_ok(run(d, addr0, &mut delta), "deploy failed");
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
    let ops = queues.iter().enumerate().flat_map(|(si, queue)| {
        let sender = signers[si].signer.address();
        queue.iter().take(24).map(move |tx| (si, sender, tx))
    });
    for (si, sender, tx) in ops {
        let (r, ws) = assert_one_ok(
            run(tx, sender, &mut delta),
            format!("op reverted (sender {si} nonce {})", tx.nonce),
        );
        assert!(!ws.accounts.is_empty());
        op_gas.push(r.gas_used);
    }

    // Check the gas profile: these are contract calls, not transfers.
    // Swap, vault, and CLOB operations must average well above 21k gas,
    // with heavy tail operations, such as cold-slot CLOB places, above 100k.
    assert!(
        !op_gas.is_empty(),
        "no operations executed; the workload produced no receipts"
    );
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
        .get(&(dep.contracts.pool, alloy_primitives::B256::ZERO));
    assert!(r0.is_some(), "reserve0 untouched — swaps never executed");
    assert_ne!(
        *r0.unwrap(),
        U256::from(10u128.pow(24)),
        "reserve0 unchanged from constructor"
    );
    let _ = addr1;
}
