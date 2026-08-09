//! Allocation profile of the per-tx execution hot path (ignored by
//! default — run explicitly):
//!
//!   cargo test -p kardamom-bench --test alloc_profile --release -- \
//!     --ignored --nocapture
//!
//! Executes a few thousand DeFi txs through `execute_tx` under the DHAT
//! heap profiler and prints allocations/tx + bytes/tx, writing
//! dhat-heap.json (per-callsite attribution, viewable with dh_view.html)
//! next to the workspace root. Numbers feed the allocation-squashing
//! campaign: the per-core execution ceiling is partly malloc-bound.

use alloy_primitives::U256;
use kardamom_bench::load::defi::{deployment_txs, pregenerate_defi};
use kardamom_bench::mnemonic;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::execute_tx;
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";
const CHAIN_ID: u64 = 412_346;
const SENDERS: usize = 8;
const TXS_PER_SENDER: usize = 400;

/// Op family under measurement: `KARDAMOM_PROFILE_OPS` =
/// mix (default) | swap | vault_deposit | vault_withdraw | clob_place |
/// clob_cancel | transfer. Per-family numbers separate the contract
/// execution cost (journal, cache inserts, logs) from the engine
/// fixed cost — the mix hides where the bytes live.
fn family() -> String {
    std::env::var("KARDAMOM_PROFILE_OPS").unwrap_or_else(|_| "mix".into())
}

#[test]
#[ignore = "profiling run — invoke explicitly with --ignored"]
fn defi_execution_allocation_profile() {
    let signers = mnemonic::derive_signers(ANVIL_PHRASE, SENDERS as u32).unwrap();
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
    let fam = family();
    let queues: Vec<Vec<kardamom_bench::load::plan::PlannedTx>> = if fam == "mix" {
        pregenerate_defi(
            &signers,
            CHAIN_ID,
            &contracts,
            TXS_PER_SENDER,
            0,
            1_000_000_000,
        )
        .unwrap()
    } else {
        kardamom_bench::load::defi::pregenerate_family(
            &signers,
            CHAIN_ID,
            &contracts,
            &fam,
            TXS_PER_SENDER,
            0,
            1_000_000_000,
        )
        .unwrap()
    };

    let env = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
    );

    // Interleave senders round-robin (realistic block shape), one flat list.
    let mut txs: Vec<(usize, kardamom_bench::load::plan::PlannedTx)> = Vec::new();
    for i in 0..TXS_PER_SENDER {
        for (si, q) in queues.iter().enumerate() {
            if let Some(t) = q.get(i) {
                txs.push((si, t.clone()));
            }
        }
    }

    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    let mut i = 0u64;

    #[allow(clippy::too_many_arguments)]
    fn run_one(
        snap: &MockStateDatabase,
        delta: &PendingDelta,
        env: ExecEnv,
        raw: &kardamom_bench::load::plan::PlannedTx,
        sender: alloy_primitives::Address,
        i: &mut u64,
        cumulative: &mut u64,
    ) -> (kardamom_types::Receipt, kardamom_engine::delta::WriteSet) {
        let e = TxEnvelope {
            correlation_id: *i,
            raw_tx: raw.raw.clone().into(),
            sender,
            tx_hash: raw.hash,
        };
        let (r, ws) = execute_tx(
            snap,
            None,
            delta,
            env,
            TxIndex(*i),
            BPosition::from_index(*i),
            &e,
            *i,
            *cumulative,
            None,
        )
        .expect("execute");
        *cumulative = r.cumulative_gas_used;
        *i += 1;
        (r, ws)
    }

    // Deploy + warm up OUTSIDE the measured window.
    for d in &deploys {
        let (_r, ws) = run_one(
            &snap,
            &delta,
            env,
            d,
            signers[0].signer.address(),
            &mut i,
            &mut cumulative,
        );
        delta.apply(ws);
    }
    for (si, t) in txs.iter().take(SENDERS * 4) {
        let (_r, ws) = run_one(
            &snap,
            &delta,
            env,
            t,
            signers[*si].signer.address(),
            &mut i,
            &mut cumulative,
        );
        delta.apply(ws);
    }

    // Measured window under DHAT — through the PRODUCTION path: ONE
    // ExecScope per simulated block, re-scoped every BLOCK_TXS to model
    // boundaries.
    const BLOCK_TXS: usize = 1000;
    let profiler = dhat::Profiler::builder().build();
    let stats0 = dhat::HeapStats::get();
    let t0 = std::time::Instant::now();
    let mut gas = 0u64;
    let measured: Vec<_> = txs.iter().skip(SENDERS * 4).collect();
    let mut scope: Option<kardamom_engine::executor::ExecScope<&MockStateDatabase>> = None;
    for (n, (si, t)) in measured.iter().enumerate() {
        if n % BLOCK_TXS == 0 {
            let mut sc =
                kardamom_engine::executor::ExecScope::new(&snap, None, env).expect("scope");
            sc.seed_layer(&delta).expect("seed");
            scope = Some(sc);
        }
        let e = TxEnvelope {
            correlation_id: i,
            raw_tx: t.raw.clone().into(),
            sender: signers[*si].signer.address(),
            tx_hash: t.hash,
        };
        let (r, ws) = scope
            .as_mut()
            .unwrap()
            .execute_tx(
                TxIndex(i),
                BPosition::from_index(i),
                &e,
                i,
                cumulative,
                None,
                None,
            )
            .expect("execute");
        cumulative = r.cumulative_gas_used;
        i += 1;
        gas += r.gas_used;
        delta.apply(ws);
    }
    let wall = t0.elapsed();
    let stats = dhat::HeapStats::get();
    let n = measured.len() as u64;

    let allocs = stats.total_blocks - stats0.total_blocks;
    let bytes = stats.total_bytes - stats0.total_bytes;
    println!(
        "==================== ALLOCATION PROFILE ({} / {} txs) ====================",
        family(),
        n
    );
    println!("allocs/tx:      {:.1}", allocs as f64 / n as f64);
    println!("bytes/tx:       {:.0}", bytes as f64 / n as f64);
    println!("peak heap:      {:.1} MB", stats.max_bytes as f64 / 1e6);
    println!(
        "wall/tx:        {:.1} us",
        wall.as_micros() as f64 / n as f64
    );
    println!("gas/tx:         {}", gas / n);
    println!(
        "implied 1-core: {:.1} Mgas/s",
        (gas as f64 / 1e6) / wall.as_secs_f64()
    );
    drop(profiler); // writes dhat-heap.json with per-callsite attribution
    let _ = std::fs::rename("dhat-heap.json", format!("dhat-heap-{}.json", family()));
}
