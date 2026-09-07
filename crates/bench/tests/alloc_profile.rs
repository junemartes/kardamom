//! This is an allocation profile of the per-transaction execution hot
//! path. It is ignored by default; run it explicitly:
//!
//!   cargo test -p kardamom-bench --test `alloc_profile` --release -- \
//!     --ignored --nocapture
//!
//! It executes a few thousand `DeFi` transactions through `execute_tx`
//! under the DHAT heap profiler, and prints allocations per transaction
//! and bytes per transaction. It writes `dhat-heap.json`, with
//! per-callsite attribution viewable with `dh_view.html`, next to the
//! workspace root. The per-core execution ceiling is partly bound by
//! malloc.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices and counters here are bounded by small, fixed test parameters (thread and sender counts), never near a truncation or precision boundary"
)]

use kardamom_bench::load::defi::{deployment_txs, pregenerate_defi};
use kardamom_bench::load::plan::TxPlanParams;
use kardamom_bench::signers::SignerSet;
use kardamom_bench::stm::{BlockAt, SeqExec};
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::BPosition;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use kardamom_bench::ANVIL_MNEMONIC as ANVIL_PHRASE;
const CHAIN_ID: u64 = 412_346;
const SENDERS: usize = 8;
const TXS_PER_SENDER: usize = 400;
/// The measured window re-scopes every this many transactions, to
/// model block boundaries.
const BLOCK_TXS: usize = 1000;

/// The operation family under measurement. Set `KARDAMOM_PROFILE_OPS` to
/// one of: mix (the default), swap, `vault_deposit`, `vault_withdraw`,
/// `clob_place`, `clob_cancel`, or transfer. Per-family numbers separate the
/// contract execution cost, such as journal, cache inserts, and logs,
/// from the engine's fixed cost. The mix hides where the bytes live.
fn family() -> String {
    std::env::var("KARDAMOM_PROFILE_OPS").unwrap_or_else(|_| "mix".into())
}

/// Rebuild `scope` from `snap`, `delta`, and `cumulative` every
/// `block_txs` transactions, to model block boundaries inside the
/// measured window. `cumulative` and the transaction index are never
/// reset at a rebuild: only the underlying scope is fresh.
fn rescope_every_block_txs<'a>(
    scope: &mut Option<SeqExec<'a, MockStateDatabase>>,
    n: usize,
    block_txs: usize,
    snap: &'a MockStateDatabase,
    delta: &PendingDelta,
    env: ExecEnv,
    cumulative: u64,
) {
    if n.is_multiple_of(block_txs) {
        *scope = Some(SeqExec::resume(snap, Some(delta), env, cumulative).expect("scope"));
    }
}

/// Drive `measured` through the production path, one scope for each
/// simulated block, re-scoped every [`BLOCK_TXS`] transactions to model
/// block boundaries. Returns the total gas consumed. This is the
/// DHAT-measured window: it changes the allocation shape not at all
/// versus running this loop inline, since it does no extra work beyond
/// the call itself.
fn drive_measured(
    measured: &[&(usize, kardamom_bench::load::plan::PlannedTx)],
    signers: &SignerSet,
    snap: &MockStateDatabase,
    delta: &mut PendingDelta,
    env: ExecEnv,
    start_index: u64,
    start_cumulative: u64,
) -> u64 {
    let mut cumulative = start_cumulative;
    let mut gas = 0u64;
    let mut scope: Option<SeqExec<'_, MockStateDatabase>> = None;
    for (i, (n, (si, t))) in (start_index..).zip(measured.iter().enumerate()) {
        rescope_every_block_txs(&mut scope, n, BLOCK_TXS, snap, delta, env, cumulative);
        let out = scope
            .as_mut()
            .unwrap()
            .run(
                TxIndex(i),
                BPosition::from_index(i),
                &t.to_envelope(signers[*si].signer.address(), i),
                i,
                None,
            )
            .expect("execute");
        cumulative = out.receipt.cumulative_gas_used;
        gas += out.receipt.gas_used;
        delta.apply(out.write_set);
    }
    gas
}

#[test]
#[ignore = "profiling run — invoke explicitly with --ignored"]
fn defi_execution_allocation_profile() {
    let signers = SignerSet::derive(ANVIL_PHRASE, SENDERS as u32).unwrap();
    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);
    let plan_params = TxPlanParams {
        chain_id: CHAIN_ID,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let dep = deployment_txs(&signers, plan_params).unwrap();
    let fam = family();
    let queues: Vec<Vec<kardamom_bench::load::plan::PlannedTx>> = if fam == "mix" {
        pregenerate_defi(&signers, &dep.contracts, TXS_PER_SENDER, plan_params).unwrap()
    } else {
        kardamom_bench::load::defi::pregenerate_family(
            &signers,
            CHAIN_ID,
            &dep.contracts,
            &fam,
            TXS_PER_SENDER,
            0,
            1_000_000_000,
        )
        .unwrap()
    };

    let env = BlockAt(0).env(CHAIN_ID);

    // Interleave senders in rotation, for a realistic block shape, into
    // one flat list.
    let txs: Vec<(usize, kardamom_bench::load::plan::PlannedTx)> = (0..TXS_PER_SENDER)
        .flat_map(|i| {
            queues
                .iter()
                .enumerate()
                .filter_map(move |(si, q)| q.get(i).map(|t| (si, t.clone())))
        })
        .collect();

    let mut delta = PendingDelta::new();
    let mut i = 0u64;

    // Deploy and warm up outside the measured window, on one scope.
    let mut warmup = SeqExec::new(&snap, None, env).expect("open warmup scope");
    for d in &dep.txs {
        let out = warmup
            .run(
                TxIndex(i),
                BPosition::from_index(i),
                &d.to_envelope(signers[0].signer.address(), i),
                i,
                None,
            )
            .expect("execute");
        delta.apply(out.write_set);
        i += 1;
    }
    for (si, t) in txs.iter().take(SENDERS * 4) {
        let out = warmup
            .run(
                TxIndex(i),
                BPosition::from_index(i),
                &t.to_envelope(signers[*si].signer.address(), i),
                i,
                None,
            )
            .expect("execute");
        delta.apply(out.write_set);
        i += 1;
    }
    let cumulative = warmup.cumulative();

    // This is the measured window under DHAT. `profiler` is declared
    // first, so it drops last, after every line below it: that drop
    // writes dhat-heap.json with per-callsite attribution, before the
    // rename that follows this block. `measured` is built inside the
    // window, exactly where the drive loop used to build it, so the
    // window's allocation shape does not change.
    {
        let profiler = dhat::Profiler::builder().build();
        let stats0 = dhat::HeapStats::get();
        let t0 = std::time::Instant::now();
        let measured: Vec<_> = txs.iter().skip(SENDERS * 4).collect();
        let gas = drive_measured(&measured, &signers, &snap, &mut delta, env, i, cumulative);
        let wall = t0.elapsed();
        let stats = dhat::HeapStats::get();
        assert!(
            !measured.is_empty(),
            "no transactions measured; increase txs relative to SENDERS * 4"
        );
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
        // `profiler`, declared first, drops last: here, at the end of
        // this block. That drop writes dhat-heap.json with per-callsite
        // attribution, before the rename that follows the block.
        let _ = &profiler;
    }
    let _ = std::fs::rename("dhat-heap.json", format!("dhat-heap-{}.json", family()));
}
