//! `kardamom-stm-p2` runs a Block-STM offline A/B test. It runs the
//! same generated blocks through the sequential engine (`Executor`)
//! and the STM engine (`kardamom-stm`), checks that receipts and the
//! delta are byte-identical on every block, and reports wall-clock
//! time for each worker count.
//!
//! The protocol for each block mirrors production learning dynamics:
//! 1. A timed sequential run: the baseline and the canonical outputs.
//! 2. A timed STM run for each worker count, each compared byte for
//!    byte against step 1.
//! 3. An untimed capture pass that trains the footprint stats.
//!    Prior-blocks-only stats feed each block's schedule: a cold
//!    start, in stream order.
//!
//! The wall-clock numbers are indicative, since they run on a shared
//! dev host. The assertion is the point; the speedup column shows the
//! shape, not a benchmark citation.

mod alloc;
mod args;
mod common;
mod drive;
mod mdbx_ab;
mod mdbx_report;
mod mock_ab;
mod pipeline;
mod scenario;

use clap::Parser;

use args::{Args, StateBackend};
use common::{EngineOpts, RunOpts, Workload};

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let signer_count = u32::try_from(a.senders.get())
        .ok()
        .and_then(|n| n.checked_add(1))
        .expect("senders count fits in u32");
    let signers =
        kardamom_bench::mnemonic::derive_signers(kardamom_bench::ANVIL_MNEMONIC, signer_count)?;

    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);

    let blocks = scenario::build_blocks(&a, &signers)?;
    let workload = a.workload(&signers, &blocks.all, blocks.n_setup);
    let engine = a.engine_opts();
    let batches = a.prune_batch.clone();

    if a.state == StateBackend::Mdbx {
        return run_mdbx_state(&a, &workload, &engine, &batches);
    }

    eprintln!(
        "==> A/B over {} blocks ({} setup) workers={:?}",
        blocks.all.len(),
        blocks.n_setup,
        a.workers
    );
    let report = mock_ab::run_mock_ab(&workload, &engine, &batches, &snap)?;
    mock_ab::print_mock_rows(&a, &report);
    Ok(())
}

/// The mdbx-backed paths: pipelined or plain A/B, each optionally
/// under a pprof CPU profile.
fn run_mdbx_state(
    a: &Args,
    workload: &Workload<'_>,
    engine: &EngineOpts,
    batches: &[usize],
) -> anyhow::Result<()> {
    let guard = kardamom_bench::pprof_guard::pprof_guard(
        a.pprof_out.as_ref(),
        kardamom_bench::pprof_guard::PPROF_HZ,
    )?;
    let r = if a.pipeline {
        pipeline::run_pipelined(workload, engine, a.pipeline_speculative)
    } else {
        let run_opts = RunOpts {
            per_block: a.per_block,
            prune_batches: batches.to_vec(),
        };
        mdbx_ab::run_mdbx_ab(workload, engine, &run_opts)
    };
    kardamom_bench::pprof_guard::write_pprof_svg(
        guard,
        a.pprof_out.as_ref().map(std::path::Path::new),
    )?;
    if let Some(path) = a.pprof_out.as_ref() {
        eprintln!("==> wrote flamegraph to {path}");
    }
    r
}
