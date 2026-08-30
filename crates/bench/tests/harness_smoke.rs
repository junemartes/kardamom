//! This is a smoke test. It runs `Harness<TransfersWorkflow>` for about
//! 800ms against the in-process ingress stand-in, and checks that it
//! completes end-to-end.
//!
//! The harness drives a real `IngressProxy`, over `MockChannels` with
//! a fake executor, and scopes flame and pprof recording to the
//! dispatch window. Ingress emits no `tracing` spans, so the code
//! skips the tracing-flame SVG on purpose when it would be empty; see
//! `harness::write_flame_output`. This test only confirms the wiring
//! runs without error. A full in-process Aeron pipeline harness, with
//! real execution and span-based flame graphs, is a follow-up item.

use std::path::PathBuf;
use std::time::Duration;

use kardamom_bench::harness::Harness;
use kardamom_bench::{Benchmark, TransfersWorkflow};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_runs_against_inprocess_ingress() {
    let flame_out: PathBuf =
        std::env::temp_dir().join(format!("kardamom-harness-smoke-{}.svg", std::process::id()));
    let _ = std::fs::remove_file(&flame_out);

    let bench = Benchmark {
        workflow: TransfersWorkflow::default(),
        // This is sized larger than the timeout can consume, so the deadline,
        // not running out of work, ends the dispatch, and the full
        // measurement window runs. It stays small enough that debug-build
        // ECDSA presigning is tolerable.
        txs_per_task: 2_000,
        max_in_flight: 8,
        timeout: Duration::from_millis(800),
        concurrency: 4,
    };

    Harness {
        chain_id: 1,
        bench,
        flame_out: flame_out.clone(),
        report_json: None,
        pprof_out: None,
    }
    .run()
    .await
    .expect("harness ran");

    // The tracing-flame SVG is skipped when no spans were recorded, since
    // ingress emits none. So this test does not check its contents; the
    // check is that the full harness wiring, in-process ingress, dispatch,
    // and report, ran to completion. Clean up the SVG and folded sidecar
    // if either was left behind.
    let _ = std::fs::remove_file(&flame_out);
    let mut sidecar = flame_out.clone().into_os_string();
    sidecar.push(".folded.tmp");
    let _ = std::fs::remove_file(PathBuf::from(sidecar));
}
