//! Smoke test: run `Harness<TransfersWorkflow>` for ~800ms against the
//! in-process **ingress** stand-in and assert it completes end-to-end.
//!
//! The harness drives a real `IngressProxy` (over `MockChannels` + a fake
//! executor) and scopes flame/pprof recording to the dispatch window. Ingress
//! emits no `tracing` spans, so the tracing-flame SVG is intentionally skipped
//! when empty (see `harness::write_flame_output`); this test just confirms the
//! wiring runs without error. The full in-process Aeron pipeline harness
//! (real execution → span-based flames) is tracked as a follow-up.

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
        // Sized larger than the timeout can consume so the deadline (not work
        // exhaustion) ends the dispatch and the full measurement window runs.
        // Kept small enough that debug-build ECDSA presigning is tolerable.
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

    // The tracing-flame SVG is skipped when no spans were recorded (ingress
    // emits none), so we don't assert on its contents — the assertion is that
    // the full harness wiring (in-process ingress + dispatch + report) ran to
    // completion. Clean up the SVG + folded sidecar if either was left behind.
    let _ = std::fs::remove_file(&flame_out);
    let mut sidecar = flame_out.clone().into_os_string();
    sidecar.push(".folded.tmp");
    let _ = std::fs::remove_file(PathBuf::from(sidecar));
}
