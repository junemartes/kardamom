//! Smoke test: run `Harness<TransfersWorkflow>` for ~800ms against an
//! in-process node and assert `flame.folded` was written with at least one
//! `kardamom_node::node` span, confirming the gated flame layer flushed
//! during the dispatch window.

use std::path::PathBuf;
use std::time::Duration;

use kardamom_bench::harness::Harness;
use kardamom_bench::{Benchmark, TransfersWorkflow};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_writes_flame_with_node_spans() {
    let flame_out: PathBuf = std::env::temp_dir().join(format!(
        "kardamom-harness-smoke-{}.folded",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&flame_out);

    let bench = Benchmark {
        workflow: TransfersWorkflow::default(),
        // Sized larger than the timeout can consume against an in-process
        // node, so the deadline (not work exhaustion) ends the dispatch
        // and the flame layer sees the full measurement window. Kept
        // small enough that debug-build ECDSA presigning is tolerable.
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

    let contents = std::fs::read_to_string(&flame_out).expect("flame.folded written");
    assert!(!contents.is_empty(), "flame.folded should not be empty");
    assert!(
        contents.contains("kardamom_node::node"),
        "flame.folded should contain at least one kardamom_node::node span; got:\n{}",
        contents.lines().take(5).collect::<Vec<_>>().join("\n")
    );

    let _ = std::fs::remove_file(&flame_out);
}
