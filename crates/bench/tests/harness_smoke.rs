//! Smoke test: run `Harness<TransfersWorkflow>` for ~800ms against an
//! in-process node and assert the tracing-flame SVG was rendered with at
//! least one `kardamom_node::node` symbol embedded, confirming the gated
//! flame layer flushed during the dispatch window and was rendered via
//! inferno into a ready-to-view SVG (no external CLI step).

use std::path::PathBuf;
use std::time::Duration;

use kardamom_bench::harness::Harness;
use kardamom_bench::{Benchmark, TransfersWorkflow};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_writes_flame_with_node_spans() {
    let flame_out: PathBuf = std::env::temp_dir().join(format!(
        "kardamom-harness-smoke-{}.svg",
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

    let contents = std::fs::read_to_string(&flame_out).expect("flame SVG written");
    assert!(!contents.is_empty(), "flame SVG should not be empty");
    assert!(
        contents.starts_with("<?xml"),
        "flame output should be an SVG, not raw folded text; first line:\n{}",
        contents.lines().next().unwrap_or_default()
    );
    // Inferno embeds frame symbols directly in the SVG text content.
    assert!(
        contents.contains("kardamom_node::node"),
        "flame SVG should contain at least one kardamom_node::node frame"
    );

    let _ = std::fs::remove_file(&flame_out);
    // Belt-and-suspenders cleanup: the harness deletes the sidecar
    // itself, but if rendering failed mid-flight it might be left over.
    let mut sidecar = flame_out.clone().into_os_string();
    sidecar.push(".folded.tmp");
    let _ = std::fs::remove_file(PathBuf::from(sidecar));
}
