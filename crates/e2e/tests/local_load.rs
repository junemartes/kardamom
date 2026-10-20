//! The end-to-end throughput and latency of one local stack.
//!
//! `kardamom-load` ramps transfers against the ingress of the stack, finds
//! the highest rate that the stack takes, and soaks at a part of it. The
//! test prints the report of the tool and the verdict counters of the
//! sequencers.
//!
//! Run it with release binaries, so that the numbers measure the code and
//! not a debug build. The harness takes the service binaries from the
//! profile of the test binary:
//!
//! ```text
//! cargo build --release --bins -p kardamom-ingress -p kardamom-sequencer \
//!   -p kardamom-executor -p kardamom-validator -p kardamom-state \
//!   -p kardamom-da-watcher -p kardamom-l1-indexer -p kardamom-reconstruct \
//!   -p kardamom-notifier -p kardamom-canary -p kardamom-bench
//! KARDAMOM_E2E_ADMISSION=1 KARDAMOM_E2E_VERDICTS=1 cargo test --release -p e2e \
//!   --features full-pipeline-e2e --test local_load -- --ignored --nocapture
//! ```
//!
//! `KARDAMOM_LOAD_TPS` sets the ramp ceiling (default 20,000 tx/s),
//! `KARDAMOM_LOAD_STEP` the ramp step (default 1,000 tx/s for 10 s), and
//! `KARDAMOM_LOAD_SECS` the soak (default 60 s). The load uses 16 of the
//! prefunded dev accounts, from index 2.

#![cfg(feature = "full-pipeline-e2e")]

use std::time::Duration;

use e2e::harness::metrics;
use e2e::harness::services::bin;
use e2e::harness::{LocalStack, StackConfig};

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack under load; run with --release --ignored --nocapture"]
async fn local_stack_load_ramp() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let target = stack.target(Duration::from_secs(30)).expect("target");
    let tps = env_or("KARDAMOM_LOAD_TPS", 20_000).to_string();
    let secs = format!("{}s", env_or("KARDAMOM_LOAD_SECS", 60));
    let step = env_or("KARDAMOM_LOAD_STEP", 1_000).to_string();
    let report = stack.root().join("load.json");
    let status = tokio::process::Command::new(bin("kardamom-load").expect("kardamom-load"))
        .args(["--rpc", &target.rpc.url])
        .args(["--chain-id", &target.chain_id.to_string()])
        .args([
            "--senders",
            "16",
            "--sender-offset",
            "2",
            "--nonce-start",
            "chain",
        ])
        .args(["--target-tps", &tps, "--duration", &secs])
        .args(["--ramp-step-tps", &step, "--ramp-step-secs", "10"])
        .args(["--max-in-flight", "4096", "--scrape", ""])
        .arg("--output")
        .arg(&report)
        .status()
        .await
        .expect("run kardamom-load");
    println!(
        "load report: {}",
        std::fs::read_to_string(&report).unwrap_or_default()
    );
    for addr in &target.sequencer_metrics {
        let scrape = metrics::scrape(*addr).await.expect("scrape a sequencer");
        println!(
            "sequencer {addr}: verdicts sent {:?}, full {:?}",
            scrape.value_where("kardamom_sequencer_verdicts_total", "outcome=\"sent\""),
            scrape.value_where("kardamom_sequencer_verdicts_total", "outcome=\"full\""),
        );
    }
    assert!(status.success(), "kardamom-load failed: {status}");
}
