//! Smoke test: run the harness for ~800ms against an in-process node and
//! assert `flame.folded` was written with at least one
//! `kardamom_node::node` span, confirming the gated flame layer flushed
//! during the measurement window.

use std::path::PathBuf;
use std::time::Duration;

use kardamom_bench::config::{self, FileConfig, MixCfg, MnemonicCfg, Workload};
use kardamom_bench::harness::{HarnessArgs, run_harness};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_writes_flame_with_node_spans() {
    let flame_out: PathBuf = std::env::temp_dir().join(format!(
        "kardamom-harness-smoke-{}.folded",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&flame_out);

    let file_config = FileConfig {
        rpc: None,
        workload: Some(Workload::Transfers),
        rate: Some(100),
        duration: Some(Duration::from_millis(800)),
        concurrency: Some(4),
        warmup: Some(Duration::from_millis(200)),
        output: None,
        mix: Some(MixCfg {
            transfers: 1,
            calls: 0,
        }),
        calls: None,
        mnemonic: Some(MnemonicCfg {
            phrase: "test test test test test test test test test test test junk".to_string(),
            balance: "1000000000000000000000".to_string(),
            count: None,
        }),
        contracts: vec![],
    };

    let cli_overrides = FileConfig {
        rpc: Some("in-process".to_string()),
        workload: None,
        rate: None,
        duration: None,
        concurrency: None,
        warmup: None,
        output: None,
        mix: None,
        calls: None,
        mnemonic: None,
        contracts: vec![],
    };

    let config = config::resolve(Some(file_config.clone()), cli_overrides).expect("resolve");

    run_harness(HarnessArgs {
        chain_id: 1,
        file_config,
        config,
        flame_out: flame_out.clone(),
        report_json: None,
    })
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
