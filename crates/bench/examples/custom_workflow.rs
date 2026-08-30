//! Example: an external `BenchWorkflow` driven from a downstream crate.
//!
//! It runs a read-only `eth_blockNumber` workload. This workload needs no
//! presigned transactions and no contracts. The example shows the minimum
//! surface an external workflow needs for `Benchmark<W>` and `Harness<W>`.
//!
//! Run against the in-process harness:
//!   `cargo run --release --example custom_workflow -p kardamom-bench`

use std::time::Duration;

use alloy_primitives::U256;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;

use kardamom_bench::Benchmark;
use kardamom_bench::benchmark::Prepared;
use kardamom_bench::harness::Harness;
use kardamom_bench::workflow::BenchWorkflow;
use kardamom_types::AllocEntry;

const METHOD: &str = "eth_blockNumber";

#[derive(Debug, Clone, Default)]
struct BlockNumberWorkflow;

impl BenchWorkflow for BlockNumberWorkflow {
    type Item = ();

    fn name(&self) -> &'static str {
        "block_number"
    }

    fn methods(&self) -> &'static [&'static str] {
        &[METHOD]
    }

    fn genesis_alloc(&self, _n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>> {
        // `eth_blockNumber` only reads a counter. It needs no accounts and no contracts.
        Ok(Vec::new())
    }

    async fn prepare(
        &self,
        _client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Prepared<Self::Item>> {
        // `eth_blockNumber` needs no real warmup. Produce unit markers so the
        // harness still exercises the warmup path.
        let warmup = vec![(); 64];
        let main = (0..n_tasks)
            .map(|_| vec![(); txs_per_task as usize])
            .collect();
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, _item: ()) -> (&'static str, bool) {
        let r: Result<U256, _> = client.request(METHOD, rpc_params![]).await;
        (METHOD, r.is_ok())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `Harness::run` installs its own tracing subscriber, with the flame layer.
    // Do not init a subscriber here.

    let bench = Benchmark {
        workflow: BlockNumberWorkflow,
        timeout: Duration::from_secs(3),
        concurrency: 8,
        txs_per_task: 5_000,
        max_in_flight: 8,
    };

    Harness {
        chain_id: 412_346,
        bench,
        flame_out: "/tmp/k-custom-flame.svg".into(),
        report_json: None,
        pprof_out: None,
    }
    .run()
    .await
}
