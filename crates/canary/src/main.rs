//! `kardamom-canary`: uses the chain as a user does and reports each
//! success and each failure as a metric. See the library docs for the
//! module map.

use anyhow::Result;
use clap::Parser;
use kardamom_canary::config::Args;
use kardamom_canary::metrics;
use kardamom_canary::probes::deposit::DepositSettings;
use kardamom_canary::service::{Canary, Settings};
use kardamom_obs::bin::wait_for_shutdown;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("canary", args.metrics_addr, args.host_id.as_str()).await?;
    metrics::describe();
    let signers = kardamom_canary::ring::signers(&args.mnemonic, args.ring_offset, args.ring_size)?;
    let deposit = DepositSettings::new(&args, args.receipt_timeout_ms.duration())?;
    let canary = Canary::start(Settings {
        timing: args.timing(),
        topup: args.topup_wei,
        safe_sample: args.safe_sample,
        deposit,
        endpoints: args.ingress,
        notifier_ws: args.notifier_ws,
        signers,
        dir: args.dir,
        l2_floor: args.l2_floor_wei,
    })
    .await?;
    wait_for_shutdown().await;
    tracing::info!("kardamom-canary: shutdown signal received");
    canary.stop();
    Ok(())
}
