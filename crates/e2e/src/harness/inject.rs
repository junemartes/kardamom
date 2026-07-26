//! Test-side stream injection (S7 — divergence detection is not vacuous).
//!
//! The test process attaches to the stack's media driver as one more Aeron
//! client and publishes deliberately corrupt frames on the real channels the
//! validator verifies against. Used with the executor SIGSTOPped
//! ([`crate::harness::LocalStack::suspend_executor`]) so the injected frame
//! faces no competition from genuine publications — that is what makes the
//! scenario deterministic rather than a race against the executor.

use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_types::{AccountChange, BlockDelta};

/// Publish a corrupt `BlockDelta` (BAL) for each of `blocks` onto `tx_bal`.
///
/// The delta claims an account write no honest re-execution produces, so the
/// validator's write-set comparison for that block MUST report divergence
/// and fail-stop. Frames are republished a few times per block because the
/// publication may still be connecting when the first offer goes out
/// (best-effort semantics, mirroring the executor's own BAL publisher).
pub async fn publish_corrupt_bal(aeron_dir: &Path, blocks: Vec<u64>) -> Result<()> {
    let aeron_dir = aeron_dir.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let channels = LogConfig::resolve(None)
            .context("resolve log config")?
            .channels;
        let rt = AeronRuntime::spawn_with_dir(&aeron_dir).context("attach injection runtime")?;
        let bal_pub = rt
            .open_publication(&channels.tx_bal_channel, channels.tx_bal_stream_id)
            .context("open tx_bal publication")?;
        for _round in 0..10 {
            for &block in &blocks {
                let delta = BlockDelta {
                    block_number: block,
                    accounts: vec![AccountChange {
                        address: Address::from([0xEE; 20]),
                        nonce: 666,
                        balance: U256::from(0xDEAD_BEEFu64),
                        code_hash: B256::ZERO,
                    }],
                    storage: vec![],
                    code: vec![],
                    receipts: vec![],
                };
                let bytes = kardamom_log::codec::encode(&delta).context("encode corrupt BAL")?;
                bal_pub.publish_best_effort(bytes);
            }
            // Spread the rounds (~3s total) so at least one frame lands while
            // the validator's BAL wait for a target block is open.
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        Ok(())
    })
    .await
    .context("injection task join")?
}
