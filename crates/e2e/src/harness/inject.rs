//! Test-side stream injection (divergence detection is not vacuous).
//!
//! The test process attaches to the stack's media driver as one more Aeron
//! client, and publishes deliberately corrupt frames on the real channels
//! the validator verifies against. Use this with the executor SIGSTOPped
//! ([`crate::harness::LocalStack::suspend_executor`]), so the injected
//! frame faces no competition from genuine publications. That is what
//! makes the scenario deterministic, instead of a race against the
//! executor.

use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::LogConfig;
use kardamom_types::{AccountChange, BlockDelta};

/// Publish a corrupt `BlockDelta` (BAL) for each block in `blocks`, onto
/// `tx_bal`.
///
/// The delta claims an account write that no honest re-execution
/// produces, so the validator's write-set comparison for that block must
/// report divergence and fail-stop. This code republishes each frame a
/// few times per block, because the publication may still be connecting
/// when the first offer goes out (best-effort semantics, the same as the
/// executor's own BAL publisher).
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
                // Use the current tx_bal wire type. A bare BlockDelta once
                // failed rkyv validation at the subscription layer and never
                // reached the cross-check, so the drill silently stopped
                // drilling. The corruption must be semantic (wrong values in
                // a structurally valid frame). Structural garbage only tests
                // the codec.
                let frame = kardamom_types::BalFrame {
                    delta,
                    bal_rlp: Vec::new(),
                    granularity: 1,
                };
                let bytes = kardamom_log::codec::encode(&frame).context("encode corrupt BAL")?;
                bal_pub.publish_best_effort(bytes);
            }
            // Spread the rounds over about 3 seconds, so at least one frame
            // lands while the validator's BAL wait for a target block is
            // open.
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        Ok(())
    })
    .await
    .context("injection task join")?
}

/// Publish a forged epoch onto `tx_deposits`: an epoch that claims L1
/// block `l1_number` with a hash L1 never produced.
///
/// The sequencer forwards it verbatim onto the canonical stream (it is
/// not the sequencer's job to know what L1 said). The sealer accepts it
/// because the origin advances. The validator, which re-derives every
/// epoch from L1, must find that the hash does not match, and fail-stop.
///
/// The bogus hash is what makes the drill deterministic. An epoch's
/// canonical id is `keccak(l1_hash)`, so a forged hash produces an id the
/// cluster has never seen, and dedup cannot swallow the injection. Pair
/// this with [`crate::harness::LocalStack::suspend_da_watcher`], so the
/// honest epoch for the same L1 block does not race it.
pub async fn publish_forged_epoch(aeron_dir: &Path, l1_number: u64) -> Result<()> {
    let aeron_dir = aeron_dir.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let channels = LogConfig::resolve(None)
            .context("resolve log config")?
            .channels;
        let rt = AeronRuntime::spawn_with_dir(&aeron_dir).context("attach injection runtime")?;
        let dep_pub = rt
            .open_publication(
                &channels.tx_deposits_channel,
                channels.tx_deposits_stream_id,
            )
            .context("open tx_deposits publication")?;
        // This is structurally valid, but semantically a lie. The fault must
        // be in what the epoch claims, not in its encoding. Otherwise the
        // drill would only test the codec.
        let forged = kardamom_types::EpochRecord {
            l1_number,
            l1_hash: B256::repeat_byte(0xF0),
            deposits: Vec::new(),
        };
        for _round in 0..10 {
            let bytes = kardamom_log::codec::encode(&forged).context("encode forged epoch")?;
            dep_pub.publish_best_effort(bytes);
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        Ok(())
    })
    .await
    .context("injection task join")?
}
