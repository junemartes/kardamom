//! Test-side stream injection.
//!
//! The test process attaches to the stack's media driver as one more `Aeron`
//! client, and publishes deliberately corrupt frames on the real channels
//! the validator verifies against. Use this with the executor `SIGSTOP`ped
//! ([`crate::harness::LocalStack::suspend_executor`]), so the injected
//! frame faces no competition from genuine publications. That is what
//! makes the scenario deterministic, instead of a race against the
//! executor.

use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use kardamom_log::aeron_live::{AeronRuntime, PubHandle};
use kardamom_log::config::LogConfig;
use kardamom_types::{AccountChange, BlockDelta};
use rkyv::util::AlignedVec;

/// Publish every one of `frames` on `publication`, once each.
fn publish_all(publication: &PubHandle, frames: &[AlignedVec]) {
    for frame in frames {
        publication.publish_best_effort(frame.clone());
    }
}

/// Attach to the media driver as one more Aeron client, open a
/// publication on `channel`/`stream_id`, and publish every one of
/// `frames`, ten rounds, spread over about 3 seconds. Republishing each
/// round covers the publication still connecting when the first offer
/// goes out (best-effort semantics, the same as the executor's own
/// publishers).
///
/// # Errors
/// Returns an error when attaching to the media driver or opening the
/// publication fails.
async fn inject_frames(
    aeron_dir: &Path,
    channel: &str,
    stream_id: i32,
    frames: Vec<AlignedVec>,
) -> Result<()> {
    let aeron_dir = aeron_dir.to_path_buf();
    let channel = channel.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let rt = AeronRuntime::spawn_with_dir(&aeron_dir).context("attach injection runtime")?;
        let publication = rt
            .open_publication(&channel, stream_id)
            .context("open publication")?;
        for _round in 0..10 {
            publish_all(&publication, &frames);
            // Spread the rounds over about 3 seconds, so at least one frame
            // lands while the validator's wait for a target block is open.
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        Ok(())
    })
    .await
    .context("injection task join")?
}

/// Publish a corrupt `BlockDelta` (BAL) for each block in `blocks`, onto
/// `tx_bal`.
///
/// The delta claims an account write that no honest re-execution
/// produces, so the validator's write-set comparison for that block must
/// report divergence and fail-stop.
///
/// # Errors
/// Returns an error when the log config cannot be resolved, when a frame
/// fails to encode, or under the conditions [`inject_frames`] documents.
pub async fn publish_corrupt_bal(aeron_dir: &Path, blocks: Vec<u64>) -> Result<()> {
    let channels = LogConfig::resolve(None)
        .context("resolve log config")?
        .channels;
    let frames = blocks
        .into_iter()
        .map(|block| {
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
            // The frame must be structurally valid: a bare `BlockDelta`
            // fails rkyv validation at the subscription layer and never
            // reaches the cross-check, so wrap it in the current tx_bal
            // wire type. The corruption must be semantic (wrong values in
            // a structurally valid frame); structural garbage only tests
            // the codec.
            let frame = kardamom_types::BalFrame {
                delta,
                bal_rlp: Vec::new(),
                granularity: 1,
            };
            kardamom_log::codec::encode(&frame).context("encode corrupt BAL")
        })
        .collect::<Result<Vec<AlignedVec>>>()?;
    inject_frames(
        aeron_dir,
        &channels.tx_bal_channel,
        channels.tx_bal_stream_id,
        frames,
    )
    .await
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
///
/// # Errors
/// Returns an error when the log config cannot be resolved, when the
/// epoch fails to encode, or under the conditions [`inject_frames`]
/// documents.
pub async fn publish_forged_epoch(aeron_dir: &Path, l1_number: u64) -> Result<()> {
    let channels = LogConfig::resolve(None)
        .context("resolve log config")?
        .channels;
    // This is structurally valid, but semantically a lie. The fault must
    // be in what the epoch claims, not in its encoding. Otherwise the
    // drill would only test the codec.
    let forged = kardamom_types::EpochRecord {
        l1_number,
        l1_hash: B256::repeat_byte(0xF0),
        deposits: Vec::new(),
    };
    let bytes = kardamom_log::codec::encode(&forged).context("encode forged epoch")?;
    inject_frames(
        aeron_dir,
        &channels.tx_deposits_channel,
        channels.tx_deposits_stream_id,
        vec![bytes],
    )
    .await
}

/// Publish one [`kardamom_types::xchain::RemoteEpochRecord`] onto
/// `tx_remote_epochs`, as the interop watcher would. The real sequencers
/// relay it onto the cluster as a kind-5 record, so the sealer's lane
/// guards see exactly the frame a misbehaving or mis-seeded watcher would
/// send. The record must be well formed (a valid body and a matching
/// canonical id), or the sealer drops it as malformed and the drill only
/// tests the codec.
pub async fn publish_remote_epoch(
    aeron_dir: &Path,
    record: kardamom_types::xchain::RemoteEpochRecord,
) -> Result<()> {
    let aeron_dir = aeron_dir.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let channels = LogConfig::resolve(None)
            .context("resolve log config")?
            .channels;
        let rt = AeronRuntime::spawn_with_dir(&aeron_dir).context("attach injection runtime")?;
        let publication = rt
            .open_publication(
                &channels.tx_remote_epochs_channel,
                channels.tx_remote_epochs_stream_id,
            )
            .context("open tx_remote_epochs publication")?;
        for _round in 0..10 {
            let bytes = kardamom_log::codec::encode(&record).context("encode remote epoch")?;
            publication.publish_best_effort(bytes);
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        Ok(())
    })
    .await
    .context("injection task join")?
}
