//! `executor_crash_recovery_is_consistent`.
//!
//! Kill the executor without warning (SIGKILL: no shutdown hook, no final
//! flush), restart it against the same state directory, and check that:
//!
//! 1. it resumes from the persisted cursor, not from genesis. The restarted
//!    process logs its resume block, and that block is the one its
//!    database had committed before the crash.
//! 2. the chain keeps working afterward: new transactions land.
//! 3. the databases stay coherent: the executor's data sweeps clean and
//!    matches the validator's data byte for byte (the validator never
//!    restarted, so it is an independent witness of what the chain should
//!    contain).
//!
//! Together with the validator-executor consistency check, this is the
//! "the database keeps a correct, uncorrupted view" half of the suite.
//! That check proves it under normal operation. This test proves it
//! across an unclean process death.

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::Target;
use crate::harness::l2;

pub struct Params {
    /// Dev-mnemonic index of the pre-crash sender.
    pub before: usize,
    /// Dev-mnemonic index of the post-restart sender. It must differ from
    /// `before`. The sequencer's per-sender nonce floor lives in memory, so
    /// reusing a sender across the crash would test the sequencer's
    /// recovery, not the executor's.
    pub after: usize,
    pub txs_each: NonZeroUsize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            before: 13,
            after: 14,
            txs_each: NonZeroUsize::new(12).unwrap(),
        }
    }
}

/// Submit `n` dense transfers from `signer`, failing on the first rejection.
async fn submit_run(t: &Target, signer: &l2::DerivedSigner, n: usize, to: Address) -> Result<()> {
    for nonce in 0..n as u64 {
        let tx = l2::sign_transfer(signer, t.chain_id, nonce, to, 1)?;
        let out = t.rpc.send_raw(&tx.raw).await;
        out.result
            .map_err(|e| anyhow::anyhow!("nonce {nonce} from {}: {e}", signer.address))?;
    }
    Ok(())
}

/// The live half. The caller crashes and restarts the executor between
/// the two phases (Target-L sends process signals; a Target-C runner
/// would use `nomad alloc signal`), then runs the offline comparison.
///
/// # Errors
/// Returns an error when a pre-crash transfer fails to send, or when no
/// block commits within 30s.
pub async fn phase_before_crash(t: &Target, p: &Params) -> Result<u64> {
    let signers = l2::dev_signers_through(p.before.max(p.after))?;
    let to = Address::from([0x9Bu8; 20]);
    submit_run(t, &signers[p.before], p.txs_each.get(), to).await?;

    // Let the block that holds them commit. The receipt is published when
    // the transaction executes, but the block lands only at the next
    // sealer boundary. A crash before that would correctly lose it.
    let committed = t
        .wait_executor_block(
            1,
            Duration::from_secs(30),
            Duration::from_millis(250),
            "executor commits the pre-crash work",
        )
        .await?;
    Ok(committed)
}

/// The post-restart half. The chain must accept new work, and the
/// restarted executor must catch back up to the validator.
///
/// # Errors
/// Returns an error when the executor does not return to
/// `pre_crash_block` within 60s, when a post-restart transfer fails to
/// send, or when the chain does not advance past `pre_crash_block` within
/// 30s.
pub async fn phase_after_restart(t: &Target, p: &Params, pre_crash_block: u64) -> Result<()> {
    let signers = l2::dev_signers_through(p.before.max(p.after))?;
    let to = Address::from([0x9Cu8; 20]);

    // The restarted executor must come back and pass its pre-crash block.
    // This proves it resumed, instead of stalling or restarting the
    // chain.
    t.wait_executor_block(
        pre_crash_block,
        Duration::from_secs(60),
        Duration::from_millis(500),
        "restarted executor reaches its pre-crash block",
    )
    .await
    .context("executor did not return to its pre-crash height")?;

    // And the chain must still work.
    submit_run(t, &signers[p.after], p.txs_each.get(), to).await?;
    // `pre_crash_block` is a metric-derived value; a bad or adversarial
    // reading must fail loudly, not silently wrap the threshold.
    let past_pre_crash = pre_crash_block
        .checked_add(1)
        .context("pre_crash_block overflows")?;
    t.wait_executor_block(
        past_pre_crash,
        Duration::from_secs(30),
        Duration::from_millis(250),
        "post-restart work commits",
    )
    .await
    .context("chain did not advance past the pre-crash block after the restart")?;
    Ok(())
}
