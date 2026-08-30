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

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::Target;
use crate::harness::l2;
use crate::harness::metrics::poll_until;

pub struct Params {
    /// Dev-mnemonic index of the pre-crash sender.
    pub before: usize,
    /// Dev-mnemonic index of the post-restart sender. It must differ from
    /// `before`. The sequencer's per-sender nonce floor lives in memory, so
    /// reusing a sender across the crash would test the sequencer's
    /// recovery, not the executor's.
    pub after: usize,
    pub txs_each: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            before: 13,
            after: 14,
            txs_each: 12,
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
pub async fn phase_before_crash(t: &Target, p: &Params) -> Result<u64> {
    let signers = l2::dev_signers(p.before.max(p.after) as u32 + 1)?;
    let to = Address::from([0x9Bu8; 20]);
    submit_run(t, &signers[p.before], p.txs_each, to).await?;

    // Let the block that holds them commit. The receipt is published when
    // the transaction executes, but the block lands only at the next
    // sealer boundary. A crash before that would correctly lose it.
    let committed = poll_until(
        "executor commits the pre-crash work",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let b = t
                .executor_metric(super::EXEC_BLOCK_NUMBER)
                .await
                .unwrap_or(0.0);
            Ok((b > 0.0).then_some(b as u64))
        },
    )
    .await?;
    Ok(committed)
}

/// The post-restart half. The chain must accept new work, and the
/// restarted executor must catch back up to the validator.
pub async fn phase_after_restart(t: &Target, p: &Params, pre_crash_block: u64) -> Result<()> {
    let signers = l2::dev_signers(p.before.max(p.after) as u32 + 1)?;
    let to = Address::from([0x9Cu8; 20]);

    // The restarted executor must come back and pass its pre-crash block.
    // This proves it resumed, instead of stalling or restarting the
    // chain.
    poll_until(
        "restarted executor reaches its pre-crash block",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || async {
            let b = t
                .executor_metric(super::EXEC_BLOCK_NUMBER)
                .await
                .unwrap_or(0.0);
            Ok((b as u64 >= pre_crash_block).then_some(()))
        },
    )
    .await
    .context("executor did not return to its pre-crash height")?;

    // And the chain must still work.
    submit_run(t, &signers[p.after], p.txs_each, to).await?;
    poll_until(
        "post-restart work commits",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let b = t
                .executor_metric(super::EXEC_BLOCK_NUMBER)
                .await
                .unwrap_or(0.0);
            Ok((b as u64 > pre_crash_block).then_some(()))
        },
    )
    .await
    .context("chain did not advance past the pre-crash block after the restart")?;
    Ok(())
}
