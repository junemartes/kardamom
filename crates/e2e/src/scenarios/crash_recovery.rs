//! S9b — `executor_crash_recovery_is_consistent`.
//!
//! Kill the executor uncleanly (SIGKILL: no shutdown hook, no final flush),
//! restart it against the same state dir, and require that:
//!
//! 1. it **resumes from the persisted cursor** rather than re-syncing from
//!    genesis — the restarted process logs its resume block, and that block
//!    is the one its DB had committed before the crash;
//! 2. the chain keeps working afterwards — new transactions land;
//! 3. the DBs are still coherent: the executor's sweeps clean and matches the
//!    validator's byte for byte (the validator never restarted, so it is an
//!    independent witness of what the chain should contain).
//!
//! Together with S6 this is the "the DB has a correct view that doesn't get
//! corrupted" half of the suite: S6 proves it under normal operation, this
//! proves it across an unclean process death.

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::Target;
use crate::harness::l2;
use crate::harness::metrics::poll_until;

pub struct Params {
    /// Dev-mnemonic index of the pre-crash sender.
    pub before: usize,
    /// Dev-mnemonic index of the post-restart sender. Must differ: the
    /// sequencer's per-sender nonce floor is in-memory, so reusing a sender
    /// across the crash would race its recovery rather than test the
    /// executor's.
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

/// The live half. The caller crashes + restarts the executor between the two
/// phases (Target-L drives process signals; a Target-C runner would use
/// `nomad alloc signal`), then runs the offline comparison.
pub async fn phase_before_crash(t: &Target, p: &Params) -> Result<u64> {
    let signers = l2::dev_signers(p.before.max(p.after) as u32 + 1)?;
    let to = Address::from([0x9Bu8; 20]);
    submit_run(t, &signers[p.before], p.txs_each, to).await?;

    // Let the block containing them commit — the receipt is published at
    // execute time, the block only lands at the next sealer boundary, and a
    // crash before that would (correctly) lose it.
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

/// The post-restart half: the chain must accept new work, and the restarted
/// executor must catch back up to the validator.
pub async fn phase_after_restart(t: &Target, p: &Params, pre_crash_block: u64) -> Result<()> {
    let signers = l2::dev_signers(p.before.max(p.after) as u32 + 1)?;
    let to = Address::from([0x9Cu8; 20]);

    // The restarted executor must come back and pass its pre-crash block —
    // proof it resumed rather than stalling or restarting the chain.
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
