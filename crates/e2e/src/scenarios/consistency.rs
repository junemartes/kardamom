//! `validator_matches_executor`.
//!
//! The validator independently re-executes the canonical stream and
//! cross-checks the executor's published BALs and receipts. This scenario
//! watches that machinery end to end, then goes one level deeper than the
//! streams can: an offline, byte-level comparison of the two libmdbx
//! databases.
//!
//! Live phase ([`run`], target-agnostic): a mixed workload of transfers and
//! contract creates, then drain, then check the result. The checks compare
//! deltas over the workload, so a validator that replayed a backlog before
//! catching up is judged on what it does now: that it verified new blocks,
//! missed no more BALs than its budget allows (zero on IPC, a small budget
//! on the cluster's lossy multicast), never diverged, ran the per-block
//! trie shadow-check with no mismatch, and caught its committed cursor up
//! to the executor's.
//!
//! Offline phase ([`verify_state_dirs`], runs after a graceful shutdown;
//! Target C gets the directories with `docker cp`): `kardamom_state::sweep`
//! on both databases must be clean, the validator must hold a persisted and
//! reproducible state root, and `deep_compare` must find the chain-state
//! tables byte-identical. The stream checks catch execution divergence; the
//! table diff catches persistence divergence.

use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{SeqCounters, Target};
use crate::harness::l2::{self, SignedTransfer};
use crate::harness::metrics::poll_until;

/// PUSH1 01 PUSH1 00 MSTORE8 PUSH1 01 PUSH1 00 RETURN: deploys a 1-byte
/// runtime. This is the smallest real CREATE.
const TINY_INIT_CODE: &[u8] = &[0x60, 0x01, 0x60, 0x00, 0x53, 0x60, 0x01, 0x60, 0x00, 0xf3];

/// Sequential transactions submitted after the validator catches up, to
/// ask "does verification work on fresh blocks?" See the probe below.
const PROBE_TXS: u64 = 5;

pub struct Params {
    pub senders: NonZeroUsize,
    pub transfers_per_sender: NonZeroUsize,
    pub sender_base: usize,
    /// How many BALs the validator may miss for this scenario's own blocks.
    ///
    /// Zero on Target L: one host, IPC transport, no lossy hop. On Target C,
    /// `tx_bal` uses UDP multicast, which is lossy by design. A dropped BAL
    /// leaves that block unverified, and the design does not treat this as
    /// a divergence. So the cluster runner allows a small budget. The
    /// check compares a delta over the workload, not an absolute count,
    /// because a validator that replayed a backlog before catching up
    /// commits those older blocks unverified on purpose (`BalBuffer`'s
    /// catch-up mode). That history says nothing about whether the
    /// validator is verifying now.
    pub max_bal_missing: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            senders: NonZeroUsize::new(4).unwrap(),
            transfers_per_sender: NonZeroUsize::new(24).unwrap(),
            sender_base: 1,
            max_bal_missing: 0.0,
        }
    }
}

/// This scenario's target, params, and shared recipient. Every step below
/// reads these as state instead of taking them as loose parameters.
struct ConsistencyRun<'a> {
    t: &'a Target,
    p: &'a Params,
    to: Address,
}

impl<'a> ConsistencyRun<'a> {
    fn new(t: &'a Target, p: &'a Params, to: Address) -> Self {
        Self { t, p, to }
    }

    /// One sender's run: `transfers_per_sender` dense transfers, then a
    /// contract CREATE.
    fn sign_sender_run(&self, signer: &l2::DerivedSigner) -> Result<Vec<SignedTransfer>> {
        let transfers_per_sender = self.p.transfers_per_sender.get();
        let mut run: Vec<SignedTransfer> = (0..transfers_per_sender)
            .map(|n| l2::sign_transfer(signer, self.t.chain_id, n as u64, self.to, 1))
            .collect::<Result<_>>()?;
        run.push(l2::sign_create(
            signer,
            self.t.chain_id,
            transfers_per_sender as u64,
            TINY_INIT_CODE,
        )?);
        Ok(run)
    }

    /// A mixed workload: dense transfers, with the last nonce of each
    /// sender a contract CREATE. This exercises the code and storage-trie
    /// paths in both databases.
    fn build_workload(&self, signers: &[l2::DerivedSigner]) -> Result<Vec<SignedTransfer>> {
        let runs = signers
            .iter()
            .map(|signer| self.sign_sender_run(signer))
            .collect::<Result<Vec<Vec<SignedTransfer>>>>()?;
        Ok(runs.into_iter().flatten().collect())
    }

    /// Wait until the validator's committed cursor reaches the executor's
    /// current block. The committed cursor chases a moving head, so poll
    /// until it reaches the executor block sampled in the same round.
    async fn await_validator_caught_up(&self) -> Result<()> {
        poll_until(
            "validator committed == executor block",
            Duration::from_secs(30),
            Duration::from_millis(250),
            || async {
                let exec = self.t.executor_metric(super::EXEC_BLOCK_NUMBER).await?;
                let committed = self
                    .t
                    .validator_metric_opt(super::VALIDATOR_COMMITTED_BLOCK)
                    .await?
                    .unwrap_or(0.0);
                Ok((committed >= exec).then_some(()))
            },
        )
        .await
    }

    /// Verification probe. The bulk workload gets the validator behind. A
    /// validator behind the head commits blocks unverified on purpose:
    /// `BalBuffer`'s catch-up mode treats a BAL older than the backlog
    /// lookbehind as unrecoverable, instead of crawling through it. So
    /// this cannot ask "did verification happen?" of burst blocks. It
    /// must ask fresh blocks instead, with the validator caught up
    /// ([`Self::await_validator_caught_up`] guarantees that). A handful
    /// of sequential transactions spans a few blocks, so one lost
    /// multicast BAL cannot decide the outcome.
    async fn run_verification_probe(&self, probe_signer: &l2::DerivedSigner) -> Result<()> {
        let verified_before = self
            .t
            .validator_metric_opt(super::VALIDATOR_BLOCKS_VERIFIED)
            .await?
            .unwrap_or(0.0);
        let missing_before = self
            .t
            .validator_metric_opt(super::VALIDATOR_BAL_MISSING)
            .await?
            .unwrap_or(0.0);
        let applied_pre_probe = self.t.executor_metric(super::EXEC_TX_APPLIED).await?;
        for i in 0..PROBE_TXS {
            // `transfers_per_sender` is a Params count; the probe's nonce
            // run continues right after it, so an overflowing sum must
            // fail loudly.
            let nonce = (self.p.transfers_per_sender.get() as u64)
                .checked_add(1)
                .and_then(|n| n.checked_add(i))
                .context("probe nonce overflows")?;
            let tx = l2::sign_transfer(probe_signer, self.t.chain_id, nonce, self.to, 1)?;
            self.t
                .rpc
                .send_raw(&tx.raw)
                .await
                .result
                .map_err(|e| anyhow::anyhow!("probe tx {i}: {e}"))?;
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "PROBE_TXS is a small constant (5), exactly representable in f64"
        )]
        let probe_txs_f64 = PROBE_TXS as f64;
        self.t
            .wait_executor_applied(applied_pre_probe + probe_txs_f64, Duration::from_secs(30))
            .await?;
        self.t
            .wait_validator_metric_above(
                super::VALIDATOR_BLOCKS_VERIFIED,
                verified_before,
                Duration::from_secs(60),
                Duration::from_millis(500),
                "validator verifies the probe blocks",
            )
            .await
            .with_context(|| {
                format!(
                    "validator verified no fresh block while caught up (stuck at \
                     {verified_before}) — every probe block was committed unverified"
                )
            })?;
        let missing = self
            .t
            .validator_metric_opt(super::VALIDATOR_BAL_MISSING)
            .await?
            .unwrap_or(0.0);
        let missed = missing - missing_before;
        anyhow::ensure!(
            missed <= self.p.max_bal_missing,
            "validator missed {missed} BALs on the probe blocks (budget {}); a missing BAL \
             leaves a block unverified — tolerated on lossy multicast, never on IPC",
            self.p.max_bal_missing
        );
        Ok(())
    }
}

/// # Errors
/// Returns an error when a transaction fails to sign or send, when the
/// executor or validator does not reach the expected state within its
/// deadline, when the sequencer's health counters moved, when the
/// validator reported a divergence or a trie shadow-check mismatch, or
/// when the trie shadow-check never ran.
pub async fn run(t: &Target, p: Params) -> Result<()> {
    // This is a total signer count already (not a highest index), so it
    // takes no `+ 1`.
    let signers = l2::dev_signers_total(
        p.sender_base
            .checked_add(p.senders.get())
            .context("sender_base + senders overflows")?,
    )?;
    let to = Address::from([0x66u8; 20]);
    let baseline = SeqCounters::snapshot(t).await?;
    let applied_before = t
        .executor_metric_opt(super::EXEC_TX_APPLIED)
        .await?
        .unwrap_or(0.0);
    let run_ctx = ConsistencyRun::new(t, &p, to);
    let planned = run_ctx.build_workload(&signers[p.sender_base..])?;
    #[allow(
        clippy::cast_precision_loss,
        reason = "a workload sized by test parameters, always small enough for f64 to \
                   represent exactly"
    )]
    let total = planned.len() as f64;
    super::submit_all(t, planned).await?;
    t.wait_executor_applied(applied_before + total, Duration::from_secs(30))
        .await?;
    baseline.assert_flat(t, "consistency workload").await?;

    // Validator verdict: caught up, verifying, never diverged, and the
    // shadow-check is active and clean.
    run_ctx.await_validator_caught_up().await?;
    run_ctx
        .run_verification_probe(&signers[p.sender_base])
        .await?;

    let divergence = t
        .validator_metric_opt(super::VALIDATOR_DIVERGENCE)
        .await?
        .unwrap_or(0.0);
    anyhow::ensure!(divergence == 0.0, "validator reported divergence");
    let checks = t.validator_metric(super::TRIE_SHADOW_CHECKS).await?;
    anyhow::ensure!(checks > 0.0, "trie shadow-check never ran");
    let mismatch = t
        .validator_metric_opt(super::TRIE_SHADOW_MISMATCH)
        .await?
        .unwrap_or(0.0);
    anyhow::ensure!(mismatch == 0.0, "trie shadow-check mismatched");
    Ok(())
}

/// Offline phase: both databases are internally coherent, the chain
/// state is byte-identical, and the validator holds a reproducible root.
/// This needs cleanly closed databases (graceful shutdown, or copies of
/// stopped services).
///
/// # Errors
/// Returns an error when either database fails to open or sweep clean,
/// when the two databases stopped at different blocks, when the workload
/// produced no blocks, when the two state roots unexpectedly match, or
/// when `deep_compare` finds a difference.
pub fn verify_state_dirs(executor_dir: &Path, validator_dir: &Path) -> Result<()> {
    let exec_env = kardamom_state::StateEnvBuilder::new(executor_dir)
        .open()
        .context("open executor state dir")?;
    let val_env = kardamom_state::StateEnvBuilder::new(validator_dir)
        .open()
        .context("open validator state dir")?;

    let exec_report = kardamom_state::sweep(&exec_env).context("sweep executor DB")?;
    anyhow::ensure!(
        exec_report.is_clean(),
        "executor DB sweep: {:?}",
        exec_report.problems
    );

    let val_report = kardamom_state::sweep(&val_env).context("sweep validator DB")?;
    anyhow::ensure!(
        val_report.is_clean(),
        "validator DB sweep: {:?}",
        val_report.problems
    );
    anyhow::ensure!(
        val_report.state_root.is_some(),
        "validator DB has no persisted state root"
    );

    // Both databases must have executed the same chain.
    anyhow::ensure!(
        exec_report.last_committed_block == val_report.last_committed_block,
        "executor stopped at block {} but validator at {}",
        exec_report.last_committed_block,
        val_report.last_committed_block
    );
    anyhow::ensure!(
        val_report.last_committed_block > 0,
        "the workload produced no blocks"
    );
    // Only the validator keeps a live trie. `seed_genesis` writes
    // meta[state_root] on every database, so the executor's plain
    // (TrieMode::Off) writer leaves a genesis-era root frozen at block 0
    // while the chain advances. This check, that the two roots differ,
    // pins down that asymmetry. It would also catch an executor that
    // silently started keeping a trie, or a validator whose root stopped
    // advancing.
    if let (Some(exec_root), Some(val_root)) = (exec_report.state_root, val_report.state_root) {
        anyhow::ensure!(
            exec_root != val_root,
            "executor and validator roots match ({exec_root}) — the executor's root should be \
             the frozen genesis root and the validator's the live one at block {}",
            val_report.last_committed_block
        );
    }

    let diffs = kardamom_state::deep_compare(&exec_env, &val_env).context("deep compare")?;
    anyhow::ensure!(
        diffs.is_empty(),
        "executor and validator DBs differ:\n{}",
        diffs.join("\n")
    );
    Ok(())
}
