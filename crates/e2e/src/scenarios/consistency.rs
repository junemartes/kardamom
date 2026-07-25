//! S6 — `validator_matches_executor`.
//!
//! The validator independently re-executes the canonical stream and
//! cross-checks the executor's published BALs and receipts; this scenario
//! observes that machinery end-to-end and then goes one level deeper than
//! the streams can: an offline, byte-level comparison of the two libmdbx
//! databases.
//!
//! Live phase ([`run`], target-agnostic): mixed workload (transfers +
//! contract creates), drain, then assert — as DELTAS over the workload, so a
//! validator that replayed a backlog before catching up is judged on what it
//! does now — that it verified new blocks, missed no more BALs than its
//! budget allows (zero on IPC, a small budget on the cluster's lossy
//! multicast), never diverged, ran the per-block trie shadow-check without a
//! mismatch, and caught its committed cursor up to the executor's.
//!
//! Offline phase ([`verify_state_dirs`], runs after a graceful shutdown —
//! Target C obtains the dirs via `docker cp`): `kardamom_state::sweep` on
//! both DBs must be clean, the validator must hold a persisted (and
//! reproducible) state root, and `deep_compare` must find the chain-state
//! tables byte-identical. Stream checks catch execution divergence; the
//! table diff catches persistence divergence.

use std::path::Path;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{SeqCounters, Target};
use crate::harness::l2::{self, SignedTransfer};
use crate::harness::metrics::poll_until;

/// PUSH1 01 PUSH1 00 MSTORE8 PUSH1 01 PUSH1 00 RETURN — deploys a 1-byte
/// runtime, the minimal real CREATE.
const TINY_INIT_CODE: &[u8] = &[0x60, 0x01, 0x60, 0x00, 0x53, 0x60, 0x01, 0x60, 0x00, 0xf3];

/// Sequential transactions submitted AFTER the validator has caught up, to
/// ask "does verification work on fresh blocks?" — see the probe below.
const PROBE_TXS: u64 = 5;

pub struct Params {
    pub senders: usize,
    pub transfers_per_sender: usize,
    pub sender_base: usize,
    /// How many BALs the validator may miss FOR THIS SCENARIO'S OWN BLOCKS.
    ///
    /// Zero on Target L: one host, IPC transport, no lossy hop. On Target C
    /// `tx_bal` is UDP multicast, which is lossy by design — a dropped BAL
    /// leaves that block unverified and is explicitly not a divergence — so
    /// the cluster runner allows a small budget. The check is a DELTA across
    /// the workload rather than an absolute count, because a validator that
    /// replayed a backlog before catching up legitimately commits those older
    /// blocks unverified (`BalBuffer`'s catch-up mode), and that history says
    /// nothing about whether it is verifying now.
    pub max_bal_missing: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            senders: 4,
            transfers_per_sender: 24,
            sender_base: 1,
            max_bal_missing: 0.0,
        }
    }
}

pub async fn run(t: &Target, p: Params) -> Result<()> {
    let signers = l2::dev_signers((p.sender_base + p.senders) as u32)?;
    let to = Address::from([0x66u8; 20]);
    let baseline = SeqCounters::snapshot(t).await?;
    let applied_before = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);
    // Baselines for the validator counters: everything below is asserted as a
    // DELTA over this scenario's workload, so a validator that started behind
    // a running chain is judged on what it does now, not on the backlog it
    // committed unverified while catching up.
    let verified_before = t
        .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
        .await
        .unwrap_or(0.0);
    let missing_before = t
        .validator_metric(super::VALIDATOR_BAL_MISSING)
        .await
        .unwrap_or(0.0);

    // Mixed workload: dense transfers, with the last nonce of each sender a
    // contract CREATE (exercises code + storage-trie paths in both DBs).
    let mut planned: Vec<SignedTransfer> = Vec::new();
    for signer in &signers[p.sender_base..] {
        for n in 0..p.transfers_per_sender {
            planned.push(l2::sign_transfer(signer, t.chain_id, n as u64, to, 1)?);
        }
        planned.push(l2::sign_create(
            signer,
            t.chain_id,
            p.transfers_per_sender as u64,
            TINY_INIT_CODE,
        )?);
    }
    let total = planned.len();

    let mut set = tokio::task::JoinSet::new();
    for tx in planned {
        let rpc = t.rpc.clone();
        set.spawn(async move {
            let out = rpc.send_raw(&tx.raw).await;
            (tx, out)
        });
    }
    while let Some(joined) = set.join_next().await {
        let (tx, out) = joined.context("submit join")?;
        out.result
            .map_err(|e| anyhow::anyhow!("sender {} nonce {}: {e}", tx.sender, tx.nonce))?;
    }
    t.wait_executor_applied(applied_before + total as f64, Duration::from_secs(30))
        .await?;
    baseline.assert_flat(t, "consistency workload").await?;

    // Validator verdict: caught up, verifying, never diverged, shadow-check
    // active and clean. (Committed chases a moving head — poll until it
    // reaches the executor block sampled in the same round.)
    poll_until(
        "validator committed == executor block",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let exec = t.executor_metric(super::EXEC_BLOCK_NUMBER).await?;
            let committed = t
                .validator_metric(super::VALIDATOR_COMMITTED_BLOCK)
                .await
                .unwrap_or(0.0);
            Ok((committed >= exec).then_some(()))
        },
    )
    .await?;
    // VERIFICATION PROBE. The bulk workload above deliberately gets the
    // validator behind, and a validator behind the head commits blocks
    // UNVERIFIED on purpose — `BalBuffer`'s catch-up mode treats a BAL older
    // than the backlog lookbehind as unrecoverable rather than crawling. So
    // "did verification happen?" cannot be asked of burst blocks; it has to
    // be asked of fresh ones, with the validator caught up (the poll above
    // guarantees that). A handful of sequential transactions spans a few
    // blocks, so a single lost multicast BAL cannot decide the outcome.
    let verified_before = t
        .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
        .await
        .unwrap_or(verified_before);
    let missing_before = t
        .validator_metric(super::VALIDATOR_BAL_MISSING)
        .await
        .unwrap_or(missing_before);
    let probe_signer = &signers[p.sender_base];
    let applied_pre_probe = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    for i in 0..PROBE_TXS {
        let nonce = p.transfers_per_sender as u64 + 1 + i;
        let tx = l2::sign_transfer(probe_signer, t.chain_id, nonce, to, 1)?;
        t.rpc
            .send_raw(&tx.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("probe tx {i}: {e}"))?;
    }
    t.wait_executor_applied(
        applied_pre_probe + PROBE_TXS as f64,
        Duration::from_secs(30),
    )
    .await?;
    let verified = poll_until(
        "validator verifies the probe blocks",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || async {
            let v = t
                .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
                .await
                .unwrap_or(0.0);
            Ok((v > verified_before).then_some(v))
        },
    )
    .await
    .with_context(|| {
        format!(
            "validator verified no fresh block while caught up (stuck at {verified_before}) — \
             every probe block was committed unverified"
        )
    })?;
    let missing = t
        .validator_metric(super::VALIDATOR_BAL_MISSING)
        .await
        .unwrap_or(0.0);
    let missed = missing - missing_before;
    anyhow::ensure!(
        missed <= p.max_bal_missing,
        "validator missed {missed} BALs on the probe blocks (budget {}); a missing BAL leaves \
         a block unverified — tolerated on lossy multicast, never on IPC",
        p.max_bal_missing
    );
    let _ = verified;
    let divergence = t
        .validator_metric(super::VALIDATOR_DIVERGENCE)
        .await
        .unwrap_or(0.0);
    anyhow::ensure!(divergence == 0.0, "validator reported divergence");
    let checks = t.validator_metric(super::TRIE_SHADOW_CHECKS).await?;
    anyhow::ensure!(checks > 0.0, "trie shadow-check never ran");
    let mismatch = t
        .validator_metric(super::TRIE_SHADOW_MISMATCH)
        .await
        .unwrap_or(0.0);
    anyhow::ensure!(mismatch == 0.0, "trie shadow-check mismatched");
    Ok(())
}

/// Offline phase: both DBs internally coherent, byte-identical chain state,
/// and the validator holds a reproducible root. Requires cleanly-closed DBs
/// (graceful shutdown / copies of stopped services).
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

    // Both DBs must have executed the same chain…
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
    // …and only the validator maintains a live trie. `seed_genesis` writes
    // meta[state_root] on every DB, so the executor's plain (TrieMode::Off)
    // writer leaves a GENESIS-era root frozen at block 0 while the chain
    // advances — asserting the two roots differ pins that asymmetry (and
    // would catch an executor that silently started maintaining a trie, or a
    // validator whose root stopped advancing).
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
