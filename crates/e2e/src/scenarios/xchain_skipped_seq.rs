//! The sealer-guard drill and the adversarial gap arm, split out of
//! `xchain.rs` to keep that file under the line budget.

use std::time::Duration;

use alloy_primitives::U256;
use anyhow::{Context, Result};
use kardamom_da_watcher::interop::mock::MockInteropFeed;
use kardamom_types::xchain::{INBOX, Inbox, derive_remote_epoch, remote_source_hash};

use super::xchain::{DeliveryOutcome, ORIGIN_CHAIN_ID, feed_msg, read_slot};
use super::{Target, assert_receipt_ok, await_l2_receipt};
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics;

/// State for the sealer-guard drill below: the stack handle, the
/// injection point, the sealer logs to grep, the executor state dir to
/// read back, and the delivery outcome the injected record's payee comes
/// from. The steps read this as state instead of taking it as loose
/// parameters.
struct SkippedSeqCheck<'a> {
    t: &'a Target,
    aeron_dir: &'a std::path::Path,
    sealer_logs: &'a [std::path::PathBuf],
    executor_state_dir: &'a std::path::Path,
    outcome: &'a DeliveryOutcome,
}

impl SkippedSeqCheck<'_> {
    /// Inject a well-formed record at seq 5 (skipping 3 and 4) in a later
    /// origin block, derived by the same rule the watcher runs: the
    /// sealer must reject it on the lane cursor alone, not on its shape.
    async fn inject(&self) -> Result<()> {
        let skipped = derive_remote_epoch(
            self.t.chain_id,
            ORIGIN_CHAIN_ID,
            5,
            &[feed_msg(5, 110, self.outcome.receiver, &[0x55], None)],
        )
        .context("derive the skipping record")?;
        anyhow::ensure!(skipped.first_seq == 5 && skipped.anchor_number == 110);
        crate::harness::inject::publish_remote_epoch(self.aeron_dir, skipped).await
    }

    /// Wait for the sequencers to count one more reject, and for the
    /// sealer to log the reason: the lane cursor was 3.
    async fn await_reject_evidence(&self, rejects_before: f64) -> Result<()> {
        metrics::poll_until(
            "a sealer REMOTE-ORIGIN-REJECT counted by the sequencers",
            Duration::from_secs(30),
            Duration::from_millis(250),
            || async {
                let now = self
                    .t
                    .sequencer_metric_sum(super::SEQ_REMOTE_ORIGIN_REJECT)
                    .await?;
                // A counter-vs-counter compare: this metric only ever
                // grows by whole units, so `+ 1.0` is exact, not a
                // float-tolerance guess.
                Ok((now >= rejects_before + 1.0).then_some(now))
            },
        )
        .await?;

        // The sealer prints before it offers, so the line is there by
        // now; the poll only covers a slow log flush.
        let needle = "REMOTE-ORIGIN-REJECT memberId=";
        let detail = format!("origin={ORIGIN_CHAIN_ID} firstSeq=5 expectedNextSeq=3 reason=1");
        metrics::poll_until(
            "the sealer REMOTE-ORIGIN-REJECT log line",
            Duration::from_secs(10),
            Duration::from_millis(250),
            || async {
                let logs: String = self
                    .sealer_logs
                    .iter()
                    .map(|p| std::fs::read_to_string(p).unwrap_or_default())
                    .collect();
                Ok(logs
                    .lines()
                    .any(|l| l.contains(needle) && l.contains(&detail))
                    .then_some(()))
            },
        )
        .await?;
        Ok(())
    }

    /// Nothing executed: the lane cursor is untouched, and the skipping
    /// record's message has no receipt.
    async fn assert_lane_untouched(&self) -> Result<()> {
        let next_seq = read_slot(
            self.executor_state_dir,
            INBOX,
            Inbox::next_seq_slot(ORIGIN_CHAIN_ID),
        )?;
        anyhow::ensure!(
            next_seq == U256::from(3),
            "Inbox.nextSeq must stay 3 after the reject, got {next_seq}"
        );
        let gone = self
            .t
            .rpc
            .receipt(remote_source_hash(ORIGIN_CHAIN_ID, 5))
            .await
            .result
            .map_err(|e| anyhow::anyhow!("receipt probe for the skipping seq: {e}"))?;
        anyhow::ensure!(
            gone.is_none(),
            "seq 5 skipped the lane and must never execute: {gone:?}"
        );
        Ok(())
    }
}

/// The sealer-guard arm: a kind-5 record that SKIPS the lane cursor
/// reaches the cluster through the real sequencers, and the sealer
/// answers with a reject frame instead of sealing a hole.
///
/// Runs after [`super::xchain::delivery`]: seqs 0..2 are delivered, seq 3
/// is pending, and the sealer's lane cursor for the origin is 3. The
/// injected record starts at seq 5. Evidence:
///
/// * the sequencers count `kardamom_sequencer_remote_origin_reject_total`
///   with reason `seq_mismatch` (the reject frame reached the offering
///   session and was decoded);
/// * the sealer logged `cluster REMOTE-ORIGIN-REJECT … expectedNextSeq=3`;
/// * nothing executed: `Inbox.nextSeq` is still 3, and seq 5 has no receipt.
///
/// The lane stays intact: [`gap_halts_pair_not_chain`] runs next and
/// delivers seq 3 through the same sealer, which proves the reject did not
/// move the lane cursor and did not poison the dedup window.
///
/// # Errors
/// Returns an error when a metrics scrape, a log grep, or the state read
/// fails, or when the evidence does not match.
pub async fn sealer_rejects_a_skipped_seq(
    t: &Target,
    aeron_dir: &std::path::Path,
    sealer_logs: &[std::path::PathBuf],
    executor_state_dir: &std::path::Path,
    outcome: &DeliveryOutcome,
) -> Result<()> {
    let check = SkippedSeqCheck {
        t,
        aeron_dir,
        sealer_logs,
        executor_state_dir,
        outcome,
    };
    let rejects_before = t
        .sequencer_metric_sum(super::SEQ_REMOTE_ORIGIN_REJECT)
        .await?;
    check.inject().await?;
    check.await_reject_evidence(rejects_before).await?;
    check.assert_lane_untouched().await
}

/// The adversarial arm: the feed swallows one seq. The watcher must
/// fail-stop — the PROCESS exits nonzero, nothing is skipped — while the
/// chain itself keeps sealing blocks. Pair-scoped fault domain, proven.
///
/// # Errors
/// Returns an error when the watcher does not exit nonzero within 60s,
/// when its log names no derivation fault, when the pending seq is never
/// delivered, when the chain does not keep sealing new blocks, or when
/// the swallowed seq unexpectedly executes.
pub async fn gap_halts_pair_not_chain(
    t: &Target,
    feed: &MockInteropFeed,
    watcher: &mut crate::harness::services::Spawned,
    outcome: DeliveryOutcome,
) -> Result<()> {
    let signers = l2::dev_signers_total(2)?;
    let sender: &DerivedSigner = &signers[0];
    let payee = signers[1].address;
    let mut nonce = outcome.next_nonce;

    // seq 4 is swallowed by the feed — the hole. seq 5 (block 104) closes
    // block 102, delivering the pending seq 3; seq 6 (block 105) closes
    // block 104, whose batch starts at 5 while the lane cursor says 4:
    // SeqSkipped, the terminal pair fault.
    feed.gap_next(1);
    feed.push_message(feed_msg(4, 103, payee, &[0x04], None));
    feed.push_message(feed_msg(5, 104, payee, &[0x05], None));
    feed.push_message(feed_msg(6, 105, payee, &[0x06], None));

    // The interop path runs ALONE in this process, so the pair fault is a
    // process exit — and it must be NONZERO: a fail-stop that looks like a
    // clean shutdown would hide the halt from the orchestrator.
    let exit = watcher
        .proc
        .wait_exit(Duration::from_secs(60))
        .context("watcher did not exit after the seq gap — it skipped or stalled")?;
    anyhow::ensure!(
        exit.is_some_and(|code| code != 0),
        "watcher must exit NONZERO on a derivation fault, got {exit:?}"
    );
    let log = std::fs::read_to_string(&watcher.proc.log_path).unwrap_or_default();
    anyhow::ensure!(
        log.contains("remote epoch derivation fault"),
        "watcher log must name the derivation fault; tail:\n{}",
        watcher.proc.log_tail(15)
    );

    // seq 3 (pending from the happy path) was delivered on the way to the
    // fault; the swallowed seq 4 and everything after it must NEVER execute.
    await_l2_receipt(
        t,
        remote_source_hash(ORIGIN_CHAIN_ID, 3),
        "xchain delivery seq 3",
    )
    .await?;

    // THE CHAIN KEEPS SEALING. New ordinary transactions land in new blocks
    // after the watcher's death — the fault stopped one pair, not the chain.
    let head_at_halt = super::metric_u64(t.executor_metric(super::EXEC_BLOCK_NUMBER).await?)?;
    for _ in 0..2 {
        let tx = l2::sign_transfer(sender, t.chain_id, nonce, payee, 1)?;
        nonce += 1;
        t.rpc
            .send_raw(&tx.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("post-halt transfer rejected: {e}"))?;
        let r = await_l2_receipt(t, tx.hash, "a post-halt transfer").await?;
        assert_receipt_ok(&r, "a post-halt transfer")?;
    }
    // `head_at_halt` is a metric-derived value; a bad or adversarial
    // reading must fail loudly, not silently wrap the threshold.
    let past_halt = head_at_halt
        .checked_add(1)
        .context("head_at_halt overflows")?;
    t.wait_executor_block(
        past_halt,
        Duration::from_secs(30),
        Duration::from_millis(250),
        "the head to advance past the halt",
    )
    .await?;

    // And the hole was never stepped over: the swallowed seq has no receipt.
    let gone = t
        .rpc
        .receipt(remote_source_hash(ORIGIN_CHAIN_ID, 4))
        .await
        .result
        .map_err(|e| anyhow::anyhow!("receipt probe for the swallowed seq: {e}"))?;
    anyhow::ensure!(
        gone.is_none(),
        "seq 4 was swallowed by the feed and must never execute: {gone:?}"
    );
    Ok(())
}
