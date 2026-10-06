//! The da-watcher follows the sealer's commit across a chaos restart.
//!
//! The sealer accepts only the epoch of the block after its L1 origin. The
//! da-watcher's cursor file holds that origin, as the boundaries confirm
//! it, and a start resumes after the origin of the sealer's first
//! boundary. The da-watcher publishes again every epoch that no boundary
//! confirms in time. So no chaos step puts the da-watcher at the sealer's
//! origin by hand: these checks assert that it got there by itself.

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::DA_WATCHER_PORT;

/// The cursor file on the aux node: one line, `<number> <hash>`.
const CURSOR_FILE: &str = "/opt/kardamom/da-watcher/l1-cursor";
const JOB: &str = "da-watcher";
/// The L1 block of the last epoch the da-watcher published.
const PUBLISHED_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";

/// How long the cursor may stand past the sealer's origin after a start:
/// the start's wait for the first boundary (20 s), then one pass.
const AHEAD_BUDGET_SECS: u64 = 60;

/// How long the sealer may take to commit every epoch the da-watcher
/// published: the sequencer's grace before `origin_gap` (90 s), which
/// holds three re-publish periods, and a full-restart election.
const COMMIT_BUDGET_SECS: u64 = 180;

/// The cursor file's line, or `None` when the file is absent.
async fn cursor_line(h: &Harness) -> anyhow::Result<Option<String>> {
    let aux = &h.probes.validator.container;
    let line = h
        .nodes
        .exec(aux, &format!("cat {CURSOR_FILE} 2>/dev/null || true"))
        .await?;
    Ok(Some(line.trim().to_string()).filter(|l| !l.is_empty()))
}

/// The block number of a cursor line.
fn cursor_number(line: &str) -> Option<u64> {
    line.split_ascii_whitespace().next()?.parse().ok()
}

/// Restart the da-watcher from its registered job, with no extra flag.
/// The start resumes after the sealer's L1 origin, and reads that block's
/// hash again through its L1 sources. This replaces a wrong hash in the
/// cursor file. Then assert that it stands at or before the sealer.
///
/// # Errors
///
/// Returns an error when a job step fails, or when the cursor stands past
/// the sealer's origin.
pub(crate) async fn restart_and_follow_sealer(h: &mut Harness, ctx: &str) -> anyhow::Result<()> {
    crate::log(format!(
        "{ctx}: restart the da-watcher; it resumes after the sealer's L1 origin by itself"
    ));
    let saved = SavedJob::capture(&h.nomad, JOB).await?;
    saved.stop().await?;
    saved.restore().await?;
    h.assert_count(JOB, 1, h.knobs.restart_slo).await?;
    assert_not_past_sealer(h, ctx).await
}

/// Assert that the da-watcher's cursor file does not stand past the
/// sealer's L1 origin. The file holds the origin the boundaries
/// confirmed, so a kill between a publish and its commit leaves it at or
/// before the sealer.
///
/// # Errors
///
/// Returns an error when a probe fails, or when the cursor stays past the
/// origin for [`AHEAD_BUDGET_SECS`].
pub(crate) async fn assert_not_past_sealer(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(AHEAD_BUDGET_SECS, 2), |_| async {
        let cursor = cursor_line(h).await?.as_deref().and_then(cursor_number);
        let origin = h.probes.sealer_l1_origin().await;
        Ok(cursor
            .zip(origin)
            .filter(|(c, o)| c <= o)
            .map(|(c, o)| format!("cursor {c}, sealer origin {o}")))
    })
    .await?;
    let (value, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the da-watcher's cursor stands past the sealer's L1 origin for {}s: it does not follow the sealer",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the da-watcher stands at or before the sealer ({value})"
    ));
    Ok(())
}

/// Assert that no origin gap remains: the sealer's L1 origin reaches the
/// last block the da-watcher published. The sealer refuses an epoch that
/// skips a block, so an origin at or past that block proves that every
/// epoch up to it is committed, with no hole. An epoch lost between its
/// publish and its commit is published again within one re-publish
/// period, with no operator step.
///
/// # Errors
///
/// Returns an error when the da-watcher reports no published epoch, or
/// when the sealer's origin does not reach it within
/// [`COMMIT_BUDGET_SECS`].
pub(crate) async fn assert_no_origin_gap(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(AHEAD_BUDGET_SECS, 2), |_| async {
        Ok(h.probes
            .aux_metric(DA_WATCHER_PORT, PUBLISHED_ORIGIN)
            .await
            .and_then(|p| u64::try_from(p).ok()))
    })
    .await?;
    let (published, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the da-watcher published no epoch within {}s",
            t.as_secs()
        )
    })?;
    let outcome = poll::until(Budget::secs(COMMIT_BUDGET_SECS, 2), |_| async {
        let origin = h.probes.sealer_l1_origin().await;
        Ok(origin.filter(|o| *o >= published))
    })
    .await?;
    let (origin, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the sealer's L1 origin did not reach the da-watcher's published block {published} within {}s: an origin gap remains",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: no origin gap remains: the sealer's origin {origin} reached the published block {published} after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::cursor_number;

    #[test]
    fn a_cursor_line_names_its_block() {
        let hash = format!("0x{}", "ab".repeat(32));
        assert_eq!(cursor_number(&format!("1631 {hash}")), Some(1631));
        assert_eq!(cursor_number(""), None);
        assert_eq!(cursor_number("x y"), None);
    }
}
