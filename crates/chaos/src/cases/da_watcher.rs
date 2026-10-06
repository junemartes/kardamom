//! The da-watcher's L1 cursor across a chaos restart.
//!
//! The sealer accepts only the epoch of the block after its L1 origin. A
//! da-watcher that resumes past that block skips epochs: the sealer
//! refuses the next one as an origin gap, and every sequencer halts on
//! `origin_gap`. So a step that restarts the da-watcher must leave it at
//! or before the sealer's origin. The operator step that puts it there is
//! the one the `l1_chain_break` and `origin_gap` runbooks give: run the job
//! once with `--l1-resume-after <origin>`, then restore the registered job.

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};

/// The cursor file on the aux node: one line, `<number> <hash>`.
const CURSOR_FILE: &str = "/opt/kardamom/da-watcher/l1-cursor";
const JOB: &str = "da-watcher";

/// How long the cursor may stand past the sealer's origin before the
/// check calls it a gap. In normal flow the two differ only between a
/// publish and its commit, milliseconds.
const AHEAD_BUDGET_SECS: u64 = 60;

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

/// The sealer's L1 origin, as the sequencers saw it on a boundary.
async fn sealer_origin(h: &Harness, ctx: &str) -> anyhow::Result<u64> {
    let outcome = poll::until(Budget::secs(60, 2), |_| async {
        Ok(h.probes.sealer_l1_origin().await)
    })
    .await?;
    let (origin, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: no sequencer reported the sealer's L1 origin within {}s",
            t.as_secs()
        )
    })?;
    Ok(origin)
}

/// The operator step: run the da-watcher once with `--l1-resume-after`
/// at the sealer's L1 origin, wait until its first tick wrote the cursor
/// file anew, and restore the registered job. The restored job resumes
/// from the file. Returns the origin it resumed after.
///
/// # Errors
///
/// Returns an error when no sequencer reports the origin, when a job
/// step fails, or when the file does not change in time.
pub(crate) async fn resume_after_sealer_origin(h: &mut Harness, ctx: &str) -> anyhow::Result<u64> {
    let origin = sealer_origin(h, ctx).await?;
    crate::log(format!(
        "{ctx}: OPERATOR STEP: run the da-watcher once with --l1-resume-after {origin}, the sealer's L1 origin"
    ));
    let before = cursor_line(h).await?;
    let saved = SavedJob::capture(&h.nomad, JOB).await?;
    saved.stop().await?;
    saved
        .restore_with_args(JOB, &["--l1-resume-after".to_string(), origin.to_string()])
        .await?;
    await_cursor_rewritten(h, before.as_deref(), ctx).await?;
    // The flag stays only for one start, as the runbooks say.
    saved.restore().await?;
    h.assert_count(JOB, 1, h.knobs.restart_slo).await?;
    Ok(origin)
}

/// Wait until the cursor file holds a line other than `before`: the first
/// tick after `--l1-resume-after` wrote the resume block and its hash.
async fn await_cursor_rewritten(
    h: &Harness,
    before: Option<&str>,
    ctx: &str,
) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(120, 2), |_| async {
        let now = cursor_line(h).await?;
        Ok(now.filter(|line| Some(line.as_str()) != before))
    })
    .await?;
    let (line, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the da-watcher did not write its cursor file within {}s of --l1-resume-after",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the da-watcher wrote its cursor `{line}` after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

/// Check that the restarted da-watcher does not stand past the sealer's
/// L1 origin. A kill between a publish and its commit loses that epoch,
/// while the cursor file keeps the publish: the da-watcher then resumes
/// past the gap. When the cursor stays past the origin, run the operator
/// step and report it.
///
/// # Errors
///
/// Returns an error when a probe fails or the operator step fails.
pub(crate) async fn assert_not_past_sealer(h: &mut Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(AHEAD_BUDGET_SECS, 2), |_| {
        let h: &Harness = h;
        async move {
            let cursor = cursor_line(h).await?.as_deref().and_then(cursor_number);
            let origin = h.probes.sealer_l1_origin().await;
            Ok(cursor
                .zip(origin)
                .filter(|(c, o)| c <= o)
                .map(|(c, o)| format!("cursor {c}, sealer origin {o}")))
        }
    })
    .await?;
    match outcome {
        poll::Outcome::Ready { value, .. } => {
            crate::log(format!(
                "{ctx}: the da-watcher resumes at or before the sealer ({value})"
            ));
            Ok(())
        }
        poll::Outcome::TimedOut { .. } => {
            crate::log(format!(
                "{ctx}: the da-watcher's cursor stands past the sealer's L1 origin for {AHEAD_BUDGET_SECS}s: an epoch was lost in the kill"
            ));
            resume_after_sealer_origin(h, ctx).await.map(|_| ())
        }
    }
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
