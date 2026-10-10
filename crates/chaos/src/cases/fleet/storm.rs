//! The restart storm: the executor job stops and starts again ten times
//! under load. Every round, each executor opens a new `tx_receipts`
//! publication on a new control port, and every subscriber attaches it
//! again. A publication that stays unconnected while its subscribers are
//! attached held an executor at one block for minutes. The executor now
//! reopens such a publication after one stall budget and exits with code
//! 3 after four, so Nomad restarts it. Each round requires every
//! executor to advance within the convergence SLO.

use crate::cases::component::executor_containers;
use crate::harness::Harness;
use crate::nomad::{SavedJob, Streams};

pub(super) const CTX: &str = "executor-restart-storm";

/// How many times the job stops and starts again.
pub(crate) const ROUNDS: u32 = 10;

/// The executor's log line of a reopened `tx_receipts` publication.
const REOPENED: &str = "tx_receipts publication reopened on a new session";

/// The executor's log line of the exit after the whole budget.
const EXITED: &str = "the process exits so the supervisor restarts it";

/// Stop and restore the executor job [`ROUNDS`] times. Every round, all
/// replicas must run again within the restart SLO, and every executor
/// must advance within the convergence SLO. The escalation lines of the
/// executors are counted and printed as evidence: a reopen shows the
/// first step engaged, an exit shows the last resort engaged. Neither
/// fails the case; a replica that does not advance does.
pub(crate) async fn executor_restart_storm(h: &mut Harness) -> anyhow::Result<()> {
    let job = SavedJob::capture(&h.nomad, "executor").await?;
    let replicas = executor_containers(h).len();
    let reopened = count(h, REOPENED).await;
    let exited = count(h, EXITED).await;
    for round in 1..=ROUNDS {
        storm_round(h, &job, replicas, round).await?;
    }
    crate::log(format!(
        "{CTX}: {ROUNDS} rounds done; every executor advanced after each one; \
         publication reopens: {}, escalation exits: {}",
        count(h, REOPENED).await.saturating_sub(reopened),
        count(h, EXITED).await.saturating_sub(exited)
    ));
    Ok(())
}

/// One round: stop the job, start it again, wait until every replica
/// runs, and require every executor to advance.
async fn storm_round(
    h: &mut Harness,
    job: &SavedJob,
    replicas: usize,
    round: u32,
) -> anyhow::Result<()> {
    crate::log(format!(
        "{CTX}: round {round}/{ROUNDS}: stop and restore the executor job"
    ));
    job.stop().await?;
    job.restore().await?;
    h.assert_count("executor", replicas, h.knobs.restart_slo)
        .await?;
    h.assert_executors_converged(&format!("{CTX} round {round}"))
        .await
}

/// The number of executor log lines that hold `needle`. A log that
/// cannot be read counts as zero: the count is evidence, not a gate.
async fn count(h: &Harness, needle: &str) -> usize {
    h.evidence
        .count_lines("executor", needle, Streams::Both)
        .await
        .unwrap_or(0)
}
