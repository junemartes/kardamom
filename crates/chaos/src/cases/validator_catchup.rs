//! validator-exec-archive-catchup: the validator catches up from the
//! executor archives after a stop.
//!
//! A validator on the executor stream (`--tx-source exec-stream`) reads
//! the live `exec_txs` stream. The live stream holds nothing for a
//! validator that was away: its new subscription starts at the live head.
//! So after a restart every record of the stop window comes from an
//! executor archive, by locator. The case stops the validator job under
//! load, starts it again, and asserts that the validator verifies past
//! its old head with no peer-checkpoint adoption and no divergence.
//!
//! The case reads the source from the registered validator job (the
//! value of `--tx-source` in the task arguments). On `tx-data` it skips
//! with a log line: that source catches up from the ingress archives,
//! which other cases cover.

use std::cell::Cell;
use std::time::Duration;

use serde_json::Value;

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};

use super::validator::{
    COMMITTED, DIVERGENCE, VERIFIED, Warm, val_debug, wait_verifying_live, warm_line,
};

const JOB: &str = "validator";
const CASE: &str = "validator-exec-archive-catchup";

/// The executor stream source of the validator.
const EXEC_STREAM: &str = "exec-stream";

/// The in-process repair of a refused replay: a peer checkpoint adopted
/// without a restart.
const RESYNC: &str = "validator_resync_total";

/// The cold-start adoption of a peer checkpoint.
const ADOPTED: &str = "adopted state from checkpoint";

/// The asks of one executor on a miss of the executor stream.
const REFETCH: &str = "kardamom_exec_stream_refetch_total";

/// The value of `--tx-source` in the task arguments of `definition`.
/// `tx-data` when the job does not pass the flag: that is the default.
fn tx_source_of(definition: &Value) -> String {
    let args: Vec<&str> = definition["TaskGroups"][0]["Tasks"][0]["Config"]["args"]
        .as_array()
        .map(|args| args.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    args.iter()
        .position(|a| *a == "--tx-source")
        .and_then(|i| args.get(i + 1))
        .map_or_else(|| "tx-data".to_owned(), |s| (*s).to_owned())
}

/// The transaction source of the registered validator job, for a log
/// line. `unknown` when the job cannot be read.
pub(crate) async fn deployed_tx_source(h: &Harness) -> String {
    SavedJob::capture(&h.nomad, JOB).await.map_or_else(
        |_| "unknown".to_owned(),
        |job| tx_source_of(job.definition()),
    )
}

/// Stop the validator past the live window, start it, and require it to
/// verify past its old head from the executor archives.
///
/// # Errors
///
/// Returns the case failure.
pub(crate) async fn exec_archive_catchup(h: &mut Harness) -> anyhow::Result<()> {
    let saved = SavedJob::capture(&h.nomad, JOB).await?;
    let source = tx_source_of(saved.definition());
    if source != EXEC_STREAM {
        crate::log(format!(
            "{CASE}: SKIP — the validator runs --tx-source {source}; the case needs {EXEC_STREAM}"
        ));
        return Ok(());
    }
    let warm = wait_verifying_live(h, Duration::from_secs(150), 15)
        .await
        .map_err(|w| {
            crate::chaos_fail!(
                "{CASE}: never warmed up within {}s ({})",
                w.elapsed.as_secs(),
                warm_line(w)
            )
        })?;
    let stop = h.knobs.validator_catchup_stop;
    crate::log(format!(
        "{CASE}: warmed up ({}); stopping the validator job for {}s",
        warm_line(warm),
        stop.as_secs()
    ));
    saved.stop().await?;
    tokio::time::sleep(stop).await;
    saved.restore().await?;
    crate::log(format!("{CASE}: validator job registered again"));
    let caught = catch_up(h, warm).await?;
    assert_no_adoption(h).await?;
    let div = h
        .probes
        .val_metric_required(DIVERGENCE, "validator-exec-archive-catchup divergence==0")
        .await?;
    if div != 0 {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "{CASE}: {div} divergence(s) after the restart"
        ));
    }
    let located = h
        .probes
        .val_metric_where(REFETCH, "outcome=\"located\"")
        .await
        .unwrap_or(0);
    crate::log(format!(
        "{CASE} PASS: verified past the old head (block {} -> {}, lag {}), {located} located refetch(es), no checkpoint adoption, 0 divergences",
        warm.block,
        caught.block,
        caught.lag()
    ));
    Ok(())
}

/// Poll until the restarted validator verifies past the block it had
/// committed before the stop, caught up with the executors. The restart
/// starts the counters again, so the committed block carries the
/// comparison.
async fn catch_up(h: &Harness, before: Warm) -> anyhow::Result<Warm> {
    let last = Cell::new(Warm::default());
    let last_ref = &last;
    let outcome = poll::until(Budget::secs(300, 10), |elapsed| async move {
        let w = Warm {
            verified: h.probes.val_metric(VERIFIED).await.unwrap_or(0),
            block: h.probes.val_metric(COMMITTED).await.unwrap_or(0),
            exec: h.probes.executor_progress().await.unwrap_or(0),
            elapsed,
        };
        last_ref.set(w);
        let caught = w.verified > 0 && w.block > before.block && w.lag() <= 25;
        Ok::<_, anyhow::Error>(caught.then_some(w))
    })
    .await?;
    let poll::Outcome::Ready { value, .. } = outcome else {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "{CASE}: the restarted validator did not verify past block {} within 300s ({})",
            before.block,
            warm_line(last.get())
        ));
    };
    Ok(value)
}

/// The restarted validator adopted no peer checkpoint: no adoption line
/// in the log of its new container, and no in-process resync.
async fn assert_no_adoption(h: &Harness) -> anyhow::Result<()> {
    let node = &h.probes.validator.container;
    let inner = h
        .nodes
        .inner_container(node, JOB)
        .await
        .ok_or_else(|| crate::chaos_fail!("{CASE}: no inner validator container on {node}"))?;
    let logs = h
        .nodes
        .inner_logs(node, &inner, 20_000)
        .await
        .unwrap_or_default();
    let resyncs = h
        .probes
        .val_metric_where(RESYNC, "outcome=\"peer-checkpoint\"")
        .await
        .unwrap_or(0);
    if logs.contains(ADOPTED) || resyncs > 0 {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "{CASE}: the validator adopted a peer checkpoint (resyncs {resyncs}); it must catch up from the executor archives"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_source_comes_from_the_task_arguments() {
        let job = |args: &[&str]| serde_json::json!({"TaskGroups": [{"Tasks": [{"Config": {"args": args}}]}]});
        assert_eq!(
            tx_source_of(&job(&["--state-dir", "/x", "--tx-source", "exec-stream"])),
            "exec-stream"
        );
        assert_eq!(tx_source_of(&job(&["--state-dir", "/x"])), "tx-data");
        assert_eq!(tx_source_of(&serde_json::json!({})), "tx-data");
    }
}
