//! The non-chaos stages of a shard, as methods on the [`Harness`]: the
//! smoke gate, the sustained load, the chain-semantics suite, the
//! ingress-churn re-smoke and the validator verdict. A shard test
//! composes them; `run-tests.sh` used to.

mod churn;
pub(crate) mod rebuild;
mod semantics;
mod soak;
mod state;
mod verdict;

use std::time::Duration;

use crate::harness::Harness;
use crate::nomad::Streams;
use crate::rpc::Rpc;

pub use churn::CHURN_ACCOUNT;

/// The funded account of the smoke gate.
pub const GATE_ACCOUNT: u32 = 0;

/// The receipt budget of one smoke transfer.
const SMOKE_BUDGET: Duration = Duration::from_mins(1);

impl Harness {
    /// The smoke gate: one transfer from `account` through ingress-0 must
    /// get a receipt before any other stage runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer gets no receipt with status 1.
    pub async fn smoke_gate(&self, account: u32) -> anyhow::Result<()> {
        crate::log(format!("smoke gate against {}", self.rpc_url));
        Rpc::new(&self.rpc_url, self.knobs.chain_id)?
            .transfer_smoke(account, SMOKE_BUDGET)
            .await?;
        self.assert_batcher_lanes_open().await
    }

    /// The running batcher still holds its `tx_data` lane subscriptions.
    /// A batcher whose Aeron runtime ended joins every record through
    /// the archive refetch: it keeps pace on a healthy cluster, and
    /// stops for good when a case removes an ingress archive. The Aeron
    /// client logs one `auto closing` line per subscription it closes,
    /// and no case has run yet, so one such line after the last start
    /// line means the runtime ended by itself.
    async fn assert_batcher_lanes_open(&self) -> anyhow::Result<()> {
        let logs = self.nomad.job_logs("batcher", Streams::Both).await?;
        let closed = lanes_closed_since_start(&logs);
        anyhow::ensure!(
            closed == 0,
            "{}: smoke gate: the batcher closed {closed} Aeron subscriptions after its start, with no fault injected — its tx_data runtime ended, and it now joins records through the archive refetch alone",
            crate::FAIL_PREFIX
        );
        Ok(())
    }
}

/// The start line of the live batcher.
const BATCHER_START: &str = "live batcher starting";

/// The Aeron client's line for a subscription it closes.
const SUBSCRIPTION_CLOSED: &str = "auto closing AeronSubscription";

/// How many subscriptions the batcher closed after its last start. The
/// log of a task holds every start of it, and a process that ends
/// closes its subscriptions in order; only the last start counts.
fn lanes_closed_since_start(logs: &str) -> usize {
    let running = logs
        .rsplit_once(BATCHER_START)
        .map_or(logs, |(_, tail)| tail);
    running.matches(SUBSCRIPTION_CLOSED).count()
}

/// The last `n` lines of `text`.
#[must_use]
pub fn tail_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// The last `n` lines of `text` that contain any of `needles`.
#[must_use]
pub fn matching_lines(text: &str, needles: &[&str], n: usize) -> String {
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| needles.iter().any(|needle| l.contains(needle)))
        .collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// The first `n` distinct lines of `text` that contain any of
/// `needles`. The first failure of a service explains the later ones,
/// and a tail hides it. Two lines that differ only in their digits
/// (the timestamp, a port, a retry count) are one line: a retry loop
/// must not use up the budget before the next failure shows.
#[must_use]
pub fn first_matching_lines(text: &str, needles: &[&str], n: usize) -> String {
    let mut seen = std::collections::HashSet::new();
    text.lines()
        .filter(|l| needles.iter().any(|needle| l.contains(needle)))
        .filter(|l| seen.insert(l.replace(|c: char| c.is_ascii_digit(), "")))
        .take(n)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The first `n` lines of `text`.
#[must_use]
pub fn head_lines(text: &str, n: usize) -> String {
    text.lines().take(n).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_and_tail_cut_lines() {
        let text = "a\nb\nc\nd";
        assert_eq!(head_lines(text, 2), "a\nb");
        assert_eq!(tail_lines(text, 3), "b\nc\nd");
        assert_eq!(tail_lines(text, 9), text);
    }

    #[test]
    fn matching_lines_keep_the_last_matches_in_order() {
        let text = "x RESYNC 1\nnoise\ncluster session opened 2\nnoise\nx RESYNC 3";
        assert_eq!(
            matching_lines(text, &["RESYNC", "cluster session"], 2),
            "cluster session opened 2\nx RESYNC 3"
        );
        assert_eq!(matching_lines(text, &["absent"], 5), "");
    }

    #[test]
    fn first_matching_lines_keep_the_first_matches_in_order() {
        let text = "INFO up\nWARN a\nnoise\nERROR b\nWARN c";
        assert_eq!(
            first_matching_lines(text, &["WARN", "ERROR"], 2),
            "WARN a\nERROR b"
        );
        assert_eq!(first_matching_lines(text, &["absent"], 5), "");
    }

    #[test]
    fn only_the_running_batcher_counts_for_closed_lanes() {
        let ended = "live batcher starting\nauto closing AeronSubscription a\n";
        let healthy = format!("{ended}live batcher starting\nsubscription open\n");
        let broken = format!("{healthy}auto closing AeronSubscription b\n");
        assert_eq!(lanes_closed_since_start(&healthy), 0);
        assert_eq!(lanes_closed_since_start(&broken), 1);
    }

    #[test]
    fn first_matching_lines_count_a_retry_loop_once() {
        let text = "01:02 WARN retry port=41\n01:04 WARN retry port=57\n01:05 ERROR join timeout";
        assert_eq!(
            first_matching_lines(text, &["WARN", "ERROR"], 2),
            "01:02 WARN retry port=41\n01:05 ERROR join timeout"
        );
    }
}
