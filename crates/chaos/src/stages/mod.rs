//! The non-chaos stages of a shard, as methods on the [`Harness`]: the
//! smoke gate, the sustained load, the chain-semantics suite, the
//! ingress-churn re-smoke and the validator verdict. A shard test
//! composes them; `run-tests.sh` used to.

mod churn;
pub(crate) mod rebuild;
mod recovery;
mod semantics;
mod soak;
mod state;
mod verdict;

use std::time::Duration;

use kardamom_types::shard_map::LANE_COUNT;

use crate::evidence::{StampedLog, number_after};
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
    /// with the channel of the subscription. A detached publisher closes
    /// the subscription of its destination, so only the close of a lane
    /// subscription counts. No case has run yet, so one such line after
    /// the last start line means the runtime ended by itself.
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

/// The channel of a publisher destination. Its close is a detach, not
/// the end of a lane.
const DESTINATION_CHANNEL: &str = "control-mode=dynamic";

/// The first `tx_data` stream id: lane `i` publishes on stream
/// `TX_DATA_STREAM_BASE + i` (`tx_data_stream_id_base` in
/// `deploy/cluster/config/channels.toml.tpl`).
const TX_DATA_STREAM_BASE: u64 = 2000;

/// How many lane subscriptions the batcher closed after its last start.
/// The log of a task holds every start of it, and a process that ends
/// closes its subscriptions in order; only the last start counts. The
/// log read returns each stream of a task in turn, so the time stamps
/// decide what comes after the last start, not the text order. With no
/// start line, every close counts.
fn lanes_closed_since_start(logs: &str) -> usize {
    let log = StampedLog(logs);
    let start = log.stamps(BATCHER_START, "").max();
    log.lines()
        .filter(|(stamp, _)| start.as_ref().is_none_or(|start| stamp > start))
        .filter(|(_, line)| line.contains(SUBSCRIPTION_CLOSED) && closes_a_lane(line))
        .count()
}

/// True when a closed subscription is a `tx_data` lane: its stream id is
/// a lane stream, and it is not the subscription of a publisher
/// destination. Every other close is not a lane: a refetch replay (its
/// stream id is far above the lanes), a cluster session that reconnects,
/// or the egress readers of a carry-mode batcher, which holds no lane.
fn closes_a_lane(line: &str) -> bool {
    let lanes = TX_DATA_STREAM_BASE..TX_DATA_STREAM_BASE + u64::from(LANE_COUNT);
    number_after(line, "stream_id: ").is_some_and(|id| lanes.contains(&id))
        && !line.contains(DESTINATION_CHANNEL)
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

    /// A batcher log line at time `t` (seconds), in the format of the
    /// services: an RFC 3339 stamp in ANSI color, the level and the text.
    fn stamped(t: u32, text: &str) -> String {
        format!("\x1b[2m2026-10-21T04:06:{t:02}.903496Z\x1b[0m \x1b[32m INFO\x1b[0m {text}\n")
    }

    /// The close line of a subscription on `channel` and `stream`.
    fn closed(t: u32, channel: &str, stream: u64) -> String {
        stamped(
            t,
            &format!(
                "rusteron_client: auto closing AeronSubscription {{ constants: Ok(AeronSubscriptionConstants {{ channel: \"{channel}\", registration_id: 60, stream_id: {stream}, channel_status_indicator_id: 172 }}) }}"
            ),
        )
    }

    const LANE: &str = "aeron:udp?control-mode=manual";

    #[test]
    fn only_the_running_batcher_counts_for_closed_lanes() {
        let ended = format!("{}{}", stamped(1, BATCHER_START), closed(2, LANE, 2000));
        let healthy = format!(
            "{ended}{}{}",
            stamped(3, BATCHER_START),
            stamped(4, "subscription open")
        );
        let detached = format!(
            "{healthy}{}",
            closed(
                5,
                "aeron:udp?endpoint=a:0|control=b:1|control-mode=dynamic",
                1002
            )
        );
        let broken = format!("{detached}{}", closed(6, LANE, 2001));
        assert_eq!(lanes_closed_since_start(&healthy), 0);
        assert_eq!(lanes_closed_since_start(&detached), 0);
        assert_eq!(lanes_closed_since_start(&broken), 1);
        assert_eq!(lanes_closed_since_start(&closed(1, LANE, 2000)), 1);
    }

    /// The log read returns the stdout of a task and then its stderr.
    /// A close on stderr by a process before the last start does not
    /// count, though the text puts it after the start line.
    #[test]
    fn the_time_stamps_order_the_streams_of_a_task() {
        let stdout = format!(
            "{}{}",
            stamped(1, BATCHER_START),
            stamped(30, BATCHER_START)
        );
        let stderr = format!("{}{}", closed(20, LANE, 2000), closed(21, LANE, 2001));
        assert_eq!(lanes_closed_since_start(&format!("{stdout}{stderr}")), 0);
        let later = format!("{stdout}{stderr}{}", closed(40, LANE, 2002));
        assert_eq!(lanes_closed_since_start(&later), 1);
        let unstamped = format!("{stdout}{stderr}Error: auto closing AeronSubscription\n");
        assert_eq!(lanes_closed_since_start(&unstamped), 0);
    }

    /// The refetch closes its replay subscription when the replay
    /// drains. That close is not a lane close.
    #[test]
    fn a_drained_refetch_replay_is_not_a_lane_close() {
        let replay = "aeron:udp?endpoint=192.168.56.10:22671|session-id=-774318160";
        let log = format!(
            "{}{}{}",
            stamped(1, BATCHER_START),
            stamped(
                2,
                "kardamom_log::refetch: tx_data refetch drained stream_id=2000"
            ),
            closed(3, replay, 1_073_741_829),
        );
        assert_eq!(lanes_closed_since_start(&log), 0);
        let low = format!("{log}{}", closed(4, replay, 2000));
        assert_eq!(lanes_closed_since_start(&low), 1);
    }

    /// A carry-mode batcher reads the egress and holds no lane. A close of
    /// another subscription, such as a cluster session that reconnects,
    /// is not a lane close.
    #[test]
    fn a_close_off_the_lane_streams_is_not_a_lane_close() {
        let session = "aeron:udp?endpoint=192.168.56.10:0";
        let log = format!(
            "{}{}{}",
            stamped(1, BATCHER_START),
            closed(2, session, 102),
            closed(3, session, 1009),
        );
        assert_eq!(lanes_closed_since_start(&log), 0);
        let lane = format!("{log}{}", closed(4, LANE, 2007));
        assert_eq!(lanes_closed_since_start(&lane), 1);
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
