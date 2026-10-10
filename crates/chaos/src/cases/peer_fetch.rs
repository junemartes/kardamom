//! The peer step of the executor join: an executor whose `tx_data`
//! sources all fail for an entry fetches the entry from the `exec_txs`
//! archive of a peer executor, with no void vote and no restart loop.

use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::Probed;

/// The executor log line of a peer fetch that joined an entry.
const FETCHED: &str = "peer fetch: a peer's archive holds the entry; joining it";

/// The executor log line of a void vote.
const VOTED: &str = "voting to void the entry";

/// The executor log line of a join that ended the reader. Each one is a
/// process restart.
const ABORTED: &str = "join timeout: TxRef has no envelope on tx_data";

/// The log field of a park or an abort after a join that every ingress
/// archive refused. Only such a join can vote.
const REFUSED: &str = "every_archive_refused=true";

/// The log field of a park or an abort after a join that an ingress
/// archive left open: the join timed out, and the data can still exist.
const TIMED_OUT: &str = "every_archive_refused=false";

/// More aborts than this in one case is a restart loop. One abort is one
/// restart: a peer that gave no answer for the whole wait.
const MAX_ABORTS: usize = 1;

/// The executor that loses its `tx_data` sources in `exec-peer-fetch`.
const VICTIM: usize = 2;

/// How long the ingress archives get no `tx_data`. It passes the image
/// liveness timeout of the archives (the Aeron stall tolerance, 30 s in
/// CI), so each recording ends and a new one starts after the gap.
const RECORDING_GAP: std::time::Duration = std::time::Duration::from_mins(1);

/// The `u32` match of a `tx_data` data frame: a data or pad frame (type 0
/// or 1, the low half of the word at Aeron offset 4) on a lane (stream
/// 2000 to 2007, little-endian, at Aeron offset 16). The UDP header is 8
/// bytes after the IP header, whose length the first word gives.
const TX_DATA_FRAME: &str = "0>>22&0x3C@12&0xFFFF=0x0:0x100 && 0>>22&0x3C@24=0xD0070000:0xD7070000";

/// The executor log counts of fetches, votes, aborts, refused joins and
/// timed-out joins at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerFetchEvidence {
    fetched: usize,
    voted: usize,
    aborted: usize,
    refused: usize,
    timed_out: usize,
}

impl PeerFetchEvidence {
    /// The counts now.
    ///
    /// # Errors
    ///
    /// Returns an error if the executor logs cannot be read.
    pub(crate) async fn read(h: &Harness) -> anyhow::Result<Self> {
        let count = |needle| h.evidence.count_lines("executor", needle, Streams::Both);
        Ok(Self {
            fetched: count(FETCHED).await?,
            voted: count(VOTED).await?,
            aborted: count(ABORTED).await?,
            refused: count(REFUSED).await?,
            timed_out: count(TIMED_OUT).await?,
        })
    }

    /// The counts since `self`.
    fn since(&self, now: Self) -> Self {
        Self {
            fetched: now.fetched.saturating_sub(self.fetched),
            voted: now.voted.saturating_sub(self.voted),
            aborted: now.aborted.saturating_sub(self.aborted),
            refused: now.refused.saturating_sub(self.refused),
            timed_out: now.timed_out.saturating_sub(self.timed_out),
        }
    }

    /// Print the peer fetches and the votes since `self` as evidence, and
    /// fail on a restart loop. Returns the counts since `self`.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs cannot be read, or if the executors
    /// aborted a join more than [`MAX_ABORTS`] times.
    pub(crate) async fn report(&self, h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let delta = self.since(Self::read(h).await?);
        crate::log(format!(
            "{ctx}: peer fetches {}, void votes {}, join aborts {}, joins refused by every archive {}, timed-out joins {} in the executor logs",
            delta.fetched, delta.voted, delta.aborted, delta.refused, delta.timed_out
        ));
        if delta.aborted > MAX_ABORTS {
            return Err(crate::chaos_fail!(
                "{ctx}: the executors aborted a join {} times: a restart loop",
                delta.aborted
            ));
        }
        Ok(delta)
    }
}

/// The signature of an in-flight frame that only some executors got: the
/// ingress archives miss a window of `tx_data`, executor-2 misses it live,
/// and executors 0 and 1 join it live. Every ingress archive then refuses
/// the window to executor-2. Executor-2 parks, asks its peers, and replays
/// the window from their `exec_txs` archives. Executors 0 and 1 joined the
/// window, so they never vote, and executor-2 votes only when every peer
/// answers `not_held`. Executor-2 must converge with no vote and no
/// restart loop, and its park must follow a join that every archive
/// refused: a park after a timed-out join never votes, so it would not
/// prove the vote rule.
pub(crate) async fn exec_peer_fetch(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "exec-peer-fetch";
    let before = PeerFetchEvidence::read(h).await?;
    let victim = h
        .probes
        .executors
        .get(VICTIM)
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no executor-{VICTIM}"))?
        .container
        .clone();
    let ingresses: Vec<&Probed> = h.probes.ingresses.iter().collect();
    let sources: Vec<String> = ingresses.iter().map(|i| i.ip.to_string()).collect();
    let archives: Vec<String> = ingresses.iter().map(|i| i.container.clone()).collect();
    let window = Window {
        victim: &victim,
        archives: &archives,
        sources: &sources,
    };
    if !window.u32_match_works(h).await {
        crate::log(format!(
            "{ctx}: SKIP: iptables u32 match unavailable on the node kernel"
        ));
        return Ok(());
    }
    let held = window.run(h, before.fetched).await;
    window.end(h).await;
    held?;
    h.assert_progress().await?;
    h.assert_executors_converged(ctx).await?;
    let delta = before.report(h, ctx).await?;
    if delta.voted > 0 {
        return Err(crate::chaos_fail!(
            "{ctx}: an executor voted to void an entry that a peer's archive holds"
        ));
    }
    if delta.timed_out > 0 {
        return Err(crate::chaos_fail!(
            "{ctx}: a join timed out: an ingress archive gave no definite answer for the window"
        ));
    }
    if delta.refused == 0 {
        return Err(crate::chaos_fail!(
            "{ctx}: no join ended with every ingress archive refusing the window"
        ));
    }
    Ok(())
}

/// A source address that no node uses (TEST-NET-1), for the probe of the
/// `u32` match.
const PROBE_SOURCE: &str = "192.0.2.1";

/// One change of the rule that drops the `tx_data` frames from one source.
#[derive(Clone, Copy)]
enum Op {
    /// Insert the rule.
    Insert,
    /// Delete the rule when it is present. A rule that is already gone is
    /// no error, so a second lift of the same rule is a no-op.
    Lift,
}

impl Op {
    /// The shell command of this change for the frames from `ip`.
    fn command(self, ip: &str) -> String {
        let rule = |flag: &str| {
            format!(
                "iptables -w 5 {flag} INPUT -p udp -s {ip} -m u32 --u32 '{TX_DATA_FRAME}' -j DROP"
            )
        };
        match self {
            Self::Insert => rule("-I"),
            Self::Lift => format!("! {} 2>/dev/null || {}", rule("-C"), rule("-D")),
        }
    }

    /// Apply this change on `node`.
    async fn apply(self, h: &Harness, node: &str, ip: &str) -> anyhow::Result<()> {
        let command = self.command(ip);
        h.nodes
            .exec(node, &command)
            .await
            .map(|_| ())
            .map_err(|e| crate::chaos_fail!("exec-peer-fetch: `{command}` on {node} failed: {e}"))
    }
}

/// The drop of `tx_data` frames from the ingress addresses: on the victim
/// executor, and on the ingress nodes, whose archives record the frames.
struct Window<'a> {
    victim: &'a str,
    archives: &'a [String],
    sources: &'a [String],
}

impl Window<'_> {
    /// Drop on the victim and on the archives, lift the drop on the
    /// archives after [`RECORDING_GAP`], then wait until an executor joins
    /// an entry from a peer's archive. The fresh join budget is 60 s, so
    /// the wait holds the gap, one budget, one peer round, and margin.
    async fn run(&self, h: &Harness, fetched: usize) -> anyhow::Result<()> {
        crate::log(format!(
            "exec-peer-fetch: dropping tx_data from {:?} on {} and on the archives of {:?}",
            self.sources, self.victim, self.archives
        ));
        self.rules(h, self.victim, Op::Insert).await?;
        self.on_archives(h, Op::Insert).await?;
        tokio::time::sleep(RECORDING_GAP).await;
        self.on_archives(h, Op::Lift).await?;
        crate::log("exec-peer-fetch: the ingress archives record tx_data again");
        let outcome = poll::until(Budget::secs(300, 5), |_| async move {
            let now = h
                .evidence
                .count_lines("executor", FETCHED, Streams::Both)
                .await?;
            Ok((now > fetched).then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "exec-peer-fetch: no peer fetch in the executor logs within {}s of the gap",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "exec-peer-fetch: peer fetches {fetched} -> {now} after {}s",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// Whether the kernel of every node of the case has the iptables
    /// `u32` match: insert and lift the drop rule for a source that no
    /// node uses. The lift runs also after a failed insert, so the probe
    /// leaves no rule behind.
    async fn u32_match_works(&self, h: &Harness) -> bool {
        let mut works = true;
        for node in std::iter::once(self.victim).chain(self.archives.iter().map(String::as_str)) {
            works &= Self::probe(h, node).await;
        }
        works
    }

    async fn probe(h: &Harness, node: &str) -> bool {
        let inserted = Op::Insert.apply(h, node, PROBE_SOURCE).await.is_ok();
        let lifted = Op::Lift.apply(h, node, PROBE_SOURCE).await.is_ok();
        inserted && lifted
    }

    /// The cleanup: lift every rule, also after a failed step. The lift
    /// of the archives' rules at the end of the gap is a step of the
    /// scenario, and a lift of a rule that is gone is a no-op.
    async fn end(&self, h: &Harness) {
        let victim = self.rules(h, self.victim, Op::Lift).await;
        let archives = self.on_archives(h, Op::Lift).await;
        crate::log(format!(
            "exec-peer-fetch: the drop is over (victim: {victim:?}, archives: {archives:?})"
        ));
    }

    async fn on_archives(&self, h: &Harness, op: Op) -> anyhow::Result<()> {
        let mut failed = Vec::new();
        for node in self.archives {
            failed.extend(self.rules(h, node, op).await.err());
        }
        failed.into_iter().next().map_or(Ok(()), Err)
    }

    /// Apply `op` on `node`, one rule per source address. Every command
    /// runs, also after a failed one, so a lift removes every rule that
    /// it can.
    async fn rules(&self, h: &Harness, node: &str, op: Op) -> anyhow::Result<()> {
        let mut failed = Vec::new();
        for ip in self.sources {
            failed.extend(op.apply(h, node, ip).await.err());
        }
        failed.into_iter().next().map_or(Ok(()), Err)
    }
}
