//! Random loss keeps some peer traffic flowing while clients transact.

use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;

use super::rules::{LOSS_CHAIN, Rules};
use super::{FaultEnd, PORTS, Partition};
use crate::cases::combined::Check;
use crate::harness::Harness;

const CTX: &str = "raft-leader-packet-loss";
const HOLD: Duration = Duration::from_secs(30);

#[derive(Debug, Default, Serialize)]
struct Counts {
    dropped: u64,
    passed: u64,
}

impl Counts {
    fn read(rules: &str, peer: Ipv4Addr) -> anyhow::Result<Self> {
        let counts = rules.lines().filter_map(|line| Self::row(line, peer)).fold(
            Self::default(),
            |mut counts, (target, packets)| {
                // Only positive activity matters; counters cap at their largest value.
                match target {
                    "DROP" => counts.dropped = counts.dropped.saturating_add(packets),
                    "RETURN" => counts.passed = counts.passed.saturating_add(packets),
                    _ => {}
                }
                counts
            },
        );
        anyhow::ensure!(
            counts.dropped > 0 && counts.passed > 0,
            "{CTX}: peer {peer} needs both dropped and passed packets: {counts:?}"
        );
        Ok(counts)
    }

    fn row(line: &str, peer: Ipv4Addr) -> Option<(&str, u64)> {
        let fields: Vec<_> = line.split_whitespace().collect();
        let packets = fields.first()?.parse::<u64>().ok()?;
        let destination = fields.get(8)?.parse::<Ipv4Addr>().ok()?;
        let protocol = fields.get(3)?;
        (destination == peer && *protocol == "udp").then_some((*fields.get(2)?, packets))
    }
}

pub(crate) struct PacketLoss;

impl PacketLoss {
    pub(crate) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        let target = Partition::capture(h).await?;
        let outcome = tokio::time::timeout(Duration::from_mins(4), Self::exercise(&target, h))
            .await
            .context("Raft packet-loss proof exceeds four minutes")
            .and_then(std::convert::identity);
        let cleared = h
            .nodes
            .exec(&target.node, &Rules::cleanup(LOSS_CHAIN))
            .await;
        let proof = serde_json::json!({
            "case": CTX, "probability": 0.1, "hold_seconds": HOLD.as_secs(),
            "direction": "selected member to peers only", "ports": PORTS,
            "member": target.leader, "term_before": target.term,
            "node": target.node, "container": target.inner, "started": target.started,
            "peers": target.peers, "traffic": outcome.as_ref().ok(),
            "failure": outcome.as_ref().err().map(|e| format!("{e:#}")),
            "cleanup_failure": cleared.as_ref().err().map(|e| format!("{e:#}")),
        });
        FaultEnd {
            ctx: CTX,
            outcome,
            cleared,
            path: "/tmp/chaos-raft-leader-packet-loss-fault.json",
            report: proof,
        }
        .conclude(h)
        .await
    }

    async fn exercise(target: &Partition, h: &Harness) -> anyhow::Result<serde_json::Value> {
        h.nodes
            .exec(&target.node, &Rules::loss(&target.peers).arm())
            .await?;
        target.assert_live(h, CTX).await?;
        Partition::transfer(h, CTX).await?;
        tokio::time::sleep(HOLD).await;
        Partition::transfer(h, CTX).await?;
        target.assert_live(h, CTX).await?;
        Check::assert_one_leader(h, CTX).await?;
        let raw = h
            .nodes
            .exec(
                &target.node,
                &format!("iptables -w 5 -L {LOSS_CHAIN} -n -v -x"),
            )
            .await?;
        let counts = target
            .peers
            .iter()
            .map(|peer| Ok((peer.to_string(), Counts::read(&raw, *peer)?)))
            .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;
        anyhow::ensure!(counts.len() == 2, "{CTX}: fault requires both Raft peers");
        crate::log(format!("{CTX}: both drops and passes observed: {counts:?}"));
        Ok(serde_json::json!({"peers": counts, "rules": raw}))
    }
}

#[cfg(test)]
#[path = "loss_tests.rs"]
mod tests;
