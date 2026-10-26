//! A live old leader loses only its outbound Raft peer traffic.

use std::fmt::Write;
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;

use crate::cases::combined::Check;
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use crate::rpc::Rpc;

const CTX: &str = "raft-leader-one-way-partition";
const CHAIN: &str = "KDM_CHAOS_RAFT";
const PORTS: [u16; 4] = [40201, 40202, 40203, 40204];
const HOLD: Duration = Duration::from_secs(30);

#[derive(Serialize)]
struct Proof {
    leader: u32,
    term: u64,
    dropped_packets: u64,
    election_seconds: f64,
}

struct Partition {
    leader: u32,
    term: u64,
    node: String,
    inner: String,
    started: String,
    peers: Vec<std::net::Ipv4Addr>,
}

impl Partition {
    async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let elected = h.evidence.cluster_elected(h.knobs.leader_slo).await?;
        let leader = elected.leader;
        let victim = h.contract.node(&format!("sealer-{leader}"))?;
        let node = victim.container.clone();
        let inner = h
            .nodes
            .inner_container(&node, CLUSTER_TASK)
            .await
            .context("the current leader has no task container")?;
        let started = h
            .nodes
            .started_at(&node, &inner)
            .await
            .context("the leader start time is unavailable")?;
        let peers = h
            .contract
            .of_role("sealer")
            .into_iter()
            .filter(|peer| peer.name != victim.name)
            .map(|peer| peer.ip)
            .collect();
        Ok(Self {
            leader,
            term: elected.term,
            node,
            inner,
            started,
            peers,
        })
    }

    fn arm_script(&self) -> String {
        let rules = self
            .peers
            .iter()
            .flat_map(|peer| PORTS.into_iter().map(move |port| (peer, port)))
            .fold(String::new(), |mut script, (peer, port)| {
                writeln!(
                    script,
                    "iptables -w 5 -A {CHAIN} -p udp -d {peer} --dport {port} -j DROP"
                )
                .expect("writing to a String is infallible");
                script
            });
        format!(
            "set -eu\niptables -w 5 -N {CHAIN}\n{rules}iptables -w 5 -I OUTPUT 1 -j {CHAIN}\niptables -w 5 -C OUTPUT -j {CHAIN}"
        )
    }

    fn clear_script() -> String {
        format!(
            "set -eu\nif iptables -w 5 -S {CHAIN} >/dev/null 2>&1; then\niptables -w 5 -D OUTPUT -j {CHAIN} 2>/dev/null || true\niptables -w 5 -F {CHAIN}\niptables -w 5 -X {CHAIN}\nfi"
        )
    }

    async fn exercise(&self, h: &Harness) -> anyhow::Result<Proof> {
        h.nodes.exec(&self.node, &self.arm_script()).await?;
        crate::log(format!(
            "{CTX}: member {} stays live; its UDP Raft traffic to {:?} is dropped",
            self.leader, self.peers
        ));
        let outcome = poll::until(
            Budget::new(h.knobs.leader_slo, Duration::from_secs(2)),
            |_| async move {
                Ok::<_, anyhow::Error>(
                    h.evidence
                        .cluster_elected(Duration::from_secs(3))
                        .await
                        .ok()
                        .filter(|elected| {
                            elected.leader != self.leader && elected.term > self.term
                        }),
                )
            },
        )
        .await?;
        let (elected, elapsed) = outcome
            .or_fail(|_| crate::chaos_fail!("{CTX}: the surviving quorum elects no new leader"))?;
        let new = elected.leader;
        self.assert_live(h).await?;
        Self::transfer(h).await?;
        tokio::time::sleep(HOLD).await;
        Self::transfer(h).await?;
        self.assert_live(h).await?;
        let dropped_packets = self.assert_dropped(h).await?;
        Check::assert_one_leader(h, CTX).await?;
        crate::log(format!(
            "{CTX}: member {new} wins after {}s; clients receive receipts with the old member still live",
            elapsed.as_secs()
        ));
        Ok(Proof {
            leader: new,
            term: elected.term,
            dropped_packets,
            election_seconds: elapsed.as_secs_f64(),
        })
    }

    async fn transfer(h: &Harness) -> anyhow::Result<()> {
        tokio::time::timeout(
            Duration::from_secs(60),
            Rpc::new(&h.rpc_url, h.knobs.chain_id)?
                .transfer_smoke(h.knobs.gate_account, Duration::from_secs(60)),
        )
        .await
        .context("one-way Raft partition blocks the client past its bound")?
    }

    async fn assert_live(&self, h: &Harness) -> anyhow::Result<()> {
        let inner = h.nodes.inner_container(&self.node, CLUSTER_TASK).await;
        let started = h
            .nodes
            .started_at(&self.node, &self.inner)
            .await
            .context("the old leader start time is unavailable")?;
        anyhow::ensure!(
            inner.as_deref() == Some(&self.inner) && started == self.started,
            "{CTX}: the old leader restarted; this is no live-leader partition"
        );
        let admin = "curl -fsS --max-time 3 http://127.0.0.1:40205/status >/dev/null";
        h.nodes.exec(&self.node, admin).await?;
        Ok(())
    }

    async fn assert_dropped(&self, h: &Harness) -> anyhow::Result<u64> {
        let script = format!("iptables -w 5 -L {CHAIN} -n -v -x");
        let rules = h.nodes.exec(&self.node, &script).await?;
        let packets = Self::dropped(&rules);
        anyhow::ensure!(
            packets > 0,
            "{CTX}: no peer-bound packet hits the partition: {rules}"
        );
        crate::log(format!(
            "{CTX}: the one-way drop rules discard {packets} peer-bound Raft packets"
        ));
        Ok(packets)
    }

    /// Only existence matters; the sum caps at the largest packet count.
    fn dropped(rules: &str) -> u64 {
        rules
            .lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                let packets = fields.next()?.parse::<u64>().ok()?;
                fields.next()?;
                (fields.next()? == "DROP").then_some(packets)
            })
            .fold(0u64, u64::saturating_add)
    }

    async fn run(&self, h: &mut Harness) -> anyhow::Result<()> {
        let during = self.exercise(h).await;
        let cleared = h.nodes.exec(&self.node, &Self::clear_script()).await;
        if let Err(error) = &cleared {
            crate::log(format!("{CTX}: clearing the partition fails: {error:#}"));
        }
        let report = serde_json::json!({
            "case": CTX, "direction": "old leader to peers only", "ports": PORTS,
            "old_leader": self.leader, "old_term": self.term, "proof": during.as_ref().ok(),
            "node": self.node, "container": self.inner, "started": self.started,
            "peers": self.peers, "hold_secs": HOLD.as_secs(),
            "failure": during.as_ref().err().map(|error| format!("{error:#}")),
            "cleanup_failure": cleared.as_ref().err().map(|error| format!("{error:#}")),
        });
        let saved = std::fs::write(
            "/tmp/chaos-raft-leader-one-way-partition-fault.json",
            serde_json::to_vec_pretty(&report)?,
        );
        during?;
        cleared?;
        saved?;
        h.assert_count(CLUSTER_TASK, 3, h.knobs.rejoin_slo).await?;
        h.assert_progress().await?;
        Check::assert_one_leader(h, CTX).await
    }
}

pub(crate) async fn leader_one_way(h: &mut Harness) -> anyhow::Result<()> {
    Partition::capture(h).await?.run(h).await
}

#[cfg(test)]
mod tests {
    use super::Partition;

    #[test]
    fn only_counted_drop_rules_prove_the_fault_was_exercised() {
        let rules = "Chain KDM_CHAOS_RAFT (1 references)\n pkts bytes target prot opt in out source destination\n 0 0 DROP udp -- * * 0.0.0.0/0 1.2.3.4 udp dpt:40201\n 2 120 DROP udp -- * * 0.0.0.0/0 1.2.3.4 udp dpt:40202\n 9 540 ACCEPT all -- * * 0.0.0.0/0 0.0.0.0/0\n";
        assert_eq!(Partition::dropped(rules), 2);
        assert_eq!(Partition::dropped(""), 0);
    }
}
