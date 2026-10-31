//! Peer Raft UDP is delayed while the selected process and client traffic stay live.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde_json::Value;

use super::{FaultEnd, PORTS, Partition};
use crate::cases::combined::Check;
use crate::harness::Harness;

const CTX: &str = "raft-leader-packet-delay";
const SCRIPT: &str = include_str!("delay/fault.py");
const HOLD: Duration = Duration::from_secs(30);

pub(crate) struct PacketDelay;

impl PacketDelay {
    pub(crate) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        let target = Partition::capture(h).await?;
        let fault = Delay {
            target,
            token: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        };
        let outcome = tokio::time::timeout(Duration::from_mins(4), fault.exercise(h))
            .await
            .context("Raft packet-delay proof exceeds four minutes")
            .and_then(std::convert::identity);
        let cleared = fault.command(h, "clear").await;
        let report = serde_json::json!({
            "case": CTX, "delay_ms": 250, "hold_seconds": HOLD.as_secs(),
            "lease_seconds": 300, "direction": "selected member to peers only", "ports": PORTS,
            "member": fault.target.leader, "term_before": fault.target.term,
            "node": fault.target.node, "container": fault.target.inner,
            "started": fault.target.started, "peers": fault.target.peers,
            "traffic": outcome.as_ref().ok(), "cleanup": cleared.as_ref().ok(),
            "failure": outcome.as_ref().err().map(|e| format!("{e:#}")),
            "cleanup_failure": cleared.as_ref().err().map(|e| format!("{e:#}")),
        });
        FaultEnd {
            ctx: CTX,
            outcome,
            cleared,
            path: "/tmp/chaos-raft-leader-packet-delay-fault.json",
            report,
        }
        .conclude(h)
        .await
    }
}

struct Delay {
    target: Partition,
    token: u128,
}

impl Delay {
    fn script(&self, mode: &str) -> String {
        let file = format!("/tmp/kardamom-raft-delay-{}.py", self.token);
        let peers = self
            .target
            .peers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "set -eu\ncat > {file} <<'PY'\n{SCRIPT}\nPY\npython3 {file} {} {mode} {peers}",
            self.token
        )
    }

    async fn command(&self, h: &Harness, mode: &str) -> anyhow::Result<Value> {
        let raw = h.nodes.exec(&self.target.node, &self.script(mode)).await?;
        serde_json::from_str(&raw).context("decode Raft delay evidence")
    }

    async fn exercise(&self, h: &Harness) -> anyhow::Result<Value> {
        let before = self.command(h, "arm").await?;
        self.target.assert_live(h, CTX).await?;
        Partition::transfer(h, CTX).await?;
        tokio::time::sleep(HOLD).await;
        Partition::transfer(h, CTX).await?;
        self.target.assert_live(h, CTX).await?;
        Check::assert_one_leader(h, CTX).await?;
        let after = self.command(h, "inspect").await?;
        Self::require_traffic(&before, &after)?;
        crate::log(format!(
            "{CTX}: both peer delay queues deliver packets during the fault"
        ));
        Ok(serde_json::json!({"before": before, "after": after}))
    }

    fn require_traffic(before: &Value, after: &Value) -> anyhow::Result<()> {
        let baseline = before["queues"]
            .as_array()
            .context("missing initial delay queues")?;
        let queues = after["queues"]
            .as_array()
            .context("missing final delay queues")?;
        anyhow::ensure!(
            baseline.len() == 2 && queues.len() == 2,
            "{CTX}: both peers require evidence"
        );
        baseline.iter().zip(queues).try_for_each(|(old, new)| {
            let first = old["packets"]
                .as_u64()
                .context("missing initial packet count")?;
            let last = new["packets"]
                .as_u64()
                .context("missing final packet count")?;
            anyhow::ensure!(
                old["peer"] == new["peer"] && last > first && new["drops"] == 0,
                "{CTX}: peer delay queue did not deliver packets without loss: {old} -> {new}"
            );
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
