//! Fault-owned OUTPUT rules leave the data ports and the host's other rules intact.

use std::net::Ipv4Addr;

use serde::Serialize;

use crate::nodes::Nodes;

const ARM: &str = r"set -eu
iptables -w 5 -N KDM_CHAOS_CP
for control_port in 4646 4647 8500 53; do
    iptables -w 5 -A KDM_CHAOS_CP -p tcp --dport $control_port -j DROP
done
iptables -w 5 -A KDM_CHAOS_CP -p udp --dport 53 -j DROP
iptables -w 5 -I OUTPUT 1 -j KDM_CHAOS_CP
iptables -w 5 -C OUTPUT -j KDM_CHAOS_CP";

const CLEAR: &str = r"set -eu
if iptables -w 5 -S KDM_CHAOS_CP >/dev/null 2>&1; then
    iptables -w 5 -D OUTPUT -j KDM_CHAOS_CP 2>/dev/null || true
    iptables -w 5 -F KDM_CHAOS_CP
    iptables -w 5 -X KDM_CHAOS_CP
fi";

const TASKS: &str = r"set -euo pipefail
docker ps --no-trunc --format '{{.ID}}' | xargs -r docker inspect --format '{{.Id}} {{.State.Running}} {{.State.StartedAt}}' | LC_ALL=C sort";

/// A UDP query can be lost on a busy runner, so the probe asks up to
/// three times. An isolated node answers none of them.
const DNS: &str = r"python3 - <<'PY'
import socket
import struct
name = b'control-plane-probe.node.consul'
question = b''.join(bytes([len(label)]) + label for label in name.split(b'.')) + b'\x00'
packet = struct.pack('!HHHHHH', 0x6b64, 0x0100, 1, 0, 0, 0) + question + struct.pack('!HH', 1, 1)
def ask():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2)
        sock.connect(('127.0.0.1', 53))
        sock.send(packet)
        response = sock.recv(4096)
        return len(response) >= 12 and response[:2] == packet[:2] and bool(response[2] & 0x80)
def answered():
    for _ in range(3):
        try:
            if ask():
                return True
        except OSError:
            pass
    return False
assert answered()
PY";

#[derive(Clone, Serialize)]
pub(super) struct NodeView {
    pub(super) node: String,
    pub(super) tasks: String,
}

#[derive(Clone)]
pub(super) struct Check {
    name: &'static str,
    script: String,
}

impl Check {
    pub(super) fn metadata(nomad: &str, server: Ipv4Addr) -> [Self; 4] {
        [
            Self { name: "Consul HTTP", script: "curl -fsS --connect-timeout 1 --max-time 2 http://127.0.0.1:8500/v1/status/leader >/dev/null".into() },
            Self { name: "Nomad HTTP", script: format!("curl -fsS --connect-timeout 1 --max-time 2 '{nomad}/v1/status/leader' >/dev/null") },
            Self { name: "Nomad RPC", script: format!("python3 - <<'PY'\nimport socket\nwith socket.create_connection(('{server}', 4647), timeout=2):\n    pass\nPY") },
            Self { name: "Consul DNS", script: DNS.into() },
        ]
    }

    /// The line of the node script that runs this check and prints its
    /// verdict as `<index>=1` when it answers, or `<index>=0`.
    fn line(&self, index: usize) -> String {
        format!(
            "if (\n{}\n) >/dev/null 2>&1; then echo {index}=1; else echo {index}=0; fi",
            self.script
        )
    }

    /// Whether the verdict lines of a node script say that check `index`
    /// answered. A missing line is an error, never an answer.
    fn answered(output: &str, index: usize) -> anyhow::Result<bool> {
        let prefix = format!("{index}=");
        output
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map(|verdict| verdict == "1")
            .ok_or_else(|| anyhow::anyhow!("no verdict for check {index} in {output:?}"))
    }

    fn judge(&self, node: &str, answered: bool, isolated: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            answered != isolated,
            "{} on {node}: {} {} while isolated={isolated}",
            super::CTX,
            self.name,
            if answered {
                "answers"
            } else {
                "does not answer"
            }
        );
        crate::log(format!(
            "{}: {node}: {} reachable={answered}",
            super::CTX,
            self.name
        ));
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Action {
    Observe,
    Available,
    Arm,
    Isolated,
    Unchanged,
    Clear,
}

pub(super) struct Step {
    pub(super) view: NodeView,
    pub(super) checks: [Check; 4],
    pub(super) action: Action,
}

impl Step {
    pub(super) async fn run(self) -> anyhow::Result<NodeView> {
        let result = self.execute().await;
        if let Err(error) = &result {
            crate::log(format!(
                "{}: {} {:?}: {error:#}",
                super::CTX,
                self.view.node,
                self.action
            ));
        }
        result
    }

    async fn execute(&self) -> anyhow::Result<NodeView> {
        let mut view = self.view.clone();
        match self.action {
            Action::Arm => {
                Nodes.exec(&view.node, ARM).await?;
            }
            Action::Clear => {
                Nodes.exec(&view.node, CLEAR).await?;
            }
            Action::Observe => {
                self.verify(false).await?;
                view.tasks = Nodes.exec(&view.node, TASKS).await?;
            }
            Action::Available => self.verify(false).await?,
            Action::Isolated => {
                self.verify(true).await?;
                self.unchanged().await?;
            }
            Action::Unchanged => self.unchanged().await?,
        }
        Ok(view)
    }

    /// Run the four checks of the node in one exec, one after the other
    /// inside the node: on a busy runner a docker exec costs seconds, and
    /// many at once time out.
    async fn verify(&self, isolated: bool) -> anyhow::Result<()> {
        let script = self
            .checks
            .iter()
            .enumerate()
            .map(|(index, check)| check.line(index))
            .collect::<Vec<_>>()
            .join("\n");
        let output = Nodes.exec(&self.view.node, &script).await?;
        self.checks
            .iter()
            .enumerate()
            .try_for_each(|(index, check)| {
                check.judge(&self.view.node, Check::answered(&output, index)?, isolated)
            })
    }

    async fn unchanged(&self) -> anyhow::Result<()> {
        Nodes
            .exec(&self.view.node, "iptables -w 5 -C OUTPUT -j KDM_CHAOS_CP")
            .await?;
        let now = Nodes.exec(&self.view.node, TASKS).await?;
        anyhow::ensure!(
            now == self.view.tasks,
            "{}: {} changes its running processes during the metadata partition; before:\n{}after:\n{now}",
            super::CTX,
            self.view.node,
            self.view.tasks
        );
        Ok(())
    }
}

#[cfg(test)]
mod script_tests {
    use super::Check;

    #[test]
    fn each_check_line_prints_its_own_verdict_and_a_missing_one_is_an_error() {
        let check = Check {
            name: "Consul HTTP",
            script: "curl -fsS http://127.0.0.1:8500".into(),
        };
        let line = check.line(2);
        assert!(
            line.starts_with("if (\ncurl") && line.ends_with("then echo 2=1; else echo 2=0; fi")
        );
        assert!(Check::answered("0=1\n1=0\n2=1\n", 2).unwrap());
        assert!(!Check::answered("0=1\n1=0\n", 1).unwrap());
        assert!(Check::answered("0=1\n", 3).is_err());
    }
}
